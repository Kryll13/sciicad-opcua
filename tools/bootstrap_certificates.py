#!/usr/bin/env python3
"""Amorce la confiance du déploiement, Part 12 §7.1.

La norme ne définit aucun amorçage en bande. §7.1 exige l'inverse : *« Clients
shall only connect to a CertificateManager which the Client has been configured
to trust. This may require an out of band configuration step which is completed
prior to starting the manual onboarding process. »* Cet outil est cette étape
hors bande, et il est le seul endroit du projet où une ancre de confiance naît.

Ce qui est produit
------------------

Pour chacun des quatre rôles — LDS, GDS, thermo-plc, protect-plc — un couple
certificat constructeur + clé privée **auto-signé**, deposited dans le répertoire
du rôle, là où ``plc_server.py`` et compagnie le cherchent déjà.

Et, dans ``pki/trusted/``, les **mêmes certificats sans leur clé**, pour la
liste de confiance du GDS. La duplication du public est sans conséquence ; la
séparation des clés ne l'est pas. Une liste de confiance est lisible par tout
client autorisé à la lire : y déposer une clé privée la rendrait lisible aussi.

Pourquoi des certificats constructeurs, et non des certificats signés par une
autorité
------------------------------------------------------------------------

Parce que la Part 12 n'ouvre pas la porte à une autorité interne, et qu'il faut
le voir plutôt que le contourner :

* aucun rôle « Certificate Authority » n'existe dans le ``NodeIds.csv``
  officiel — la recherche sur ``CertificateAuthorit`` ne rend **aucun** nœud ;
* ``CertificateRequest`` est défini (3.1.3) comme *« a PKCS #10 encoded
  structure used to request a new Certificate from a Certificate Authority »* —
  la CA est extérieure par définition ;
* ``StartSigning`` / ``StopSigning`` / ``CreateSelfSignedCertificate`` ont
  disparu du modèle courant.

Le certificat constructeur **est** donc l'ancre, et il n'a pas à être remplacé
pour devenir inutile. C'est seulement au renouvellement que la CA entre, par le
rôle *CertificateManager* du GDS : ``CreateSigningRequest`` produit la demande,
une CA externe la signe, ``UpdateCertificate`` l'installe. Le GDS ne signe
jamais.

    python tools/bootstrap_certificates.py
    python tools/bootstrap_certificates.py --host lds=192.168.1.20 --host gds=193.168.1.20
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

from asyncua import ua
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from loguru import logger

# Reuse le générateur deja eprouve plutot que de dupliquer le profil Table 50.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from crypto_opcua import generate  # noqa: E402

#: Repertoire des certificats publics destines a la liste de confiance du GDS.
TRUSTED_DIR = Path("pki/trusted")

#: L'emplacement de la confiance elle-meme. Hors du repertoire de chaque
#: application, donc un deploiement peut le monter en lecture seule, ou le
#: remplacer par un magasin de confiance d'entreprise sans toucher au code.
TRUSTED_MODE = 0o644


@dataclass(frozen=True)
class Role:
    """Un role du deploiement, et le qu'il lui faut."""

    name: str
    directory: Path
    application_uri: str
    description: str


#: Les quatre roles. Les URI d'application sont ceux que les serveurs annoncent
#: effectivement — LDS et GDS depuis leur YAML, les PLCs depuis leur constante
#: de module. Une URI divergente produirait un certificat dont le SAN ne
#: correspond pas a ce que le serveur declare, et le defaut n'apparaitrait
#: qu'a la premiere connexion securisee.
ROLES: tuple[Role, ...] = (
    Role(
        "lds",
        Path("lds"),
        "urn:SCIICAD:lds",
        "Local Discovery Server",
    ),
    Role(
        "gds",
        Path("gds"),
        "urn:SCIICAD:gds",
        "Global Discovery Server",
    ),
    Role(
        "thermo-plc",
        Path("thermo-plc"),
        "urn:SCIICAD:thermo-plc",
        "simulateur de temperature",
    ),
    Role(
        "protect-plc",
        Path("protect-plc"),
        "urn:SCIICAD:protect-plc",
        "simulateur de protection",
    ),
)


def profile_issues(certificate: x509.Certificate) -> list[str]:
    """Écarts au profil de la Table 50 de la Part 6, pour un certificat donné.

    Sert de garde-fou sur ce que ``generate`` produit, et de démonstration de ce
    que la validation d'un certificat d'application devrait vérifier. Le profil
    est normatif : *« For RSA keys, the keyUsage shall include
    digitalSignature, nonRepudiation, keyEncipherment and dataEncipherment »*, et
    pour un auto-signé *« shall also include keyCertSign »*.
    """
    issues: list[str] = []
    try:
        usage = certificate.extensions.get_extension_for_class(x509.KeyUsage).value
    except x509.ExtensionNotFound:
        return ["extension KeyUsage absente"]
    required = {
        "digitalSignature": usage.digital_signature,
        "nonRepudiation": usage.content_commitment,
        "keyEncipherment": usage.key_encipherment,
        "dataEncipherment": usage.data_encipherment,
    }
    for name, present in required.items():
        if not present:
            issues.append(f"keyUsage: {name} absent (exigé par la Table 50)")
    if certificate.issuer == certificate.subject and not usage.key_cert_sign:
        issues.append("keyUsage: keyCertSign absent (exigé pour un auto-signé)")
    return issues


def install_public_copy(cert_path: Path, role: Role) -> Path:
    """Copie le certificat public dans le répertoire d'ancrage, sans sa clé."""
    TRUSTED_DIR.mkdir(parents=True, exist_ok=True)
    target = TRUSTED_DIR / f"{role.name}.pem"
    payload = cert_path.read_bytes()
    target.write_bytes(payload)
    target.chmod(TRUSTED_MODE)
    return target


def generate_for(
    role: Role,
    hostnames: list[str],
    key_size: int,
    validity_days: int,
    force: bool,
) -> tuple[Path, Path] | None:
    """Génère le couple d'un rôle, ou ``None`` s'il existe déjà.

    L'existence n'est pas un obstacle : réécrire une clé par-dessus une clé
    déjà en service détruirait le certificat correspondant, qu'aucune liste de
    confiance ne pourrait plus valider. C'est pourquoi l'outil refuse
    d'écraser sans ``--force``, et dit ce qu'il va casser quand on le demande.
    """
    cert_path = role.directory / "server_certificate.pem"
    key_path = role.directory / "server_private_key.pem"
    if cert_path.exists() and key_path.exists() and not force:
        logger.info(
            f"{role.name} : certificat deja present, conserve "
            f"({cert_path})"
        )
        return None
    if force and (cert_path.exists() or key_path.exists()):
        logger.warning(
            f"{role.name} : --force ecrase le certificat existant. Toute liste de "
            f"confiance et tout canal securise deja etabli avec cette cle "
            f"deviendront invalides, sans preavis et sans mecanisme de "
            f"transition : c'est le prix d'un renouvellement d'ancre."
        )

    # Le premier nom d'hote est le certificat ; les suivants sont des alias
    # DNS/IP ajoutees au SAN par crypto_opcua, ce qui est necessaire quand le
    # client joint par une adresse que le premier nom ne designe pas.
    primary = hostnames[0] if hostnames else role.name
    aliases = hostnames[1:]
    cert_path, key_path = generate(
        hostname=primary,
        output_dir=str(role.directory),
        key_size=key_size,
        validity_days=validity_days,
        application_uri=role.application_uri,
    )

    certificate = x509.load_pem_x509_certificate(cert_path.read_bytes())
    issues = profile_issues(certificate)
    if issues:
        for issue in issues:
            logger.error(f"{role.name} : PROFIL NON CONFORME — {issue}")
    public = install_public_copy(cert_path, role)
    logger.info(
        f"{role.name} : {role.description}\n"
        f"    URI d'application : {role.application_uri}\n"
        f"    certificat         : {cert_path}\n"
        f"    clé privée         : {key_path} (0600)\n"
        f"    copie publique     : {public}"
        + (f"\n    alias SAN          : {', '.join(aliases)}" if aliases else "")
        + f"\n    profil Table 50    : {'conforme' if not issues else str(len(issues)) + ' écart(s)'}"
    )
    return cert_path, key_path


def parse_args(argv: list[str] | None = None):
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Genere les certificats constructeurs des quatre roles et l'ancre "
            "de confiance du GDS (Part 12 §7.1)."
        )
    )
    parser.add_argument(
        "--host",
        action="append",
        default=[],
        metavar="ROLE=HOST",
        help=(
            "Nom d'hote a inscrire au SAN d'un role, repetable. "
            "Le premier est l'identite principale, les suivants des alias. "
            "Defaut : le nom du role."
        ),
    )
    parser.add_argument("--key-size", type=int, default=2048, help="bits (defaut 2048)")
    parser.add_argument(
        "--validity-days",
        type=int,
        default=365,
        help="duree de validite (defaut 365)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Ecrase les certificats existants. Casse toute confiance deja "
            "etablie avec la cle actuelle."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    hosts: dict[str, list[str]] = {}
    for item in args.host:
        if "=" not in item:
            logger.error(
                f"--host attend ROLE=HOST, reçu {item!r} "
                f"(exemple : --host gds=193.168.1.20)"
            )
            return 2
        role_name, _, host = item.partition("=")
        hosts.setdefault(role_name.strip(), []).append(host.strip())

    known = {role.name for role in ROLES}
    for unknown in sorted(set(hosts) - known):
        logger.error(
            f"Role inconnu dans --host : {unknown!r}. "
            f"Roles connus : {', '.join(sorted(known))}"
        )
        return 2

    created = 0
    for role in ROLES:
        if generate_for(
            role,
            hosts.get(role.name, []),
            args.key_size,
            args.validity_days,
            args.force,
        ):
            created += 1

    # L'ancre est lue par le GDS au demarrage ; le chemin se rappelle dans la
    # sortie plutot que d'etre suppose.
    logger.info("")
    logger.info("Ancre de confiance a declarer dans gds/gds_config.yaml :")
    logger.info("  certificates:")
    logger.info(f"    trusted_certificates: [ {TRUSTED_DIR.as_posix()} ]")
    logger.info(
        f"    trusted_certificates_group: DefaultApplicationGroup"
    )
    logger.info("")
    logger.info(
        f"{created} couple(s) cree(s) sur {len(ROLES)} ; "
        f"{len(list(TRUSTED_DIR.glob('*.pem')))} ancre(s) dans {TRUSTED_DIR}"
    )
    if created == 0:
        logger.info(
            "Aucun couple cree : tout existait deja. Relancer avec --force pour "
            "renouveler, en sachant que cela invalide la confiance en cours."
        )
    return 0


if __name__ == "__main__":
    from sciicad.console import setup

    setup()
    sys.exit(main())
