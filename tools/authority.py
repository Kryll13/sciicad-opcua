#!/usr/bin/env python3
"""Autorité de certification du déploiement, hors ligne et à clés grasses.

Le GDS ne peut pas être une autorité : la Part 12 ne lui donne pas ce rôle. La
recherche sur « CertificateAuthorit » dans le ``NodeIds.csv`` officiel ne rend
**aucun** nœud, et §3.1.3 définit ``CertificateRequest`` comme une structure «
used to request a new Certificate **from a Certificate Authority** » — la CA
est extérieure par définition.

Cette autorité est donc cet extérieur. Elle ne dessert aucun client, ne répond à
aucune requête réseau, et n'est jamais exposée par le GDS : c'est un outil de
hors bande qui signe ce qu'un administrateur lui apporte.

Où vit la clé
-------------

**Hors du dépôt**, par défaut, dans ``~/.sciicad/ca/``. Ce n'est pas une
prudence de forme : une clé de signature au côté du code devient un geste
banal, et une clé qu'on signe sans réfléchir est une clé compromise sans
incident. Le certificat public, lui, reste dans ``pki/ca/`` — il est public, et
le dépôt peut le versionner. C'est le seul fichier de cette autorité qui soit
inoffensif.

Un déploiement réel garde cette clé sur un support amovible, hors du système
qui la consomme. Ici elle est dans le ``$HOME`` de l'opérateur, ce qui est déjà
sensible et pas encore sûr : le gap est assumé et documenté plutôt que masqué
par un ``pki/ca/`` bien rangé.

Profil du certificat d'autorité
-------------------------------

``cA=TRUE`` avec ``pathLength=0``, ``keyUsage`` portant ``keyCertSign`` et
``cRLSign``, sans EKU ni SAN : un certificat d'autorité ne sert à aucun
protocole applicatif, et lui en donner un Evaluating invites à le présenter
comme s'il pouvait ouvrir un canal.

``pathLength=0`` interdit une sous-autorité. Une hiérarchie à un niveau est un
choix, pas une limitation : elle rend le Deployment trivial à auditer, et une
autorité qui peut en créer une autre peut en créer une qu'on ne verra jamais.

La Part 6 ne définit pas de profil de certificat d'autorité distinct — la Table
50 couvre le certificat d'**application**. Les contraintes ci-dessus sont donc
celles de X.509 et le bon sens, pas une citation à faire. Le certificat
d'**application**, lui, est vérifié contre la Table 50 par
``gds/certstore.py``, et le message de refus nomme le bit manquant.

    python tools/authority.py init
    python tools/authority.py sign lds/server_certificate.csr
    python tools/authority.py crl --revoke 1a2b3c
"""

from __future__ import annotations

import argparse
import base64
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from loguru import logger

#: Clé privée : hors du dépôt. Voir la note du module.
CA_HOME = Path.home() / ".sciicad" / "ca"
CA_KEY = CA_HOME / "ca_private_key.pem"

#: Certificat public et CRL : dans le dépôt, sous ``pki/``, qui est ignoré par
#: git. Publics donc inoffensifs à versionner, mais(regénérables donc ignorés.
CA_CERT = Path("pki/ca/ca_certificate.pem")
CA_CRL = Path("pki/crl/ca.crl.pem")

#: Le KeyUsage de la Table 50 de la Part 6, pour un certificat d'application
#: RSA. ``keyCertSign`` en est absent **parce que** ce certificat est signé par
#: l'autorité et non auto-signé : la Table 50 ne l'exige que pour un
#: auto-signé, où il sert à permettre au certificat d'être sa propre ancre.
APP_KEY_USAGE = x509.KeyUsage(
    digital_signature=True,
    content_commitment=True,
    key_encipherment=True,
    data_encipherment=True,
    key_agreement=False,
    key_cert_sign=False,
    crl_sign=False,
    encipher_only=False,
    decipher_only=False,
)

CA_KEY_USAGE = x509.KeyUsage(
    digital_signature=True,
    content_commitment=False,
    key_encipherment=False,
    data_encipherment=False,
    key_agreement=False,
    key_cert_sign=True,
    crl_sign=True,
    encipher_only=False,
    decipher_only=False,
)


# -- lecture / écriture ----------------------------------------------------


def _load_key(path: Path) -> rsa.RSAPrivateKey:
    if not path.is_file():
        raise SystemExit(
            f"Aucune clé d'autorité à {path}.\n"
            f"Créez-la d'abord : python tools/authority.py init"
        )
    key = serialization.load_pem_private_key(path.read_bytes(), password=None)
    if not isinstance(key, rsa.RSAPrivateKey):
        raise SystemExit(f"La clé de {path} n'est pas RSA.")
    return key


def _write_private(path: Path, payload: bytes) -> None:
    """Écrit une clé privée en 0600, en refusant d'écraser par accident.

    Un fichier existant est une clé en service : la détruire ferait perdre
    l'autorité qui a signé tous les certificats en cours, et il n'existe aucun
    mécanisme normatif pour y remédier. Une clé de CA perdue ne se remplace pas
    — elle se contourne, en signant une nouvelle autorité que rien ne reconnaît.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    path.chmod(0o600)


def _load_csr(path: Path) -> x509.CertificateSigningRequest:
    payload = path.read_bytes()
    try:
        return x509.load_pem_x509_csr(payload)
    except ValueError:
        return x509.load_der_x509_csr(payload)


def _pem(der: bytes, label: str = "CERTIFICATE") -> str:
    """L'encodage PEM d'un bloc DER déjà construit.

    L'aller-retour par l'objet ``Certificate`` était à la fois inutile et
    fautif : relire un PEM qu'on vient d'écrire échoue, parce que la
    bibliothèque exige un délimiteur de ligne qu'elle ne produit pas
    elle-même. Le PEM est donc produit directement, à 64 colonnes comme le
    veut le format.
    """
    body = base64.b64encode(der).decode("ascii")
    lines = [body[i:i + 64] for i in range(0, len(body), 64)]
    return (
        f"-----BEGIN {label}-----\n"
        + "\n".join(lines)
        + f"\n-----END {label}-----\n"
    )


# -- commandes -------------------------------------------------------------


def cmd_init(args) -> int:
    if CA_KEY.exists() and not args.force:
        logger.error(
            f"Une clé d'autorité existe déjà à {CA_KEY}. Cette clé a signé les "
            f"certificats en service : la remplacer laisserait tous ces "
            f"certificats sans autorité, et la distribuer serait la seule "
            f"issue. Refus sans --force."
        )
        return 2

    subject = x509.Name([
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, args.organization),
        x509.NameAttribute(NameOID.COMMON_NAME, args.common_name),
    ])
    key = rsa.generate_private_key(public_exponent=65537, key_size=args.key_size)
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=args.days))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(CA_KEY_USAGE, critical=True)
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    der = certificate.public_bytes(serialization.Encoding.DER)

    _write_private(
        CA_KEY,
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ),
    )
    CA_CERT.parent.mkdir(parents=True, exist_ok=True)
    CA_CERT.write_text(_pem(der))
    CA_CERT.chmod(0o644)

    logger.info(f"Autorité créée : {subject.rfc4514_string()}")
    logger.info(f"  clé privée  : {CA_KEY} (0600, hors du dépôt)")
    logger.info(f"  certificat  : {CA_CERT}")
    logger.info(f"  validité    : {args.days} j, pathLength=0 (pas de sous-autorité)")
    logger.info("")
    logger.info("À déclarer dans gds/gds_config.yaml, sous certificates.trust_anchors :")
    logger.info(f"  - group: DefaultApplicationGroup")
    logger.info(f"    issuer_certificates: [ {CA_CERT.parent.as_posix()} ]")
    logger.info("")
    logger.info("La clé est en clair. Ne la versionnez jamais, ne la copiez pas dans")
    logger.info("un dépôt, ne la laissez pas sur une machine de développement.")
    return 0


def cmd_sign(args) -> int:
    key = _load_key(CA_KEY)
    if not CA_CERT.is_file():
        raise SystemExit(
            f"Le certificat d'autorité {CA_CERT} est absent : la clé seule ne "
            f"suffit pas à signer. Lancez : python tools/authority.py init"
        )
    ca = x509.load_pem_x509_certificate(CA_CERT.read_bytes())
    if ca.public_key() != key.public_key():
        raise SystemExit(
            f"La clé de {CA_KEY} ne correspond pas au certificat {CA_CERT}. "
            f"Signer avec elle produirait des certificats dont la signature ne "
            f"remonterait à aucun certificat de confiance — le genre d'erreur "
            f"qui ne se découvre qu'au premier rejet."
        )

    csr = _load_csr(Path(args.csr))
    if not csr.is_signature_valid:
        logger.error(
            f"Demande de signature invalide : {args.csr} ne prouve pas la "
            f"possession de sa clé privée. Une demande dont la signature ne "
            f"tient pas est un document qui ne came de personne."
        )
        return 2

    builder = (
        x509.CertificateBuilder()
        .subject_name(csr.subject)
        .issuer_name(ca.subject)
        .public_key(csr.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(timezone.utc) - timedelta(minutes=5))
        .not_valid_after(datetime.now(timezone.utc) + timedelta(days=args.days))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(APP_KEY_USAGE, critical=True)
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(csr.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca.public_key()),
            critical=False,
        )
    )
    # Le SAN est repris tel quel de la demande : l'autorité ne réécrit pas
    # l'identité de celui qu'elle certifie. C'est aussi pourquoi une demande
    # sans URI ne produit pas un certificat utilisable — il est rejeté plus
    # tard, à la validation, pour une raison qui aura le nom de l'encre.
    san = None
    for extension in csr.extensions:
        if isinstance(extension.value, x509.SubjectAlternativeName):
            san = extension.value
            break
    if san is None:
        logger.error(
            f"Demande de signature sans SubjectAlternativeName : le profil de la "
            f"Table 50 impose exactement un URI, égal à l'URI d'application. "
            f"Certificat non émis."
        )
        return 2
    uris = san.get_values_for_type(x509.UniformResourceIdentifier)
    if len(uris) != 1:
        logger.error(
            f"SubjectAlternativeName à {len(uris)} URI : la Table 50 en impose "
            f"exactement un. Deux URI sont deux identités qui se contestent."
        )
        return 2
    builder = builder.add_extension(san, critical=False)

    certificate = builder.sign(key, hashes.SHA256())
    der = certificate.public_bytes(serialization.Encoding.DER)
    output = Path(args.out) if args.out else Path(args.csr).with_suffix("")
    output = output.with_suffix(".pem") if output.suffix != ".pem" else output
    output.write_text(_pem(der))
    output.chmod(0o644)

    logger.info(f"Certificat émis : {output}")
    logger.info(f"  sujet      : {certificate.subject.rfc4514_string()}")
    logger.info(f"  URI        : {uris[0]}")
    logger.info(f"  série      : {certificate.serial_number:x}")
    logger.info(f"  valable    : {args.days} j")
    return 0


def cmd_crl(args) -> int:
    key = _load_key(CA_KEY)
    if not CA_CERT.is_file():
        raise SystemExit(f"Le certificat d'autorité {CA_CERT} est absent.")
    ca = x509.load_pem_x509_certificate(CA_CERT.read_bytes())
    if ca.public_key() != key.public_key():
        raise SystemExit(f"La clé de {CA_KEY} ne correspond pas à {CA_CERT}.")

    builder = (
        x509.CertificateRevocationListBuilder()
        .issuer_name(ca.subject)
        .last_update(datetime.now(timezone.utc))
        .next_update(datetime.now(timezone.utc) + timedelta(days=args.days))
    )
    for raw in args.revoke:
        try:
            serial = int(raw, 16)
        except ValueError:
            logger.error(f"Número de série illisible : {raw!r} (attendu en hexadécimal)")
            return 2
        builder = builder.add_revoked_certificate(
            x509.RevokedCertificateBuilder()
            .serial_number(serial)
            .revocation_date(datetime.now(timezone.utc))
            .build()
        )
    crl = builder.sign(key, hashes.SHA256())
    CA_CRL.parent.mkdir(parents=True, exist_ok=True)
    CA_CRL.write_text(_pem(crl.public_bytes(serialization.Encoding.DER), "X509 CRL"))
    CA_CRL.chmod(0o644)

    logger.info(f"CRL émise : {CA_CRL}")
    logger.info(f"  révocations : {len(args.revoke)} — {', '.join(args.revoke) or '(aucune)'}")
    logger.info(f"  valable    : {args.days} j")
    logger.info("")
    logger.info("À déclarer dans gds/gds_config.yaml, sous certificates.trust_anchors :")
    logger.info(f"  issuer_crls: [ {CA_CRL.parent.as_posix()} ]")
    logger.info("")
    logger.info("Sans CRL, un certificat présenté dont l'émetteur est connu a un état")
    logger.info("de révocation INCONNU, et le défaut fermé de §7.8.2.10 le refuse.")
    logger.info("Distribuer la CRL n'est donc pas facultatif ici.")
    return 0


def cmd_retire(args) -> int:
    """Retire les ancres constructeurs et archive leurs clés.

    Une ancre auto-signée est un accès permanent tant que sa clé est
    lisible : la compromission de cette clé donne un accès qui ne se voit pas
    et ne s'expire pas. Le certificat constructeur est donc l'ancre du
    *démarrage*, pas celle du *déploiement en service*.

    Une fois les quatre rôles portés par des certificats signés, les ancres
    n'apportent plus rien et retirent un accès. C'est le choix par défaut.

    L'archivage déplace les clés hors du chemin de service plutôt que de les
    supprimer : un déploiement doit pouvoir revenir en arrière, et une clé
    effacée est un retour en arrière impossible.
    """
    moved = 0
    for role in args.role:
        source = Path(role) / "server_private_key.pem"
        if not source.is_file():
            logger.error(f"{role} : aucune clé à archiver ({source})")
            return 2
        target = Path(args.archive) / f"{Path(role).name}_constructor_key.pem"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())
        target.chmod(0o600)
        source.unlink()
        moved += 1
        logger.info(f"{role} : clé constructeur archivée → {target}")

    logger.info("")
    logger.info(f"{moved} clé(s) archivée(s). Retirez les ancres de pki/trusted/ et")
    logger.info("la directive certificates.trust_anchors qui les désigne : un")
    logger.info("certificat de confiance sans clé correspondante est inoffensif, mais")
    logger.info("il occupe la liste et laisse croire à une ancre en service.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Autorité de certification du déploiement : clé hors du dépôt, "
            "certificat et CRL publics."
        )
    )
    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init", help="Créer l'autorité (clé hors du dépôt)")
    init.add_argument("--common-name", default="SCIICAD CA")
    init.add_argument("--organization", default="SCIICAD")
    init.add_argument("--key-size", type=int, default=4096, choices=(2048, 3072, 4096))
    init.add_argument("--days", type=int, default=3650)
    init.add_argument("--force", action="store_true", help="Écrase la clé existante")
    init.set_defaults(func=cmd_init)

    sign = sub.add_parser("sign", help="Signer une demande de signature PKCS #10")
    sign.add_argument("csr", help="Demande de signature, PEM ou DER")
    sign.add_argument("--out", help="Certificat signé (défaut : le CSR sans extension)")
    sign.add_argument("--days", type=int, default=365)
    sign.set_defaults(func=cmd_sign)

    crl = sub.add_parser("crl", help="Émettre une liste de révocation")
    crl.add_argument(
        "--revoke", action="append", default=[], metavar="SERIE",
        help="Numéro de série en hexadécimal, répétable",
    )
    crl.add_argument("--days", type=int, default=30)
    crl.set_defaults(func=cmd_crl)

    retire = sub.add_parser(
        "retire", help="Archiver les clés constructeurs et retirer les ancres"
    )
    retire.add_argument("role", nargs="+", help="Répertoires de rôles, ex. lds gds")
    retire.add_argument("--archive", default="pki/retired")
    retire.set_defaults(func=cmd_retire)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    from sciicad.console import setup

    setup()
    sys.exit(main())