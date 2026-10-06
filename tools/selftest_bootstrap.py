#!/usr/bin/env python3
"""Auto-test de l'amorçage de confiance, Part 12 §7.1 et Part 6 Table 50.

La Part 12 ne définit aucun amorçage en bande : *« Clients shall only connect
to a CertificateManager which the Client has been configured to trust. This may
require an out of band configuration step which is completed prior to starting
the manual onboarding process. »* Cet auto-test vérifie que cette étape hors
bande existe, qu'elle produit des certificates **conformes au profil normatif**,
et qu'elle aboutit réellement dans la liste de confiance du GDS.

Trois choses sont vérifiées, et chacune peut échouer indépendamment
------------------------------------------------------------------------

* **Le profil.** La Table 50 de la Part 6 impose, pour une clé RSA,
  ``digitalSignature``, ``nonRepudiation``, ``keyEncipherment`` et
  ``dataEncipherment``, plus ``keyCertSign`` pour un auto-signé. C'était
  précisément ce que ``crypto_opcua.py`` ne produisait pas, et aucune
  bibliothèque ne le signale : ``cryptography`` écrit ce qu'on lui demande sans
  le confronter à un profil. Un certificat non conforme ne se découvre qu'aupres
  d'un validateur, ou jamais.

* **La non-fuite.** Les ancres de confiance sont des **copies publiques**. Une
  liste de confiance est lisible par tout client autorisé à la lire : y déposer
  une clé privée la rendrait lisible aussi. Le test lit réellement les fichiers
  déposés et le vérifie, plutôt que de faire confiance à l'intention.

* **La concentration.** Les quatre ancres doivent être dans
  ``DefaultApplicationGroup`` et nowhere ailleurs. Le GDS ignore un ancrage
  visant un groupe non rattaché plutôt que de le rediriger : une ancre dans le
  mauvais groupe accepterait des présentations qui ne doivent pas l'être.

Trois contrôles négatifs
------------------------

Un jeu de tests qui passe ne prouve rien s'il ne peut pas échouer. Les trois
certificats forgés ci-dessous sont conformes à tout sauf à un point, et
doivent donc être refusés — c'est ce qui distingue une validation working d'une
validation qui rend toujours ``Good`` :

* ``keyUsage`` incomplet pour une clé RSA (les deux bits que l'ancien code
  exigeait, et lui seul) ;
* ``serverAuth`` absent de l'EKU ;
* ``keyCertSign`` absent sur un auto-signé.

    python tools/selftest_bootstrap.py
"""

from __future__ import annotations

import asyncio
import os
import stat
import sys
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path

from asyncua import Client, ua
from asyncua.ua.ua_binary import from_binary
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from loguru import logger

from gds.certstore import CertificateStore
from gds.config import GDSConfig
from gds.server import GlobalDiscoveryServer
from gds.trustlist import CertificateGroup, thumbprint
from sciicad.selftest import Report, free_port

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bootstrap_certificates import ROLES, TRUSTED_DIR, profile_issues  # noqa: E402

APP_URI = "urn:SCIICAD:gds"
GROUP = "DefaultApplicationGroup"


# -- certificats forgés pour les contrôles négatifs ------------------------


def _name(common: str) -> x509.Name:
    return x509.Name([
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "SCIICAD"),
        x509.NameAttribute(NameOID.COMMON_NAME, common),
    ])


def _self_signed(
    common: str,
    application_uri: str,
    *,
    key_usage: x509.KeyUsage,
    eku: list,
) -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    """Certificat auto-signé dont un seul point est piloté par l'appelant."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.now(timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(_name(common))
        .issuer_name(_name(common))
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(key_usage, critical=True)
        .add_extension(x509.SubjectAlternativeName(
            [x509.UniformResourceIdentifier(application_uri)]
        ), critical=False)
    )
    if eku:
        builder = builder.add_extension(x509.ExtendedKeyUsage(eku), critical=False)
    return key, builder.sign(key, hashes.SHA256())


#: Le KeyUsage historique de ``crypto_opcua.py`` : deux bits, où la Table 50 en
#: exige quatre. C'est le certificat que l'ancien validateur acceptait.
LEGACY_KEY_USAGE = x509.KeyUsage(
    digital_signature=True,
    content_commitment=False,
    key_encipherment=True,
    data_encipherment=False,
    key_agreement=False,
    key_cert_sign=False,
    crl_sign=False,
    encipher_only=False,
    decipher_only=False,
)

FULL_KEY_USAGE = x509.KeyUsage(
    digital_signature=True,
    content_commitment=True,
    key_encipherment=True,
    data_encipherment=True,
    key_agreement=False,
    key_cert_sign=True,
    crl_sign=False,
    encipher_only=False,
    decipher_only=False,
)


# -- 1. les fichiers produits par l'amorçage --------------------------------


def check_files(report: Report) -> dict[str, bytes]:
    """Vérifie les fichiers déposés, et rend les DER des quatre ancres."""
    anchors: dict[str, bytes] = {}
    for role in ROLES:
        cert_path = role.directory / f"{role.prefix}_certificate.pem"
        key_path = role.directory / f"{role.prefix}_private_key.pem"
        if not (cert_path.is_file() and key_path.is_file()):
            report.check(
                f"{role.name} : couple certificat/clé présent",
                False,
                f"manquant : {cert_path} ou {key_path}",
            )
            continue
        report.check(
            f"{role.name} : couple certificat/clé présent",
            True,
            f"{cert_path}",
        )

        # La clé privée ne doit pas être lisible par tous. Elle est écrite sans
        # chiffrement, donc 0600 n'est pas une précaution de style.
        mode = key_path.stat().st_mode
        report.check(
            f"{role.name} : clé privée en 0600",
            stat.S_IMODE(mode) == 0o600,
            f"{oct(stat.S_IMODE(mode))}",
        )

        certificate = x509.load_pem_x509_certificate(cert_path.read_bytes())

        # La Table 50, champ par champ.
        issues = profile_issues(certificate)
        report.check(
            f"{role.name} : profil Table 50 conforme",
            not issues,
            "; ".join(issues) if issues else "DS, NR, KE, DE, keyCertSign, serverAuth",
        )

        # subjectAltName : « shall have exactly one URI », égal à l'URI
        # d'application. Un SAN à plusieurs URI est ambigu : deux applications
        # peuvent se réclamer de la même identité.
        san = certificate.extensions.get_extension_for_class(
            x509.SubjectAlternativeName
        ).value
        uris = san.get_values_for_type(x509.UniformResourceIdentifier)
        report.check(
            f"{role.name} : exactement un URI, égal à l'URI d'application",
            uris == [role.application_uri],
            f"{uris} (attendu [{role.application_uri!r}])",
        )

        constraints = certificate.extensions.get_extension_for_class(
            x509.BasicConstraints
        ).value
        report.check(
            f"{role.name} : cA FALSE",
            constraints.ca is False,
            f"cA={constraints.ca}",
        )

        anchors[role.name] = certificate.public_bytes(serialization.Encoding.DER)

    report.check(
        f"{len(ROLES)} couples produits par l'amorçage",
        len(anchors) == len(ROLES),
        f"{len(anchors)}/{len(ROLES)}",
    )
    return anchors


def check_no_key_leak(report: Report) -> None:
    """Aucune clé privée ne doit avoir franchi dans le répertoire d'ancrage.

    Le test lit les fichiers réellement déposés. Une intention exprimée dans un
    commentaire ne protège rien ; seul le contenu du fichier protège.
    """
    if not TRUSTED_DIR.is_dir():
        report.check(
            f"{TRUSTED_DIR} existe",
            False,
            "répertoire absent : lancez tools/bootstrap_certificates.py",
        )
        return
    files = sorted(TRUSTED_DIR.glob("*.pem"))
    report.check(
        f"{TRUSTED_DIR} contient une ancre par rôle",
        len(files) == len(ROLES),
        f"{len(files)} fichier(s) : {', '.join(f.name for f in files)}",
    )
    for path in files:
        payload = path.read_bytes()
        report.check(
            f"{path.name} : aucun en-tête PRIVATE KEY",
            b"PRIVATE KEY" not in payload,
            f"{len(payload)} octets, "
            f"{'contient une clé' if b'PRIVATE KEY' in payload else 'public seulement'}",
        )
        try:
            x509.load_pem_x509_certificate(payload)
            parsable = True
        except Exception:
            parsable = False
        report.check(
            f"{path.name} : se décode comme un certificat",
            parsable,
            "PEM valide" if parsable else "PEM illisible",
        )


# -- 2. le GDS charge-t-il réellement les ancres ? ---------------------------


async def check_gds_loads(report: Report, anchors: dict[str, bytes]) -> None:
    """Démarre un GDS réel avec la configuration du dépôt, et lit la liste.

    La lecture se fait **par le réseau**, depuis un client ordinaire, et non par
    inspection de la mémoire du serveur : ce qui compte est ce qu'un client peut
    voir, pas ce que le serveur croit avoir chargé.
    """
    config = GDSConfig.load()
    config.server.bind_address = "127.0.0.1"
    config.server.port = free_port()
    config.server.advertise_host = "127.0.0.1"
    config.database.enabled = False
    if not config.certificates.trusted_certificates:
        report.check(
            "la configuration déclare des ancrages de confiance",
            False,
            "certificates.trusted_certificates est vide dans gds/gds_config.yaml",
        )
        return
    report.check(
        "la configuration déclare des ancrages de confiance",
        True,
        f"{config.certificates.trusted_certificates} -> "
        f"{config.certificates.trusted_certificates_group!r}",
    )

    server = GlobalDiscoveryServer(config)
    await server.setup()
    try:
        await server.start()
        url = server.server.endpoint.geturl()

        node = server.certificate_groups[GROUP]
        in_memory = {thumbprint(d) for d in node.group.trusted_certificates}
        report.check(
            "les quatre ancres sont en mémoire dans le groupe visé",
            in_memory == {thumbprint(d) for d in anchors.values()},
            f"{len(in_memory)} certificat(s) de confiance",
        )

        # Concentration : une ancre dans un autre groupe accepterait des
        # présentations qui ne doivent pas l'être.
        for other, other_node in server.certificate_groups.items():
            if other == GROUP:
                continue
            report.check(
                f"aucune ancre dispersée dans {other}",
                not other_node.group.trusted_certificates,
                f"{len(other_node.group.trusted_certificates)} certificat(s)",
            )

        async with Client(url) as client:
            trust_list = None
            for group_node in await _certificate_group_nodes(client):
                for child in await group_node.get_children():
                    if (await child.read_browse_name()).Name == "TrustList":
                        trust_list = child

            if trust_list is None:
                report.check("TrustList accessible depuis un client", False, "introuvable")
                return
            report.check("TrustList accessible depuis un client", True, str(trust_list.nodeid))

            # Les méthodes de la Part 20 sont les ENFANTS de TrustList, pas
            # ceux du groupe : les chercher au mauvais niveau donne un dictionnaire
            # vide, et l'erreur qui suit — « KeyError: 'Open' » — ne dit pas que
            # la recherche était mal ciblée.
            methods = {
                (await child.read_browse_name()).Name: child
                for child in await trust_list.get_children()
                if (await child.read_node_class()) == ua.NodeClass.Method
            }
            missing = {"Open", "Read", "Close"} - set(methods)
            if missing:
                report.check(
                    "les méthodes de la Part 20 sont présentes",
                    False,
                    f"manquantes : {', '.join(sorted(missing))}",
                )
                return
            report.check(
                "les méthodes de la Part 20 sont présentes",
                True,
                f"{', '.join(sorted(methods))}",
            )

            handle = await trust_list.call_method(
                methods["Open"], ua.OpenFileMode.Read
            )
            handle = handle[0] if isinstance(handle, (list, tuple)) else handle
            blob = await trust_list.call_method(methods["Read"], handle, 65535)
            blob = blob[0] if isinstance(blob, (list, tuple)) else blob
            await trust_list.call_method(methods["Close"], handle)

            data = from_binary(ua.TrustListDataType, BytesIO(blob))
            seen = {thumbprint(d) for d in data.TrustedCertificates}
            report.check(
                "les quatre ancres sont lisibles sur le réseau",
                seen == {thumbprint(d) for d in anchors.values()},
                f"{len(seen)} certificat(s) de confiance, "
                f"{len(data.IssuerCertificates)} émetteur(s), "
                f"{len(data.IssuerCrls)} CRL(s)",
            )
            report.check(
                "la liste ne contient aucune clé privée",
                all(b"PRIVATE KEY" not in d for d in data.TrustedCertificates),
                "contenu public seulement",
            )
    finally:
        for node in server.certificate_groups.values():
            node.group.close_all()
        await server.stop()


async def _certificate_group_nodes(client: Client) -> list:
    """Descend jusqu'aux groupes de certificats, comme le fait un client."""

    async def by_name(parent, name):
        for child in await parent.get_children():
            if (await child.read_browse_name()).Name == name:
                return child
        return None

    configuration = await by_name(client.nodes.server, "ServerConfiguration")
    if configuration is None:
        return []
    folder = await by_name(configuration, "CertificateGroups")
    if folder is None:
        return []
    result = []
    for child in await folder.get_children():
        if (await child.read_browse_name()).Name == GROUP:
            result.append(child)
    return result


# -- 3. contrôles négatifs : la validation sait-elle refuser ? ---------------


def _install_and_report(
    report: Report,
    store: CertificateStore,
    group: CertificateGroup,
    label: str,
    key_usage: x509.KeyUsage,
    eku: list,
    *,
    expect_accept: bool,
) -> None:
    """Passe un certificat forgé par le chemin réseau, et rend le verdict.

    Le certificat est construit à partir d'une demande de signature du magasin,
    et non indépendamment de lui. C'est la condition qui rend le test valide :
    le magasin doit connaître la clé privée, faute de quoi il refuse d'abord
    pour *clé absente* — avec le même ``BadCertificateInvalid`` que le profil.
    Un contrôle négatif qui passe pour le mauvais motif ne teste rien.

    Cette construction est aussi le cas constructeur réel : l'application
    génère son propre couple, s'auto-signe avec, et le magasin n'a plus qu'à
    connaître la clé. C'est exactement ce que produit
    ``bootstrap_certificates.py``.

    Le statut **et** son motif sont rendus. Un statut seul ne dit pas quel
    contrôle a parlé, et deviner serait le moyen le plus sûr de corriger le
    mauvais.
    """
    # La norme en exige 32 octets quand RegeneratePrivateKey est vrai, et la
    #la valeur doit donc être exactement cette longueur : un nonce plus court
    # ferait échouer la demande avant même que la forge n'aboutisse, et le
    # contrôle négatif testerait alors la longueur du nonce.
    nonce = label.encode()[:32].ljust(32, b"\x00")
    csr_der = store.create_signing_request(
        GROUP, None, f"CN={label[:40]}", True, nonce
    )
    entry = store.entry(GROUP, None)
    key = entry.private_key
    csr = x509.load_der_x509_csr(csr_der)
    now = datetime.now(timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(csr.subject)
        .issuer_name(csr.subject)          # auto-signé
        .public_key(csr.public_key())     # la clé que le magasin connaît
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(key_usage, critical=True)
    )
    if eku:
        builder = builder.add_extension(x509.ExtendedKeyUsage(eku), critical=False)
    for extension in csr.extensions:
        if isinstance(extension.value, x509.SubjectAlternativeName):
            builder = builder.add_extension(extension.value, extension.critical)
    certificate = builder.sign(key, hashes.SHA256())
    der = certificate.public_bytes(serialization.Encoding.DER)
    group.add(der, is_trusted=True)

    try:
        store.update_certificate(GROUP, None, der, [], "", b"")
        status, reason = "Good", ""
    except Exception as exc:
        code = getattr(exc, "status", None)
        name = ua.StatusCode(code).name if code else type(exc).__name__
        status, reason = name, str(exc)

    detail = f"{status} : {reason}" if reason else status
    if expect_accept:
        report.check(f"accepté : {label}", status == "Good", detail)
    else:
        report.check(f"refusé : {label}", status != "Good", detail)
    group.remove(thumbprint(der), is_trusted=True)


def check_profile_refusals(report: Report) -> None:
    """Trois certificats conformes à tout sauf à un point, donc refusés.

    C'est le cœur de l'auto-test. Une validation qui rend toujours ``Good``
    passerait tous les autres contrôles ; celle-ci doit échouer, sinon elle ne
    teste rien.

    L'ancrage est la clé publique du certificat forgé, ajoutée comme
    ``trusted_certificate`` : la chaîne est donc valide et la seule cause
    possible de refus est le profil.
    """
    store = CertificateStore(
        groups={GROUP: CertificateGroup(GROUP)}, application_uri=APP_URI
    )
    group = store.groups[GROUP]
    # La révision est neutralisée ici, et c'est délibéré : ce test isole UNE
    # variable, le profil du certificat. Sans cette pose, le défaut fermé de
    # §7.8.2.10 refuse tout certificat dépourvu de CRL — y compris le témoin
    # conforme — et le refus masquerait exactement ce qu'on cherche à observer.
    # La révocation a son propre auto-test, ``selftest_revocation.py``.
    group.default_validation_options = int(
        ua.TrustListValidationOptions.SuppressRevocationStatusUnknown
    )

    cases = (
        (
            "KeyUsage incomplet pour RSA (2 bits sur 4 exigés)",
            LEGACY_KEY_USAGE,
            [ExtendedKeyUsageOID.SERVER_AUTH],
        ),
        (
            "serverAuth absent de l'EKU",
            FULL_KEY_USAGE,
            [ExtendedKeyUsageOID.CLIENT_AUTH],
        ),
        (
            "keyCertSign absent sur un auto-signé",
            x509.KeyUsage(
                digital_signature=True, content_commitment=True,
                key_encipherment=True, data_encipherment=True,
                key_agreement=False, key_cert_sign=False, crl_sign=False,
                encipher_only=False, decipher_only=False,
            ),
            [ExtendedKeyUsageOID.SERVER_AUTH],
        ),
    )
    for label, key_usage, eku in cases:
        _install_and_report(
            report, store, group, label, key_usage, eku, expect_accept=False
        )

    # Le témoin : le même chemin avec un certificat conforme passe. Sans lui, un
    # refus attribuable à un autre motif passerait pour une preuve.
    _install_and_report(
        report,
        store,
        group,
        "conformité complète",
        FULL_KEY_USAGE,
        [ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH],
        expect_accept=True,
    )

async def main() -> int:
    report = Report("amorçage de confiance (Part 12 §7.1, Part 6 Table 50)")

    anchors = check_files(report)
    check_no_key_leak(report)
    if anchors:
        check_profile_refusals(report)
        try:
            await check_gds_loads(report, anchors)
        except Exception as exc:
            report.check(
                "amorçage du GDS exécuté sans exception", False, f"{type(exc).__name__}: {exc}"
            )
            logger.exception("Détail")
    else:
        report.check(
            "amorçage exécuté",
            False,
            "aucun couple de certificats : lancez tools/bootstrap_certificates.py",
        )

    return report.finish()


if __name__ == "__main__":
    from sciicad.console import setup

    setup()
    os.chdir(Path(__file__).resolve().parent.parent)
    sys.exit(asyncio.run(main()))
