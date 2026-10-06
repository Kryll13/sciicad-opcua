#!/usr/bin/env python3
"""Auto-test de la validation côté client, phase 3.

Ferme le dernier trou de la chaîne de confiance. Les phases 0 à 2 ont rendu le GDS
 capable de valider ses clients ; celle-ci fait l'inverse : un client vérifie le
certificat qu'on lui présente avant d'accepter la session.

L'attaque que ce test reproduit
-------------------------------

Un homme du milieu. Il intercepte ``OpenSecureChannel``, présente son **propre**
certificat — parfaitement conforme au profil de la Table 50, et même signé par
l'autorité de confiance — puis relaie tout le trafic. Chaque message reste
chiffré et authentifié : pour l'attaquant, qui détient la clé correspondant au
certificat qu'il présente.

C'est le point que ces tests doivent rendre visible : **le chiffrement ne dit rien
de l'identité**. Sans validation, la seule chose qui distingue le vrai GDS d'un
imposteur est l'adresse à laquelle on s'est connecté.

Trois contrôles, un seul qui compte
------------------------------

La validation repose sur trois vérifications, et il est utile de voir que les
deux premières ne suffisent pas :

* **chaîne** — le certificat remonte-t-il à une ancre ? Un auto-signé échoue ici,
  sauf s'il est lui-même ancre.
* **cohérence** — l'URI déclarée est-elle dans le SAN ? Un serveur mal configuré
  échoue ici.
* **attente** — l'URI déclarée est-elle celle qu'on attendait ? **C'est le
  contrôle qui manque d'ordinaire, et le seul que l'attaquant ne peut pas
  contourner.** Il contrôle ce qu'il présente, pas ce qu'on attend. Un
  imposteur présentant un certificat valide, signé par l'autorité du
  déploiement et portant l'URI d'une application existante, passe les deux
  premiers et échoue au troisième.

Ce test échoue volontairement sur chacun. Un test qui passe ne prouve rien s'il
ne peut pas échouer.

Contrôle négatif final
----------------------

Le test le plus important est le dernier : **sans brancher de validateur, le faux
serveur est accepté**. Sans lui, les refus pourraient être dûs à n'importe quoi
d'autre — et cette suite le démontre en rejetant pour des motifs sans rapport.
C'est ce contrôle qui attribue le refus à sa cause.

    python tools/selftest_client_trust.py
"""

from __future__ import annotations

import asyncio
import sys
import tempfile
from pathlib import Path

from asyncua import ua
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from loguru import logger

from sciicad.selftest import Report

sys.path.insert(0, str(Path(__file__).resolve().parent))
from selftest_authority import Lab, _der  # noqa: E402
from sciicad.trusted import ServerValidator, TrustStore  # noqa: E402

GDS_URI = "urn:SCIICAD:gds"
IHM_URI = "urn:SCIICAD:ihm"


def write_pair(directory: Path, name: str, key, certificate) -> tuple[Path, Path]:
    cert_path = directory / f"{name}.pem"
    key_path = directory / f"{name}_key.pem"
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    key_path.chmod(0o600)
    return cert_path, key_path


def _load(directories) -> TrustStore:
    """Charge plusieurs répertoires, comme le client réel le fait."""
    store = TrustStore()
    for directory in directories:
        for certificate in TrustStore.from_directory(directory)._anchors:
            store._add(certificate.public_bytes(serialization.Encoding.DER))
    return store


def _leaf_signed_by_repository_ca(*, uris=(IHM_URI,)):
    """Un certificat d'application signé par l'autorité du dépôt.

    Elle est en `~/.sciicad/ca/`, hors du dépôt, donc jamais versionnée — ce
    qui est exactement le but. Le certificat public, lui, est dans `pki/ca/`.
    """
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from authority import CA_CERT, CA_KEY

    from datetime import datetime, timedelta, timezone

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    authority_key = serialization.load_pem_private_key(CA_KEY.read_bytes(), None)
    authority = x509.load_pem_x509_certificate(CA_CERT.read_bytes())
    now = datetime.now(timezone.utc)
    usage = x509.KeyUsage(
        digital_signature=True, content_commitment=True, key_encipherment=True,
        data_encipherment=True, key_agreement=False, key_cert_sign=False,
        crl_sign=False, encipher_only=False, decipher_only=False,
    )
    certificate = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "IHM")]))
        .issuer_name(authority.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(usage, critical=True)
        .add_extension(
            x509.ExtendedKeyUsage(
                [ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH]
            ),
            critical=False,
        )
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.UniformResourceIdentifier(u) for u in uris]
            ),
            critical=False,
        )
        .sign(authority_key, hashes.SHA256())
    )
    return key, certificate


def describe(uri: str) -> ua.ApplicationDescription:
    return ua.ApplicationDescription(ApplicationUri=uri)


async def check_controls(report: Report, lab: Lab, directory: Path) -> None:
    """Les trois contrôles, et l'imposteur qui passe par deux."""
    # Un serveur légitime : certificat de l'autorité, URI déclarée correcte.
    server_key, server_cert = lab.leaf(uris=(GDS_URI,))

    # L'imposteur : lui aussi signé par l'autorité, lui aussi profil-conforme,
    # et il porte l'URI d'une application RÉELLEMENT présente dans le
    # déploiement. Il ne se distingue que par ce qu'on attend de lui.
    impostor_key, impostor_cert = lab.leaf(uris=(IHM_URI,))

    # Ancre : le certificat du serveur légitime est déclaré de confiance.
    # Deux répertoires, comme le déploiement réel : les ancres d'application et
    # l'autorité. Un seul ne suffit pas — une signature ne se contrôle qu'avec
    # la clé de celui qui a signé, et le certificat du serveur n'est pas
    # auto-signé dès qu'une autorité existe.
    anchors = directory / "anchors"
    anchors.mkdir()
    (anchors / "gds.pem").write_bytes(server_cert.public_bytes(serialization.Encoding.PEM))
    authority_dir = directory / "ca"
    authority_dir.mkdir()
    (authority_dir / "ca.pem").write_bytes(lab.certificate.public_bytes(serialization.Encoding.PEM))
    both = [anchors, authority_dir]

    store = _load(both)
    report.check(
        "les ancres sont chargées depuis les répertoires hors bande",
        len(store) == 2,
        f"{len(store)} ancre(s) : {len(store)} Certificates — feuille ET autorité",
    )

    # -- 1. serveur légitime, URI attendue --------------------------------
    validator = ServerValidator(store, expected_uri=GDS_URI)
    try:
        await validator(server_cert, describe(GDS_URI))
        report.check("accepté : le serveur attendu, certificat conforme", True, "validé")
    except Exception as exc:
        report.check(
            "accepté : le serveur attendu, certificat conforme", False,
            f"{type(exc).__name__}: {getattr(exc, 'message', str(exc))[:64]}",
        )

    # -- 2. l'imposteur présente un certificat valide d'une application réelle
    impostor = ServerValidator(store, expected_uri=IHM_URI)
    try:
        await impostor(impostor_cert, describe(IHM_URI))
        report.check(
            "accepté : si l'on attendait cette URI, le certificat est valide",
            True,
            "aucun contrôle ne distingue cette URI — il faut attendre",
        )
    except Exception as exc:
        report.check(
            "accepté : si l'on attendait cette URI, le certificat est valide",
            False,
            f"{type(exc).__name__}: {getattr(exc, 'message', str(exc))[:64]}",
        )

    # -- 3. contrôle d'attente : le même certificat, mais on attend le GDS --
    strict = ServerValidator(store, expected_uri=GDS_URI)
    try:
        await strict(impostor_cert, describe(IHM_URI))
        report.check(
            "refusé : l'imposteur présente un certificat valide d'une autre application",
            False,
            "ACCEPTÉ <<< l'homme du milieu passerait",
        )
    except Exception as exc:
        report.check(
            "refusé : l'imposteur présente un certificat valide d'une autre application",
            True,
            getattr(exc, 'message', str(exc))[:70],
    )

    # -- 4. cohérence : le certificat ne porte pas l'URI déclarée ----------
    anonymous = ServerValidator(store, expected_uri=None)
    try:
        await anonymous(server_cert, describe("urn:SCIICAD:inexistant"))
        report.check(
            "refusé : l'URI déclarée est absente du certificat présenté",
            False,
            "ACCEPTÉ <<<",
        )
    except Exception as exc:
        report.check(
            "refusé : l'URI déclarée est absente du certificat présenté",
            True,
            getattr(exc, 'message', str(exc))[:60],
        )

    # -- 5. aucun magasin : tout est refusé --------------------------------
    empty = ServerValidator(TrustStore(), expected_uri=GDS_URI)
    try:
        await empty(server_cert, describe(GDS_URI))
        report.check(
            "refusé : aucune ancre configurée", False, "ACCEPTÉ <<<"
        )
    except Exception as exc:
        report.check(
            "refusé : aucune ancre configurée", True, getattr(exc, 'message', str(exc))[:60]
        )


async def check_revocation(report: Report, lab: Lab, directory: Path) -> None:
    """Un certificat révoqué est refusé, si le client connaît la CRL."""
    key, certificate = lab.leaf(uris=(GDS_URI,))
    anchors_revocable = directory / "anchors-rev"
    anchors_revocable.mkdir()
    (anchors_revocable / "gds.pem").write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    ca_rev = directory / "ca-rev"
    ca_rev.mkdir()
    (ca_rev / "ca.pem").write_bytes(lab.certificate.public_bytes(serialization.Encoding.PEM))
    store = _load([anchors_revocable, ca_rev])

    from gds.trustlist import thumbprint

    issuer = lab.certificate
    issuer_print = thumbprint(_der(issuer))

    validator = ServerValidator(store, expected_uri=GDS_URI)
    try:
        await validator(certificate, describe(GDS_URI))
        report.check("accepté : certificat valide, aucune CRL connue", True, "validé")
    except Exception as exc:
        report.check(
            "accepté : certificat valide, aucune CRL connue", False,
            f"{type(exc).__name__}: {getattr(exc, 'message', str(exc))[:60]}",
        )

    revoked = ServerValidator(
        store, expected_uri=GDS_URI, crls={issuer_print: [_der(lab.crl([certificate.serial_number]))]}
    )
    try:
        await revoked(certificate, describe(GDS_URI))
        report.check(
            "refusé : certificat révoqué par une CRL connue du client",
            False,
            "ACCEPTÉ <<<",
        )
    except Exception as exc:
        report.check(
            "refusé : certificat révoqué par une CRL connue du client",
            True,
            getattr(exc, 'message', str(exc))[:60],
        )

    # Une CRL signée par une autre clé ne doit pas être appliquée — la même
    # leçon que pour le GDS, appliquée côté client.
    from cryptography.hazmat.primitives.asymmetric import rsa
    from datetime import datetime, timedelta, timezone

    rogue = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    forged = lab.crl(
        [certificate.serial_number], signer_key=rogue, issuer_name=issuer.subject
    )
    with_forgery = ServerValidator(
        store, expected_uri=GDS_URI, crls={issuer_print: [_der(forged)]}
    )
    try:
        await with_forgery(certificate, describe(GDS_URI))
        report.check(
            "accepté : CRL au bon nom mais signée par une autre clé, ignorée",
            True,
            "la CRL étrangère n'a pas révoqué",
        )
    except Exception as exc:
        report.check(
            "accepté : CRL au bon nom mais signée par une autre clé, ignorée",
            False,
            f"{type(exc).__name__}: {getattr(exc, 'message', str(exc))[:60]}",
        )


async def check_end_to_end(report: Report, lab: Lab, directory: Path) -> None:
    """Un vrai GDS, un vrai client, et un faux serveur qui se déguise."""
    from gds.config import GDSConfig
    from gds.server import GlobalDiscoveryServer
    from sciicad.selftest import free_port
    from sciicad.trusted import secure_client

    # Le certificat du CLIENT, et non celui du serveur. Un certificat
    # d'application porte une seule URI ; utiliser celui du GDS comme certificat
    # client ferait qu'il déclare « urn:SCIICAD:gds » alors qu'il se présente
    # comme « urn:SCIICAD:ihm ». La pile le signale puis continue, et la
    # connexion échoue plus tard, ailleurs.
    #
    # Il est signé par l'autorité **du dépôt**, celle que le GDS connaît : le
    # validateur du GDS contrôle le certificat client contre SES ancres, et une
    # autorité de laboratoire n'y figure pas. Le refus serait alors correct, mais
    # attribuable au serveur — ce qui masquerait ce que ce test vérifie, à
    # savoir la validation du serveur **par le client**.
    from cryptography.hazmat.primitives.asymmetric import rsa

    client_key, client_cert = _leaf_signed_by_repository_ca(uris=(IHM_URI,))
    client_cert_path, client_key_path = write_pair(
        directory, "client", client_key, client_cert
    )

    port = free_port()
    config = GDSConfig.load()
    config.server.bind_address = "127.0.0.1"
    config.server.port = port
    config.server.advertise_host = "127.0.0.1"
    config.database.enabled = False

    server = GlobalDiscoveryServer(config)
    await server.setup()
    await server.start()
    url = server.server.endpoint.geturl()

    # Le GDS doit declarer de confiance le certificat du client : c'est lui qui
    # decide, et il refuse un certificat non declare. Sans cela, le refus
    # serait correct mais attribuable au serveur, ce qui masquerait ce que ce
    # test verifie — la validation du certificat du serveur par le client.
    from gds.trustlist import _split_certificates

    server.certificate_groups["DefaultApplicationGroup"].group.add(
        _split_certificates(client_cert_path.read_bytes())[0], is_trusted=True
    )

    # Ancre hors bande : le certificat du serveur tel qu'il est, déposé dans un
    # répertoire. C'est §7.1 : la confiance se configure avant, pas pendant.
    anchors = directory / "e2e-anchors"
    anchors.mkdir()
    real_cert = x509.load_pem_x509_certificate(
        Path("gds/server_certificate.pem").read_bytes()
    )
    (anchors / "gds.pem").write_bytes(real_cert.public_bytes(serialization.Encoding.PEM))
    ca_e2e = directory / "ca-e2e"
    ca_e2e.mkdir()
    (ca_e2e / "ca.pem").write_bytes(
        x509.load_pem_x509_certificate(Path("pki/ca/ca_certificate.pem").read_bytes())
        .public_bytes(serialization.Encoding.PEM)
    )

    try:
        client = await secure_client(
            url,
            client_cert_path,
            client_key_path,
            trust_directories=[anchors, ca_e2e],
            expected_uri=GDS_URI,
            application_uri=IHM_URI,
        )
        async with client:
            await client.connect()
            nodes = await client.nodes.server.get_children()
            report.check(
                "un client validé se connecte au vrai GDS et parcourt l'espace",
                len(nodes) > 0,
                f"{len(nodes)} nœud(s) sous Server",
            )
            await client.disconnect()
    except Exception as exc:
        report.check(
            "un client validé se connecte au vrai GDS et parcourt l'espace",
            False,
            f"{type(exc).__name__}: {str(exc)[:60]}",
        )
    finally:
        for node in server.certificate_groups.values():
            node.group.close_all()
        await server.stop()

    # -- le contrôle d'absence : sans validateur, l'imposteur passe --------
    impostor_key, impostor_cert = lab.leaf(uris=(GDS_URI,))
    imp_cert_path, imp_key_path = write_pair(
        directory, "impostor", impostor_key, impostor_cert
    )

    # Un serveur factice qui répond avec le certificat de l'imposteur.
    from asyncua import Server

    fake = Server()
    await fake.init()
    fake.set_endpoint(url)
    fake.set_security_policy(
        [ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt]
    )
    await fake.load_certificate(str(imp_cert_path))
    await fake.load_private_key(str(imp_key_path))

    port2 = free_port()
    fake.set_endpoint(f"opc.tcp://127.0.0.1:{port2}/GlobalDiscoveryServer")
    await fake.start()
    try:
        # Sans validateur : accepté.
        naive = await secure_client(
            f"opc.tcp://127.0.0.1:{port2}/GlobalDiscoveryServer",
            client_cert_path,
            client_key_path,
            trust_directories=[anchors, ca_e2e],
            expected_uri=GDS_URI,
            application_uri=IHM_URI,
        )
        # On retire le validateur pour observer ce qui se passerait sans lui.
        naive.certificate_validator = None
        try:
            async with naive:
                await naive.connect()
                await naive.disconnect()
            naive_ok = True
            naive_detail = "connexion établie"
        except Exception as exc:
            naive_ok = False
            naive_detail = f"{type(exc).__name__}: {str(exc)[:44]}"

        # Avec validateur : refusé.
        guarded = await secure_client(
            f"opc.tcp://127.0.0.1:{port2}/GlobalDiscoveryServer",
            client_cert_path,
            client_key_path,
            trust_directories=[anchors, ca_e2e],
            expected_uri=GDS_URI,
            application_uri=IHM_URI,
        )
        try:
            async with guarded:
                await guarded.connect()
                await guarded.disconnect()
            guarded_ok = True
            guarded_detail = "connexion établie <<< refus attendu"
        except Exception as exc:
            guarded_ok = False
            guarded_detail = f"{type(exc).__name__}: {str(exc)[:50]}"

        report.check(
            "sans validateur, l'imposteur est accepté (le refus lui est dû)",
            naive_ok,
            naive_detail,
        )
        report.check(
            "avec validateur, le même imposteur est refusé",
            not guarded_ok,
            guarded_detail,
        )
    finally:
        await fake.stop()


async def main() -> int:
    report = Report("validation du certificat serveur, cote client (phase 3)")

    lab = Lab()
    try:
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            await check_controls(report, lab, directory)
            await check_revocation(report, lab, directory)
            await check_end_to_end(report, lab, directory)
    except Exception as exc:
        report.check("exécution sans exception", False, f"{type(exc).__name__}: {exc}")
        logger.exception("Détail")

    return report.finish()


if __name__ == "__main__":
    from sciicad.console import setup

    setup()
    sys.exit(asyncio.run(main()))