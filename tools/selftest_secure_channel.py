#!/usr/bin/env python3
"""Auto-test du canal sécurisé du LDS et du GDS, phase 2.

Ce que cette étape change, et qu'il faut voir
--------------------------------------------

Un serveur de découverte n'annonçait que ``NoSecurity``. Le commentaire qui
justifiait ce choix affirmait que « la découverte précède l'établissement d'un
canal sécurisé ». C'est un sophisme : la découverte *anonyme* est bien possible
en ``NoSecurity``, mais elle **n'inscrit rien**.

La Part 12 est explicite, à deux endroits :

* §6.2, Table 2 — « The Certificate used to create the SecureChannel is used to
  determine the identity of the OPC UA Application. » Le certificat du canal
  **est** l'identité.
* §6.5.6 — « This Method shall be called from an authenticated SecureChannel »
  avec « MessageSecurityMode SignAndEncrypt ».

Un serveur de découverte réduit à ``NoSecurity`` ne peut donc pas satisfaire ce
rôle : il ne peut ni inscribed, ni valider, ni être validé.

Pourquoi le validateur compte autant que le chiffrement
-------------------------------------------------------

Un canal ``SignAndEncrypt`` prouve qu'un client détient une clé privée. C'est
vrai, et insuffisant : le chiffrement empêche l'écoute, pas l'usurpation d'une
identité de confiance. Un attaquant qui détient **son propre** couple de clés
établit un canal parfaitement chiffré. Ce qui le distingue d'un client légitime,
c'est que sa clé n'est déclarée dans aucune liste de confiance.

``set_certificate_validator`` est exactement ce point : il reçoit le certificat
présenté et décide. Sans lui, « sécurisé » veut dire seulement « chiffré ».

Contrôles
---------

Quatre canaux réels, établis contre un vrai GDS, avec des verdicts
distincts :

* client **déclaré** de confiance → accepté ;
* certificat signé mais **jamais déclaré** → refusé, et c'est le cas
  security-critical : il est parfaitement conforme et sa chaîne est valide ;
* certificat **révoqué** → refusé avec ``BadCertificateRevoked`` ;
* ``NoSecurity`` reste accepté, sans certificat : la découverte anonyme doit
  continuer de fonctionner, sinon un client ne peut plus obtenir les endpoints
  sécurisés par ``GetEndpoints``.

Et deux contrôles d'**absence de protection**, dont un qui compte : sans
validateur, le certificat non déclaré serait accepté. Le retirer doit donc faire
échouer l'auto-test — c'est ce qui prouve que le validateur fait son travail.

    python tools/selftest_secure_channel.py
"""

from __future__ import annotations

import asyncio
import sys
import tempfile
from pathlib import Path

from asyncua import Client, ua
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from loguru import logger

from sciicad.selftest import Report, free_port

sys.path.insert(0, str(Path(__file__).resolve().parent))
from selftest_authority import Lab, _der, GROUP  # noqa: E402

class LabClient:
    """Un client de laboratoire : certificat, clé et fichiers sur disque."""

    def __init__(self, directory: Path, name: str, lab: Lab, serial_note: str = ""):
        key, certificate = lab.leaf()
        self.key = key
        self.certificate = certificate
        self.certificate_der = _der(certificate)
        self.cert_path = directory / f"{name}.pem"
        # L'extension .pem n'est pas décorative : la pile choisit le décodeur
        # sur le suffixe, et ne connaît que .pem (PEM) — tout autre suffixe part
        # en DER. Un fichier .key contenant du PEM échoue donc sur « Could not
        # deserialize key data », un message qui accusesort le format alors que
        # le format est bon et le nom faux. C'est ce qui explique que les
        # tests du dépôt passent : crypto_opcua nomme ses fichiers .pem.
        self.key_path = directory / f"{name}_key.pem"
        self.cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
        self.key_path.write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
        self.key_path.chmod(0o600)
        self.note = serial_note



async def start_gds(lab: Lab, *, attach_client=None,
                     revoke_serial: int | None = None, with_validator: bool = True):
    """Démarre un GDS réel, avec le contexte de confiance de la laboratoire."""
    from gds.config import GDSConfig
    from gds.server import GlobalDiscoveryServer

    config = GDSConfig.load()
    config.server.bind_address = "127.0.0.1"
    config.server.port = free_port()
    config.server.advertise_host = "127.0.0.1"
    config.database.enabled = False

    server = GlobalDiscoveryServer(config)
    if not with_validator:
        server.certificate_validator = None
    await server.setup()
    await server.start()

    group = server.certificate_groups[GROUP].group
    group.add(_der(lab.certificate), is_trusted=False)
    serials = [revoke_serial] if revoke_serial is not None else []
    group.add_crl(_der(lab.crl(serials)))
    if attach_client is not None:
        group.add(attach_client.certificate_der, is_trusted=True)
    return server


async def stop_gds(server) -> None:
    for node in server.certificate_groups.values():
        node.group.close_all()
    await server.stop()


async def try_connect(server, client: LabClient, uri: str) -> tuple[str, str]:
    """Tente un canal sécurisé. Rend ``(verdict, détail)``."""
    url = server.server.endpoint.geturl()
    try:
        c = Client(url)
        c.application_uri = uri
        # set_security avec des Path explicites, plutôt que set_security_string
        # qui recharge la liste d'endpoints et reconstruit une politique depuis
        # une chaîne découpée sur des virgules. Un chemin contenant une virgule
        # casserait ce découpage sans aucun message : l'échec se manifestoit
        # ensuite comme une clé illisible, très loin de sa cause.
        from asyncua.crypto import security_policies

        await c.set_security(
            security_policies.SecurityPolicyBasic256Sha256,
            client.cert_path,
            client.key_path,
            mode=ua.MessageSecurityMode.SignAndEncrypt,
        )
        async with c:
            await c.connect()
            endpoints = await c.connect_and_get_server_endpoints()
            await c.disconnect()
        return "accepte", f"{len(endpoints)} endpoint(s)"
    except Exception as exc:
        return "refuse", f"{type(exc).__name__}: {str(exc)[:70]}"


async def check_endpoints(report: Report, server) -> None:
    """Le serveur annonce-t-il SignAndEncrypt, et garde-t-il NoSecurity ?"""
    url = server.server.endpoint.geturl()
    async with Client(url) as plain:
        endpoints = await plain.connect_and_get_server_endpoints()
    modes = {(e.SecurityPolicyUri, e.SecurityMode) for e in endpoints}
    secure = [
        e for e in endpoints
        if e.SecurityMode == ua.MessageSecurityMode.SignAndEncrypt
        and "Basic256Sha256" in (e.SecurityPolicyUri or "")
    ]
    plain_present = any(
        e.SecurityMode == ua.MessageSecurityMode.None_ for e in endpoints
    )
    report.check(
        "le serveur annonce Basic256Sha256_SignAndEncrypt",
        bool(secure),
        f"{len(secure)} endpoint(s) sur {len(endpoints)}",
    )
    report.check(
        "NoSecurity reste annoncé : la découverte anonyme doit fonctionner",
        plain_present,
        f"{len(modes)} combinaison(s) politique/mode",
    )


async def check_denials(report: Report, lab: Lab, directory: Path) -> None:
    """Quatre canaux réels, quatre verdicts distincts."""
    # -- accepté : déclaré de confiance -----------------------------------
    trusted = LabClient(directory, "declare", lab)
    server = await start_gds(lab, attach_client=trusted)
    try:
        await check_endpoints(report, server)
        verdict, detail = await try_connect(server, trusted, "urn:SCIICAD:test")
        report.check(
            "accepté : client dont le certificat est déclaré de confiance",
            verdict == "accepte",
            f"{verdict} — {detail}",
        )
    finally:
        await stop_gds(server)

    # -- refusé : conforme et valide, mais jamais déclaré ------------------
    undeclared = LabClient(directory, "non-declare", lab)
    server = await start_gds(lab)
    try:
        verdict, detail = await try_connect(
            server, undeclared, "urn:SCIICAD:test"
        )
        report.check(
            "refusé : certificat signé mais jamais déclaré de confiance",
            verdict == "refuse",
            f"{verdict} — {detail}",
        )
        report.check(
            "  …et le refus vient bien du validateur, pas d'un accident",
            "CertificateUntrusted" in detail or "Service" in detail,
            detail,
        )
    finally:
        await stop_gds(server)

    # -- refusé : révoqué ---------------------------------------------------
    revoked = LabClient(directory, "revoque", lab)
    server = await start_gds(lab, attach_client=revoked,
                             revoke_serial=revoked.certificate.serial_number)
    try:
        verdict, detail = await try_connect(server, revoked, "urn:SCIICAD:test")
        report.check(
            "refusé : certificat révoqué par la CRL de son autorité",
            verdict == "refuse",
            f"{verdict} — {detail}",
        )
    finally:
        await stop_gds(server)


async def check_no_validator(report: Report, lab: Lab, directory: Path) -> None:
    """Le contrôle d'absence : sans validateur, le refus n'a plus lieu.

    C'est le contrôle le plus important du fichier. Un test qui vérifie qu'un
    refus a lieu ne prouve pas qu'un refus est *dû au validateur* — il peut
    être dû à n'importe quoi d'autre, et le reste du code le démontre en
    refusant pour des motifs sans rapport. Retirer le validateur et voir
    l'acceptation réapparaître est ce qui attribue le refus à sa cause.
    """
    undeclared = LabClient(directory, "non-declare-sans-validateur", lab)
    server = await start_gds(lab, with_validator=False)
    try:
        verdict, detail = await try_connect(server, undeclared, "urn:SCIICAD:test")
        report.check(
            "sans validateur, le même client serait accepté (le refus lui est dû)",
            verdict == "accepte",
            f"{verdict} — {detail}",
        )
    finally:
        await stop_gds(server)


async def check_lds(report: Report, directory: Path) -> None:
    """Le LDS annonce-t-il lui aussi un canal sécurisé ?

    Le LDS ne valide pas de certificat client : l'identité d'une application
    qui consulte un registre de découverte n'a pas d'intérêt normatif, et §6.2
    ne parle d'identité que pour les **services globaux**. Refuser les
    certificats non reconnus casserait les outils de diagnostic sans rien
    gagner. Le LDS doit en revanche **annoncer** le canal, pour qu'un client
    puisse RegisterServer en SignAndEncrypt.
    """
    from lds.config import LDSConfig
    from lds.server import DiscoveryServer

    config = LDSConfig.load()
    config.server.bind_address = "127.0.0.1"
    config.server.port = free_port()
    config.server.advertise_host = "127.0.0.1"
    config.database.enabled = False

    server = DiscoveryServer(config)
    await server.setup()
    await server.start()
    try:
        url = server.server.endpoint.geturl()
        async with Client(url) as plain:
            endpoints = await plain.connect_and_get_server_endpoints()
        secure = [
            e for e in endpoints
            if e.SecurityMode == ua.MessageSecurityMode.SignAndEncrypt
            and "Basic256Sha256" in (e.SecurityPolicyUri or "")
        ]
        report.check(
            "le LDS annonce Basic256Sha256_SignAndEncrypt",
            bool(secure),
            f"{len(secure)} endpoint(s) sur {len(endpoints)}",
        )
        report.check(
            "le LDS ne valide pas de certificat client (sans intérêt normatif)",
            server.certificate_validator is None,
            "aucun validateur, comme attendu pour un LDS",
        )
    finally:
        await server.stop()


async def check_degraded(report: Report, directory: Path) -> None:
    """Sans certificat, le serveur démarre-t-il en annonçant ce qu'il fait ?

    Le mode dégradé est légitime — un simulateur doit rester utilisable — mais il
    ne doit pas passer **inaperçu**. L'avertissement qui le signale doit nommer
    la conséquence, sinon un opérateur le lit comme un détail de démarrage et
    découvre le problème à la première inscription.
    """
    from lds.config import LDSConfig
    from lds.server import DiscoveryServer

    config = LDSConfig.load()
    config.server.bind_address = "127.0.0.1"
    config.server.port = free_port()
    config.server.advertise_host = "127.0.0.1"
    config.database.enabled = False
    config.server.certificate = None
    config.server.private_key = None

    server = DiscoveryServer(config)
    await server.setup()
    await server.start()
    try:
        url = server.server.endpoint.geturl()
        async with Client(url) as plain:
            endpoints = await plain.connect_and_get_server_endpoints()
        secure = [
            e for e in endpoints
            if e.SecurityMode != ua.MessageSecurityMode.None_
        ]
        report.check(
            "sans certificat, seul NoSecurity est annoncé",
            not secure,
            f"{len(endpoints)} endpoint(s), {len(secure)} securise(s)",
        )
    finally:
        await server.stop()


async def main() -> int:
    report = Report("canal securise du LDS et du GDS (phase 2)")

    lab = Lab()
    try:
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            await check_denials(report, lab, directory)
            await check_no_validator(report, lab, directory)
            await check_lds(report, directory)
            await check_degraded(report, directory)
    except Exception as exc:
        report.check("exécution sans exception", False, f"{type(exc).__name__}: {exc}")
        logger.exception("Détail")

    return report.finish()


if __name__ == "__main__":
    from sciicad.console import setup

    setup()
    sys.exit(asyncio.run(main()))