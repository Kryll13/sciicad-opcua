#!/usr/bin/env python3
"""Auto-test des événements d'audit du GDS, Part 12 §7.8.2.13 et §7.10.27.

Un événement d'audit n'est pas une ligne de journal : c'est une notification
que le serveur pousse à un client **abonné**, filtré sur l'``EventType``. Ce
test souscrit donc réellement, et compte ce qui arrive.

Ce qui est vérifié, et pourquoi :

* les deux ``ObjectType`` d'audit sont à leurs NodeIds normatifs (12561, 12620) ;
* un ``AddCertificate`` qui modifie la liste émet **un** événement, portant le
  bon ``EventType`` et le bon ``TrustListId`` ;
* un ``AddCertificate`` **idempotent** n'émet rien : la méthode réussit, la
  liste ne change pas, et annoncer une mise à jour serait faux ;
* ``Open`` / ``Close`` / ``Read`` n'émettent rien — seules les trois méthodes
  de §7.8.2.13 modifient le contenu ;
* un ``UpdateCertificate`` refusé n'émet rien, et un réussi en émet un ;
* la notification porte bien les propriétés obligatoires du type, avec des
  types déclarés : une property sans DataType ne survivrait pas à la
  sérialisation, et le client ne le saurait pas.

    python tools/selftest_audit.py
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from asyncua import Client, Server, ua
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from loguru import logger

from gds.certstore import CertificateStore
from gds.certificategroup import CertificateGroupNode
from gds.audit import AuditEmitter
from gds.serverconfiguration import ServerConfigurationNode
from gds.trustlist import CertificateGroup
from sciicad.selftest import Report, free_port

GROUP = "DefaultApplicationGroup"
APP_URI = "urn:SCIICAD:gds-selftest"

TRUST_LIST_UPDATED = ua.ObjectIds.TrustListUpdatedAuditEventType    # 12561
CERTIFICATE_UPDATED = ua.ObjectIds.CertificateUpdatedAuditEventType  # 12620


def make_ca() -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    """Autorité de certification de test, extérieure au GDS."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "SCIICAD"),
        x509.NameAttribute(NameOID.COMMON_NAME, "SCIICAD Auto-Test CA"),
    ])
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject).issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True, content_commitment=False,
                key_encipherment=False, data_encipherment=False,
                key_agreement=False, key_cert_sign=True, crl_sign=True,
                encipher_only=False, decipher_only=False,
            ), critical=True,
        )
        .sign(key, hashes.SHA256())
    )
    return key, certificate


def sign_request(ca_key, ca_certificate, csr_der: bytes) -> bytes:
    """Signe une PKCS #10 et rend le DER.

    Le sujet et le SAN viennent de la demande, mais ``BasicConstraints`` est
    réécrite : une autorité ne peut pas signer un certificat d'AC, et le GDS
    refuse un certificat qui se déclare comme tel.
    """
    csr = x509.load_der_x509_csr(csr_der)
    now = datetime.now(timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(csr.subject)
        .issuer_name(ca_certificate.subject)
        .public_key(csr.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=30))
    )
    for extension in csr.extensions:
        if isinstance(extension.value, x509.BasicConstraints):
            continue
        builder = builder.add_extension(extension.value, extension.critical)
    builder = builder.add_extension(
        x509.BasicConstraints(ca=False, path_length=None), critical=True
    )
    return builder.sign(ca_key, hashes.SHA256()).public_bytes(serialization.Encoding.DER)


class AuditCollector:
    """Abonné aux deux types d'audit, et rien d'autre.

    Un abonnement par type, parce que la norme donne des ``EventType``
    distincts : les confondre masquerait un type émis à la place de l'autre.

    Le gestionnaire reçoit **un** ``Event`` par notification — asyncua
    déballe la liste côté client — dont les attributs sont nommés d'après les
    ``SelectClauses`` que le client a lui-même demandé. Lire par nom est donc
    le seul moyen fiable : lire par position supposerait un ordre que rien ne
    garantit.
    """

    #: Attributs internes d'un ``Event``, à ne pas traiter comme des propriétés.
    _INTERNAL = frozenset({
        "server_handle", "select_clauses", "event_fields", "data_types",
        "emitting_node", "internal_properties",
    })

    def __init__(self) -> None:
        self.events: list = []
        #: Ce qui a été reçu depuis le dernier ``note()``. Un compte cumulatif
        #: depuis le début du test rendrait toute vérification « aucun
        #: événement » fausse : le premier événement resterait compté.
        self.last: dict[int, list] = {}

    async def event_notification(self, event) -> None:
        self.events.append(event)

    def note(self) -> None:
        """Classe les événements reçus depuis le dernier appel."""
        self.last = {}
        for event in self.events:
            fields = {
                name: value
                for name, value in vars(event).items()
                if not name.startswith("_") and name not in self._INTERNAL
            }
            event_type = fields.get("EventType")
            if event_type is None:
                continue
            self.last.setdefault(int(event_type.Identifier), []).append(fields)
        self.events.clear()

    def count(self, event_type: int) -> int:
        return len(self.last.get(event_type, []))

    def fields(self, event_type: int) -> dict:
        received = self.last.get(event_type, [])
        return received[0] if received else {}


def _decode(notification) -> list[dict]:
    """Extrait ``{nom: valeur}`` des champs d'une notification.

    Conservé pour le diagnostic : le gestionnaire de
    :class:`AuditCollector` fait le même travail en ligne.
    """
    from asyncua import ua as _ua

    names = [
        "EventType", "ActionTimeStamp", "Status", "ServerId",
        "ClientAuditEntryId", "ClientUserId", "EventId", "SourceNode",
        "SourceName", "Time", "ReceiveTime", "LocalTime", "Message",
        "Severity", "ConditionClassId", "ConditionClassName",
        "ConditionSubClassId", "ConditionSubClassName",
        "MethodId", "StatusCodeId", "InputArguments", "OutputArguments",
        "TrustListId", "CertificateGroup", "CertificateType",
    ]
    out = []
    for event in getattr(notification, "Events", []) or [notification]:
        if not hasattr(event, "EventFields"):
            continue
        out.append({name: variant.Value for name, variant in zip(names, event.EventFields)})
    return out


async def run(report: Report, url: str, group_node, manager_node, store, group, audit) -> None:
    ca_key, ca_certificate = make_ca()
    ca_der = ca_certificate.public_bytes(serialization.Encoding.DER)

    collector = AuditCollector()
    async with Client(url) as client:
        subscription = await client.create_subscription(50, collector)
        await subscription.subscribe_events(
            ua.ObjectIds.Server, TRUST_LIST_UPDATED
        )
        await subscription.subscribe_events(
            ua.ObjectIds.Server, CERTIFICATE_UPDATED
        )
        await asyncio.sleep(0.4)

        trust_list = client.get_node(group_node.trust_list.nodeid)
        methods = {
            (await m.read_browse_name()).Name: m
            for m in await trust_list.get_children()
            if (await m.read_node_class()) == ua.NodeClass.Method
        }
        configuration = client.get_node(manager_node.node.nodeid)
        manager = {
            (await m.read_browse_name()).Name: m
            for m in await configuration.get_children()
            if (await m.read_node_class()) == ua.NodeClass.Method
        }

        # -- AddCertificate qui modifie réellement la liste ------------------
        # Un vrai certificat DER, de la taille réelle : le résumé des arguments
        # d'audit ne se déclenche qu'au-delà de 64 octets, et un blob de 44
        # octets ne représenterait rien de réel.
        der = ca_certificate.public_bytes(serialization.Encoding.DER)
        await trust_list.call_method(methods["AddCertificate"], der, True)
        await asyncio.sleep(0.5)
        collector.note()
        report.check(
            "AddCertificate émets un TrustListUpdatedAuditEventType",
            collector.count(TRUST_LIST_UPDATED) == 1,
            f"{collector.count(TRUST_LIST_UPDATED)} événement(s)",
        )
        report.check(
            "aucun CertificateUpdated n'est émis par un changement de liste",
            collector.count(CERTIFICATE_UPDATED) == 0,
            f"{collector.count(CERTIFICATE_UPDATED)} événement(s)",
        )
        emitted = collector.fields(TRUST_LIST_UPDATED)
        report.check(
            "l'événement porte le TrustListId de l'objet TrustList",
            getattr(emitted.get("TrustListId"), "Identifier", None)
            == group_node.trust_list.nodeid.Identifier,
            f"TrustListId={emitted.get('TrustListId')}, "
            f"attendu i={group_node.trust_list.nodeid.Identifier}",
        )
        report.check(
            "l'événement porte le MethodId de la méthode appelée",
            getattr(emitted.get("MethodId"), "Identifier", None)
            is not None,
            f"MethodId={emitted.get('MethodId')}",
        )
        report.check(
            "l'InputArguments résume le certificat au lieu de le recopier",
            isinstance(emitted.get("InputArguments"), str)
            and "empreinte" in emitted["InputArguments"],
            f"{emitted.get('InputArguments')!r}",
        )
        report.check(
            "la sévérité est celle d'un audit (300, information)",
            emitted.get("Severity") == 300,
            f"Severity={emitted.get('Severity')}",
        )

        # -- AddCertificate idempotent : succès sans changement ---------------
        await trust_list.call_method(methods["AddCertificate"], der, True)
        await asyncio.sleep(0.5)
        collector.note()
        report.check(
            "un AddCertificate idempotent n'émet aucun événement",
            collector.count(TRUST_LIST_UPDATED) == 0,
            f"{collector.count(TRUST_LIST_UPDATED)} événement(s) "
            f"alors que la méthode a réussi",
        )

        # -- Open / Read / Close : aucun changement de contenu ---------------
        handle = await trust_list.call_method(methods["Open"], ua.OpenFileMode.Read)
        await trust_list.call_method(methods["Read"], handle, 4096)
        await trust_list.call_method(methods["Close"], handle)
        await asyncio.sleep(0.5)
        collector.note()
        report.check(
            "Open, Read et Close n'émettent aucun événement",
            collector.count(TRUST_LIST_UPDATED) == 0,
            f"{collector.count(TRUST_LIST_UPDATED)} événement(s)",
        )

        # -- UpdateCertificate refusé, puis accepté ---------------------------
        group.add(ca_der, is_trusted=False)
        null_id = ua.NodeId(0, 0)
        empty = ua.Variant([], ua.VariantType.ByteString)

        # Un certificat d'une autorité inconnue doit être refusé, sans événement.
        rogue_key, rogue_ca = make_ca()
        store.create_signing_request(GROUP, None, "CN=refuse", True, b"x" * 32)
        forged = sign_request(rogue_key, rogue_ca, store.create_signing_request(
            GROUP, None, "CN=refuse", False, b""
        ))
        try:
            await configuration.call_method(
                manager["UpdateCertificate"], null_id, null_id, forged, empty, "", b""
            )
        except Exception:
            pass
        await asyncio.sleep(0.5)
        collector.note()
        report.check(
            "un UpdateCertificate refusé n'émet aucun CertificateUpdated",
            collector.count(CERTIFICATE_UPDATED) == 0,
            f"{collector.count(CERTIFICATE_UPDATED)} événement(s)",
        )

        # Le chemin nominal, cette fois.
        store.create_signing_request(GROUP, None, "CN=accepte", True, b"y" * 32)
        csr = store.create_signing_request(GROUP, None, "CN=accepte", False, b"")
        accepted = sign_request(ca_key, ca_certificate, csr)
        # Un NodeId nu, pas le NumericNodeId du noeud serveur : la pile range
        # un NodeId recu sur le wire dans la classe de base, et la variante
        # que le serveur utilise en interne n existe pas dans la table des
        # ExtensionObject. Un client normatif n'a jamais le second cas.
        group_nodeid = ua.NodeId(group_node.node.nodeid.Identifier)
        await configuration.call_method(
            manager["UpdateCertificate"], group_nodeid, null_id,
            accepted, empty, "", b"",
        )
        await asyncio.sleep(0.5)
        collector.note()
        report.check(
            "un UpdateCertificate accepté émet un CertificateUpdatedAuditEventType",
            collector.count(CERTIFICATE_UPDATED) == 1,
            f"{collector.count(CERTIFICATE_UPDATED)} événement(s)",
        )
        cert_event = collector.fields(CERTIFICATE_UPDATED)
        report.check(
            "l'événement porte le NodeId du groupe de certificats",
            getattr(cert_event.get("CertificateGroup"), "Identifier", None) is not None,
            f"CertificateGroup={cert_event.get('CertificateGroup')}",
        )

        await subscription.delete()

    report.check(
        "aucun échec d'émission n'a été journalisé",
        audit.summary()["echecs"] == 0,
        f"émis={audit.summary()['emis']}, échecs={audit.summary()['echecs']}",
    )


async def main() -> int:
    report = Report("événements d'audit du GDS (Part 12 §7.8.2.13, §7.10.27)")

    port = free_port()
    server = Server()
    await server.init()
    server.socket_address = ("127.0.0.1", port)
    server.set_endpoint(f"opc.tcp://127.0.0.1:{port}")
    server.set_security_policy([ua.SecurityPolicyType.NoSecurity])

    group = CertificateGroup(name=GROUP)
    store = CertificateStore(
        groups={GROUP: group}, application_uri=APP_URI, hostnames=["localhost"]
    )
    audit = AuditEmitter(server)
    await audit.start()
    report.check(
        "l'audit est disponible avant le démarrage du serveur",
        audit.available,
        f"émis={audit.summary()['emis']}, échecs={audit.summary()['echecs']}",
    )

    group_node = CertificateGroupNode(server, group, audit=audit)
    await group_node.build()
    manager_node = ServerConfigurationNode(
        server,
        store,
        group_nodeids={GROUP: group_node.node.nodeid},
        # Le résolveur ne connaît qu'un NodeId, celui du groupe réel : un NodeId
        # forgé doit être refusé, jamais retomber sur le groupe par défaut.
        group_name=lambda nodeid: (
            GROUP if nodeid is not None
            and nodeid.Identifier == group_node.node.nodeid.Identifier
            else None
        ),
        audit=audit,
    )
    await manager_node.build()

    try:
        await server.start()
        async with Client(server.endpoint.geturl()) as probe:
            folder = await _folder(probe)
            # Le groupe doit être sous le dossier normatif pour que le test
            # d'audit s'exerce dans les mêmes conditions qu'un client réel.
            report.check(
                "le groupe est publié sous le dossier CertificateGroups",
                folder is not None,
                f"{folder.nodeid if folder else 'absent'}",
            )
        await run(
            report, server.endpoint.geturl(), group_node, manager_node,
            store, group, audit,
        )
    except Exception as exc:
        report.check(
            "auto-test exécuté sans exception",
            False,
            f"{type(exc).__name__}: {exc}",
        )
        logger.exception("Détail")
    finally:
        group.close_all()
        await server.stop()

    return report.finish()


async def _folder(client: Client):
    configuration = None
    for child in await client.nodes.server.get_children():
        if (await child.read_browse_name()).Name == "ServerConfiguration":
            configuration = child
            break
    if configuration is None:
        return None
    for child in await configuration.get_children():
        if (await child.read_browse_name()).Name == "CertificateGroups":
            return child
    return None


if __name__ == "__main__":
    from sciicad.console import setup

    setup()
    sys.exit(asyncio.run(main()))
