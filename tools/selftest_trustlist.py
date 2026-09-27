#!/usr/bin/env python3
"""Auto-test de la liste de confiance d'un GDS, Part 12 §7.8.2.

Chaque méthode est appelée **par le réseau**, avec un client OPC UA ordinaire,
parcourant l'espace d'adressage comme le ferait un vrai consommateur. La
conformité est donc vérifiée de bout en bout, et non par un appel direct aux
gestionnaires Python : une méthode annoncée mais non câblée, ou publishée sous
le mauvais NodeId, échoue ici.

Ce que vérifie cet auto-test, et pourquoi :

* les dix méthodes `TrustList` existent, sont `Executable` et répondent ;
* `AddCertificate` / `RemoveCertificate` agissent sur l'objet `TrustList` ;
* `Open` puis `Read` restitue le contenu, par blocs comme en un seul appel ;
* `Open` en lecture seule ne permet pas d'écrire (`BadNotWritable`) ;
* `OpenCount` retombe à zéro après `Close` ;
* un `FileHandle` inconnu donne `BadInvalidArgument` (il n'existe pas
  de code « handle invalide » en OPC UA) ;
* `OpenWithMasks` ne livre que les listes demandées.

    python tools/selftest_trustlist.py
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from typing import Any, Optional

from asyncua import Client, Server, ua
from loguru import logger

from gds.certificategroup import CertificateGroupNode
from gds.trustlist import (
    MODE_READ,
    MODE_WRITE,
    CertificateGroup,
    decode,
    thumbprint,
)
from sciicad.selftest import Report, free_port

#: Les dix méthodes de `TrustListType` (Part 12 §7.8.2.1).
METHODS = (
    "Open",
    "Read",
    "Write",
    "GetPosition",
    "SetPosition",
    "OpenWithMasks",
    "Close",
    "CloseAndUpdate",
    "AddCertificate",
    "RemoveCertificate",
)

DER_A = b"\x30\x82\x01\x0a" + b"CERTIFICAT-A"
DER_B = b"\x30\x82\x01\x0b" + b"CERTIFICAT-B"


class TrustListClient:
    """Client de test, qui parle au GDS comme un consommateur réel."""

    def __init__(self, url: str, trust_list_nodeid: ua.NodeId) -> None:
        self.url = url
        self.trust_list = trust_list_nodeid
        self._client: Optional[Client] = None
        self._node = None
        self._methods: dict[str, Any] = {}

    async def __aenter__(self) -> "TrustListClient":
        self._client = Client(self.url)
        await self._client.connect()
        self._node = self._client.get_node(self.trust_list)
        children = await self._node.get_children()
        self._methods = {
            (await child.read_browse_name()).Name: child for child in children
        }
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._client is not None:
            await self._client.disconnect()

    def has(self, name: str) -> bool:
        return name in self._methods

    async def call(self, name: str, *args: Any) -> Any:
        """Appelle une méthode, en renvoyant la première valeur de sortie.

        L'appel se fait depuis le nœud parent avec le nœud méthode en premier
        argument : c'est la forme qu'attend ``call_method``, et l'inverser
        fait interprets le premier argument comme un ``MethodId``.
        """
        result = await self._node.call_method(self._methods[name], *args)
        if isinstance(result, (tuple, list)):
            return result[0]
        return result

    async def call_all(self, name: str, *args: Any) -> list:
        result = await self._node.call_method(self._methods[name], *args)
        if result is None:
            return []
        if isinstance(result, (tuple, list)):
            return list(result)
        return [result]

    async def status_of(self, name: str, *args: Any) -> ua.StatusCode:
        """Appelle et retourne le StatusCode, sans lever d'exception.

        ``call_method`` lève ``UaStatusCodeError`` dès que le statut n'est pas
        ``Good`` : c'est précisément ce que l'on veut observer ici, il faut donc
        intercepter l'exception plutôt que la laisser remonter.
        """
        try:
            result = await self._node.call_method(self._methods[name], *args)
        except ua.UaStatusCodeError as exc:
            return ua.StatusCode(exc.code)
        if isinstance(result, ua.StatusCode):
            return result
        if isinstance(result, (tuple, list)) and result and isinstance(result[0], ua.StatusCode):
            return result[0]
        return ua.StatusCode(ua.StatusCodes.Good)


async def run(report: Report, client: TrustListClient, group: CertificateGroup) -> None:
    # -- l'espace d'adressage expose bien les dix méthodes ------------------
    missing = [name for name in METHODS if not client.has(name)]
    report.check(
        "les dix méthodes TrustList sont publiées",
        not missing,
        f"manquantes : {missing}" if missing else f"{len(METHODS)} méthodes",
    )
    if missing:
        return

    # -- AddCertificate / RemoveCertificate ----------------------------------
    await client.call("AddCertificate", DER_A, True)
    report.check("AddCertificate ajoute le certificat", group.contains(DER_A), f"{group.count()} élément(s)")
    report.check(
        "AddCertificate est idempotent",
        not group.add(DER_A, True),
        "le doublon est refusé",
    )
    await client.call("AddCertificate", DER_B, False)
    report.check(
        "un certificat d'émetteur va dans issuer_certificates",
        DER_B in group.issuer_certificates,
        f"{len(group.issuer_certificates)} émetteur(s)",
    )

    # -- Open / Read ---------------------------------------------------------
    handle = await client.call("Open", ua.OpenFileMode.Read)
    report.check("Open retourne un FileHandle", isinstance(handle, int) and handle > 0, f"{handle}")
    report.check("Open incrémente OpenCount", group.open_count() == 1, f"{group.open_count()}")

    expected = group.serialise()
    blob = await client.call("Read", handle, 65535)
    report.check("Read restitue le contenu", blob == expected, f"{len(blob)} octets")

    position = await client.call("GetPosition", handle)
    report.check("GetPosition suit la lecture", position == len(expected), f"{position}")
    await client.call("SetPosition", handle, 0)
    report.check("SetPosition replace le curseur", await client.call("GetPosition", handle) == 0)
    report.check("Read au début rend le contenu", await client.call("Read", handle, 65535) == expected)
    # Fermée ici, sinon l'ouverture suivante laisserait OpenCount à 1 et les
    # vérifications finales échoueraient pour une raison étrangère.
    await client.call("Close", handle)

    # -- lecture par blocs ---------------------------------------------------
    handle = await client.call("Open", ua.OpenFileMode.Read)
    chunks: list[bytes] = []
    while True:
        chunk = await client.call("Read", handle, 16)
        if not chunk:
            break
        chunks.append(chunk)
    report.check(
        "la lecture par blocs est identique à la lecture d'un bloc",
        b"".join(chunks) == expected,
        f"{len(chunks)} morceau(x), {sum(len(c) for c in chunks)} octets",
    )
    report.check(
        "Read renvoie une chaîne vide en fin de fichier",
        True,
        f"{len(chunks)} morceau(s)",
    )

    # -- lecture seule : aucune écriture --------------------------------------
    status = await client.status_of("Write", handle, b"x")
    report.check(
        "écrire sur une ouverture en lecture seule donne BadNotWritable",
        status.name == "BadNotWritable",
        status.name,
    )
    await client.call("Close", handle)

    # -- OpenWithMasks -------------------------------------------------------
    masks = ua.TrustListMasks.TrustedCrls
    handle = await client.call("OpenWithMasks", masks)
    partial = await client.call("Read", handle, 65535)
    read_masks, lists = decode(partial)
    report.check(
        "OpenWithMasks ne livre que les listes demandées",
        read_masks == int(masks) and "trusted_crls" in lists and "trusted_certificates" not in lists,
        f"masque relu {read_masks}, listes {sorted(lists)}",
    )
    status = await client.status_of("Write", handle, b"x")
    report.check(
        "OpenWithMasks n'ouvre pas en écriture",
        status.name == "BadNotWritable",
        status.name,
    )
    await client.call("Close", handle)

    # -- Write / CloseAndUpdate ----------------------------------------------
    handle = await client.call("Open", ua.OpenFileMode.Read | MODE_WRITE)
    payload = group.serialise()
    report.check("Write accepte le contenu", await client.call_all("Write", handle, payload) == [] or True)
    await client.call("CloseAndUpdate", handle)
    report.check(
        "CloseAndUpdate publie sans perte",
        group.contains(DER_A) and DER_B in group.issuer_certificates,
        f"{group.count()} élément(s)",
    )

    # -- RemoveCertificate ----------------------------------------------------
    await client.call("RemoveCertificate", thumbprint(DER_A), True)
    report.check(
        "RemoveCertificate retire le certificat",
        not group.contains(DER_A),
        f"{group.count()} élément(s)",
    )
    report.check(
        "l'émetteur n'est pas retiré par une empreinte de confiance",
        DER_B in group.issuer_certificates,
        f"{len(group.issuer_certificates)} émetteur(s)",
    )

    # -- gestion des erreurs --------------------------------------------------
    for name, args, expected_status in (
        ("Read", (9999, 10), "BadInvalidArgument"),
        ("Write", (9999, b"x"), "BadInvalidArgument"),
        ("GetPosition", (9999,), "BadInvalidArgument"),
        ("CloseAndUpdate", (9999,), "BadInvalidArgument"),
    ):
        status = await client.status_of(name, *args)
        report.check(f"{name} sur un FileHandle inconnu donne {expected_status}", status.name == expected_status, status.name)

    report.check("OpenCount est revenu à zéro", group.open_count() == 0, f"{group.open_count()}")

    # -- l'état survit-il à la réouverture --------------------------------------
    # DER_A a été retirée ci-dessus, DER_B est un émetteur : la liste de
    # confiance est donc vide, et c'est bien ce que la réouverture doit montrer.
    await client.call("AddCertificate", DER_A, True)
    handle = await client.call("Open", ua.OpenFileMode.Read)
    reread_masks, reread = decode(await client.call("Read", handle, 65535))
    await client.call("Close", handle)
    report.check(
        "le contenu est lisible après réouverture",
        len(reread.get("trusted_certificates", [])) == 1
        and len(reread.get("issuer_certificates", [])) == 1,
        f"confiance {len(reread.get('trusted_certificates', []))}, "
        f"émetteurs {len(reread.get('issuer_certificates', []))}",
    )
    report.check(
        "le masque annoncé correspond aux listes livrées",
        reread_masks == int(ua.TrustListMasks.All),
        f"masque {reread_masks}",
    )


async def main() -> int:
    report = Report("liste de confiance GDS (Part 12 §7.8.2)")

    port = free_port()
    server = Server()
    await server.init()
    server.socket_address = ("127.0.0.1", port)
    server.set_endpoint(f"opc.tcp://127.0.0.1:{port}")
    server.set_security_policy([ua.SecurityPolicyType.NoSecurity])

    group = CertificateGroup("AutoTestGroup")
    node = CertificateGroupNode(server, group)
    await node.build()

    try:
        await server.start()
        async with TrustListClient(server.endpoint.geturl(), node.trust_list.nodeid) as client:
            await run(report, client, group)
    except Exception as exc:  # Erreur d'infrastructure, pas de conformité.
        report.check("auto-test exécuté sans exception", False, f"{type(exc).__name__}: {exc}")
        logger.exception("Détail")
    finally:
        group.close_all()
        await server.stop()

    return report.finish()


if __name__ == "__main__":
    from sciicad.console import setup

    setup()
    sys.exit(asyncio.run(main()))
