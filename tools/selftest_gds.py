#!/usr/bin/env python3
"""Auto-test de conformité du GDS aux services de découverte de la Part 4.

Ce test existe parce que le GDS précédent déclarait des « Discovery Services »
— FindServers, FindServersOnNetwork, RegisterServer, RegisterServer2 — qui
n'étaient que des nœuds ``Method`` à NodeIds inventés, en ``String``, qu'aucun
client normatif n'appelle. Le serveur démarrait, annonçait ces services dans
son journal, et ne répondait à rien.

Chaque vérification interroge donc le service par son NodeId normatif, depuis
un client OPC UA ordinaire. Si une réponse change de forme, le test échoue.

Est vérifié aussi ce qui distingue un GDS d'un LDS : la portée globale. Une
inscription non renouvelée survit, parce que rien n'expire.

    python tools/selftest_gds.py
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from typing import Any, Optional

from asyncua import Client, Server, ua
from loguru import logger

from gds.config import GDSConfig
from gds.server import GlobalDiscoveryServer
from lds.config import DatabaseConfig, DiscoveryConfig, ServerConfig
from lds.store import ServerStore
from sciicad.discovery import LdsRegistrar
from sciicad.identity import set_application_identity
from sciicad.selftest import Report, free_port, wait_for

GDS_URI = "urn:SCIICAD:gds-selftest"
PLC_URI = "urn:SCIICAD:plc-gds"
PROTECT_URI = "urn:SCIICAD:protect-gds"


# ---------------------------------------------------------------------------
# Sondes : chaque appel interroge un service par son NodeId normatif
# ---------------------------------------------------------------------------


async def on_network(url: str) -> list[Any]:
    """Appelle FindServersOnNetwork (NodeId 12208) sans session.

    Lève une exception si le service ne répond pas : c'est le défaut que ce
    test doit détecter.
    """
    async with Client(url=url) as client:
        result = await client.connect_and_find_servers_on_network()
        return list(result.Servers or [])


async def names(url: str) -> list[str]:
    return [str(entry.ServerName) for entry in await on_network(url)]


async def entry(url: str, uri: str) -> Optional[Any]:
    for candidate in await on_network(url):
        if str(candidate.ServerName) == uri:
            return candidate
    return None


async def stored_uris(db_path: str) -> list[str]:
    """URI effectivement présentes en base.

    La liste est plus parlante qu'un compte : elle permet d'affirmer *laquelle*
    des deux entrées a survécu.
    """
    store = ServerStore(db_path)
    try:
        return [str(row.get("application_uri")) for row in store.load_all()]
    finally:
        store.close()


async def _by_name(parent, name: str):
    """Enfant portant ce browse name, ou ``None``.

    Les NodeIds d'instance étant alloués par la pile, le browse name est le
    seul moyen stable de retrouver un nœud — et c'est aussi le chemin qu'un
    client réel suivrait.
    """
    if parent is None:
        return None
    for child in await parent.get_children():
        if (await child.read_browse_name()).Name == name:
            return child
    return None


def check_scope(report: Report, gds: GlobalDiscoveryServer) -> None:
    """Vérifie qu'aucune entrée n'est jugée expirée en portée globale.

    Le même registre, avec la portée ``local``, évacuerait ces entrées : c'est
    donc bien le rôle qui est vérifié, et pas l'absence d'horodatage.
    """
    uris = gds.registry.application_uris()
    report.check(
        "aucune entrée n'est jugée expirée en portée globale",
        not any(gds.registry.is_expired(uri) for uri in uris),
        f"{uris}",
    )


def config_for(db_path: str) -> GDSConfig:
    """Configuration GDS éphémère : boucle locale, base temporaire."""
    return GDSConfig(
        server=ServerConfig(
            bind_address="127.0.0.1",
            port=free_port(),
            advertise_host="127.0.0.1",
            application_name="GDS selftest",
            application_uri=GDS_URI,
        ),
        discovery=DiscoveryConfig(scope="global"),
        database=DatabaseConfig(enabled=True, path=db_path, event_log=True),
    )


# ---------------------------------------------------------------------------
# Scénario
# ---------------------------------------------------------------------------


async def run(report: Report, gds: GlobalDiscoveryServer, db_path: str) -> None:
    url = gds.config.server.endpoint_url

    # -- identité ----------------------------------------------------------
    report.check(
        "l'endpoint annoncé porte le chemin du GDS",
        url.endswith("/GlobalDiscoveryServer"),
        url,
    )
    report.check(
        "le rôle global est bien celui configuré",
        gds.role == "GDS" and gds.scope == "global",
        f"{gds.role} / {gds.scope}",
    )

    # -- groupes de certificats (Part 12 §7.8) ------------------------------
    # Le GDS publie un CertificateGroupType par groupe configuré. Sans lui, le
    # GDS n'est qu'un LDS : c'est la partie qui le distingue.
    expected_groups = gds.config.certificates.groups
    report.check(
        "les groupes de certificats configurés sont publiés",
        set(gds.certificate_groups) == set(expected_groups),
        f"{sorted(gds.certificate_groups)} (attendu {sorted(expected_groups)})",
    )
    for name in expected_groups:
        group = gds.certificate_group(name)
        report.check(
            f"le groupe « {name} » est joignable par le registre",
            group is not None,
            f"{group.describe() if group else 'absent'}",
        )
    if gds.certificate_groups:
        # L'espace d'adressage doit exposer la TrustList, sinon un client
        # découvre un dossier vide.
        node = next(iter(gds.certificate_groups.values()))
        async with Client(url=url) as client:
            children = await client.get_node(node.trust_list.nodeid).get_children()
            methods = {
                (await child.read_browse_name()).Name
                for child in children
                if (await child.read_node_class()) == ua.NodeClass.Method
            }
        report.check(
            "l'objet TrustList est publié avec ses méthodes",
            len(methods) >= 9,
            f"{len(methods)} méthode(s) : {', '.join(sorted(methods))}",
        )

    # -- emplacement normatif des groupes (Part 12 §7.8.3.3) ---------------
    # Ces vérifications visent la faute que le câblage initial commettait : les
    # groupes étaient créés sous le nœud Server, et non sous
    # ServerConfiguration.CertificateGroups. Un test qui vise un NodeId ne voit
    # rien ; un client qui parcourt le chemin normal, lui, aboutit au groupe
    # d'asyncua — non câblé — et reçoit BadNothingToDo.
    async with Client(url=url) as client:
        server = client.nodes.server
        configuration = await _by_name(server, "ServerConfiguration")
        folder = await _by_name(configuration, "CertificateGroups") if configuration else None
        report.check(
            "le dossier CertificateGroups est sous ServerConfiguration (§7.8.3.3)",
            folder is not None
            and (await folder.read_type_definition()).Identifier
            == ua.ObjectIds.CertificateGroupFolderType,
            f"{folder.nodeid if folder else 'absent'}",
        )

        # Aucun groupe ne doit rester directement sous Server.
        stray = [
            child
            for child in await server.get_children()
            if (await child.read_node_class()) == ua.NodeClass.Object
            and (await child.read_type_definition()).Identifier
            == ua.ObjectIds.CertificateGroupType
        ]
        report.check(
            "aucun CertificateGroupType n'est publié hors du dossier normatif",
            not stray,
            f"{len(stray)} groupe(s) égaré(s) : "
            + ", ".join(str(node.nodeid) for node in stray)
            if stray
            else "aucun",
        )

        # Chaque groupe doit être atteint par le chemin normatif, et répondre.
        if folder is not None:
            reachable = {
                (await child.read_browse_name()).Name: child
                for child in await folder.get_children()
            }
            for name in expected_groups:
                group = reachable.get(name)
                report.check(
                    f"le groupe « {name} » est atteignable par le chemin normatif",
                    group is not None,
                    f"{group.nodeid if group else 'absent du dossier'}",
                )
                if group is None:
                    continue
                trust_list = await _by_name(group, "TrustList")
                report.check(
                    f"le groupe « {name} » expose sa TrustList",
                    trust_list is not None,
                    f"{trust_list.nodeid if trust_list else 'absente'}",
                )
                types = await _by_name(group, "CertificateTypes")
                value = await types.read_value() if types is not None else None
                report.check(
                    f"« {name} » renseigne CertificateTypes, propriété obligatoire",
                    bool(value),
                    ", ".join(f"i={v.Identifier}" for v in value) if value else "vide",
                )
                # Le test décisif : la méthode répond-elle, ou est-elle muette ?
                if trust_list is not None:
                    call = await _by_name(trust_list, "Open")
                    close = await _by_name(trust_list, "Close")
                    try:
                        handle = await trust_list.call_method(call, ua.OpenFileMode.Read)
                        # Une sortie unique est rendue telle quelle par
                        # call_method, sans être emballée dans une liste.
                        if isinstance(handle, (list, tuple)):
                            handle = handle[0]
                        await trust_list.call_method(close, handle)
                        status = "Good"
                    except Exception as exc:
                        status = ua.StatusCode(
                            getattr(exc, "code", ua.StatusCodes.BadInternalError)
                        ).name
                    report.check(
                        f"la TrustList de « {name} » répond à Open (câblée)",
                        status == "Good",
                        status,
                    )

    # -- FindServersOnNetwork, service que le serveur asyncua ne route pas --
    # C'est la vérification discriminante : sans le patch, la requête tombe
    # dans la branche « pas de session » d'asyncua (uaprocessor.py : la liste
    # des types exemptés omet FindServersOnNetwork) et reçoit
    # BadUserAccessDenied. C'est exactement ce que renvoyait le GDS précédent.
    try:
        initial = await names(url)
        reachable, detail = True, f"{initial}"
    except Exception as exc:
        reachable, detail = False, f"{type(exc).__name__}: {exc}"
    report.check(
        "FindServersOnNetwork répond sans session (NodeId 12208)", reachable, detail
    )
    if not reachable:
        return  # Les vérifications suivantes en dépendent.

    report.check("le GDS se liste lui-même", GDS_URI in initial, f"{initial}")

    # -- inscription par RegisterServer (NodeId 428) -----------------------
    plc = await register(PLC_URI, url, report)
    listed = await names(url)
    report.check("RegisterServer rend le serveur découvrable", PLC_URI in listed, f"{listed}")

    found = await entry(url, PLC_URI)
    report.check(
        "l'URL de découverte enregistrée est restituée",
        found is not None and bool(found.DiscoveryUrl),
        f"{getattr(found, 'DiscoveryUrl', None)}",
    )
    report.check(
        "l'entrée porte un RecordId (clause 5.5.3.1)",
        found is not None and isinstance(found.RecordId, int) and found.RecordId > 0,
        f"{getattr(found, 'RecordId', None)}",
    )

    # -- portée globale : ce qui distingue le GDS du LDS --------------------
    # Un LDS évacuerait cette entrée après le TTL. Un GDS la conserve, et son
    # registre restauré au démarrage fait foi.
    report.check(
        "le registre ne lance pas de tâche d'expiration",
        gds._sweeper is None,
        f"sweeper={gds._sweeper!r}",
    )
    check_scope(report, gds)

    # -- le GDS survit à son propre redémarrage -----------------------------
    # C'est la différence de fond avec un LDS, dont les entrées restaurées sont
    # soumises au TTL. Un GDS doit donc retrouver son inscription, non
    # renouvelée depuis l'arrêt, là où un LDS l'aurait déjà évacuée.
    #
    # Le serveur inscrit est abandonné sans retrait : un arrêt brutal (coupure
    # de courant, SIGKILL) ne laisse pas le temps de se désenregistrer. C'est
    # précisément le cas que le TTL d'un LDS sait traiter, et qu'un GDS doit
    # laisser en place.
    await register(PROTECT_URI, url, report)

    # Le port est libéré puis repris, ce qui prouve au passage que l'écoute
    # n'est pas attachée à une configuration figée.
    await gds.stop()
    restarted = GlobalDiscoveryServer(config_for(db_path))
    await restarted.start()
    try:
        url = restarted.config.server.endpoint_url
        listed = await names(url)
        report.check(
            "le registre restauré fait foi au redémarrage",
            PROTECT_URI in listed and PLC_URI in listed,
            f"{listed}",
        )
        report.check(
            "l'entrée restaurée survit sans renouvellement",
            not restarted.registry.is_expired(PROTECT_URI),
            f"{restarted.registry.describe()}",
        )
        check_scope(report, restarted)

        # -- persistance, lue avant tout retrait --------------------------
        # Un retrait supprime l'entrée de la base autant que du registre : la
        # compter après verrait le PLC absent, non parce que la persistance est
        # cassée mais parce que le test l'aurait vidé lui-même.
        persisted = await stored_uris(db_path)
        report.check(
            "les inscriptions sont persistées",
            PLC_URI in persisted and PROTECT_URI in persisted,
            f"{persisted}",
        )

        # -- retrait explicite, sur le serveur redémarré ------------------
        # Le PLC se ré-enregistre d'abord auprès du GDS redémarré : c'est ce
        # que fait un vrai serveur, qui renewe périodiquement son inscription.
        # Le retrait porte donc sur une inscription fraîche, et son registre
        # contient en outre une entrée restaurée, jamais renouvelée. Le
        # retirer prouve que la restauration n'a pas figé l'état.
        # Le registrar mémorise son URL à la construction : il faut la mettre à
        # jour, puis le relancer. Avec period=0, start() ne fait qu'une
        # inscription, ce qui convient ici.
        plc.lds_url = url
        plc.start()
        await withdraw(plc)
        listed = await names(url)
        report.check(
            "le retrait (IsOnline=False) évacue l'entrée", PLC_URI not in listed, f"{listed}"
        )
        report.check(
            "l'entrée restaurée est conservée (aucun effet de bord)",
            PROTECT_URI in listed,
            f"{listed}",
        )
        report.check(
            "le retrait supprime l'entrée correspondante en base",
            PLC_URI not in await stored_uris(db_path),
            f"{await stored_uris(db_path)}",
        )
    finally:
        await restarted.stop()


async def register(uri: str, url: str, report: Report) -> LdsRegistrar:
    """Enregistre un serveur auprès du GDS, par le chemin de production.

    L'URL du GDS est passée explicitement : le registrar la mémorise à la
    construction, et le GDS de l'auto-test est redémarré en cours de route.
    """
    server = Server()
    await server.init()
    server.set_endpoint(f"opc.tcp://127.0.0.1:{free_port()}")
    await set_application_identity(server, uri, server_name=uri)

    registrar = LdsRegistrar(server, url, period=0)
    registrar.start()

    async def listed() -> bool:
        try:
            return any(uri == n for n in await names(url))
        except Exception:
            return False

    # Attente conditionnelle plutôt qu'un délai fixe : sous charge, un sleep
    # court ferait échouer le test de façon aléatoire.
    if not await wait_for(listed, timeout=15.0):
        report.check(f"{uri} s'inscrit auprès du GDS", False, "inscription absente")
    return registrar


async def withdraw(registrar: LdsRegistrar) -> None:
    """Retire le serveur du registre, comme le fait un arrêt de processus."""
    from sciicad.lifecycle import withdraw_from_lds

    await withdraw_from_lds(registrar)
    await registrar.stop()
    try:
        await registrar.server.stop()
    except Exception:
        pass


async def main() -> int:
    report = Report("conformité du GDS aux services de la Part 4")
    temp_dir = tempfile.mkdtemp(prefix="gds-selftest-")
    db_path = os.path.join(temp_dir, "gds.db")

    first = GlobalDiscoveryServer(config_for(db_path))

    try:
        await first.start()
        await run(report, first, db_path)
    except Exception as exc:  # Erreur d'infrastructure, pas de conformité.
        report.check("auto-test exécuté sans exception", False, f"{type(exc).__name__}: {exc}")
        logger.exception("Détail de l'erreur")
    finally:
        # run() peut avoir arrêté et relancé le GDS ; stop() est idempotent.
        await first.stop()

    return report.finish()


if __name__ == "__main__":
    # Le rapport passe par loguru : il ne faut pas neutraliser les sorties, sans
    # quoi aucune vérification n'est visible.
    from sciicad.console import setup

    setup()
    sys.exit(asyncio.run(main()))
