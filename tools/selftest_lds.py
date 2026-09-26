#!/usr/bin/env python3
"""
Auto-test du LDS : registre, persistance, expiration, `FindServersOnNetwork`.

Vérifie les quatre comportements qui ne sont pas couverts par la pile asyncua :

* persistance SQLite et survie au redémarrage ;
* expiration des entrées dont le renouvellement a cessé ;
* retrait immédiat sur ``IsOnline = False`` ;
* pagination de ``FindServersOnNetwork`` selon la clause 5.5.3.1.

Le LDS est éphémère (boucle locale, base temporaire) : aucun LDS de
production n'est touché.

    uv run tools/selftest_lds.py
"""

import asyncio
import sqlite3
import sys

from asyncua import Client, Server, ua

from sciicad.console import setup
from sciicad.discovery import LdsRegistrar
from sciicad.identity import set_application_identity
from sciicad.selftest import LdsHarness, Report, free_port, temp_database, wait_for

THERMO_URI = "urn:SCIICAD:thermo-plc-selftest"
PROTECT_URI = "urn:SCIICAD:protect-plc-selftest"


async def start_plc(port: int, app_uri: str) -> Server:
    """Démarre un serveur OPC UA minimal et le rend enregistrable."""
    plc = Server()
    await plc.init()
    plc.socket_address = ("127.0.0.1", port)
    plc.set_endpoint(f"opc.tcp://127.0.0.1:{port}")
    plc.set_server_name(app_uri.rsplit(":", 1)[-1])
    await set_application_identity(plc, app_uri)
    plc.set_security_policy([ua.SecurityPolicyType.NoSecurity])
    await plc.start()
    return plc


async def main() -> int:
    setup()
    report = Report("LDS")

    with temp_database("lds") as tmp:
        # --- enregistrement et persistance -----------------------------
        report.section("1. Enregistrement, persistance et retrait (IsOnline=False)")
        async with LdsHarness(tmp.path(), sweep_interval=1) as lds:
            plc = await start_plc(free_port(), THERMO_URI)
            if True:  # serveur déjà démarré
                registrar = LdsRegistrar(plc, lds.url, period=0)
                registrar.start()
                # Attente conditionnelle plutôt qu'un délai fixe : sous charge,
                # un sleep court ferait échouer le test de façon aléatoire.
                report.check(
                    "le PLC est enregistré",
                    await wait_for(lambda: THERMO_URI in lds.uris(), timeout=15.0),
                    str(lds.uris()),
                )
                report.check("l'entrée est en base", lds.store.count() == 1,
                             f"count={lds.store.count()}")

                # FindServers est un service sans session : on évite
                # d'ouvrir une session complète, qui laisserait une tâche de
                # surveillance active après la fermeture.
                client = Client(lds.url)
                await client.connect_sessionless()
                found = [s.ApplicationUri for s in await client.find_servers()]
                report.check("le PLC apparaît dans FindServers", THERMO_URI in found,
                             str(found))
                await client.disconnect_sessionless()

                # Retrait immédiat : pas d'attente du TTL.
                report.check("le retrait est acquitté", await registrar.stop())

            report.check("l'entrée disparaît du registre", THERMO_URI not in lds.uris(),
                         str(lds.uris()))
            report.check("l'entrée disparaît de la base", lds.store.count() == 0,
                         f"count={lds.store.count()}")
            events = [(e["action"], e["application_uri"]) for e in lds.store.recent_events(10)]
            report.check("le journal trace le retrait",
                         any(a == "unregister" for a, _ in events), str(events))

        # --- persistance après redémarrage -----------------------------
        report.section("2. Persistance : le registre survit au redémarrage")
        db_path = tmp.path("restart")
        async with LdsHarness(db_path, ttl_seconds=300) as lds:
            plc = await start_plc(free_port(), THERMO_URI)
            if True:  # serveur déjà démarré
                await plc.register_to_discovery(lds.url, 0)
                await asyncio.sleep(0.5)
            report.check("entree présente avant redémarrage", THERMO_URI in lds.uris())

        # Viecit l'entrée : elle doit être restaurée puis évacuée, pas ressuscitée.
        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE registered_servers SET last_registered_at = 0")
        conn.commit()
        conn.close()

        async with LdsHarness(db_path, ttl_seconds=300, sweep_interval=1) as lds:
            report.check("l'entrée est rechargée en mémoire", THERMO_URI in lds.uris(),
                         str(lds.uris()))
            report.check("l'entrée restaurée est marquée périmée",
                         lds.registry.is_expired(THERMO_URI))
            await wait_for(lambda: _is_gone(lds, THERMO_URI), timeout=8.0)
            report.check("le balayage évacue l'entrée périmée", _is_gone(lds, THERMO_URI),
                         str(lds.uris()))
            report.check("la base a été alignée", lds.store.count() == 0,
                         f"count={lds.store.count()}")

        # --- expiration contre renouvellement -----------------------
        report.section("3. Expiration : un serveur qui se réenregistre est conservé")
        async with LdsHarness(tmp.path("renew"), ttl_seconds=10, sweep_interval=1) as lds:
            plc = await start_plc(free_port(), PROTECT_URI)
            if True:  # serveur déjà démarré
                # Renouvellement toutes les 2 s, pour un TTL de 10 s.
                await plc.register_to_discovery(lds.url, 2)
                await asyncio.sleep(25)
                report.check(
                    "l'entrée survit grâce au renouvellement",
                    PROTECT_URI in lds.uris(), str(lds.uris()),
                )
                await plc.unregister_from_discovery(lds.url)
            await asyncio.sleep(18)
            report.check(
                "l'entrée expire après arrêt du renouvellement",
                PROTECT_URI not in lds.uris(), str(lds.uris()),
            )

        # --- écoute large, annonce ciblée ---------------------------
        report.section("4. Écoute large, annonce ciblée")
        from lds.config import DiscoveryConfig, LDSConfig, ServerConfig

        port = free_port()
        config = LDSConfig(
            server=ServerConfig(
                bind_address="0.0.0.0", port=port,
                advertise_host="192.0.2.10",  # TEST-NET-1 : non route, mais annoncé
            ),
            discovery=DiscoveryConfig(sweep_interval_seconds=3600),
            database={"enabled": False},
        )
        from lds.server import LocalDiscoveryServer

        lds2 = LocalDiscoveryServer(config)
        await lds2.start()
        try:
            report.check("le LDS démarre en écoutant toutes interfaces",
                         lds2.server is not None)
            endpoints = await lds2.server.get_endpoints()
            report.check(
                "GetEndpoints annonce l'hôte configuré, pas 0.0.0.0",
                all("192.0.2.10" in e.EndpointUrl for e in endpoints),
                endpoints[0].EndpointUrl,
            )
        finally:
            await lds2.stop()

        # --- FindServersOnNetwork -------------------------------------
        report.section("5. FindServersOnNetwork : RecordId monotones et pagination")
        async with LdsHarness(tmp.path("fson"), sweep_interval=3600) as lds:
            servers = []
            for index in range(5):
                plc = await start_plc(free_port(), f"urn:SCIICAD:plc-page-{index}")
                await plc.register_to_discovery(lds.url, 0)
                servers.append(plc)

            try:
                client = Client(lds.url)
                await client.connect()

                async def page(start: int, maximum: int) -> list[tuple[int, str]]:
                    result = await client.uaclient.find_servers_on_network(
                        ua.FindServersOnNetworkParameters(
                            StartingRecordId=start, MaxRecordsToReturn=maximum
                        )
                    )
                    records = result if isinstance(result, (list, tuple)) else [result]
                    return [(e.RecordId, e.ServerName) for r in records for e in r.Servers]

                everything = await page(0, 0)
                ids = [i for i, _ in everything]
                report.check("les 5 serveurs et le LDS sont listés",
                             len(everything) == 6, str(len(everything)))
                # Clause 5.5.3.1 : identifiant croissant, jamais réutilisé.
                report.check("les RecordId sont uniques", len(set(ids)) == len(ids), str(ids))
                report.check("les RecordId sont croissants", ids == sorted(ids), str(ids))

                first = await page(0, 2)
                report.check("MaxRecordsToReturn limite la page", len(first) == 2, str(first))
                last_id = max(i for i, _ in first)
                rest = await page(last_id, 0)
                report.check("StartingRecordId exclut ce qui a été vu", len(rest) == 4, str(rest))
                report.check("les pages ne se recouvrent pas",
                             not (set(first) & set(rest)))
                report.check("l'union des pages redonne la liste complète",
                             set(first) | set(rest) == set(everything))
                await client.disconnect()
            finally:
                for plc in servers:
                    await plc.stop()

    return report.finish()


def _is_gone(lds, uri: str) -> bool:
    return uri not in lds.uris()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
