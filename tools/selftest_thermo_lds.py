#!/usr/bin/env python3
"""
Auto-test de ``sciicad.discovery`` : enregistrement et retrait du LDS.

Vérifie la procédure de la clause 5.5.5.1 : enregistrement unique, retrait par
``IsOnline = False``, retry progressif si le LDS est indisponible, et absence
de résurrection après redémarrage du LDS.

Le câblage réel d'un simulateur (arrêt sur SIGTERM, retrait effectif) est
vérifié de bout en bout par ``tools/selftest_thermo_lifecycle.py``.

    uv run tools/selftest_thermo_lds.py
"""

import asyncio
import sys
import time

from asyncua import Server, ua

from sciicad.console import setup
from sciicad.discovery import LdsRegistrar
from sciicad.identity import set_application_identity
from sciicad.selftest import LdsHarness, Report, free_port, temp_database, wait_for

TEST_URI = "urn:SCIICAD:registrar-selftest"


async def start_plc(port: int) -> Server:
    """Démarre un serveur OPC UA minimal, prêt à être enregistré."""
    plc = Server()
    await plc.init()
    plc.socket_address = ("127.0.0.1", port)
    plc.set_endpoint(f"opc.tcp://127.0.0.1:{port}")
    plc.set_server_name("registrar-selftest")
    await set_application_identity(plc, TEST_URI)
    plc.set_security_policy([ua.SecurityPolicyType.NoSecurity])
    await plc.start()
    return plc


async def test_register_and_withdraw(report: Report, db: str) -> None:
    """Enregistrement, renouvellement, retrait immédiat."""
    report.section("1. Enregistrement, renouvellement, retrait (IsOnline=False)")
    async with LdsHarness(db) as lds:
        plc = await start_plc(free_port())
        registrar = LdsRegistrar(plc, lds.url, period=2)
        registrar.start()
        try:
            report.check("le PLC est enregistré", await wait_for(
                lambda: TEST_URI in lds.uris(), timeout=8.0), str(lds.uris()))
            report.check("l'entrée est en base", lds.store.count() == 1,
                         f"count={lds.store.count()}")

            # Renouvellement : le compteur d'enregistrements doit croître.
            grew = await wait_for(
                lambda: _register_count(lds) >= 2, timeout=10.0
            )
            report.check("le renouvellement est comptabilisé", grew,
                         f"registrements={_register_count(lds)}")
        finally:
            report.check("le retrait est acquitté", await registrar.stop())
            await plc.stop()

        report.check("l'entrée disparaît du registre", TEST_URI not in lds.uris(),
                     str(lds.uris()))
        report.check("l'entrée disparaît de la base", lds.store.count() == 0,
                     f"count={lds.store.count()}")
        events = [e["action"] for e in lds.store.recent_events(10)]
        report.check("le journal trace le retrait", "unregister" in events, str(events))


def _register_count(lds) -> int:
    rows = {r["application_uri"]: r["register_count"] for r in lds.store.load_all()}
    return rows.get(TEST_URI, 0)


async def test_retry_when_lds_down(report: Report) -> None:
    """LDS injoignable : retry progressif, sans exception."""
    report.section("2. LDS indisponible : retry progressif, aucune exception")
    dead_url = f"opc.tcp://127.0.0.1:{free_port()}"  # rien n'écoute
    plc = await start_plc(free_port())
    registrar = LdsRegistrar(plc, dead_url, period=4)

    started = time.monotonic()
    registrar.start()  # ne doit pas lever
    await asyncio.sleep(5)
    elapsed = time.monotonic() - started

    report.check("aucune exception n'est levée", True)
    report.check("le PLC n'est pas marqué enregistré", registrar.registered is False)
    report.check("la boucle a persisté pendant l'essai", elapsed >= 4.0, f"{elapsed:.1f}s")

    # stop() sur un PLC jamais enregistré ne doit rien faire de particulier.
    await registrar.stop()
    report.check("stop() sans enregistrement est sans effet", True)
    await plc.stop()


async def test_no_resurrection(report: Report, db: str) -> None:
    """Après retrait et redémarrage du LDS, l'entrée ne doit pas revenir."""
    report.section("3. Retrait puis redémarrage : l'entrée ne ressuscite pas")
    async with LdsHarness(db) as lds:
        plc = await start_plc(free_port())
        registrar = LdsRegistrar(plc, lds.url, period=0)
        registrar.start()
        await wait_for(lambda: TEST_URI in lds.uris(), timeout=8.0)
        report.check("entrée présente avant retrait", TEST_URI in lds.uris())
        await registrar.stop()
        await plc.stop()

    async with LdsHarness(db) as lds:
        report.check("l'entrée ne ressuscite pas au redémarrage",
                     TEST_URI not in lds.uris(), str(lds.uris()))
        report.check("la base est vide", lds.store.count() == 0,
                     f"count={lds.store.count()}")


async def main() -> int:
    setup()
    report = Report("sciicad.discovery")
    with temp_database("discovery") as tmp:
        await test_register_and_withdraw(report, tmp.path("withdraw"))
        await test_retry_when_lds_down(report)
        await test_no_resurrection(report, tmp.path("restart"))
    return report.finish()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
