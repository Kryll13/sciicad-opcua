#!/usr/bin/env python3
"""
Auto-test de bout en bout du cycle de vie d'un PLC face à un LDS.

Lance le simulateur dans un **vrai sous-processus**, vérifie son
enregistrement, envoie ``SIGTERM`` (ce qu'enverront systemd et
``docker stop``) et contrôle que son entrée disparaît de la découverte
immédiatement, sans attendre l'expiration.

C'est le seul test qui valide le câblage complet : installation des signaux,
annulation de la boucle de simulation, retrait du LDS.

    uv run tools/selftest_thermo_lifecycle.py               # les deux PLC
    uv run tools/selftest_thermo_lifecycle.py protect-plc   # un seul
"""

import asyncio
import signal
import subprocess
import sys
import time
from pathlib import Path

from asyncua import Client

from sciicad.console import setup
from sciicad.selftest import LdsHarness, Report, temp_database, wait_for

ROOT = Path(__file__).resolve().parent.parent

# (dossier, applicationUri attendue dans le registre du LDS)
PLCS = {
    "thermo-plc": "urn:SCIICAD:thermo-plc",
    "protect-plc": "urn:SCIICAD:protect-plc",
}

TIMEOUT_START = 45.0
TIMEOUT_STOP = 25.0


async def registry_uris(url: str) -> list[str]:
    """URI d'application enregistrées selon le LDS, via un appel sans session."""
    client = Client(url)
    try:
        await client.connect_sessionless()
        return [s.ApplicationUri for s in await client.find_servers()]
    finally:
        try:
            await client.disconnect_sessionless()
        except Exception:
            pass


def _tail(proc, lines: int = 12) -> str:
    """Extrait les dernières lignes utiles du journal du sous-processus."""
    try:
        output = proc.stdout.read() if proc.stdout else ""
    except Exception:
        return "(journal indisponible)"
    keep = [
        line for line in (output or "").splitlines()
        if any(m in line for m in ("LDS", "erreur", "Error", "Traceback", "arrêt"))
    ]
    return " | ".join(keep[-lines:]) or (output or "")[-400:]


def main() -> int:
    setup()
    targets = sys.argv[1:] or list(PLCS)
    unknown = [t for t in targets if t not in PLCS]
    if unknown:
        print(f"Composant(s) inconnu(s) : {', '.join(unknown)}")
        print(f"Choix possibles : {', '.join(PLCS)}")
        return 2

    # Un rapport unique pour tous les simulateurs : le bilan couvre ainsi
    # l'ensemble de la campagne, et pas seulement le dernier composant.
    report = Report("cycle de vie")
    for target in targets:
        with temp_database(f"lifecycle-{target}") as tmp:
            asyncio.run(_run(target, PLCS[target], report, tmp.path("lds.db")))
    return report.finish()


async def _run(plc_dir: str, plc_uri: str, report: Report, db: str) -> None:
    """Corps du test, avec la base fournie."""
    report.section(f"Cycle de vie réel de {plc_dir} avec SIGTERM")
    proc = None
    async with LdsHarness(db) as lds:
        try:
            proc = subprocess.Popen(
                [
                    sys.executable, str(ROOT / plc_dir / "plc_server.py"),
                    "--lds", lds.url, "--advertise", "127.0.0.1", "--bind", "127.0.0.1",
                ],
                cwd=str(ROOT),
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            )

            registered = False
            deadline = time.monotonic() + TIMEOUT_START
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    report.check("le simulateur démarre", False, "arrêt prématuré")
                    report.info("journal", _tail(proc))
                    return
                try:
                    if plc_uri in await registry_uris(lds.url):
                        registered = True
                        break
                except Exception:
                    pass
                await asyncio.sleep(1.0)

            if not report.check("le simulateur est enregistré", registered,
                                str(lds.uris())):
                report.info("journal", _tail(proc))
                return

            proc.send_signal(signal.SIGTERM)
            deadline = time.monotonic() + TIMEOUT_STOP
            while proc.poll() is None and time.monotonic() < deadline:
                await asyncio.sleep(0.2)

            if not report.check("le simulateur s'arrête sur SIGTERM",
                                proc.poll() is not None, f"code={proc.returncode}"):
                return

            gone = await wait_for(lambda: plc_uri not in lds.uris(), timeout=8.0)
            report.check("l'entrée disparaît immédiatement (sans TTL)", gone,
                         str(lds.uris()))
            report.check("le LDS reste opérationnel",
                         lds.config.server.application_uri in lds.uris(),
                         str(lds.uris()))
            report.check("la base ne contient plus le simulateur",
                         lds.store.count() == 0, f"count={lds.store.count()}")
        finally:
            if proc is not None and proc.poll() is None:
                proc.kill()
                await asyncio.to_thread(proc.wait, 10)


if __name__ == "__main__":
    sys.exit(main())
