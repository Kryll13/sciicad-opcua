"""Cycle d'arrêt des simulateurs PLC.

Un simulateur expose une boucle infinie de simulation : sans aménagement,
un signal d'arrêt ne serait jamais observé et la tâche ne pourrait pas être
annulée. Ces utilitaires rendent la boucle annulable et garantissent que le
retrait du LDS a lieu même en cas d'exception.
"""

from __future__ import annotations

import asyncio
import signal
from typing import Awaitable

from loguru import logger

from .discovery import REGISTER_PERIOD, LdsRegistrar


def install_signal_handlers(stopped: asyncio.Event) -> None:
    """Branche SIGINT et SIGTERM sur l'événement d'arrêt.

    SIGTERM est traité explicitement car c'est ce qu'envoient systemd et
    docker stop. Sans ce branchement, le processus meurt sans exécuter son
    bloc de finalisation et laisse une entrée morte dans le LDS.
    """
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stopped.set)
        except NotImplementedError:
            # Plateforme sans add_signal_handler (Windows) : Ctrl+C reste
            # géré par asyncio.run via KeyboardInterrupt.
            pass


async def run_until_stopped(
    simulation: Awaitable[None],
    stopped: asyncio.Event,
    label: str = "simulation",
) -> None:
    """Lance la simulation et attend l'arrêt, puis annule la simulation.

    La simulation est placée dans une tâche séparée pour pouvoir l'annuler
    dès que l'événement est signalé. Sans cela, une boucle infinie ignorerait
    le signal et le serveur ne s'arrêterait pas.
    """
    task = asyncio.create_task(simulation, name=label)
    waiter = asyncio.create_task(stopped.wait(), name="shutdown-waiter")
    try:
        await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for pending in (task, waiter):
            if not pending.done():
                pending.cancel()
        logger.info(f"{label.capitalize()} arrêtée.")


async def withdraw_from_lds(registrar: LdsRegistrar) -> None:
    """Retire le serveur du LDS à l'arrêt, sans jamais faire échouer l'arrêt.

    Le retrait est un accélérateur : en cas d'échec, le LDS évacue l'entrée
    par expiration. Un arrêt ne doit donc jamais être bloqué ou interrompu par
    l'indisponibilité du LDS.
    """
    logger.info("Signalement de l'arrêt au LDS...")
    try:
        if await registrar.stop():
            logger.info("Entrée retirée de la découverte.")
        elif registrar.registered:
            logger.warning("Le LDS n'a pas acquitté le retrait ; l'entrée expirera par TTL.")
        else:
            logger.info("Le serveur n'était pas enregistré : rien à retirer.")
    except Exception as exc:
        logger.warning(
            f"Retrait du LDS impossible ({type(exc).__name__}: {exc}) : "
            f"l'entrée expirera par TTL (~{REGISTER_PERIOD * 5} s)."
        )
