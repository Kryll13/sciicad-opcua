"""Configuration de la sortie console des outils en ligne de commande.

Les outils du projet ``tools/`` utilisaient tantôt ``print``, tantôt la
bibliothèque standard, tantôt loguru — et ``opcua_utils`` configurait la
racine des logs **au moment de l'import**, ce qui Leakait dans tout le
processus dès qu'un outil en importait un autre.

Ce module centralise la mise en place : un seul point d'entrée, un seul
format, et le bruit des bibliothèques tierces réduit par défaut.
"""

from __future__ import annotations

import logging
import sys

from loguru import logger

# Bibliothèques dont les logs noieraient la sortie d'un outil de diagnostic.
_NOISY_LOGGERS = ("asyncua", "asyncio", "urllib3")


def setup(level: str = "INFO", quiet_third_party: bool = True) -> None:
    """Configure la sortie console pour un outil en ligne de commande.

    Met en place loguru sur la sortie d'erreur, au format ``message`` seul :
    les outils affichent des rapports lisibles, pas des traces horodatées.
    """
    logger.remove()
    logger.add(sys.stderr, level=level, format="{message}")
    if quiet_third_party:
        reduce_noise()


def reduce_noise(level: int = logging.ERROR) -> None:
    """Réduit au minimum les logs des bibliothèques tierces.

    ``ERROR`` par défaut : les avertissements d'asyncua sur l'absence de
    certificat (« No signing policy available »…) sont sans effet dans un
    simulateur fonctionnant en NoSecurity, et noieraient la sortie.
    """
    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(level)


def banner(text: str, width: int = 60, char: str = "=") -> str:
    """Retourne un titre encadré, aligné sur une largeur donnée."""
    return f" {text} ".center(width, char)


__all__ = ["logger", "setup", "reduce_noise", "banner"]
