"""Configuration de la sortie console : un seul point d'entrée pour loguru.

Deux règles, et pas une seule règle
----------------------------------

**Toute la verbosité passe par loguru.** Aucun ``print`` dans les serveurs ni
dans les simulateurs : la sortie standard est réservée à l'affichage interactif
d'un client, où un humain lit un écran. Un serveur qui écrit deux flux — un
journal et des lignes brutes — oblige l'administrateur à deux outils pour lire
une seule vie du processus.

**Le format dépend de qui lit.** Un outil de diagnostic produit un rapport, et
l'horodatage de chaque ligne y est du bruit. Un serveur produit une trace, et
l'exploitant a besoin du *quand* et du *gravité*. Réutiliser le format nu des
outils pour un serveur ferait perdre l'information la plus utile en cas
d'incident ; appliquer un format horodaté aux outils noierait leur rapport.
D'où deux fonctions, et non une avec un paramètre de plus.

``reduce_noise`` fait partie de la règle : sans elle, les ``WARNING`` d'asyncua
sur l'absence de certificat noieraient la sortie d'un simulateur en
``NoSecurity``, et l'avertissement réel deviendrait introuvable.
"""

from __future__ import annotations

import logging
import sys

from loguru import logger

# Bibliothèques dont les logs noieraient la sortie d'un outil de diagnostic.
_NOISY_LOGGERS = ("asyncua", "asyncio", "urllib3")

#: Niveaux acceptés pour ``--log-level``, du plus bavard au plus discret.
LOG_LEVELS = ("TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

#: Format d'un serveur : l'heure et la gravité sont le sens de la ligne.
_SERVER_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - "
    "<level>{message}</level>"
)


def check_level(value: str) -> str:
    """Valide un niveau de journalisation, insensible à la casse."""
    upper = value.strip().upper()
    if upper not in LOG_LEVELS:
        raise ValueError(
            f"niveau inconnu : {value!r} (attendu : {', '.join(LOG_LEVELS)})"
        )
    return upper


def setup(level: str = "INFO", quiet_third_party: bool = True) -> None:
    """Configure la sortie d'un **outil** de diagnostic.

    Message seul, sans horodatage : l'outil affiche un rapport, pas une trace.
    Pour un serveur, voir :func:`setup_server`.
    """
    logger.remove()
    logger.add(sys.stderr, level=check_level(level), format="{message}")
    if quiet_third_party:
        reduce_noise()


def setup_server(level: str = "INFO", quiet_third_party: bool = True) -> None:
    """Configure la sortie d'un **serveur** ou d'un **simulateur**.

    Ligne horodatée et gradée : c'est ce que l'on lit après coup, dans un
    fichier, pour dater un incident. Le même réglage à trois reprises dans les
    points d'entrée aurait divergé le jour où l'un d'eux aurait oublié
    ``reduce_noise``.
    """
    logger.remove()
    logger.add(sys.stderr, level=check_level(level), format=_SERVER_FORMAT)
    if quiet_third_party:
        reduce_noise()


def reduce_noise(level: int = logging.ERROR) -> None:
    """Réduit au minimum les logs des bibliothèques tierces.

    ``ERROR`` par défaut : les avertissements d'asyncua sur l'absence de
    certificat (« No signing policy available »…) sont sans effet dans un
    simulateur fonctionnant en NoSecurity, et noieraient la sortie.

    C'est aussi ce qui fait de loguru la seule voie : asyncua journalise via le
    module ``logging`` de la bibliothèque standard, et ce sont ces
    ``getLogger().setLevel()`` qui font transiter ses messages par loguru. Les
    retirer ferait revenir la pile vers ``logging`` et le serveur émettrait
    alors deux flux, de deux formats.
    """
    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(level)


def banner(text: str, width: int = 60, char: str = "=") -> str:
    """Retourne un titre encadré, aligné sur une largeur donnée.

    Ne journalise rien : c'est un texte, que l'appelant décide d'afficher ou de
    journaliser.
    """
    return f" {text} ".center(width, char)


__all__ = [
    "logger",
    "setup",
    "setup_server",
    "reduce_noise",
    "check_level",
    "banner",
    "LOG_LEVELS",
]
