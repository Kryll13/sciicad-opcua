"""Banc de test partagé par les auto-tests du projet.

Les scripts ``tools/selftest_*.py`` construisaient chacun leur serveur de
découverte, leurs propres assertions et leur propre rapport. Ce module
factorise ces éléments : il ne reste dans les scripts que le scénario propre
à chacun.
"""

from __future__ import annotations

import asyncio
import inspect
import socket
import tempfile
from pathlib import Path
from typing import Optional

from lds.config import DiscoveryConfig, LDSConfig, ServerConfig
from lds.server import LocalDiscoveryServer

from .console import logger


class Report:
    """Accumule les vérifications d'un auto-test et décide du code de sortie."""

    def __init__(self, title: str = "Auto-test") -> None:
        self.title = title
        self.failures: list[str] = []
        self.checks = 0

    def check(self, label: str, condition: bool, detail: str = "") -> bool:
        """Journalise une vérification. Retourne sa valeur."""
        self.checks += 1
        marker = "OK   " if condition else "ECHEC"
        logger.info(f"  [{marker}] {label}{f' -- {detail}' if detail else ''}")
        if not condition:
            self.failures.append(label)
        return condition

    def section(self, title: str) -> None:
        logger.info(f"\n{title}")

    def finish(self) -> int:
        """Affiche le bilan et retourne le code de sortie."""
        logger.info("")
        if self.failures:
            logger.info(
                f"{len(self.failures)} vérification(s) en échec sur {self.checks} :"
            )
            for name in self.failures:
                logger.info(f"  - {name}")
            return 1
        logger.info(f"Toutes les vérifications sont passées ({self.checks}).")
        return 0


def free_port() -> int:
    """Réserve un port TCP libre sur la boucle locale."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class LdsHarness:
    """Serveur de découverte éphémère, pour les auto-tests.

    N'écoute que sur la boucle locale et utilise un fichier SQLite
    temporaire : aucun auto-test ne touche un LDS de production ni ne laisse
    d'artefact derrière lui.
    """

    def __init__(
        self,
        db_path: str,
        application_uri: str = "urn:SCIICAD:lds-selftest",
        ttl_seconds: int = 300,
        sweep_interval: int = 3600,
    ) -> None:
        self.config = LDSConfig(
            server=ServerConfig(
                bind_address="127.0.0.1",
                port=free_port(),
                advertise_host="127.0.0.1",
                application_uri=application_uri,
            ),
            discovery=DiscoveryConfig(
                entry_ttl_seconds=ttl_seconds,
                sweep_interval_seconds=sweep_interval,
            ),
            database={"enabled": True, "path": db_path, "event_log": True},
        )
        self.server: Optional[LocalDiscoveryServer] = None

    @property
    def url(self) -> str:
        return self.config.server.endpoint_url

    @property
    def registry(self):
        assert self.server is not None and self.server.registry is not None
        return self.server.registry

    @property
    def store(self):
        assert self.server is not None
        return self.server.store

    async def __aenter__(self) -> "LdsHarness":
        self.server = LocalDiscoveryServer(self.config)
        await self.server.start()
        return self

    async def __aexit__(self, *exc_info) -> None:
        if self.server is not None:
            await self.server.stop()

    def uris(self) -> list[str]:
        """URI d'application actuellement enregistrées, en mémoire."""
        return self.registry.application_uris()


class temp_database:
    """Répertoire temporaire pour la base d'un auto-test."""

    def __init__(self, name: str = "selftest") -> None:
        self._dir = tempfile.TemporaryDirectory(prefix=f"sciicad-{name}-")
        self._counter = 0

    def path(self, suffix: str = "") -> str:
        """Chemin d'un fichier de base neuf dans le répertoire temporaire."""
        self._counter += 1
        stem = f"db{self._counter}" if not suffix else suffix
        return str(Path(self._dir.name) / f"{stem}.db")

    def __enter__(self) -> "temp_database":
        return self

    def __exit__(self, *exc_info) -> None:
        self._dir.cleanup()


async def wait_for(predicate, timeout: float = 10.0, interval: float = 0.25) -> bool:
    """Attend qu'un prédicat devienne vrai, sans bloquer la boucle d'événements.

    ``predicate`` peut être synchrone ou coroutine. Les pauses sont des
    ``await`` : indispensable dans un auto-test où un serveur de découverte
    tourne dans la même boucle et doit pouvoir répondre.

    L'itération finale est évaluée après la boucle, de sorte qu'un prédicat
    déjà vrai au dernier tour ne soit pas manqué.
    """
    async def evaluate() -> bool:
        result = predicate()
        if inspect.isawaitable(result):
            result = await result
        return bool(result)

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if await evaluate():
            return True
        await asyncio.sleep(interval)
    return await evaluate()
