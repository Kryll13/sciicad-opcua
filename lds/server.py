"""Assemblage et cycle de vie du Local Discovery Server."""

from __future__ import annotations

import asyncio
import signal
from datetime import datetime
from typing import Optional

from asyncua import Server, ua
from loguru import logger

from . import services
from sciicad.identity import set_application_identity

from .config import LDSConfig
from .registry import ServerRegistry
from .store import ServerStore


class LocalDiscoveryServer:
    """LDS conforme OPC UA Part 4, avec persistance et expiration.

    Écoute sur ``server.bind_address`` (par défaut toutes les interfaces) et
    annonce ``server.endpoint_url``. Les deux sont découplés : l'écoute sur
    ``0.0.0.0`` évite les échecs « could not bind on any address » lorsque la
    résolution DNS du hostname ne correspond à aucune interface locale, tandis
    que l'hôte annoncé doit rester joignable par les clients.
    """

    def __init__(self, config: Optional[LDSConfig] = None) -> None:
        self.config = config or LDSConfig()
        self.server: Optional[Server] = None
        self.store: Optional[ServerStore] = None
        self.registry: Optional[ServerRegistry] = None
        self._sweeper: Optional[asyncio.Task] = None
        self._stopped = asyncio.Event()

    # -- construction -------------------------------------------------------

    async def setup(self) -> None:
        """Construit le serveur, le registre et le store. Ne démarre rien."""
        cfg = self.config
        server_cfg = cfg.server

        self.server = Server()
        await self.server.init()

        # Écoute large, annonce ciblée : voir _get_bind_socket_info() dans
        # asyncua/server/server.py, qui privilégie socket_address.
        self.server.socket_address = (server_cfg.bind_address, server_cfg.port)
        self.server.set_endpoint(server_cfg.endpoint_url)
        self.server.set_server_name(server_cfg.application_name)
        self.server.manufacturer_name = server_cfg.manufacturer_name
        # Passe par le module partagé : set_application_uri seul laisse
        # ServerArray sur l'URI par défaut d'asyncua.
        await set_application_identity(
            self.server,
            server_cfg.application_uri,
            product_uri=server_cfg.product_uri,
            server_name=server_cfg.application_name,
        )
        await self.server.set_build_info(
            server_cfg.product_uri,
            server_cfg.manufacturer_name,
            server_cfg.application_name,
            server_cfg.software_version,
            "1",
            datetime.now(),
        )

        # Un serveur de découverte ne doit exposer que NoSecurity : la
        # découverte précède l'établissement d'un canal sécurisé.
        self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        self.server.discovery_server_flag = True

        if cfg.database.enabled:
            self.store = ServerStore(cfg.database.path, event_log=cfg.database.event_log)
        else:
            logger.warning("Persistance désactivée : le registre sera perdu au redémarrage")

        self.registry = ServerRegistry(
            self.server.iserver,
            store=self.store,
            ttl_seconds=cfg.discovery.entry_ttl_seconds,
        )

        services.install(enable_find_servers_on_network=cfg.discovery.find_servers_on_network)

    # -- cycle de vie -------------------------------------------------------

    async def start(self) -> None:
        """Démarre l'écoute, restaure le registre et lance le balayage."""
        if self.server is None or self.registry is None:
            await self.setup()

        assert self.server is not None and self.registry is not None

        await self.server.start()
        logger.success(
            f"LDS démarré : écoute {self.config.server.bind_address}:"
            f"{self.config.server.port}, annonce {self.config.server.endpoint_url}"
        )

        restored = await self.registry.restore()
        if self.store is not None:
            logger.info(
                f"Registre : {len(self.registry.application_uris())} entrée(s) en mémoire, "
                f"{self.store.count()} en base, {restored} restaurée(s)"
            )

        interval = self.config.discovery.sweep_interval_seconds
        self._sweeper = asyncio.create_task(self._sweep_loop(interval))
        logger.info(
            f"Expiration active : TTL {self.config.discovery.entry_ttl_seconds}s, "
            f"balayage toutes les {interval}s"
        )

    async def _sweep_loop(self, interval: float) -> None:
        """Évacue périodiquement les entrées dont le renouvellement a cessé."""
        while not self._stopped.is_set():
            try:
                await asyncio.wait_for(self._stopped.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
            else:
                return
            try:
                await self.registry.sweep()  # type: ignore[union-attr]
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.error(f"Balayage du registre échoué : {exc}")

    async def stop(self) -> None:
        """Arrête le serveur et ferme le store."""
        self._stopped.set()

        if self._sweeper is not None:
            self._sweeper.cancel()
            try:
                await self._sweeper
            except (asyncio.CancelledError, Exception):
                pass
            self._sweeper = None

        if self.server is not None:
            try:
                await self.server.stop()
            except Exception as exc:
                logger.warning(f"Arrêt du serveur LDS imparfait : {exc}")

        if self.store is not None:
            self.store.close()
            logger.info("Store du registre fermé")

        services.uninstall()
        logger.info("LDS arrêté")

    async def run_forever(self) -> None:
        """Démarre puis attend l'arrêt (Ctrl+C ou signal)."""
        await self.start()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._stopped.set)
            except NotImplementedError:  # plateforme sans add_signal_handler
                pass

        try:
            await self._stopped.wait()
        finally:
            await self.stop()
