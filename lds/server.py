"""Assemblage et cycle de vie d'un serveur de découverte OPC UA.

La Part 12 définit deux rôles pour un même câblage de services : le LDS
(portée ``local``, inscriptions expirantes) et le GDS (portée ``global``,
inscriptions conservées jusqu'à retrait explicite). :class:`DiscoveryServer`
implémente le commun ; chaque rôle n'est qu'une configuration.
"""

from __future__ import annotations

import asyncio
import signal
import weakref
from datetime import datetime
from typing import Optional

from asyncua import Server, ua
from loguru import logger

from . import services
from sciicad.identity import set_application_identity

from .config import LDSConfig
from .registry import ServerRegistry
from .store import ServerStore


#: Serveurs de découverte actifs dans le processus. Les patchs d'asyncua
#: installés par :mod:`lds.services` sont globaux au processus, ils ne peuvent
#: donc être retirés que lorsque le dernier serveur s'en est servi. Sans ce
#: décompte, un arrêt précoce laisserait les services désinstallés alors
#: qu'un autre serveur de découverte tourne encore.
_live_servers: "weakref.WeakSet[DiscoveryServer]" = weakref.WeakSet()


class DiscoveryServer:
    """Serveur de découverte conforme OPC UA Part 4, persistant et expirant.

    Le rôle est déterminé par ``config.discovery.scope`` :

    * ``local``  — LDS : une inscription non renouvelée est évacuée après
      ``entry_ttl_seconds`` (300 s par défaut).
    * ``global`` — GDS : aucune inscription n'expire, et le registre restauré
      fait foi au redémarrage.

    Écoute sur ``server.bind_address`` (par défaut toutes les interfaces) et
    annonce ``server.endpoint_url``, chemin compris. Les deux sont découplés :
    l'écoute sur ``0.0.0.0`` évite les échecs « could not bind on any address »
    lorsque la résolution DNS du hostname ne correspond à aucune interface
    locale, tandis que l'hôte annoncé doit rester joignable par les clients.
    """

    def __init__(self, config: Optional[LDSConfig] = None) -> None:
        self.config = config or LDSConfig()
        self.server: Optional[Server] = None
        self.store: Optional[ServerStore] = None
        self.registry: Optional[ServerRegistry] = None
        self._sweeper: Optional[asyncio.Task] = None
        self._stopped = asyncio.Event()
        self._closed = False

    def __del__(self) -> None:  # pragma: no cover - ramasse-miettes
        # Un objet abandonné sans stop() ne doit pas laisser son serveur dans
        # _live_servers, ce qui empêcherait tout désinstallement ultérieur des
        # patchs d'asyncua.
        _live_servers.discard(self)

    @property
    def role(self) -> str:
        """Nom du rôle, pour les messages de journal."""
        return "GDS" if self.scope == "global" else "LDS"

    @property
    def scope(self) -> str:
        return self.config.discovery.scope

    # -- construction -------------------------------------------------------

    async def setup(self) -> None:
        """Construit le serveur, le registre et le store. Ne démarre rien."""
        cfg = self.config
        server_cfg = cfg.server

        # Un arrêt suivi d'un redémarrage sur le même objet est possible (le
        # test de redémarrage du GDS le fait) : la garde doit être remise à
        # zéro, sans quoi le second start() serait ignoré.
        self._closed = False

        self.server = Server()
        await self.server.init()

        # Écoute large, annonce ciblée : voir _get_bind_socket_info() dans
        # asyncua/server/server.py, qui privilégie socket_address. C'est ce qui
        # permet à un GDS d'écouter sur 0.0.0.0:4840 tout en annonçant
        # opc.tcp://<hôte>:4840/GlobalDiscoveryServer.
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
            scope=cfg.discovery.scope,
        )

        services.install(enable_find_servers_on_network=cfg.discovery.find_servers_on_network)
        _live_servers.add(self)

    # -- cycle de vie -------------------------------------------------------

    async def start(self) -> None:
        """Démarre l'écoute, restaure le registre et lance le balayage."""
        if self.server is None or self.registry is None:
            await self.setup()

        assert self.server is not None and self.registry is not None

        await self.server.start()
        logger.success(
            f"{self.role} démarré : écoute {self.config.server.bind_address}:"
            f"{self.config.server.port}, annonce {self.config.server.endpoint_url}"
        )

        restored = await self.registry.restore()
        if self.store is not None:
            logger.info(
                f"Registre : {len(self.registry.application_uris())} entrée(s) en mémoire, "
                f"{self.store.count()} en base, {restored} restaurée(s)"
            )

        if self.scope == "global":
            # Rien n'expire en portée globale : une tâche de balayage ne ferait
            # que boucler pour rien.
            logger.info(
                "Portée globale : les inscriptions sont conservées jusqu'à "
                "retrait explicite, aucun renouvellement n'est imposé"
            )
            return

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
        """Arrête le serveur et ferme le store.

        Idempotent : ``run_forever`` passe par ce chemin dans un ``finally``,
        et un appelant peut aussi appeler ``stop()`` explicitement après un
        ``start()`` raté. Sans cette garde, le store serait fermé deux fois.
        Les références sont vidées après usage, jamais avant : les mettre à
        ``None`` en entrée ferait perdre la seule chance d'arrêter le serveur.
        """
        if self._closed:
            return
        self._closed = True
        self._stopped.set()

        if self._sweeper is not None:
            self._sweeper.cancel()
            try:
                await self._sweeper
            except (asyncio.CancelledError, Exception):
                pass
            self._sweeper = None

        if self.server is not None:
            server, self.server = self.server, None
            try:
                await server.stop()
            except Exception as exc:
                logger.warning(f"Arrêt du serveur {self.role} imparfait : {exc}")

        if self.store is not None:
            store, self.store = self.store, None
            store.close()
            logger.info("Store du registre fermé")

        # Les patchs sont globaux au processus : ne les retirer que si aucun
        # autre serveur de découverte n'est actif. Deux serveurs dans un même
        # test scénario (redémarrage du GDS) en dépendent simultanément.
        _live_servers.discard(self)
        if not _live_servers:
            services.uninstall()
        logger.info(f"{self.role} arrêté")

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


class LocalDiscoveryServer(DiscoveryServer):
    """LDS : serveur de découverte local, entrées expirantes.

    Rôle « local » : une inscription doit être renouvelée, faute de quoi
    l'entrée est évacuée après le TTL.
    """
