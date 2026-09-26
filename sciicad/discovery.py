"""Enregistrement et retrait d'un serveur auprès d'un LDS.

La norme OPC UA ne définit **aucun** service de désenregistrement :
``UnregisterServer`` n'existe pas. La clause 5.5.5.1 (Discovery Service Set)
prescrit la procédure suivante :

* le serveur s'enregistre périodiquement, au plus tard toutes les 10 minutes ;
* si l'enregistrement échoue, il retente en doublant l'intervalle à chaque
  essai jusqu'à la période nominale ;
* lorsqu'il s'arrête proprement, il s'enregistre **une dernière fois** avec
  ``IsOnline = False`` pour signaler qu'il passe hors ligne.

``asyncua`` ne permet pas ce dernier appel : ``Client.register_server`` force
``serv.IsOnline = True`` (client/client.py), et
``Server.unregister_from_discovery`` émet un ``UnregisterServer`` que le
serveur d'asyncua ne route pas. On construit donc le ``RegisteredServer``
explicitement.

En cas d'échec du retrait, l'entrée reste visible jusqu'à ce que le LDS
l'évince par expiration. Le retrait est un accélérateur, jamais une garantie.
"""

from __future__ import annotations

import asyncio
from typing import Optional

from asyncua import Client, Server, ua
from loguru import logger

# Période de renouvellement. La norme plafonne à 600 s ; 60 s laisse une marge
# confortable et reste très en deçà du TTL d'expiration du LDS (300 s par
# défaut côté SCIICAD), qui doit être plus long que cette période.
REGISTER_PERIOD = 60

# Délai maximum entre deux tentatives après un échec. La spec 5.5.5.1 demande
# un doublement progressif jusqu'à la période nominale.
RETRY_MAX = REGISTER_PERIOD


class LdsRegistrar:
    """Gère l'enregistrement périodique et le retrait d'un serveur."""

    def __init__(
        self,
        server: Server,
        lds_url: str,
        period: int = REGISTER_PERIOD,
    ) -> None:
        self.server = server
        self.lds_url = lds_url
        self.period = period
        self._task: Optional[asyncio.Task] = None
        self._registered = False
        self._stopped = False

    @property
    def registered(self) -> bool:
        return self._registered

    def start(self) -> asyncio.Task:
        """Lance la boucle d'enregistrement en tâche de fond.

        Ne lève pas si le LDS est indisponible : l'échec initial est traité
        par la même boucle de retry que les suivants. L'enregistrement doit
        intervenir après la mise en écoute du serveur, jamais avant, sinon un
        échec de bind laisse une entrée morte dans le LDS.
        """
        self._task = asyncio.create_task(self._loop(), name="lds-registration")
        return self._task

    async def stop(self) -> bool:
        """Signale l'arrêt au LDS. Retourne True si le retrait est acquitté.

        À appeler après l'arrêt du serveur OPC UA : l'entrée doit disparaître
        de la découverte, pas seulement cesser d'être renewée.
        """
        self._stopped = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

        if not self._registered:
            return False
        return await self._send(online=False)

    async def _loop(self) -> None:
        """Enregistre, puis réenregistre périodiquement avec retry progressif.

        Une période nulle ou négative signifie « enregistrer une seule fois,
        sans renouvellement » (convention de ``register_to_discovery``). Sans
        ce cas particulier, une période nulle produirait une boucle sans
        temporisation, saturant le processeur et le réseau.
        """
        once = self.period <= 0
        delay = 1.0
        while not self._stopped:
            try:
                await self._send(online=True)
                self._registered = True
                if once:
                    return
                delay = float(self.period)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._registered = False
                logger.warning(
                    f"LDS indisponible ({self.lds_url}) : nouvel essai dans {delay:.0f}s ({exc})"
                )
                # Spec 5.5.5.1 : double l'intervalle jusqu'à la période nominale.
                delay = min(delay * 2, float(RETRY_MAX))
            try:
                await asyncio.wait_for(self._stopped_wait(), timeout=delay)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise

    async def _stopped_wait(self) -> None:
        """Attente interruptible : se termine dès que l'arrêt est demandé."""
        while not self._stopped:
            await asyncio.sleep(0.5)

    async def _send(self, online: bool) -> bool:
        """Envoie un RegisterServer avec le drapeau ``IsOnline`` voulu."""
        client = Client(self.lds_url)
        await client.connect_sessionless()
        try:
            registered = ua.RegisteredServer()
            registered.ServerUri = self.server.get_application_uri()
            registered.ProductUri = self.server.product_uri or ""
            registered.DiscoveryUrls = [self.server.endpoint.geturl()]
            registered.ServerType = self.server.application_type
            registered.ServerNames = [ua.LocalizedText("en", self.server.name)]
            registered.IsOnline = online
            await client.uaclient.register_server(registered)
        finally:
            await client.disconnect_sessionless()

        if online:
            logger.info(f"Enregistré auprès du LDS : {registered.ServerUri} -> {self.lds_url}")
        else:
            logger.info(f"Retrait du LDS acquitté : {registered.ServerUri} était hors ligne")
        return True
