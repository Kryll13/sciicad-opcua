"""Services de découverte OPC UA absents du serveur asyncua.

asyncua route ``FindServers``, ``GetEndpoints``, ``RegisterServer`` et
``RegisterServer2``, mais pas ``FindServersOnNetwork`` : le type et la
réponse existent pourtant déjà dans la bibliothèque
(``ua.FindServersOnNetworkResponse`` porte bien le TypeId 12209). Il manque
uniquement la branche de dispatch.

Point d'extension : le routage des requêtes est un ``if/elif`` codé en dur
dans ``UaProcessor._process_message``, et ``UaProcessor`` est instancié sans
possibilité d'injection depuis ``OPCUAServer._make_protocol``. Il n'existe donc
aucun point d'accroche public. La seule approche supportée consiste à
envelopper la méthode : les services déjà gérés sont délégués à
l'implémentation d'origine, seul ``FindServersOnNetwork`` est traité ici.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from asyncua import ua
from asyncua.server.internal_server import InternalServer
from asyncua.server.uaprocessor import UaProcessor
from asyncua.ua.ua_binary import struct_from_binary
from loguru import logger

# NodeId d'encodage binaire, OPC UA Part 6 (valeur normative).
FIND_SERVERS_ON_NETWORK_REQUEST = ua.NodeId(12208)

# Etat d'installation des patchs, pour rester idempotent.
_installed = False
_original_process_message: Optional[Callable] = None
_original_register_server: Optional[Callable] = None


def is_installed() -> bool:
    """Indique si les patchs sont actifs."""
    return _installed


def install(enable_find_servers_on_network: bool = True) -> None:
    """Installe les patchs. Idempotent.

    A appeler une fois, apres la creation du serveur et avant son demarrage.
    """
    global _installed, _original_process_message, _original_register_server

    if _installed:
        return

    _original_process_message = UaProcessor._process_message
    UaProcessor._process_message = _make_process_message(
        _original_process_message, enable_find_servers_on_network
    )

    _original_register_server = InternalServer.register_server
    InternalServer.register_server = _make_register_server(_original_register_server)

    _installed = True
    logger.info("Extensions LDS installees (persistance + FindServersOnNetwork)")


def uninstall() -> None:
    """Retire les patchs. Utilise par les tests."""
    global _installed, _original_process_message, _original_register_server

    if not _installed:
        return

    if _original_process_message is not None:
        UaProcessor._process_message = _original_process_message
    if _original_register_server is not None:
        InternalServer.register_server = _original_register_server

    _original_process_message = None
    _original_register_server = None
    _installed = False


# ---------------------------------------------------------------------------
# Persistance des enregistrements
# ---------------------------------------------------------------------------


def _make_register_server(original: Callable) -> Callable:
    def register_server(
        self: InternalServer, server: ua.RegisteredServer, conf: Any = None
    ) -> None:
        original(self, server, conf)
        registry = getattr(self, "_sciicad_registry", None)
        if registry is not None:
            # Volontairement synchrone : une ecriture SQLite de registre dure
            # moins d'une milliseconde et n'a lieu qu'a chaque renouvellement
            # (une fois par minute et par serveur). Une tache fire-and-forget
            # serait en revanche susceptible de perdre l'ecriture si le
            # processus s'arrete juste apres, ou de masquer une exception.
            registry.register(server, conf)

    return register_server


# ---------------------------------------------------------------------------
# FindServersOnNetwork
# ---------------------------------------------------------------------------


def _make_process_message(original: Callable, enabled: bool) -> Callable:
    async def _process_message(
        self: UaProcessor,
        typeid: ua.NodeId,
        requesthdr: ua.RequestHeader,
        seqhdr: Any,
        body: bytes,
    ) -> bool:
        if enabled and typeid == FIND_SERVERS_ON_NETWORK_REQUEST:
            return await _handle_find_servers_on_network(self, requesthdr, seqhdr, body)
        return await original(self, typeid, requesthdr, seqhdr, body)

    return _process_message


async def _handle_find_servers_on_network(
    self: UaProcessor, requesthdr: ua.RequestHeader, seqhdr: Any, body: bytes
) -> bool:
    """Traite une requete FindServersOnNetwork.

    Defi d'asyncua : les services autres que ceux de decouverte exigent une
    session active, alors que la decouverte fonctionne sans. Cette methode est
    donc branchee avant le controle de session.
    """
    registry = getattr(self.iserver, "_sciicad_registry", None)

    try:
        params = struct_from_binary(ua.FindServersOnNetworkParameters, body)
        starting = _as_int(getattr(params, "StartingRecordId", 0))
        maximum = _as_int(getattr(params, "MaxRecordsToReturn", 0))
        # Le champ est une liste cote asyncua : on la transmet telle quelle,
        # le registre sait la normaliser. Ne surtout pas la convertir en
        # chaine, sinon '[]' deviendrait un filtre non vide.
        capability = getattr(params, "ServerCapabilityFilter", None)
    except Exception as exc:
        logger.warning(f"FindServersOnNetwork - parametres illisibles: {exc}")
        starting, maximum, capability = 0, 0, None

    if registry is None:
        logger.warning("FindServersOnNetwork recu sans registre : reponse vide")
        result = ua.FindServersOnNetworkResult(Servers=[])
    else:
        # Evacue les entrees perimees avant de repondre : le client ne doit
        # jamais recevoir un endpoint qui ne repond plus.
        try:
            await registry.sweep()
        except Exception as exc:
            logger.error(f"Balayage avant FindServersOnNetwork echoue: {exc}")
        result = registry.find_servers_on_network(
            starting, maximum, capability, sockname=getattr(self, "sockname", None)
        )

    response = ua.FindServersOnNetworkResponse(Parameters=result)
    self.send_response(requesthdr.RequestHandle, seqhdr, response)
    logging.getLogger("asyncua.internal").debug(
        "find servers on network request -> %d server(s)", len(result.Servers)
    )
    return True


def _as_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    for attr in ("Value", "value"):
        if hasattr(value, attr):
            value = getattr(value, attr)
            break
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

