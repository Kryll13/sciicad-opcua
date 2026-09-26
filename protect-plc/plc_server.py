"""
Simulateur OPC UA d'un PLC de protection d'installation industrielle (SCIICAD).

Lancement:
    uv run protect-plc/plc_server.py --lds opc.tcp://193.168.1.20:4840

Le modèle d'adressage est propre à ce simulateur ; la partie commune (réseau,
enregistrement LDS, arrêt, CLI) vient du paquet ``sciicad``.
"""

import argparse
import asyncio
import contextlib

from asyncua import Server, ua
from asyncua.common.node import Node
from loguru import logger

from sciicad.cli import add_plc_arguments
from sciicad.discovery import LdsRegistrar
from sciicad.identity import set_application_identity
from sciicad.lifecycle import install_signal_handlers, run_until_stopped, withdraw_from_lds
from sciicad.net import get_host_info, resolve_endpoints

# Identité du serveur
SERVER_NAME = "SCIICAD PLC Protect Server"
APPLICATION_URI = "urn:SCIICAD:protect-plc"
PRODUCT_URI = "urn:CEC:Python-Asyncua.Application:plc"

# État de la simulation, republié via la variable OPC UA.
MAINTENANCE_MODE = False


async def create_protection_object(parent_node: Node, idx: int) -> dict:
    """Crée l'objet Protection et sa variable dans l'espace d'adressage.

    Structure :
        Objects
        └── Protection (Object)
            └── MaintenanceMode (Boolean) - mode maintenance, lecture/écriture

    ``idx`` est l'index du namespace. Attention : asyncua interprète un entier
    passé en première position de ``add_*`` comme un index de namespace, puis
    attribue lui-même les identifiants numériques. Résoudre donc toujours les
    nœuds par browse name (voir ``docs/depannage.md``).
    """
    protection_obj = await parent_node.add_object(idx, "Protection")

    maintenance_var = await protection_obj.add_variable(idx, "MaintenanceMode", False)
    await maintenance_var.set_writable(True)

    return {"maintenance_mode": maintenance_var}


async def protection_simulation(nodes: dict) -> None:
    """Suit le mode maintenance et journalise son activation.

    Boucle infinie : elle est annulée par ``run_until_stopped`` lors de l'arrêt
    du serveur.

    Attention, il ne s'agit pour l'instant que d'un **stub** : ce simulateur
    n'expose que l'état du mode maintenance et ne modélise aucun processus de
    protection (seuils, alarmes, déclenchements). Le modèle physique reste à
    écrire.
    """
    global MAINTENANCE_MODE

    while True:
        MAINTENANCE_MODE = await nodes["maintenance_mode"].get_value()
        if MAINTENANCE_MODE:
            logger.info("MODE MAINTENANCE ACTIVÉ - Boucle de protection arrêtée")
        await asyncio.sleep(1)


async def main(
    port: int = 4840,
    lds_url: str = "opc.tcp://lds:4840",
    bind_address: str = "0.0.0.0",
    advertise_host: str | None = None,
) -> None:
    """Point d'entrée principal du serveur OPC UA."""
    host, ip = get_host_info()
    bind_address, port, endpoint = resolve_endpoints(bind_address, port, advertise_host)

    logger.info("=" * 50)
    logger.info("  Serveur OPCUA - Protection Simulation")
    logger.info("=" * 50)
    logger.info(f"Hostname du serveur: {host}")
    logger.info(f"IP détectée: {ip}")
    logger.info(f"Écoute: {bind_address}:{port}")
    logger.info(f"Endpoint annoncé: {endpoint}")
    logger.info("=" * 50)

    server = Server()
    await server.init()
    # socket_address fixe l'écoute, set_endpoint fixe l'annonce.
    server.socket_address = (bind_address, port)
    server.set_endpoint(endpoint)
    server.set_server_name(SERVER_NAME)
    # Passe par le module partagé : set_application_uri seul laisse ServerArray
    # sur l'URI par défaut d'asyncua.
    await set_application_identity(
        server, APPLICATION_URI, product_uri=PRODUCT_URI, server_name=SERVER_NAME
    )

    server.set_security_policy(
        [
            ua.SecurityPolicyType.NoSecurity,
            ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt,
        ]
    )

    # Sans certificat, asyncua n'annonce que NoSecurity. L'échec est un
    # warning et non une erreur : le simulateur doit rester utilisable.
    try:
        await server.load_certificate("protect-plc/server_certificate.pem")
        await server.load_private_key("protect-plc/server_private_key.pem")
        logger.info("Certificat et clé privée chargés pour Basic256Sha256_SignAndEncrypt")
    except Exception as exc:
        logger.warning(
            f"Certificat indisponible ({exc}) : seul NoSecurity sera annoncé. "
            "Générer les fichiers avec tools/crypto_opcua.py --output-dir protect-plc"
        )

    idx = await server.register_namespace(APPLICATION_URI)
    logger.info(f"Namespace enregistré: idx={idx}, uri={APPLICATION_URI}")

    objects_node = server.get_objects_node()
    nodes = await create_protection_object(objects_node, idx)

    logger.debug("Node IDs créés:")
    for name, node in nodes.items():
        logger.debug(f"  - {name}: {node.nodeid}")

    registrar = LdsRegistrar(server, lds_url) if lds_url else None
    stopped = asyncio.Event()
    install_signal_handlers(stopped)

    try:
        async with server:
            logger.info(f"Serveur OPCUA démarré sur {endpoint}")
            logger.info("Nodes disponibles:")
            logger.info("  - Protection.MaintenanceMode (Boolean, lecture/écriture)")
            logger.info("Appuyez sur Ctrl+C pour arrêter...\n")

            # L'enregistrement n'est lancé qu'une fois le serveur en écoute :
            # sinon un échec de bind laisserait une entrée morte dans le LDS.
            if registrar is not None:
                logger.info(f"Enregistrement auprès du LDS : {lds_url}")
                registrar.start()

            with contextlib.suppress(asyncio.CancelledError):
                await run_until_stopped(
                    protection_simulation(nodes), stopped, label="surveillance de la protection"
                )
    finally:
        # Le finally garantit le retrait du LDS même en cas d'exception :
        # le LDS ne doit jamais annoncer un endpoint fermé.
        if registrar is not None:
            await withdraw_from_lds(registrar)


if __name__ == "__main__":
    parser = add_plc_arguments(
        argparse.ArgumentParser(
            description="Simulateur OPC UA - PLC de protection SCIICAD"
        )
    )
    args = parser.parse_args()

    try:
        asyncio.run(main(args.port, args.lds, args.bind, args.advertise))
    except KeyboardInterrupt:
        # Propagé seulement après la fin de asyncio.run : le retrait du LDS a
        # donc déjà eu lieu dans main().
        logger.info("Serveur OPCUA arrêté.")
