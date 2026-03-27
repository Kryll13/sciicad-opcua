"""
Serveur OPCUA pour simulation du système de protection d'une installation industrielle

Dépendances:
    uv add asyncua

Lancement:
    uv run plc_server.py
"""

import argparse
import asyncio
import logging
import socket
import sys
from asyncua import Server, ua
from asyncua.common.node import Node
from loguru import logger

# Configuration du logging pour supprimer les warnings asyncua
#logger.remove()
#logger.add(
#    sys.stderr,
#    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
#    level="INFO"
#)
#logging.getLogger("asyncua").setLevel(logging.WARNING)

# Configuration du serveur
SERVER_NAME = "SCIICAD PLC Protect Server"

# Variables de simulation
MAINTENANCE_MODE = False


def get_host_info():
    """Get hostname and IP address."""
    hostname = socket.gethostname()
    try:
        ip = socket.gethostbyname(hostname)
    except socket.gaierror:
        ip = "127.0.0.1"
    return hostname, ip


async def create_protection_object(server: Server, parent_node: Node, idx: int):
    """
    Crée un objet Protection dans l'espace d'adressage OPCUA.
    
    Structure:
        Objects
        └── Protection (Object)
            └── MaintenanceMode (Boolean) - Mode maintenance activé
    """
    # Créer l'objet Protection
    protection_obj = await parent_node.add_object(
        idx,
        "Protection"
    )
        
    # Variable MaintenanceMode (lecture/écriture)
    maintenance_var = await protection_obj.add_variable(
        idx,
        "MaintenanceMode",
        False
    )
    await maintenance_var.set_writable(True)
    
    return {
        "maintenance_mode": maintenance_var
    }


async def protection_simulation(nodes: dict):
    """
    Tâche asynchrone simulant la variation de température.
    """
    global MAINTENANCE_MODE
    
    while True:
        # Lire l'état du mode maintenance
        MAINTENANCE_MODE = await nodes["maintenance_mode"].get_value()
        
        # Si mode maintenance, ne pas simuler
        if MAINTENANCE_MODE:
            logger.info("MODE MAINTENANCE ACTIVÉ - Boucle de protection arrêtée")
            await asyncio.sleep(1)
            continue
        
        await asyncio.sleep(1)


async def main(port: int = 4840, lds_url: str = "opc.tcp://lds:4840"):
    """
    Point d'entrée principal du serveur OPCUA.
    """
    # Afficher les informations du serveur
    host, ip = get_host_info()
    endpoint = f"opc.tcp://{ip}:{port}"
    application_uri = f"urn:SCIICAD:protect-plc"

    logger.info("=" * 50)
    logger.info("  Serveur OPCUA - Protection Simulation")
    logger.info("=" * 50)
    logger.info(f"IP du serveur: {ip}")
    logger.info(f"Hostname du serveur: {host}")
    logger.info(f"Endpoint: {endpoint}")
    logger.info("=" * 50)
    

    server = Server()
    await server.init()
    server.set_endpoint(endpoint)
    server.set_server_name(SERVER_NAME)
    await server.set_application_uri(application_uri)
    server.product_uri = "urn:CEC:Python-Asyncua.Application:plc"
    
    # Configurer la politique de sécurité et les méthodes d'authentification
    server.set_security_policy(
        [
            ua.SecurityPolicyType.NoSecurity,
            ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt,
        ]
    )

    # Charger le certificat et la clé privée pour Basic256Sha256_SignAndEncrypt
    try:
        await server.load_certificate("protect-plc/server_certificate.pem")
        await server.load_private_key("protect-plc/server_private_key.pem")
        logger.info("Certificat et clé privée chargés pour Basic256Sha256_SignAndEncrypt")
    except Exception as e:
        logger.warning(f"Impossible de charger le certificat: {e}")

    # Enregistrer le serveur auprès du LDS
    logger.info(f"Enregistrement auprès du LDS: {lds_url}")
    try:
        await server.register_to_discovery(lds_url, 60)
        logger.info("Serveur enregistré auprès du LDS!")
    except Exception as e:
        logger.warning(f"LDS non disponible: {e}")
    
    # Configurer les méthodes d'authentification supportées
    # Par défaut, asyncua supporte Anonymous, Basic256 et Certificate
    # Aucune configuration supplémentaire n'est nécessaire pour anonymous
    
    # Enregistrer un namespace
    idx = await server.register_namespace(application_uri)
    logger.info(f"Namespace enregistré: idx={idx}, uri={application_uri}")
    
    # Créer l'objet Protection et ses variables
    objects_node = server.get_objects_node()
    nodes = await create_protection_object(server, objects_node, idx)
    
    # Afficher les Node IDs pour debug
    logger.debug("Node IDs créés:")
    for name, node in nodes.items():
        logger.debug(f"  - {name}: {node.nodeid}")
    
    # Démarrer le serveur en arrière-plan
    async with server:
        logger.info(f"Serveur OPCUA démarré sur {endpoint}")
        logger.info("Nodes disponibles:")
        logger.info("  - Protection.MaintenanceMode (Boolean, lecture/écriture)")
        logger.info("Appuyez sur Ctrl+C pour arrêter...\n")
        
        # Capturer les interruptions clavier pour un arrêt propre
        try:
            await protection_simulation(nodes)
        except asyncio.CancelledError:
            logger.info("Simulation de protection interrompue par l'utilisateur.")

        # Désenregistrer le serveur du LDS à l'arrêt
        logger.info("Désenregistrement du serveur du LDS...")
        try:
            await server.unregister_from_discovery(lds_url)
            logger.info("Serveur désenregistré du LDS.")
        except Exception as e:
            logger.warning(f"Erreur lors du désenregistrement du LDS: {e}")


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Serveur OPCUA pour simulation PLC")
    parser.add_argument(
        "--port",
        type=int,
        default=4840,
        help="Port OPCUA (défaut: 4840)"
    )
    parser.add_argument(
        "--lds",
        type=str,
        default="opc.tcp://lds:4840",
        help="URL du service LDS (défaut: opc.tcp://lds:4840)"
    )
    args = parser.parse_args()
    
    try:
        asyncio.run(main(args.port, args.lds))
    except KeyboardInterrupt:
        logger.info("Serveur OPCUA arrêté.")
