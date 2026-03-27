#!/usr/bin/env python3
"""
Script de test pour vérifier la découverte OPCUA.
Ce script teste la connexion au LDS et au PLC.
"""

import asyncio
import argparse
from asyncua import Client
from opcua_utils import logger


# Default configuration
DEFAULT_LDS_URL = "opc.tcp://127.0.0.1:4840"
DEFAULT_PLC_URL = "opc.tcp://plc:4841"


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Test OPCUA Discovery'
    )
    parser.add_argument('--lds-url', type=str, default=DEFAULT_LDS_URL,
                        help=f'LDS URL (default: {DEFAULT_LDS_URL})')
    parser.add_argument('--plc-url', type=str, default=DEFAULT_PLC_URL,
                        help=f'PLC URL (default: {DEFAULT_PLC_URL})')
    parser.add_argument('--skip-plc', action='store_true',
                        help='Skip PLC connection tests')
    parser.add_argument('--wait', type=int, default=3,
                        help='Wait time in seconds before testing (default: 3)')
    return parser.parse_args()


async def test_lds_discovery(lds_url: str):
    """
    Test de découverte via le serveur LDS.
    """
    logger.info("=== Test de découverte LDS ===")
    
    client = Client(url=lds_url)
    
    try:
        await client.connect()
        logger.info("Connecté au serveur LDS")
        
        # Récupération des serveurs découverts
        servers = await client.connect_and_get_server_endpoints(lds_url)
        
        logger.info(f"Nombre de serveurs découverts: {len(servers)}")
        
        for i, server in enumerate(servers):
            logger.info(f"Serveur {i+1}:")
            logger.info(f"  - Nom: {server.server_name}")
            logger.info(f"  - URI du produit: {server.product_uri}")
            logger.info(f"  - Type de serveur: {server.server_type}")
            
            for endpoint in server.endpoints:
                logger.info(f"  - Endpoint: {endpoint.endpoint_url}")
                logger.info(f"    Security Policy: {endpoint.security_policy_uri}")
                logger.info(f"    Security Mode: {endpoint.security_mode}")
        
        await client.disconnect()
        logger.info("Déconnexion du LDS réussie")
        
    except Exception as e:
        logger.error(f"Erreur lors du test LDS: {e}")


async def test_plc_connection(plc_url: str):
    """
    Test de connexion directe au PLC.
    """
    logger.info("\n=== Test de connexion PLC ===")
    
    client = Client(url=plc_url)
    
    try:
        await client.connect()
        logger.info("Connecté au serveur PLC")
        
        # Récupération des namespaces
        namespaces = await client.get_namespace_array()
        logger.info(f"Namespaces: {namespaces}")
        
        # Accès aux variables du PLC
        uri = "http://thylds/plc"
        idx = await client.get_namespace_index(uri)
        
        # Lecture des variables
        objects = client.nodes.objects
        plc_node = await objects.get_child(f"0:PLC")
        
        if plc_node:
            # Lecture de chaque variable
            door_open = await plc_node.get_child(f"{idx}:DoorOpen")
            temperature = await plc_node.get_child(f"{idx}:Temperature")
            pressure = await plc_node.get_child(f"{idx}:Pressure")
            motor_running = await plc_node.get_child(f"{idx}:MotorRunning")
            timestamp = await plc_node.get_child(f"{idx}:Timestamp")
            
            logger.info("\nValeurs du PLC:")
            logger.info(f"  - DoorOpen: {await door_open.get_value()}")
            logger.info(f"  - Temperature: {await temperature.get_value()} °C")
            logger.info(f"  - Pressure: {await pressure.get_value()} bar")
            logger.info(f"  - MotorRunning: {await motor_running.get_value()}")
            logger.info(f"  - Timestamp: {await timestamp.get_value()}")
        else:
            logger.warning("Noeud PLC non trouvé")
        
        await client.disconnect()
        logger.info("Déconnexion du PLC réussie")
        
    except Exception as e:
        logger.error(f"Erreur lors du test PLC: {e}")


async def test_plc_via_lds(lds_url: str):
    """
    Test de connexion au PLC via le LDS.
    """
    logger.info("\n=== Test PLC via LDS ===")
    
    try:
        # Connexion via l'endpoint du PLC через LDS
        client = Client(url=f"{lds_url}/discovery")
        
        await client.connect()
        logger.info("Connecté via discovery endpoint")
        
        # Les endpoints du PLC devraient être découverts
        servers = await client.connect_and_get_server_endpoints(lds_url)
        
        plc_endpoints = [s for s in servers if "plc" in s.server_name.lower()]
        
        if plc_endpoints:
            logger.info("Endpoints PLC découverts via LDS:")
            for endpoint in plc_endpoints[0].endpoints:
                logger.info(f"  - {endpoint.endpoint_url}")
        
        await client.disconnect()
        
    except Exception as e:
        logger.error(f"Erreur lors du test PLC via LDS: {e}")


async def main():
    """
    Point d'entrée principal.
    """
    args = parse_args()
    
    logger.info(" Démarrage des tests OPCUA ".center(50, "="))
    
    # Attendre que les services soient prêts
    if args.wait > 0:
        logger.info(f"Attente de {args.wait} secondes pour laisser les services démarrer...")
        await asyncio.sleep(args.wait)
    
    # Exécution des tests
    await test_lds_discovery(args.lds_url)
    
    if not args.skip_plc:
        await test_plc_connection(args.plc_url)
        await test_plc_via_lds(args.lds_url)
    
    logger.info("\n" + " Tests terminés ".center(50, "="))


if __name__ == "__main__":
    asyncio.run(main())
