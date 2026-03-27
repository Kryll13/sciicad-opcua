#!/usr/bin/env python3
"""
Script de test pour vérifier la découverte OPCUA via le LDS.
"""

import asyncio
import argparse
from asyncua import Client
from opcua_utils import logger


# Default LDS URL
DEFAULT_LDS_URL = "opc.tcp://127.0.0.1:4840"


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Test OPCUA LDS Discovery'
    )
    parser.add_argument('--url', type=str, default=DEFAULT_LDS_URL,
                        help=f'LDS URL (default: {DEFAULT_LDS_URL})')
    return parser.parse_args()


async def test_discovery(lds_url: str):
    """Teste la découverte via le LDS."""
    
    print("=" * 50)
    print("  TEST DE DÉCOUVERTE OPCUA")
    print("=" * 50)
    print(f"Connexion au LDS: {lds_url}\n")
    
    client = Client(url=lds_url)
    
    try:
        await client.connect()
        print("Connecté au LDS!\n")
        
        # Récupérer les serveurs enregistrés
        servers = await client.find_servers()
        
        print(f"Nombre de serveurs enregistrés: {len(servers)}\n")
        for i, server in enumerate(servers, 1):
            print(f"Serveur {i}:")
            print(f"  - Nom: {server.ApplicationName}")
            print(f"  - URI: {server.ApplicationUri}")
            print(f"  - Type: {server.ApplicationType}")
            print(f"  - DiscoveryUrls: {server.DiscoveryUrls}")
            print()
        
        # Récupérer les endpoints
        endpoints = await client.get_endpoints()
        
        print(f"Endpoints disponibles: {len(endpoints)}")
        for ep in endpoints:
            print(f"  - {ep.EndpointUrl}")
            print(f"    Security: {ep.SecurityPolicyUri}")
            print(f"    Mode: {ep.SecurityMode}")
            print()
        
    except Exception as e:
        logger.error(f"Erreur: {e}")
    finally:
        await client.disconnect()
        print("Déconnecté du LDS")


async def main():
    """Point d'entrée principal."""
    args = parse_args()
    await test_discovery(args.url)


if __name__ == "__main__":
    asyncio.run(main())
