#!/usr/bin/env python3
"""
Serveur OPCUA LDS (Local Discovery Server) minimal
Implémente les services de découverte OPCUA
"""

import asyncio
from asyncua import Server, ua
from opcua_utils import get_server_ip, print_server_banner, wait_for_interrupt


async def main():
    """Point d'entrée principal."""
    server_ip = get_server_ip()
    endpoint = "opc.tcp://0.0.0.0:4840"
    
    print_server_banner("SERVEUR OPCUA - LDS (Discovery Server)", server_ip, endpoint)
    print(f"[INFO] Endpoint: {endpoint}")
    
    # Création du serveur avec flag LDS
    server = Server()
    await server.init()
    
    # Configuration comme serveur de découverte
    server.set_endpoint(endpoint)
    server.name = "OPCUA Local Discovery Server"
    server.product_uri = "urn:asyncua:lds:server"
    await server.set_application_uri("urn:thylds:lds:server")
    
    # Activer le mode discovery server
    # Cela permet de répondre aux requêtes FindServers, GetEndpoints
    # et d'enregistrer les serveurs via RegisterServer
    server.discovery_server_flag = True
    
    print("\nServices LDS disponibles:")
    print("  - FindServers: Liste des serveurs enregistrés")
    print("  - GetEndpoints: Endpoints du serveur")
    print("  - RegisterServer: Enregistrement d'un serveur")
    print("  - RegisterServer2: Enregistrement étendu")
    print()
    
    # Démarrer le serveur
    async with server:
        print(f"Serveur LDS démarré sur opc.tcp://0.0.0.0:4840")
        print("En attente de connexions...\n")
        
        # Garder le serveur actif
        await wait_for_interrupt()


if __name__ == "__main__":
    asyncio.run(main())
