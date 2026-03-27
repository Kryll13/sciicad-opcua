#!/usr/bin/env python3
"""
Serveur OPCUA - Basic
"""

import asyncio
from asyncua import Server, ua
from opcua_utils import (
    get_server_ip,
    parse_server_args,
    print_server_banner,
    create_maintenance_object,
    wait_for_interrupt
)


# Configuration du serveur
args = parse_server_args('Serveur OPCUA - Basic')
SERVER_ENDPOINT = f"opc.tcp://{args.host}:{args.port}/basic/server"
SERVER_NAME = "Basic Server"


async def main():
    """Point d'entrée principal."""
    
    # Messages de démarrage
    server_ip = get_server_ip()
    print_server_banner("SERVEUR OPCUA - BASIC", server_ip, SERVER_ENDPOINT)
    
    # Création du serveur
    server = Server()
    await server.init()
    server.set_endpoint(SERVER_ENDPOINT)
    server.set_server_name(SERVER_NAME)
    await server.set_application_uri("urn:thylds:maintenance:server")
    server.product_uri = "urn:thylds:asyncua:server"
    
    # Politique de sécurité
    server.set_security_policy([ua.SecurityPolicyType.NoSecurity])

    try:
        await server.register_to_discovery("opc.tcp://127.0.0.1:4840", 10)
    except Exception as e:
        print(f"Note: Enregistrement LDS non disponible: {e}")
    
    # Enregistrer le namespace
    uri = "http://maintenance.example.org/"
    idx = await server.register_namespace(uri)
    print(f"Namespace enregistré: idx={idx}")
    
    # Créer l'objet Maintenance
    objects_node = server.get_objects_node()
    nodes = await create_maintenance_object(server, objects_node, idx, uri)
    
    print(f"\nNode créé:")
    print(f"  - Maintenance.MaintenanceMode (Boolean, lecture/écriture)")
    
    # Démarrer le serveur
    async with server:
        print(f"\nServeur OPCUA démarré sur {SERVER_ENDPOINT}")
        print("Appuyez sur Ctrl+C pour arrêter...\n")
        
        # Boucle de surveillance du mode maintenance
        while True:
            maintenance_mode = await nodes["maintenance"].get_value()
            if maintenance_mode:
                print(f"[MAINTENANCE] Mode maintenance ACTIVÉ")
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
