#!/usr/bin/env python3
"""
Serveur OPCUA - Mode Maintenance avec enregistrement LDS
"""

import asyncio
import re
import socket
from asyncua import Server, ua
from opcua_utils import (
    get_server_ip,
    check_port_available,
    parse_server_args,
    print_server_banner,
    create_maintenance_object,
    wait_for_interrupt
)


# Configuration du serveur
args = parse_server_args('Serveur OPCUA - Mode Maintenance')
SERVER_ENDPOINT = f"opc.tcp://{args.host}:{args.port}/maintenance/server"
SERVER_NAME = "Maintenance Server"


async def main():
    """Point d'entrée principal."""
    
    # Messages de démarrage
    server_ip = get_server_ip()
    print_server_banner("SERVEUR OPCUA - MODE MAINTENANCE", server_ip, SERVER_ENDPOINT)
    
    # Extraire l'IP et le port de l'endpoint
    match = re.search(r":\/\/([^:]+):(\d+)", SERVER_ENDPOINT)
    if not match:
        print(f"Erreur: Format d'endpoint invalide: {SERVER_ENDPOINT}")
        return
    
    host = match.group(1)
    port = int(match.group(2))
    
    # Vérifier si le port est disponible
    if not check_port_available(host, port):
        print(f"ERREUR: Le port {port} est déjà utilisé!")
        return
    
    print(f"Le port {port} est disponible.")
    
    # Création du serveur
    server = Server()
    await server.init()
    server.set_endpoint(SERVER_ENDPOINT)
    server.set_server_name(SERVER_NAME)
    server.product_uri = "urn:asyncua:plc:server"    
    await server.set_application_uri("urn:thylds:maintenance:server")
    
    # Politique de sécurité
    server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
    
    # Enregistrer le serveur auprès du LDS
    lds_url = "opc.tcp://127.0.0.1:4840"
    print(f"Enregistrement auprès du LDS: {lds_url}")
    try:
        await server.register_to_discovery(lds_url, 60)
        print("Serveur enregistré auprès du LDS!")
    except Exception as e:
        print(f"Note: LDS non disponible: {e}")
    
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
