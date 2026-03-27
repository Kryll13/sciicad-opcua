#!/usr/bin/env python3
"""
IHM Client OPCUA pour interroger le PLC en continu.
"""

import asyncio
import argparse
import sys
from asyncua import Client


# Configuration OPCUA par défaut
DEFAULT_HOST = "thermo-plc"
DEFAULT_PORT = 4840


def parse_args():
    """Parse les arguments de la ligne de commande."""
    parser = argparse.ArgumentParser(description='IHM OPCUA - Client pour PLC Thermostat')
    parser.add_argument('--host', type=str, default=DEFAULT_HOST,
                        help=f'Hôte du serveur OPCUA (défaut: {DEFAULT_HOST})')
    parser.add_argument('--port', type=int, default=DEFAULT_PORT,
                        help=f'Port du serveur OPCUA (défaut: {DEFAULT_PORT})')
    return parser.parse_args()


def signal_handler(sig, frame):
    """Capture le signal Ctrl+C."""
    sys.exit(0)


# Installer le gestionnaire de signal
import signal
signal.signal(signal.SIGINT, signal_handler)


async def get_thermostat_status(client: Client):
    """Récupère l'état du thermostat."""
    objects = client.nodes.objects
    children = await objects.get_children()
    
    # Chercher le node Thermostat (namespace index 2)
    thermostat = None
    for child in children:
        if child.nodeid.NamespaceIndex == 2:
            thermostat = child
            break
    
    if thermostat is None:
        return None
    
    # Lire les variables
    thermo_children = await thermostat.get_children()
    
    # Mapper: NodeId Identifier -> (nom, valeur)
    values = {}
    for child in thermo_children:
        try:
            nodeid = child.nodeid
            val = await child.get_value()
            values[nodeid.Identifier] = val
        except:
            pass
    
    return {
        "Heating": values.get(2, False),
        "Temperature": values.get(3, 0),
        "HighTempAlarm": values.get(4, False),
        "LowTempAlarm": values.get(5, False),
        "MaintenanceMode": values.get(6, False)
    }


async def interactive_mode(client: Client, url: str):
    """Mode interactif avec raffraîchissement toutes les 500ms."""
    print("-" * 50)
    print("Appuyez sur Ctrl+C pour quitter")
    print("-" * 50)
    
    running = True
    
    while running:
        try:
            status = await get_thermostat_status(client)
            
            if status is None:
                print("\nThermostat non trouvé!")
                break
            
            # Affichage formaté
            heating = "On" if status["Heating"] else "Off"
            maintenance = "On" if status["MaintenanceMode"] else "Off"
            high_alarm = "Vrai" if status["HighTempAlarm"] else "Faux"
            low_alarm = "Vrai" if status["LowTempAlarm"] else "Faux"
            
            print(f"\rT={status['Temperature']:5.1f}°C | Chauffage={heating} | Maintenance={maintenance} | >25={high_alarm} | <15={low_alarm}     ", end="")
            
            await asyncio.sleep(0.5)
            
        except KeyboardInterrupt:
            running = False
        except Exception as e:
            print(f"\nErreur: {e}")
            break


async def main():
    """Point d'entrée principal."""
    args = parse_args()
    
    print("=" * 50)
    print("  IHM OPCUA - THERMOSTAT")
    print("=" * 50)
    
    url = f"opc.tcp://{args.host}:{args.port}"
    print(f"\nConnexion au PLC: {url}")
    
    client = Client(url=url)
    
    try:
        await client.connect()
        print("Connecté au PLC")
        await interactive_mode(client, url)
        
    except Exception as e:
        print(f"Erreur: {e}")
    finally:
        await client.disconnect()
        print("\n\nDéconnecté du PLC")


if __name__ == "__main__":
    asyncio.run(main())
