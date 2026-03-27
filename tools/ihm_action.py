#!/usr/bin/env python3
"""
IHM Client OPCUA pour interroger le PLC et contrôler le chauffage.
"""

import asyncio
import argparse
import sys
from asyncua import Client, ua
from opcua_utils import logger


# Node ID mappings for thermostat variables
THERMOSTAT_NODE_NAMES = {
    2: "Heating",
    3: "Temperature",
    4: "HighTempAlarm",
    5: "LowTempAlarm",
    6: "MaintenanceMode"
}


def parse_args():
    """Parse les arguments de la ligne de commande."""
    parser = argparse.ArgumentParser(
        description='IHM OPCUA - Client pour PLC Thermostat'
    )
    parser.add_argument('--ip', type=str, default='localhost',
                        help='Adresse IP du serveur OPCUA (défaut: localhost)')
    parser.add_argument('--port', type=int, default=4840,
                        help='Port du serveur OPCUA (défaut: 4840)')
    parser.add_argument('--heat', type=str, choices=['on', 'off', 'status'],
                        help='Actionner le chauffage: on, off, ou status pour voir l\'état')
    parser.add_argument('--maintenance', type=str, choices=['on', 'off', 'status'],
                        help='Mode maintenance: on, off, ou status pour voir l\'état')
    return parser.parse_args()


async def find_node_by_identifier(parent_node, namespace_index: int, identifier: int):
    """
    Find a child node by its namespace index and identifier.
    
    Args:
        parent_node: The parent node to search.
        namespace_index: The namespace index to match.
        identifier: The node identifier to match.
    
    Returns:
        The found node or None.
    """
    try:
        children = await parent_node.get_children()
        for child in children:
            try:
                nodeid = child.nodeid
                if nodeid.NamespaceIndex == namespace_index and nodeid.Identifier == identifier:
                    return child
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Error searching for node: {e}")
    return None


async def find_thermostat_node(client: Client):
    """Trouve automatiquement le node Thermostat."""
    objects = client.nodes.objects
    
    # Obtenir tous les enfants de Objects
    children = await objects.get_children()
    
    # Afficher pour debug
    logger.info(f"Nombre d'enfants sous Objects: {len(children)}")
    for child in children:
        logger.info(f"  Node: {child.nodeid}")
    
    # Chercher le Thermostat par NodeId avec namespace 2
    for child in children:
        try:
            nodeid = child.nodeid
            # Chercher par namespace index 2 (thermo namespace)
            if nodeid.NamespaceIndex == 2:
                logger.info(f"Thermostat trouvé: {nodeid}")
                return child
        except Exception:
            pass
    
    return None


async def set_heating(client: Client, thermostat_node, state: bool):
    """Active ou désactive le chauffage."""
    try:
        # Trouver le node Heating (NodeId identifier = 2)
        heating_node = await find_node_by_identifier(thermostat_node, 2, 2)
        if heating_node:
            await heating_node.set_value(state)
            logger.info(f"Chauffage {'ACTIVÉ' if state else 'DÉSACTIVÉ'}")
            return True
        logger.warning("Node Heating non trouvé")
        return False
    except Exception as e:
        logger.error(f"Erreur écriture chauffage: {e}")
        return False


async def set_maintenance(client: Client, thermostat_node, state: bool):
    """Active ou désactive le mode maintenance."""
    try:
        # Trouver le node MaintenanceMode (NodeId identifier = 6)
        maintenance_node = await find_node_by_identifier(thermostat_node, 2, 6)
        if maintenance_node:
            await maintenance_node.set_value(state)
            logger.info(f"Mode Maintenance {'ACTIVÉ' if state else 'DÉSACTIVÉ'}")
            return True
        logger.warning("Node MaintenanceMode non trouvé")
        return False
    except Exception as e:
        logger.error(f"Erreur écriture maintenance: {e}")
        return False


async def display_status(client: Client):
    """Affiche l'état complet du thermostat."""
    print("\n" + "=" * 50)
    print("  ÉTAT DU THERMOSTAT")
    print("=" * 50)
    
    try:
        # Trouver le thermostat automatiquement
        thermostat = await find_thermostat_node(client)
        
        if thermostat is None:
            print("Thermostat non trouvé!")
            print("Nodes disponibles sous Objects:")
            
            # Afficher tous les nodes disponibles
            objects = client.nodes.objects
            children = await objects.get_children()
            for child in children:
                try:
                    nodeid = child.nodeid
                    print(f"  - NodeId: {nodeid}")
                except Exception:
                    print("  - (node sans ID)")
            
            print("=" * 50 + "\n")
            return
        
        # Lire les variables du Thermostat
        children = await thermostat.get_children()
        
        values = {}
        for child in children:
            try:
                nodeid = child.nodeid
                val = await child.get_value()
                name = THERMOSTAT_NODE_NAMES.get(nodeid.Identifier, f"Node_{nodeid.Identifier}")
                values[name] = val
                print(f"  {name}: {val}")
            except Exception:
                pass
        
        heating = values.get("Heating", False)
        temperature = values.get("Temperature", 0)
        high_alarm = values.get("HighTempAlarm", False)
        low_alarm = values.get("LowTempAlarm", False)
        maintenance = values.get("MaintenanceMode", False)
        
        print()
        print(f"Température: {temperature} °C")
        print(f"Chauffage: {'ON' if heating else 'OFF'}")
        print(f"Alarme haute température: {'ALERTE' if high_alarm else 'OK'}")
        print(f"Alarme basse température: {'ALERTE' if low_alarm else 'OK'}")
        print(f"Mode Maintenance: {'ACTIF' if maintenance else 'INACTIF'}")
        
    except Exception as e:
        logger.error(f"Erreur: {e}")
    
    print("=" * 50 + "\n")


async def main():
    """Point d'entrée principal."""
    args = parse_args()
    
    # Construire l'URL de connexion
    url = f"opc.tcp://{args.ip}:{args.port}/thermo/server/"
    
    logger.info(f"Connexion au PLC: {url}")
    
    client = Client(url=url)
    
    try:
        # Connexion au PLC
        await client.connect()
        logger.info("Connecté au PLC\n")
        
        # Trouver le thermostat
        thermostat = await find_thermostat_node(client)
        if thermostat is None:
            logger.error("Thermostat non trouvé!")
            return
        
        # Traiter la commande --heat
        if args.heat in ['on', 'off']:
            state = (args.heat == 'on')
            await set_heating(client, thermostat, state)
            await asyncio.sleep(0.5)
        
        # Traiter la commande --maintenance
        if args.maintenance in ['on', 'off']:
            state = (args.maintenance == 'on')
            await set_maintenance(client, thermostat, state)
            await asyncio.sleep(0.5)
        
        # Afficher le status
        await display_status(client)
        
    except Exception as e:
        logger.error(f"Erreur: {e}")
        sys.exit(1)
    finally:
        await client.disconnect()
        logger.info("Déconnecté du PLC")


if __name__ == "__main__":
    asyncio.run(main())
