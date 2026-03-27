#!/usr/bin/env python3
"""
Outil d'analyse d'un serveur OPCUA PLC.

Affiche les variables exposées, leur mode d'accès et leur valeur actuelle.

Usage:
    python analyze.py -u <url_plc>

Exemple:
    python analyze.py -u opc.tcp://localhost:4840
"""

import argparse
import asyncio
import sys
from asyncua import Client, ua


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Analyse les variables d'un serveur OPCUA PLC"
    )
    parser.add_argument(
        '-u', '--url',
        type=str,
        required=True,
        help="URL du serveur OPCUA (ex: opc.tcp://localhost:4840)"
    )
    return parser.parse_args()


async def get_access_mode(node, client) -> str:
    """
    Détermine le mode d'accès à partir du AccessLevel.
    
    Args:
        node: Node OPCUA
        client: Client OPCUA
        
    Returns:
        Chaîne décrivant le mode d'accès
    """
    # Bit positions according to OPC UA spec
    CURRENT_READ = 1    # bit 0
    CURRENT_WRITE = 2   # bit 1
    
    try:
        # Lire AccessLevel
        access_level = await node.read_attribute(ua.AttributeIds.AccessLevel)
        if access_level.StatusCode.is_good() and access_level.Value is not None:
            val = access_level.Value
            if hasattr(val, 'Value'):
                access = val.Value
            else:
                access = val
            
            # Vérifier si c'est un entier
            if isinstance(access, int):
                can_read = bool(access & CURRENT_READ)
                can_write = bool(access & CURRENT_WRITE)
                
                if can_read and can_write:
                    return "lecture/écriture"
                elif can_read:
                    return "lecture seule"
    except Exception as e:
        pass
    
    return "lecture seule"


async def browse_variables(node, client, depth=0, max_depth=3, parent_name=""):
    """
    Parcourt récursivement les nodes pour trouver les variables.
    
    Args:
        node: Node OPCUA à parcourir
        client: Client OPCUA
        depth: Profondeur actuelle
        max_depth: Profondeur maximale
        parent_name: Nom du parent pour construire le chemin
        
    Returns:
        Liste de tuples (nom_complet, mode_accès, valeur)
    """
    from asyncua.common.node import Node
    variables = []
    
    if depth > max_depth:
        return variables
    
    try:
        # Obtenir les enfants du node
        children = await node.get_children()
        
        for child in children:
            try:
                # Assurer que c'est un Node
                if not isinstance(child, Node):
                    child = Node(client, child)
                
                # Obtenir le browse name
                browse_name = await child.read_browse_name()
                if hasattr(browse_name, 'Name'):
                    name = browse_name.Name
                else:
                    name = str(browse_name)
                
                # Obtenir le node class pour déterminer le type
                node_class = await child.read_node_class()
                
                # Si c'est une variable
                if node_class == ua.NodeClass.Variable:
                    # Obtenir la valeur
                    try:
                        value = await child.get_value()
                        
                        # Obtenir le mode d'accès
                        access_mode = await get_access_mode(child, client)
                        
                        full_name = f"{parent_name}.{name}" if parent_name else name
                        variables.append((full_name, access_mode, str(value)))
                    except Exception as e:
                        pass
                elif node_class == ua.NodeClass.Object:
                    # C'est un objet, parcourir ses enfants
                    # Ignorer les objets système (Server, Aliases, etc.)
                    if name in ("Server", "Alias", "Aliases", "Types", "Views", "Views", "Objects", "Localization"):
                        continue
                    full_parent = f"{parent_name}.{name}" if parent_name else name
                    child_vars = await browse_variables(child, client, depth + 1, max_depth, full_parent)
                    variables.extend(child_vars)
                        
            except Exception:
                continue
                
    except Exception:
        pass
    
    return variables


async def analyze_plc(url: str) -> None:
    """
    Analyse un serveur OPCUA et affiche ses variables.
    
    Args:
        url: URL du serveur OPCUA
    """
    print("=" * 60)
    print(f"  ANALYSE DU PLC: {url}")
    print("=" * 60)
    
    client = Client(url=url)
    
    try:
        # Connexion au serveur
        print(f"[INFO] Connexion à {url}...")
        await client.connect()
        print("[INFO] Connecté avec succès!\n")
        
        # Obtenir le node racine
        root = client.nodes.root
        objects_node = client.nodes.objects
        
        # Afficher les informations du serveur
        try:
            server_info = await client.get_endpoints()
            if server_info:
                print(f"[INFO] Serveur: {server_info[0].Server.ApplicationName.Text}")
                print(f"[INFO] URI: {server_info[0].Server.ApplicationUri}\n")
        except Exception:
            pass
        
        print(f"{'Variable':<40} | {'Accès':<18} | {'Valeur'}")
        print("-" * 75)
        
        # Parcourir les variables
        variables = await browse_variables(objects_node, client)
        
        for var_name, var_access, var_value in variables:
            print(f"{var_name:<40} | {var_access:<18} | {var_value}")
        
        if not variables:
            print("Aucune variable trouvée.")
        
        print("-" * 75)
        print(f"\n[INFO] Analyse terminée.")
        
    except Exception as e:
        print(f"[ERREUR] Impossible de se connecter au serveur: {e}")
        #import traceback
        #traceback.print_exc()
        #sys.exit(1)
        
    finally:
        try:
            await client.disconnect()
            print("[INFO] Déconnecté.")
        except Exception:
            pass


def main() -> None:
    """Point d'entrée principal."""
    args = parse_args()
    asyncio.run(analyze_plc(args.url))


if __name__ == "__main__":
    main()
