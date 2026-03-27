"""
Serveur OPCUA pour simulation de régulation thermique

Dépendances:
    uv add asyncua

Lancement:
    uv run plc_server.py
"""

import asyncio
import random
import socket
from asyncua import Server, ua
from asyncua.common.node import Node

# Configuration du serveur
SERVER_NAME = "SCIICAD PLC Thermostat Server"

# Variables de simulation
TEMPERATURE = 20.0
HEATING_ON = False
MAINTENANCE_MODE = False
TEMP_MIN = 10.0
TEMP_MAX = 30.0


def get_host_info():
    """Get hostname and IP address."""
    hostname = socket.gethostname()
    try:
        ip = socket.gethostbyname(hostname)
    except socket.gaierror:
        ip = "127.0.0.1"
    return hostname, ip


async def create_thermometer_object(server: Server, parent_node: Node, idx: int):
    """
    Crée un objet Thermostat dans l'espace d'adressage OPCUA.
    
    Structure:
        Objects
        └── Thermostat (Object)
            ├── Heating (Boolean) - Commande chauffage ON/OFF
            ├── Temperature (Float) - Température courante (°C)
            ├── HighTempAlarm (Boolean) - Alarme haute température (>25°C)
            └── LowTempAlarm (Boolean) - Alarme basse température (<15°C)
    """
    # Créer l'objet Thermostat
    thermostat_obj = await parent_node.add_object(
        idx,
        "Thermostat"
    )
    
    # Variable Heating (lecture/écriture)
    heating_var = await thermostat_obj.add_variable(
        idx,
        "Heating",
        False
    )
    await heating_var.set_writable(True)
    
    # Variable Temperature (lecture seule)
    temperature_var = await thermostat_obj.add_variable(
        idx,
        "Temperature",
        20.0
    )
    
    # Variable HighTempAlarm (lecture seule)
    high_temp_var = await thermostat_obj.add_variable(
        idx,
        "HighTempAlarm",
        False
    )
    
    # Variable LowTempAlarm (lecture seule)
    low_temp_var = await thermostat_obj.add_variable(
        idx,
        "LowTempAlarm",
        False
    )
    
    # Variable MaintenanceMode (lecture/écriture)
    maintenance_var = await thermostat_obj.add_variable(
        idx,
        "MaintenanceMode",
        False
    )
    await maintenance_var.set_writable(True)
    
    return {
        "heating": heating_var,
        "temperature": temperature_var,
        "high_temp": high_temp_var,
        "low_temp": low_temp_var,
        "maintenance": maintenance_var
    }


async def temperature_simulation(nodes: dict):
    """
    Tâche asynchrone simulant la variation de température.
    """
    global TEMPERATURE, HEATING_ON, MAINTENANCE_MODE
    
    while True:
        # Lire l'état du mode maintenance
        MAINTENANCE_MODE = await nodes["maintenance"].get_value()
        
        # Si mode maintenance, ne pas simuler
        if MAINTENANCE_MODE:
            print(f"MODE MAINTENANCE ACTIVÉ - Boucle de température arrêtée")
            await asyncio.sleep(1)
            continue
        
        # Lire l'état du chauffage
        HEATING_ON = await nodes["heating"].get_value()
        
        # Faire varier la température
        if HEATING_ON:
            TEMPERATURE += random.uniform(0.1, 0.5)
        else:
            TEMPERATURE -= random.uniform(0.1, 0.3)
        
        # Bornes de température
        TEMPERATURE = max(TEMP_MIN, min(TEMP_MAX, TEMPERATURE))
        
        # Calculer les alarmes
        high_temp = TEMPERATURE > 25.0
        low_temp = TEMPERATURE < 15.0
        
        # Écrire les valeurs dans les nodes OPCUA
        await nodes["temperature"].set_value(TEMPERATURE)
        await nodes["high_temp"].set_value(high_temp)
        await nodes["low_temp"].set_value(low_temp)
        
        # Affichage console
        status = "ON" if HEATING_ON else "OFF"
        print(f"T = {TEMPERATURE:.1f} °C | Chauffage={status} | >25={high_temp} | <15={low_temp}")
        
        await asyncio.sleep(1)


async def main():
    """
    Point d'entrée principal du serveur OPCUA.
    """
    # Afficher les informations du serveur
    host, ip = get_host_info()
    endpoint = f"opc.tcp://{ip}:4840"
    application_uri = f"urn:SCIICAD:{host}"

    print(f"=" * 50)
    print(f"  Serveur OPCUA - Thermostat Simulation")
    print(f"=" * 50)
    print(f"[INFO] IP du serveur: {ip}")
    print(f"[INFO] Hostname du serveur: {host}")
    print(f"[INFO] Endpoint: {endpoint}")
    print(f"=" * 50)
    

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
        ]
    )

    # Enregistrer le serveur auprès du LDS
    lds_url = "opc.tcp://lds:4840"
    print(f"Enregistrement auprès du LDS: {lds_url}")
    try:
        await server.register_to_discovery(lds_url, 60)
        print("Serveur enregistré auprès du LDS!")
    except Exception as e:
        print(f"Note: LDS non disponible: {e}")

    # TODO: Enregistrer le serveur auprès du LDS (si nécessaire)
    # await server.register_to_discovery("opc.tcp://127.0.0.1:4840", 60)
    
    # Configurer les méthodes d'authentification supportées
    # Par défaut, asyncua supporte Anonymous, Basic256 et Certificate
    # Aucune configuration supplémentaire n'est nécessaire pour anonymous
    
    # Enregistrer un namespace
    uri = "http://thermo.example.org/"
    idx = await server.register_namespace(uri)
    print(f"Namespace enregistré: idx={idx}, uri={uri}")
    
    # Créer l'objet Thermostat et ses variables
    objects_node = server.get_objects_node()
    nodes = await create_thermometer_object(server, objects_node, idx)
    
    # Afficher les Node IDs pour debug
    print(f"\nNode IDs créés:")
    for name, node in nodes.items():
        print(f"  - {name}: {node.nodeid}")
    
    # Démarrer le serveur en arrière-plan
    async with server:
        print(f"Serveur OPCUA démarré sur {endpoint}")
        print("Nodes disponibles:")
        print(f"  - Thermostat.Heating (Boolean, lecture/écriture)")
        print(f"  - Thermostat.Temperature (Float, lecture seule)")
        print(f"  - Thermostat.HighTempAlarm (Boolean, lecture seule)")
        print(f"  - Thermostat.LowTempAlarm (Boolean, lecture seule)")
        print("\nAppuyez sur Ctrl+C pour arrêter...\n")
        
        # Lancer la simulation de température
        await temperature_simulation(nodes)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServeur OPCUA arrêté.")
