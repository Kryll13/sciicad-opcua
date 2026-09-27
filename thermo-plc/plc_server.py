"""
Simulateur OPC UA d'un PLC de régulation thermique (SCIICAD).

Lancement:
    uv run thermo-plc/plc_server.py --lds opc.tcp://193.168.1.20:4840

Le modèle d'adressage et la simulation thermique sont propres à ce
simulateur ; la partie commune (réseau, enregistrement LDS, arrêt, CLI) vient
du paquet ``sciicad``.
"""

import argparse
import asyncio
import contextlib
import random

from asyncua import Server, ua
from asyncua.common.node import Node
from loguru import logger

from sciicad.cli import add_plc_arguments
from sciicad.console import setup_server
from sciicad.discovery import LdsRegistrar
from sciicad.identity import set_application_identity
from sciicad.lifecycle import install_signal_handlers, run_until_stopped, withdraw_from_lds
from sciicad.net import get_host_info, resolve_endpoints

# Identité du serveur
SERVER_NAME = "SCIICAD PLC Thermostat Server"
APPLICATION_URI = "urn:SCIICAD:thermo-plc"
PRODUCT_URI = "urn:CEC:Python-Asyncua.Application:plc"

# Seuils de la simulation
TEMP_MIN = 10.0
TEMP_MAX = 30.0
TEMP_INIT = 20.0
HIGH_TEMP_THRESHOLD = 25.0
LOW_TEMP_THRESHOLD = 15.0

# État de la simulation. Ces globales n'ont pas vocation à être partagées :
# le serveur ne publie que des valeurs OPC UA, jamais ces variables.
TEMPERATURE = TEMP_INIT
HEATING_ON = False
MAINTENANCE_MODE = False


async def create_thermometer_object(parent_node: Node, idx: int) -> dict:
    """Crée l'objet Thermostat et ses variables dans l'espace d'adressage.

    Structure :
        Objects
        └── Thermostat (Object)
            ├── Heating (Boolean) - commande chauffage, lecture/écriture
            ├── Temperature (Float) - température courante, lecture seule
            ├── HighTempAlarm (Boolean) - alarme haute température
            ├── LowTempAlarm (Boolean) - alarme basse température
            └── MaintenanceMode (Boolean) - mode maintenance, lecture/écriture

    ``idx`` est l'index du namespace. Attention : asyncua interprète un entier
    passé en première position de ``add_*`` comme un index de namespace, puis
    attribue lui-même les identifiants numériques. Les NodeIds obtenus
    dépendent donc de l'ordre de création, pas du code : il faut toujours
    résoudre les nœuds par browse name (voir ``docs/depannage.md``).
    """
    thermostat_obj = await parent_node.add_object(idx, "Thermostat")

    heating_var = await thermostat_obj.add_variable(idx, "Heating", False)
    await heating_var.set_writable(True)

    # Température et alarmes : lectures seules, l'état vient du simulateur.
    temperature_var = await thermostat_obj.add_variable(idx, "Temperature", TEMP_INIT)
    high_temp_var = await thermostat_obj.add_variable(idx, "HighTempAlarm", False)
    low_temp_var = await thermostat_obj.add_variable(idx, "LowTempAlarm", False)

    maintenance_var = await thermostat_obj.add_variable(idx, "MaintenanceMode", False)
    await maintenance_var.set_writable(True)

    return {
        "heating": heating_var,
        "temperature": temperature_var,
        "high_temp": high_temp_var,
        "low_temp": low_temp_var,
        "maintenance": maintenance_var,
    }


async def temperature_simulation(nodes: dict) -> None:
    """Fait varier la température et met à jour les variables OPC UA.

    Boucle infinie : elle est annulée par ``run_until_stopped`` lors de l'arrêt
    du serveur.
    """
    global TEMPERATURE, HEATING_ON, MAINTENANCE_MODE

    while True:
        MAINTENANCE_MODE = await nodes["maintenance"].get_value()
        if MAINTENANCE_MODE:
            logger.info("MODE MAINTENANCE ACTIVÉ - Boucle de température arrêtée")
            await asyncio.sleep(1)
            continue

        HEATING_ON = await nodes["heating"].get_value()

        if HEATING_ON:
            TEMPERATURE += random.uniform(0.1, 0.5)
        else:
            TEMPERATURE -= random.uniform(0.1, 0.3)
        TEMPERATURE = max(TEMP_MIN, min(TEMP_MAX, TEMPERATURE))

        high_temp = TEMPERATURE > HIGH_TEMP_THRESHOLD
        low_temp = TEMPERATURE < LOW_TEMP_THRESHOLD

        await nodes["temperature"].set_value(TEMPERATURE)
        await nodes["high_temp"].set_value(high_temp)
        await nodes["low_temp"].set_value(low_temp)

        status = "ON" if HEATING_ON else "OFF"
        logger.info(
            f"T = {TEMPERATURE:.1f} °C | Chauffage={status} | "
            f">{HIGH_TEMP_THRESHOLD:.0f}={high_temp} | "
            f"<{LOW_TEMP_THRESHOLD:.0f}={low_temp}"
        )
        await asyncio.sleep(1)


async def main(
    port: int = 4840,
    lds_url: str = "opc.tcp://lds:4840",
    bind_address: str = "0.0.0.0",
    advertise_host: str | None = None,
) -> None:
    """Point d'entrée principal du serveur OPC UA."""
    host, ip = get_host_info()
    bind_address, port, endpoint = resolve_endpoints(bind_address, port, advertise_host)

    logger.info("=" * 50)
    logger.info("  Serveur OPCUA - Thermostat Simulation")
    logger.info("=" * 50)
    logger.info(f"Hostname du serveur: {host}")
    logger.info(f"IP détectée: {ip}")
    logger.info(f"Écoute: {bind_address}:{port}")
    logger.info(f"Endpoint annoncé: {endpoint}")
    logger.info("=" * 50)

    server = Server()
    await server.init()
    # socket_address fixe l'écoute, set_endpoint fixe l'annonce.
    server.socket_address = (bind_address, port)
    server.set_endpoint(endpoint)
    server.set_server_name(SERVER_NAME)
    # Passe par le module partagé : set_application_uri seul laisse ServerArray
    # sur l'URI par défaut d'asyncua.
    await set_application_identity(
        server, APPLICATION_URI, product_uri=PRODUCT_URI, server_name=SERVER_NAME
    )

    server.set_security_policy(
        [
            ua.SecurityPolicyType.NoSecurity,
            ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt,
        ]
    )

    # Sans certificat, asyncua n'annonce que NoSecurity. L'échec est un
    # warning et non une erreur : le simulateur doit rester utilisable.
    try:
        await server.load_certificate("thermo-plc/server_certificate.pem")
        await server.load_private_key("thermo-plc/server_private_key.pem")
        logger.info("Certificat et clé privée chargés pour Basic256Sha256_SignAndEncrypt")
    except Exception as exc:
        logger.warning(
            f"Certificat indisponible ({exc}) : seul NoSecurity sera annoncé. "
            "Générer les fichiers avec tools/crypto_opcua.py --output-dir thermo-plc"
        )

    idx = await server.register_namespace(APPLICATION_URI)
    logger.info(f"Namespace enregistré: idx={idx}, uri={APPLICATION_URI}")

    objects_node = server.get_objects_node()
    nodes = await create_thermometer_object(objects_node, idx)

    logger.debug("Node IDs créés:")
    for name, node in nodes.items():
        logger.debug(f"  - {name}: {node.nodeid}")

    registrar = LdsRegistrar(server, lds_url) if lds_url else None
    stopped = asyncio.Event()
    install_signal_handlers(stopped)

    try:
        async with server:
            logger.info(f"Serveur OPCUA démarré sur {endpoint}")
            logger.info("Nodes disponibles:")
            logger.info("  - Thermostat.Heating (Boolean, lecture/écriture)")
            logger.info("  - Thermostat.Temperature (Float, lecture seule)")
            logger.info("  - Thermostat.HighTempAlarm (Boolean, lecture seule)")
            logger.info("  - Thermostat.LowTempAlarm (Boolean, lecture seule)")
            logger.info("  - Thermostat.MaintenanceMode (Boolean, lecture/écriture)")
            logger.info("Appuyez sur Ctrl+C pour arrêter...\n")

            # L'enregistrement n'est lancé qu'une fois le serveur en écoute :
            # sinon un échec de bind laisserait une entrée morte dans le LDS.
            if registrar is not None:
                logger.info(f"Enregistrement auprès du LDS : {lds_url}")
                registrar.start()

            with contextlib.suppress(asyncio.CancelledError):
                await run_until_stopped(
                    temperature_simulation(nodes), stopped, label="simulation de température"
                )
    finally:
        # Le finally garantit le retrait du LDS même en cas d'exception :
        # le LDS ne doit jamais annoncer un endpoint fermé.
        if registrar is not None:
            await withdraw_from_lds(registrar)


if __name__ == "__main__":
    parser = add_plc_arguments(
        argparse.ArgumentParser(
            description="Simulateur OPC UA - PLC de régulation thermique SCIICAD"
        )
    )
    args = parser.parse_args()

    # Toute la verbosité passe par loguru : le niveau se règle ici, et nulle
    # part ailleurs dans le processus.
    setup_server(args.log_level)

    try:
        asyncio.run(main(args.port, args.lds, args.bind, args.advertise))
    except KeyboardInterrupt:
        # Propagé seulement après la fin de asyncio.run : le retrait du LDS a
        # donc déjà eu lieu dans main().
        logger.info("Serveur OPCUA arrêté.")
