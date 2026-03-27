import asyncio
import socket
from asyncua import Server, ua
from datetime import datetime
from loguru import logger


PRODUCT_URI = "urn:CEC:Python-Asyncua.Application:lds"
MANUFACTURER_NAME = "CEC"
PRODUCT_NAME = "Python Asyncua LDS Server"
SOFTWARE_VERSION = "1.0"
BUILD_NUMBER = '1'
BUILD_DATE = datetime.now()

SERVER_NAME = "SCIICAD LDS Server"

def get_host_info():
    """Get hostname and IP address."""
    hostname = socket.gethostname()
    try:
        ip = socket.gethostbyname(hostname)
    except socket.gaierror:
        ip = "127.0.0.1"
    return hostname, ip

async def main():
    host, ip = get_host_info()
    endpoint = f"opc.tcp://{ip}:4840"
    application_uri = f"urn:SCIICAD:lds"

    server = Server()
    await server.init()
    server.set_endpoint(endpoint)
    server.set_server_name(SERVER_NAME)

    server.set_security_policy(
        [
            ua.SecurityPolicyType.NoSecurity,
        ]
    )   

    await server.set_application_uri(application_uri)
    await server.set_build_info(PRODUCT_URI, MANUFACTURER_NAME, PRODUCT_NAME, SOFTWARE_VERSION, BUILD_NUMBER, BUILD_DATE)
    server.product_uri = PRODUCT_URI
    server.manufacturer_name = MANUFACTURER_NAME

    # Activer le mode discovery server
    # Cela permet de répondre aux requêtes FindServers, GetEndpoints
    # et d'enregistrer les serveurs via RegisterServer
    server.discovery_server_flag = True
    
    logger.success(f"LDS démarré sur {endpoint}")
    logger.info("Services LDS disponibles:")
    logger.info("- FindServers: Liste des serveurs enregistrés")
    logger.info("- GetEndpoints: Endpoints du serveur")
    logger.info("- RegisterServer: Enregistrement d'un serveur")
    logger.info("- RegisterServer2: Enregistrement étendu")
    logger.info("Appuyez sur Ctrl+C pour arrêter...\n")    

    async with server:
        # Capturer les interruptions clavier pour un arrêt propre
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.error("Fonctionnement interrompu par l'utilisateur.")


if __name__ == "__main__":
    asyncio.run(main())
