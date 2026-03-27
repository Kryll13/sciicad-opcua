"""
Shared utilities for OPCUA scripts.
Contains common functions used across multiple tools.
"""

import socket
import argparse
import asyncio
import logging
from typing import Optional
from asyncua import Server, ua
from asyncua.common.node import Node


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_server_ip() -> str:
    """
    Determines the server's IP address by connecting to an external address.
    
    Returns:
        str: The server's IP address, or '127.0.0.1' if detection fails.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def check_port_available(host: str, port: int) -> bool:
    """
    Checks if a port is available for use.
    
    Args:
        host: The host address to check.
        port: The port number to check.
    
    Returns:
        bool: True if the port is available, False otherwise.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1)
            result = sock.connect_ex((host, port))
            return result != 0
    except socket.error:
        return True


def parse_server_args(description: str, default_port: int = 4840) -> argparse.Namespace:
    """
    Parse command line arguments for OPCUA server scripts.
    
    Args:
        description: Description for the argument parser.
        default_port: Default port number (default: 4840).
    
    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--port', type=int, default=default_port,
                        help=f'Port du serveur OPCUA (défaut: {default_port})')
    parser.add_argument('--host', type=str, default='0.0.0.0',
                        help='Adresse IP du serveur (défaut: 0.0.0.0)')
    return parser.parse_args()


async def init_server(
    endpoint: str,
    server_name: str,
    product_uri: str,
    application_uri: str,
    namespace_uri: str,
    security_policy: ua.SecurityPolicyType = ua.SecurityPolicyType.NoSecurity,
    lds_url: Optional[str] = None,
    lds_period: int = 60
) -> tuple[Server, int, dict]:
    """
    Initialize an OPCUA server with common configuration.
    
    Args:
        endpoint: The endpoint URL.
        server_name: Name of the server.
        product_uri: Product URI for the server.
        application_uri: Application URI.
        namespace_uri: Namespace URI to register.
        security_policy: Security policy to use.
        lds_url: Optional LDS URL for server registration.
        lds_period: Registration period in seconds.
    
    Returns:
        tuple: (server, namespace_index, created_nodes dict)
    """
    # Create and initialize server
    server = Server()
    await server.init()
    server.set_endpoint(endpoint)
    server.set_server_name(server_name)
    server.product_uri = product_uri
    await server.set_application_uri(application_uri)
    server.set_security_policy([security_policy])
    
    # Register with LDS if URL provided
    if lds_url:
        try:
            await server.register_to_discovery(lds_url, lds_period)
            logger.info(f"Serveur enregistré auprès du LDS: {lds_url}")
        except Exception as e:
            logger.warning(f"LDS non disponible: {e}")
    
    # Register namespace
    idx = await server.register_namespace(namespace_uri)
    logger.info(f"Namespace enregistré: idx={idx}")
    
    return server, idx, {}


async def create_maintenance_object(
    server: Server,
    parent_node: Node,
    idx: int,
    namespace_uri: str
) -> dict:
    """
    Creates a Maintenance object with MaintenanceMode variable.
    
    Args:
        server: The OPCUA server instance.
        parent_node: The parent node to add the object to.
        idx: The namespace index.
        namespace_uri: The namespace URI (used as identifier prefix).
    
    Returns:
        dict: Dictionary containing the created nodes.
    """
    # Create Maintenance object
    maintenance_obj = await parent_node.add_object(idx, "Maintenance")
    
    # Create MaintenanceMode variable (read/write)
    maintenance_var = await maintenance_obj.add_variable(
        idx,
        "MaintenanceMode",
        False
    )
    await maintenance_var.set_writable(True)
    
    return {"maintenance": maintenance_var}


async def create_thermostat_object(
    server: Server,
    parent_node: Node,
    idx: int,
    namespace_uri: str
) -> dict:
    """
    Creates a Thermostat object with common control variables.
    
    Args:
        server: The OPCUA server instance.
        parent_node: The parent node to add the object to.
        idx: The namespace index.
        namespace_uri: The namespace URI.
    
    Returns:
        dict: Dictionary containing the created nodes.
    """
    # Create Thermostat object
    thermostat_obj = await parent_node.add_object(idx, "Thermostat")
    
    # Create variables
    heating_var = await thermostat_obj.add_variable(idx, "Heating", False)
    await heating_var.set_writable(True)
    
    temperature_var = await thermostat_obj.add_variable(idx, "Temperature", 20.0)
    await temperature_var.set_writable(True)
    
    high_temp_alarm_var = await thermostat_obj.add_variable(idx, "HighTempAlarm", False)
    await high_temp_alarm_var.set_writable(True)
    
    low_temp_alarm_var = await thermostat_obj.add_variable(idx, "LowTempAlarm", False)
    await low_temp_alarm_var.set_writable(True)
    
    maintenance_mode_var = await thermostat_obj.add_variable(idx, "MaintenanceMode", False)
    await maintenance_mode_var.set_writable(True)
    
    return {
        "thermostat": thermostat_obj,
        "heating": heating_var,
        "temperature": temperature_var,
        "high_temp_alarm": high_temp_alarm_var,
        "low_temp_alarm": low_temp_alarm_var,
        "maintenance_mode": maintenance_mode_var
    }


async def create_plc_object(
    server: Server,
    parent_node: Node,
    idx: int,
    namespace_uri: str
) -> dict:
    """
    Creates a PLC object with common industrial control variables.
    
    Args:
        server: The OPCUA server instance.
        parent_node: The parent node to add the object to.
        idx: The namespace index.
        namespace_uri: The namespace URI.
    
    Returns:
        dict: Dictionary containing the created nodes.
    """
    # Create PLC object
    plc_obj = await parent_node.add_object(idx, "PLC")
    
    # Create variables
    door_open_var = await plc_obj.add_variable(idx, "DoorOpen", False)
    await door_open_var.set_writable(True)
    
    temperature_var = await plc_obj.add_variable(idx, "Temperature", 0.0)
    await temperature_var.set_writable(True)
    
    pressure_var = await plc_obj.add_variable(idx, "Pressure", 0.0)
    await pressure_var.set_writable(True)
    
    motor_running_var = await plc_obj.add_variable(idx, "MotorRunning", False)
    await motor_running_var.set_writable(True)
    
    timestamp_var = await plc_obj.add_variable(idx, "Timestamp", "")
    await timestamp_var.set_writable(True)
    
    return {
        "plc": plc_obj,
        "door_open": door_open_var,
        "temperature": temperature_var,
        "pressure": pressure_var,
        "motor_running": motor_running_var,
        "timestamp": timestamp_var
    }


def print_server_banner(title: str, server_ip: str, endpoint: str) -> None:
    """
    Print a formatted banner for server startup.
    
    Args:
        title: Title to display in the banner.
        server_ip: IP address of the server.
        endpoint: Endpoint URL.
    """
    print("=" * 50)
    print(f"  {title}")
    print("=" * 50)
    print(f"[INFO] IP du serveur: {server_ip}")
    print(f"[INFO] Endpoint: {endpoint}")
    print("=" * 50)


async def wait_for_interrupt() -> None:
    """
    Wait for keyboard interrupt in a clean way.
    """
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\n" + "=" * 50)
        print("ARRÊT DU SERVEUR")
        print("=" * 50)
