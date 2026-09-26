#!/usr/bin/env python3
"""
Interrogation et commande du PLC thermostat SCIICAD.

    uv run tools/ihm_action.py --ip 193.168.1.90
    uv run tools/ihm_action.py --ip 193.168.1.90 --heat on
    uv run tools/ihm_action.py --ip 193.168.1.90 --heat off --maintenance on

Code de sortie non nul si la connexion, la lecture ou l'écriture échoue.
"""

import argparse
import asyncio
import sys

from asyncua import Client

from sciicad.console import banner, logger, setup
from sciicad.model import (
    THERMOSTAT_OBJECT,
    THERMOSTAT_UNITS,
    THERMOSTAT_VARIABLE_NAMES,
)
from sciicad.nodes import find_node_by_name, read_child_values


def parse_args(argv=None):
    """Analyse les arguments de la ligne de commande."""
    parser = argparse.ArgumentParser(
        description="Interrogation et commande du PLC thermostat SCIICAD",
    )
    parser.add_argument(
        "--ip", default="localhost",
        help="adresse du PLC (défaut : localhost)",
    )
    parser.add_argument(
        "--port", type=int, default=4840,
        help="port du PLC (défaut : 4840)",
    )
    parser.add_argument(
        "--heat", choices=("on", "off"),
        help="passer le chauffage on ou off (absent : ne rien écrire)",
    )
    parser.add_argument(
        "--maintenance", choices=("on", "off"),
        help="passer le mode maintenance on ou off (absent : ne rien écrire)",
    )
    parser.add_argument(
        "--timeout", type=float, default=15.0,
        help="délai maximal par appel réseau, en secondes (défaut : 15)",
    )
    return parser.parse_args(argv)


async def find_thermostat(client: Client):
    """Retourne le nœud ``Objects/Thermostat``, ou ``None``."""
    return await find_node_by_name(client.nodes.objects, THERMOSTAT_OBJECT)


async def write_flag(thermostat, name: str, state: bool) -> bool:
    """Écrit une variable booléenne du thermostat et confirme par relecture."""
    node = await find_node_by_name(thermostat, name)
    if node is None:
        logger.error(f"Variable {name} introuvable sous {THERMOSTAT_OBJECT}")
        return False
    try:
        await node.set_value(state)
    except Exception as exc:
        logger.error(f"Écriture de {name} refusée : {type(exc).__name__}: {exc}")
        return False

    # Relecture : une écriture acceptée par le transport peut être refusée par
    # le serveur (variable non inscriptible, droits insuffisants).
    confirmed = await node.get_value()
    if bool(confirmed) is not state:
        logger.error(
            f"{name} : écriture demandée {state}, valeur relue {confirmed} "
            "(écriture refusée par le serveur ?)"
        )
        return False
    logger.info(f"{name} = {'ON' if state else 'OFF'} (confirmé par relecture)")
    return True


def format_value(name: str, value) -> str:
    """Met en forme une valeur pour l'affichage, avec son unité."""
    if isinstance(value, float):
        text = f"{value:.2f}"
    elif isinstance(value, bool):
        text = "ON" if value else "OFF"
    else:
        text = str(value)
    unit = THERMOSTAT_UNITS.get(name)
    return f"{text} {unit}" if unit else text


async def main(argv=None) -> int:
    setup()
    args = parse_args(argv)
    url = f"opc.tcp://{args.ip}:{args.port}"

    client = Client(url=url)
    try:
        await asyncio.wait_for(client.connect(), args.timeout)
        logger.info(f"Connecté au PLC : {url}\n")

        thermostat = await find_thermostat(client)
        if thermostat is None:
            logger.error(
                f"Objet {THERMOSTAT_OBJECT} introuvable. Nœuds sous Objects :"
            )
            for child in await client.nodes.objects.get_children():
                try:
                    logger.info(f"    - {child.nodeid}")
                except Exception:
                    logger.info("    - (nœud sans identifiant)")
            return 1

        failures = 0
        if args.heat is not None:
            if not await write_flag(thermostat, "Heating", args.heat == "on"):
                failures += 1

        if args.maintenance is not None:
            if not await write_flag(
                thermostat, "MaintenanceMode", args.maintenance == "on"
            ):
                failures += 1

        values = await read_child_values(thermostat, list(THERMOSTAT_VARIABLE_NAMES))

        logger.info(banner("ÉTAT DU THERMOSTAT", 50))
        for name in THERMOSTAT_VARIABLE_NAMES:
            logger.info(f"  {name:18s} : {format_value(name, values.get(name))}")

    except Exception as exc:
        logger.error(f"Erreur : {type(exc).__name__}: {exc}")
        return 1
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
