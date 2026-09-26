#!/usr/bin/env python3
"""
Inspecte l'espace d'adressage d'un serveur OPC UA.

Pour chaque variable, affiche le chemin, le niveau d'accès réel et la
valeur. C'est l'outil de référence pour vérifier ce qu'un PLC publie
réellement, indépendamment de ce que dit la documentation.

Code de sortie non nul si le serveur est injoignable ou ne publie aucune
variable.

    uv run tools/analyze.py -u opc.tcp://193.168.1.90:4840
"""

import argparse
import asyncio
import sys

from asyncua import Client

from sciicad.console import banner, logger, setup
from sciicad.nodes import (
    STANDARD_TREES,
    access_label,
    find_by_path,
    list_children,
    server_array,
)


def parse_args(argv=None):
    """Analyse les arguments de la ligne de commande."""
    parser = argparse.ArgumentParser(
        description="Inspecte l'espace d'adressage d'un serveur OPC UA",
    )
    parser.add_argument(
        "-u", "--url", required=True,
        help="URL du serveur, ex : opc.tcp://193.168.1.90:4840",
    )
    parser.add_argument(
        "--depth", type=int, default=3,
        help="profondeur de parcours (défaut : 3)",
    )
    parser.add_argument(
        "--timeout", type=float, default=15.0,
        help="délai maximal par appel, en secondes (défaut : 15)",
    )
    return parser.parse_args(argv)


def format_value(value) -> str:
    """Met en forme une valeur pour l'affichage en colonne."""
    if isinstance(value, float):
        return f"{value:.3f}"
    if isinstance(value, bool):
        return "Vrai" if value else "Faux"
    if isinstance(value, (list, tuple)) and len(value) > 4:
        return f"[{len(value)} éléments]"
    text = str(value)
    return text if len(text) <= 40 else text[:37] + "..."


async def analyse(url: str, depth: int, timeout: float) -> int:
    """Parcourt l'espace d'adressage et affiche le résultat."""
    client = Client(url=url)
    try:
        await asyncio.wait_for(client.connect(), timeout)
    except Exception as exc:
        logger.error(f"Connexion impossible : {type(exc).__name__}: {exc}")
        return 1

    try:
        logger.info(f"Connecté à {url}")
        identity = await server_array(client)
        if identity:
            logger.info(f"  applicationUri : {identity}")
        logger.info(f"  namespaces     : {await client.get_namespace_array()}")

        # On parcourt puis on retrouve chaque nœud par browse name : c'est le
        # seul moyen d'en lire le niveau d'accès sans identifiant codé en dur.
        rows = await asyncio.wait_for(
            list_children(client.nodes.objects, depth, skip_roots=STANDARD_TREES), timeout
        )
        variables = [row for row in rows if row[1] is not None]
        if not variables:
            logger.warning(
                "Aucune variable lisible. L'espace d'adressage est-il vide, "
                "ou le parcours trop court ? (--depth)"
            )
            return 1

        logger.info("")
        logger.info(f"{'Variable':40s} | {'Accès':16s} | Valeur")
        logger.info("-" * 74)
        for path, _ in variables:
            node = await find_by_path(client.nodes.objects, path)
            if node is None:
                continue
            level = await access_label(node)
            value = await node.get_value()
            logger.info(f"{path:40s} | {level:16s} | {format_value(value)}")
        logger.info("-" * 74)
        logger.info(f"{len(variables)} variable(s) affichée(s)")

    except Exception as exc:
        logger.error(f"Erreur : {type(exc).__name__}: {exc}")
        return 1
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass

    return 0


async def main(argv=None) -> int:
    setup()
    args = parse_args(argv)
    logger.info(banner("Analyse de l'espace d'adressage"))
    logger.info(f"  {args.url}\n")
    return await analyse(args.url, args.depth, args.timeout)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
