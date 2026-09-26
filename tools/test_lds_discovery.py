#!/usr/bin/env python3
"""
Liste les serveurs enregistrés auprès d'un serveur de découverte.

Complément léger de ``test_discovery.py``, qui va jusqu'à la lecture de
l'espace d'adressage d'un PLC. Utile pour vérifier d'un coup d'œil ce qu'un
LDS publie réellement.

Code de sortie non nul si l'endpoint est injoignable.

    uv run tools/test_lds_discovery.py --url opc.tcp://193.168.1.20:4840
"""

import argparse
import asyncio
import sys

from asyncua import Client

from sciicad.console import banner, logger, setup
from sciicad.nodes import server_array

# ApplicationType de la norme, pour un affichage lisible.
_TYPE_NAMES = {0: "Server", 1: "Client", 2: "ClientAndServer", 3: "DiscoveryServer"}


def parse_args(argv=None):
    """Analyse les arguments de la ligne de commande."""
    parser = argparse.ArgumentParser(
        description="Liste les serveurs enregistrés d'un serveur de découverte",
    )
    parser.add_argument(
        "--url", default="opc.tcp://127.0.0.1:4840",
        help="URL du LDS (défaut : opc.tcp://127.0.0.1:4840)",
    )
    parser.add_argument(
        "--timeout", type=float, default=15.0,
        help="délai maximal par appel, en secondes (défaut : 15)",
    )
    return parser.parse_args(argv)


async def main(argv=None) -> int:
    setup()
    args = parse_args(argv)

    logger.info(banner(f"Découverte — {args.url}"))

    # GetEndpoints passe par une session dans asyncua, contrairement à
    # FindServers : une session complète est donc ouverte. L'outil est de
    # courte durée, la tâche de surveillance est nettoyée à la
    # déconnexion.
    client = Client(args.url)
    try:
        await asyncio.wait_for(client.connect(), args.timeout)
    except Exception as exc:
        logger.error(f"Connexion impossible : {type(exc).__name__}: {exc}")
        return 1

    try:
        identity = await server_array(client)
        if identity:
            logger.info(f"  applicationUri : {identity}")
        namespaces = await asyncio.wait_for(client.get_namespace_array(), args.timeout)
        logger.info(f"  namespaces     : {namespaces}")

        servers = await asyncio.wait_for(client.find_servers(), args.timeout)
        logger.info(f"\n  {len(servers)} serveur(s) enregistré(s) :")
        for index, server in enumerate(sorted(servers, key=lambda s: s.ApplicationUri or ""), 1):
            name = getattr(getattr(server, "ApplicationName", None), "Text", "")
            kind = _TYPE_NAMES.get(getattr(server, "ApplicationType_", None), "?")
            logger.info(f"\n  {index}. {server.ApplicationUri}")
            logger.info(f"     nom          : {name}")
            logger.info(f"     type         : {kind}")
            logger.info(f"     productUri   : {server.ProductUri or '(aucun)'}")
            for url in server.DiscoveryUrls or []:
                logger.info(f"     endpoint     : {url}")

        endpoints = await asyncio.wait_for(client.get_endpoints(), args.timeout)
        logger.info(f"\n  Endpoints publiés par ce serveur : {len(endpoints)}")
        for endpoint in endpoints:
            logger.info(
                f"     {endpoint.EndpointUrl}  "
                f"[{endpoint.SecurityMode.name}, "
                f"{endpoint.SecurityPolicyUri.rsplit('#', 1)[-1]}]"
            )

    except Exception as exc:
        logger.error(f"Erreur : {type(exc).__name__}: {exc}")
        return 1
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass

    logger.info(banner("Fin"))
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
