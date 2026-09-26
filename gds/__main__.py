"""Point d'entrée en ligne de commande du GDS.

    python -m gds [--config gds_config.yaml] [--port 4840]
                  [--bind 0.0.0.0] [--advertise <hôte>] [--no-database]

``--ttl`` est volontairement absent : en portée globale une inscription ne
expire pas, le paramètre n'aurait aucun effet. Les autres arguments sont
ceux du LDS, gérés par :mod:`sciicad.cli`.
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from loguru import logger
from sciicad.cli import add_discovery_arguments, load_discovery_config

from .config import DEFAULT_CONFIG_FILENAME, GDSConfig
from .server import GlobalDiscoveryServer


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m gds",
        description="Serveur de découverte global OPC UA (GDS) — projet SCIICAD",
    )
    add_discovery_arguments(parser, DEFAULT_CONFIG_FILENAME, with_ttl=False)
    return parser.parse_args(argv)


def build_config(args: argparse.Namespace) -> GDSConfig:
    """Construit la configuration, la ligne de commande primant sur le YAML."""
    config, error = load_discovery_config(GDSConfig, args)
    if isinstance(error, FileNotFoundError):
        raise error
    if error is not None:
        logger.error(f"Configuration illisible ({error}) : valeurs par défaut")
    return config


async def _main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        config = build_config(args)
    except Exception as exc:
        logger.error(f"Configuration invalide : {exc}")
        return 2

    logger.remove()
    logger.add(sys.stderr, level="INFO")

    # Toujours dire d'où vient la configuration : une valeur par défaut
    # appliquée silencieusement est indétectable le jour où le YAML diverge.
    logger.info(f"Configuration : {config.source}")
    logger.info(
        f"Valeurs effectives : port={config.server.port} "
        f"bind={config.server.bind_address} "
        f"advertise={config.server.resolve_advertise_host()} "
        f"portée={config.discovery.scope} "
        f"base={'désactivée' if not config.database.enabled else config.database.path}"
    )
    logger.info(f"Endpoint annoncé : {config.server.endpoint_url}")

    gds = GlobalDiscoveryServer(config)
    try:
        await gds.run_forever()
    except asyncio.CancelledError:
        await gds.stop()
    except OSError as exc:
        logger.error(f"Démarrage impossible : {exc}")
        await gds.stop()
        return 1
    return 0


def main() -> None:
    sys.exit(asyncio.run(_main()))


if __name__ == "__main__":
    main()
