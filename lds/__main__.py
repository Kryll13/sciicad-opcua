"""Point d'entrée en ligne de commande du LDS.

    python -m lds [--config lds_config.yaml] [--port 4840]
                  [--bind 0.0.0.0] [--advertise <hôte>] [--no-database]
                  [--log-level DEBUG]
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from loguru import logger
from sciicad.cli import add_discovery_arguments, load_discovery_config
from sciicad.console import setup_server

from .config import DEFAULT_CONFIG_FILENAME, LDSConfig
from .server import LocalDiscoveryServer


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m lds",
        description="Serveur de découverte local OPC UA (LDS) — projet SCIICAD",
    )
    add_discovery_arguments(parser, DEFAULT_CONFIG_FILENAME, with_ttl=True)
    return parser.parse_args(argv)


def build_config(args: argparse.Namespace) -> LDSConfig:
    """Construit la configuration, la ligne de commande primant sur le YAML."""
    config, error = load_discovery_config(LDSConfig, args)
    if isinstance(error, FileNotFoundError):
        # Un --config explicite et absent est une erreur de l'utilisateur : il
        # faut le dire, pas démarrer silencieusement avec les défauts.
        raise error
    if error is not None:
        logger.error(f"Configuration illisible ({error}) : valeurs par défaut")
    return config


async def _main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    # Avant toute journalisation : une erreur de configuration doit être
    # rapportée au niveau demandé, pas au format par défaut de loguru.
    setup_server(args.log_level)

    try:
        config = build_config(args)
    except Exception as exc:
        logger.error(f"Configuration invalide : {exc}")
        return 2

    # Toujours dire d'où vient la configuration : une valeur par défaut
    # appliquée silencieusement est indétectable le jour où le YAML diverge.
    logger.info(f"Configuration : {config.source}")
    logger.info(
        f"Valeurs effectives : port={config.server.port} "
        f"bind={config.server.bind_address} "
        f"advertise={config.server.resolve_advertise_host()} "
        f"ttl={config.discovery.entry_ttl_seconds}s "
        f"base={'désactivée' if not config.database.enabled else config.database.path}"
    )

    lds = LocalDiscoveryServer(config)
    try:
        await lds.run_forever()
    except asyncio.CancelledError:
        await lds.stop()
    except OSError as exc:
        logger.error(f"Démarrage impossible : {exc}")
        await lds.stop()
        return 1
    return 0


def main() -> None:
    sys.exit(asyncio.run(_main()))


if __name__ == "__main__":
    main()
