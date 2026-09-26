"""Point d'entrée en ligne de commande du LDS.

    python -m lds [--config lds_config.yaml] [--port 4840]
                  [--bind 0.0.0.0] [--advertise <hôte>] [--no-database]
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from loguru import logger

from .config import DEFAULT_CONFIG_FILENAME, LDSConfig
from .server import LocalDiscoveryServer


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m lds",
        description="Serveur de découverte local OPC UA (LDS) — projet SCIICAD",
    )
    parser.add_argument(
        "--config",
        default=None,
        help=(
            f"fichier de configuration YAML. Par défaut, cherche "
            f"{DEFAULT_CONFIG_FILENAME} dans le répertoire courant puis dans lds/."
        ),
    )
    parser.add_argument(
        "--port", type=int, default=None, help="port d'écoute (défaut : valeur du config)"
    )
    parser.add_argument(
        "--bind",
        default=None,
        help="adresse d'écoute (défaut : 0.0.0.0, soit toutes les interfaces)",
    )
    parser.add_argument(
        "--advertise",
        default=None,
        help="hôte annoncé aux clients (défaut : hostname de la machine)",
    )
    parser.add_argument(
        "--ttl",
        type=int,
        default=None,
        help="durée de vie d'une entrée sans renouvellement, en secondes (défaut : 300)",
    )
    parser.add_argument(
        "--database",
        default=None,
        help="chemin du fichier SQLite (défaut : lds.db)",
    )
    parser.add_argument(
        "--no-database",
        action="store_true",
        help="désactive la persistance (registre en mémoire seule)",
    )
    args = parser.parse_args(argv)
    # Distingue un --config absent (recherche des emplacements par défaut) d'un
    # --config explicite, qui doit exister sous peine d'erreur.
    args.config_was_given = args.config is not None
    return args

def build_config(args: argparse.Namespace) -> LDSConfig:
    """Construit la configuration, la ligne de commande primant sur le YAML."""
    try:
        config = LDSConfig.load(args.config if args.config_was_given else None)
    except FileNotFoundError as exc:
        logger.error(str(exc))
        raise
    except Exception as exc:
        logger.error(f"Configuration illisible ({exc}) : valeurs par défaut")
        config = LDSConfig()

    if args.port is not None:
        config.server.port = args.port
    if args.bind is not None:
        config.server.bind_address = args.bind
    if args.advertise is not None:
        config.server.advertise_host = args.advertise
    if args.ttl is not None:
        config.discovery.entry_ttl_seconds = args.ttl
    if args.no_database:
        config.database.enabled = False
    if args.database is not None:
        config.database.path = args.database

    # Revalider après surcharge : pydantic ne rejoue pas les validateurs.
    # model_validate crée un nouvel objet, l'origine du fichier doit donc être
    # reportée explicitement, sans quoi le diagnostic dirait à tort
    # « valeurs par défaut ».
    revalidated = LDSConfig.model_validate(config.model_dump())
    revalidated._loaded_from = config._loaded_from
    return revalidated


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
