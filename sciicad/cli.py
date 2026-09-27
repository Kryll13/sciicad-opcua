"""Arguments de ligne de commande communs aux simulateurs PLC et aux serveurs
de découverte.
"""

from __future__ import annotations

import argparse
from typing import Any, Optional

# Valeurs acceptées pour neutraliser l'enregistrement auprès du LDS.
_LDS_DISABLED = ("", "none", "off", "disabled")

_LDS_SCHEMES = ("opc.tcp://", "opc.wss://", "opc.https://")


def port(value: str) -> int:
    """Valide un port. argparse accepte n'importe quelle chaîne, sinon."""
    try:
        parsed = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"port invalide : {value!r}")
    if not 1 <= parsed <= 65535:
        raise argparse.ArgumentTypeError(f"port hors plage : {parsed} (attendu 1..65535)")
    return parsed


def ttl(value: str) -> int:
    """Valide une durée en secondes.

    Borne basse alignée sur le validateur de ``DiscoveryConfig`` (10 s) : une
    valeur plus courte expirerait les entrées avant leur premier
    renouvellement.
    """
    try:
        parsed = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"durée invalide : {value!r}")
    if parsed < 10:
        raise argparse.ArgumentTypeError(
            f"durée trop courte : {parsed}s (attendu >= 10s)"
        )
    return parsed


def lds_url(value: str) -> str:
    """Valide l'URL d'un LDS, et permet de désactiver l'enregistrement.

    Retourne une chaîne vide pour ``vide``, ``none``, ``off`` ou ``disabled`` :
    l'appelant décide alors de ne pas créer de :class:`LdsRegistrar`.
    """
    if not value or value.lower() in _LDS_DISABLED:
        return ""
    if not value.startswith(_LDS_SCHEMES):
        raise argparse.ArgumentTypeError(
            f"URL de LDS invalide : {value!r} (attendu opc.tcp://hôte:port)"
        )
    return value


def log_level(value: str) -> str:
    """Valide un niveau de journalisation pour ``--log-level``.

    Toute la verbosité des serveurs et des simulateurs passe par loguru, donc
    le niveau doit se régler ici et nulle part ailleurs. Un niveau arbitraire
    serait silencieusement accepté puis jamais atteint : mieux vaut un refus à
    la ligne de commande qu'un ``--log-level VERBOSE`` qui n'verbe pas.
    """
    from sciicad.console import LOG_LEVELS, check_level

    try:
        return check_level(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc))


def add_log_level(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Ajoute ``--log-level``, commun à tous les rôles en service."""
    parser.add_argument(
        "--log-level",
        type=log_level,
        default="INFO",
        metavar="NIVEAU",
        help=(
            "niveau de journalisation loguru : "
            "TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL (défaut : INFO). "
            "Toute la verbosité passe par loguru."
        ),
    )
    return parser


def add_plc_arguments(
    parser: argparse.ArgumentParser,
    default_lds: str = "opc.tcp://lds:4840",
) -> argparse.ArgumentParser:
    """Ajoute les arguments communs à un simulateur PLC et renvoie le parser."""
    parser.add_argument(
        "--port",
        type=port,
        default=4840,
        help="Port OPC UA (défaut: 4840)",
    )
    parser.add_argument(
        "--lds",
        type=lds_url,
        default=default_lds,
        help=(
            f"URL du service LDS (défaut: {default_lds}). "
            "Passer une valeur vide ou 'none' pour ne pas s'enregistrer."
        ),
    )
    parser.add_argument(
        "--bind",
        type=str,
        default="0.0.0.0",
        help="adresse d'écoute (défaut: 0.0.0.0, soit toutes les interfaces)",
    )
    parser.add_argument(
        "--advertise",
        type=str,
        default=None,
        help=(
            "hôte annoncé aux clients et au LDS (défaut: IP détectée). "
            "Sur une VM, renseigner une IP joignable par les clients."
        ),
    )
    add_log_level(parser)
    return parser


# ---------------------------------------------------------------------------
# Serveurs de découverte (LDS et GDS partagent ces arguments)
# ---------------------------------------------------------------------------


def add_discovery_arguments(
    parser: argparse.ArgumentParser,
    config_filename: str,
    with_ttl: bool = True,
) -> argparse.ArgumentParser:
    """Ajoute les arguments communs au LDS et au GDS.

    ``with_ttl`` est à ``False`` pour le GDS : en portée globale rien
    n'expire, donc le TTL n'aurait aucun effet. L'exposer quand même
    inviterait à croire qu'il pilote quelque chose.
    """
    parser.add_argument(
        "--config",
        default=None,
        help=(
            f"fichier de configuration YAML. Par défaut, cherche "
            f"{config_filename} dans le répertoire courant puis dans le "
            f"dossier du composant."
        ),
    )
    parser.add_argument(
        "--port", type=port, default=None, help="port d'écoute (défaut : valeur du config)"
    )
    parser.add_argument(
        "--bind",
        type=str,
        default=None,
        help="adresse d'écoute (défaut : 0.0.0.0, soit toutes les interfaces)",
    )
    parser.add_argument(
        "--advertise",
        type=str,
        default=None,
        help="hôte annoncé aux clients (défaut : hostname de la machine)",
    )
    if with_ttl:
        parser.add_argument(
            "--ttl",
            type=ttl,
            default=None,
            help=(
                "durée de vie d'une entrée sans renouvellement, en secondes "
                "(défaut : 300)"
            ),
        )
    parser.add_argument(
        "--database",
        type=str,
        default=None,
        help="chemin du fichier SQLite (défaut : valeur du config)",
    )
    parser.add_argument(
        "--no-database",
        action="store_true",
        help="désactive la persistance (registre en mémoire seule)",
    )
    add_log_level(parser)
    return parser


def apply_discovery_arguments(config: Any, args: argparse.Namespace) -> Any:
    """Applique la ligne de commande sur une configuration, puis revalide.

    pydantic ne rejoue pas les validateurs après une affectation directe, et
    ``model_validate`` crée un objet neuf : l'origine du fichier doit donc être
    reportée, sans quoi le diagnostic dirait à tort « valeurs par défaut ».
    ``type(config)`` est utilisé pour préserver la classe (GDSConfig et non
    LDSConfig).
    """
    if getattr(args, "port", None) is not None:
        config.server.port = args.port
    if getattr(args, "bind", None) is not None:
        config.server.bind_address = args.bind
    if getattr(args, "advertise", None) is not None:
        config.server.advertise_host = args.advertise
    if getattr(args, "ttl", None) is not None:
        config.discovery.entry_ttl_seconds = args.ttl
    if getattr(args, "no_database", False):
        config.database.enabled = False
    if getattr(args, "database", None) is not None:
        config.database.path = args.database

    revalidated = type(config).model_validate(config.model_dump())
    revalidated._loaded_from = config._loaded_from
    return revalidated


def load_discovery_config(
    config_class: Any, args: argparse.Namespace
) -> tuple[Any, Optional[Exception]]:
    """Charge la configuration d'un serveur de découverte.

    Distingue un ``--config`` absent (recherche des emplacements par défaut)
    d'un ``--config`` explicite, qui doit exister sous peine d'erreur.

    Retourne ``(config, None)`` ou ``(None, erreur)`` : l'appelant décide du
    code de sortie, qui n'est pas le même dans les deux cas.
    """
    explicit: Optional[str] = getattr(args, "config", None)
    try:
        config = config_class.load(explicit)
    except FileNotFoundError as exc:
        return None, exc
    except Exception as exc:
        return config_class(), exc

    return apply_discovery_arguments(config, args), None
