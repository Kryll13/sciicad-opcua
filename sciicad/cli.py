"""Arguments de ligne de commande communs aux simulateurs PLC."""

from __future__ import annotations

import argparse

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
    return parser
