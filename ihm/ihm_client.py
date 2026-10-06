#!/usr/bin/env python3
"""
IHM de supervision : affichage continu du PLC thermostat SCIICAD.

    uv run ihm/ihm_client.py --host 193.168.1.90

L'affichage est rafraîchi en continu sur une même ligne. L'affichage suppose
un terminal : redirigé vers un fichier, la sortie est bufferisée (utiliser
PYTHONUNBUFFERED=1 pour la journaliser).
"""

import argparse
import asyncio
import contextlib
import signal
import sys

from asyncua import Client

from sciicad.console import banner, logger
from sciicad.trusted import secure_client
from sciicad.model import (
    THERMOSTAT_HIGH_THRESHOLD,
    THERMOSTAT_LOW_THRESHOLD,
    THERMOSTAT_OBJECT,
    THERMOSTAT_VARIABLE_NAMES,
)
from sciicad.nodes import find_node_by_name, read_child_values

DEFAULT_HOST = "thermo-plc"
DEFAULT_PORT = 4840
REFRESH_SECONDS = 0.5


async def build_secure_client(args, url):
    """Construit le client sécurisé, ou ``None`` si les fichiers manquent.

    Un ``None`` n'est pas un échec silencieux : l'appelant l'annonce et
    explique ce que le mode dégradé signifie. Le contraire — un client qui
    se connecte en laissant croire à une validation — serait le pire des deux.
    """
    from pathlib import Path

    if not (Path(args.client_cert).is_file() and Path(args.client_key).is_file()):
        logger.warning(
            f"Certificat client absent : {args.client_cert} / {args.client_key}. "
            f"Générez-le : python tools/bootstrap_certificates.py --signed"
        )
        return None

    directories = [args.trust, "pki/ca"]
    return await secure_client(
        url,
        args.client_cert,
        args.client_key,
        trust_directories=directories,
        expected_uri=args.expect,
        application_uri="urn:SCIICAD:ihm",
    )


def parse_args(argv=None):
    """Analyse les arguments de la ligne de commande."""
    parser = argparse.ArgumentParser(
        description="IHM de supervision du PLC thermostat SCIICAD",
    )
    parser.add_argument(
        "--host", default=DEFAULT_HOST,
        help=f"hôte du PLC (défaut : {DEFAULT_HOST})",
    )
    parser.add_argument(
        "--port", type=int, default=DEFAULT_PORT,
        help=f"port du PLC (défaut : {DEFAULT_PORT})",
    )
    parser.add_argument(
        "--timeout", type=float, default=15.0,
        help="délai maximal de connexion, en secondes (défaut : 15)",
    )
    parser.add_argument(
        "--insecure", action="store_true",
        help=(
            "Se connecter sans certificat et sans valider celui du serveur. "
            "Utile pour un diagnostic, et.Unsafe en exploitation : le canal "
            "sera chiffré mais l'identité du PLC ne sera pas vérifiée."
        ),
    )
    parser.add_argument(
        "--trust", default="pki/trusted",
        help=(
            "répertoire des certificats de confiance du PLC (défaut : "
            "pki/trusted). Ignoré avec --insecure."
        ),
    )
    parser.add_argument(
        "--expect", default=None,
        help=(
            "URI d'application attendue du PLC (ex : urn:SCIICAD:thermo-plc). "
            "C'est le contrôle qui empêche un homme du milieu : sans lui, un "
            "certificat valide d'une autre application serait accepté."
        ),
    )
    parser.add_argument(
        "--client-cert", default="ihm/client_certificate.pem",
        help="certificat du client (défaut : ihm/client_certificate.pem)",
    )
    parser.add_argument(
        "--client-key", default="ihm/client_private_key.pem",
        help="clé privée du client (défaut : ihm/client_private_key.pem)",
    )
    return parser.parse_args(argv)


def format_status(values: dict) -> str:
    """Compose la ligne d'affichage à partir des valeurs lues."""
    def flag(name: str) -> str:
        return "On" if values.get(name) else "Off"

    temperature = values.get("Temperature")
    shown = f"{temperature:5.1f}" if isinstance(temperature, float) else "  n/a"
    return (
        f"\rT={shown}°C | Chauffage={flag('Heating')} | "
        f"Maintenance={flag('MaintenanceMode')} | "
        f">{THERMOSTAT_HIGH_THRESHOLD:.0f}={'Vrai' if values.get('HighTempAlarm') else 'Faux'}"
        f" | <{THERMOSTAT_LOW_THRESHOLD:.0f}={'Vrai' if values.get('LowTempAlarm') else 'Faux'}   "
    )


async def read_status(client: Client) -> dict | None:
    """Lit l'état du thermostat, ou ``None`` si l'objet est introuvable."""
    thermostat = await find_node_by_name(client.nodes.objects, THERMOSTAT_OBJECT)
    if thermostat is None:
        return None
    return await read_child_values(thermostat, list(THERMOSTAT_VARIABLE_NAMES))


async def supervise(client: Client, stopped: asyncio.Event) -> None:
    """Affiche l'état en boucle jusqu'à ce qu'un signal d'arrêt arrive.

    L'arrêt est Cooperative : l'événement est attendu entre deux rafraîchis,
    ce qui évite qu'un Ctrl+C coupe l'affichage au milieu d'une ligne.
    """
    while not stopped.is_set():
        try:
            values = await read_status(client)
        except Exception as exc:
            print(f"\nErreur de lecture : {type(exc).__name__}: {exc}")
            return

        if values is None:
            print(f"\nObjet {THERMOSTAT_OBJECT} introuvable sous Objects.")
            return

        sys.stdout.write(format_status(values))
        sys.stdout.flush()

        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stopped.wait(), timeout=REFRESH_SECONDS)


async def main(argv=None) -> int:
    args = parse_args(argv)
    url = f"opc.tcp://{args.host}:{args.port}"

    # Le gestionnaire de signal est installé ici, et non au moment de
    # l'import : l'ancien placement rendait le module inutilisable hors
    # interface en ligne de commande.
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stopped.set)

    print(banner("IHM OPCUA - THERMOSTAT", 50))
    print(f"Connexion au PLC : {url}")

    # Connexion sécurisée ou non. Le repli n'est jamais silencieux : sans
    # certificat, ou sans ancres, la connexion échoue et le dit. Ce qui serait
    # inacceptable, c'est un client qui se connecte en croyant avoir validé et
    # qui ne valide rien.
    client = None
    if not args.insecure:
        client = await build_secure_client(args, url)
    if client is None:
        client = Client(url=url)
        if not args.insecure:
            print(
                "Mode dégradé : connexion sans validation du certificat du PLC. "
                "Le canal sera chiffré, mais un homme du milieu ne sera pas "
                "détecté. --insecure est alors explicite, ou le certificat "
                "manque."
            )
    try:
        await asyncio.wait_for(client.connect(), args.timeout)
    except Exception as exc:
        print(f"Connexion impossible : {type(exc).__name__}: {exc}")
        return 1

    print("Connecté au PLC — Ctrl+C pour quitter\n")
    try:
        await supervise(client, stopped)
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
        print("\n\nDéconnecté du PLC")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
