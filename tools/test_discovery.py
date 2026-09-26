#!/usr/bin/env python3
"""
Test de bout en bout de la découverte OPC UA du projet SCIICAD.

Vérifie la chaîne complète :

1. les services de découverte du LDS (``FindServers``,
   ``FindServersOnNetwork``) ;
2. la résolution d'un endpoint de PLC **depuis le registre du LDS** ;
3. la connexion à ce PLC et la lecture de son espace d'adressage.

Aucune hypothèse n'est faite sur le modèle de données du PLC : l'espace
d'adressage est parcouru et affiché tel quel.

Code de sortie non nul si le LDS ne répond pas, si le PLC attendu est absent
du registre, ou si la lecture échoue.

    uv run tools/test_discovery.py --lds-url opc.tcp://193.168.1.20:4840
"""

import argparse
import asyncio
import sys

from asyncua import Client, ua
from sciicad.console import banner, logger, setup
from sciicad.nodes import list_children

DEFAULT_LDS_URL = "opc.tcp://127.0.0.1:4840"
DEFAULT_PLC_URI = "urn:SCIICAD:thermo-plc"

# Sous-arbres de l'espace d'adressage standard OPC UA : présents sur tout
# serveur, sans rapport avec le projet, et très verbeux à afficher.
STANDARD_TREES = frozenset(
    {"Server", "Aliases", "Locations", "Types", "Views", "ServerDiagnostics", "Zv"}
)


def parse_args(argv=None):
    """Analyse les arguments de la ligne de commande."""
    parser = argparse.ArgumentParser(
        description="Test de découverte OPC UA (LDS puis PLC)",
    )
    parser.add_argument(
        "--lds-url", default=DEFAULT_LDS_URL,
        help=f"URL du LDS (défaut : {DEFAULT_LDS_URL})",
    )
    parser.add_argument(
        "--plc-uri", default=DEFAULT_PLC_URI,
        help=f"applicationUri du PLC à retrouver dans le LDS (défaut : {DEFAULT_PLC_URI})",
    )
    parser.add_argument(
        "--skip-plc", action="store_true",
        help=" tester uniquement le LDS",
    )
    parser.add_argument(
        "--wait", type=int, default=0,
        help="attente avant les tests, en secondes (défaut : 0)",
    )
    parser.add_argument(
        "--timeout", type=float, default=15.0,
        help="délai maximal par appel réseau, en secondes (défaut : 15)",
    )
    parser.add_argument(
        "--depth", type=int, default=3,
        help="profondeur de parcours de l'espace d'adressage (défaut : 3)",
    )
    return parser.parse_args(argv)


def check(label: str, condition: bool, detail: str = "") -> bool:
    """Journalise une vérification et retourne sa valeur."""
    marker = "OK   " if condition else "ECHEC"
    logger.info(f"[{marker}] {label}{f' -- {detail}' if detail else ''}")
    return condition


def _describe(servers) -> list:
    """Décrit les ApplicationDescription d'un registre de découverte."""
    rows = []
    for server in servers:
        name = getattr(getattr(server, "ApplicationName", None), "Text", None)
        rows.append(
            {
                "uri": getattr(server, "ApplicationUri", None),
                "name": name,
                "urls": list(getattr(server, "DiscoveryUrls", None) or []),
            }
        )
    return rows


async def test_lds(lds_url: str, timeout: float) -> list:
    """Interroge les services de découverte du LDS.

    Retourne la liste des serveurs enregistrés, vide en cas d'échec.
    """
    logger.info("=== 1. Services de découverte du LDS ===")
    client = Client(url=lds_url)
    client.session_timeout = timeout * 1000
    ok = False
    rows: list = []
    try:
        await asyncio.wait_for(client.connect(), timeout)
        logger.info(f"Connecté au LDS : {lds_url}")

        servers = await asyncio.wait_for(client.find_servers(), timeout)
        rows = _describe(servers)
        ok = check("FindServers répond", True, f"{len(rows)} serveur(s)")
        for row in rows:
            logger.info(
                f"    {row['uri']} -> {', '.join(row['urls']) or '(aucune URL)'}"
            )

        # FindServersOnNetwork n'est pas routé par asyncua : c'est une
        # extension du projet. Son absence n'est pas un défaut du LDS.
        try:
            result = await asyncio.wait_for(
                client.connect_and_find_servers_on_network(), timeout
            )
            records = result if isinstance(result, (list, tuple)) else [result]
            on_network = [
                (e.RecordId, e.ServerName, e.DiscoveryUrl)
                for r in records
                for e in r.Servers
            ]
            check(
                "FindServersOnNetwork pris en charge",
                True,
                f"{len(on_network)} enregistrement(s)",
            )
            for record_id, name, url in on_network:
                logger.info(f"    [{record_id}] {name} -> {url}")
        except Exception as exc:
            logger.info(f"    FindServersOnNetwork non pris en charge ({type(exc).__name__})")

        return rows if ok else []

    except Exception as exc:
        check("FindServers répond", False, f"{type(exc).__name__}: {exc}")
        return []
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


async def test_plc_via_lds(lds_url: str, plc_uri: str, timeout: float, depth: int) -> bool:
    """Résout l'endpoint du PLC dans le LDS, s'y connecte, lit l'espace d'adressage."""
    logger.info(f"\n=== 2. PLC {plc_uri} via le LDS ===")
    lds = Client(url=lds_url)
    plc_url = None
    try:
        await asyncio.wait_for(lds.connect(), timeout)
        servers = await asyncio.wait_for(lds.find_servers(), timeout)
        match = next(
            (s for s in servers if getattr(s, "ApplicationUri", None) == plc_uri), None
        )
        if not check("PLC présent dans le registre du LDS", match is not None, plc_uri):
            available = [getattr(s, "ApplicationUri", None) for s in servers]
            logger.info(f"    entrées présentes : {available}")
            return False
        urls = list(getattr(match, "DiscoveryUrls", None) or [])
        check("le PLC publie au moins une URL", bool(urls), ", ".join(urls))
        if not urls:
            return False
        plc_url = urls[0]
    except Exception as exc:
        check("lecture du registre du LDS", False, f"{type(exc).__name__}: {exc}")
        return False
    finally:
        try:
            await lds.disconnect()
        except Exception:
            pass

    return await _read_address_space(plc_url, timeout, depth)


async def test_plc_direct(plc_url: str, timeout: float, depth: int) -> bool:
    """Se connecte directement au PLC et lit son espace d'adressage."""
    logger.info(f"\n=== 3. Connexion directe {plc_url} ===")
    return await _read_address_space(plc_url, timeout, depth)


async def _read_address_space(url: str, timeout: float, depth: int) -> bool:
    """Connexion puis parcours de l'espace d'adressage."""
    client = Client(url=url)
    try:
        await asyncio.wait_for(client.connect(), timeout)
        namespaces = await asyncio.wait_for(client.get_namespace_array(), timeout)
        logger.info(f"Namespaces : {namespaces}")

        # Les nœuds sont résolus par browse name, jamais par NodeId.
        objects = client.nodes.objects
        rows = await asyncio.wait_for(
            list_children(objects, depth, skip_roots=STANDARD_TREES), timeout
        )
        variables = [row for row in rows if row[1] is not None]

        if not check("des variables sont publiées", bool(variables), f"{len(rows)} nœud(s)"):
            logger.info("    (aucune variable lisible ; l'espace d'adressage est-il vide ?)")
            return False

        logger.info("Espace d'adressage :")
        for path, value in variables:
            shown = f"{value:.3f}" if isinstance(value, float) else value
            logger.info(f"    {path:44s} = {shown}")
        return True

    except Exception as exc:
        check("lecture de l'espace d'adressage", False, f"{type(exc).__name__}: {exc}")
        return False
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


async def main(argv=None) -> int:
    args = parse_args(argv)

    setup()
    logger.info(banner("Test de découverte OPC UA"))
    if args.wait > 0:
        logger.info(f"Attente de {args.wait} s...")
        await asyncio.sleep(args.wait)

    failures = 0
    rows = await test_lds(args.lds_url, args.timeout)
    if not rows:
        failures += 1

    if not args.skip_plc:
        if not await test_plc_via_lds(args.lds_url, args.plc_uri, args.timeout, args.depth):
            failures += 1

    logger.info(banner("Bilan"))
    if failures:
        logger.info(f"{failures} vérification(s) en échec.")
        return 1
    logger.info("Toutes les vérifications sont passées.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
