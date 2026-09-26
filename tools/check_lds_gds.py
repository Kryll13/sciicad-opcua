#!/usr/bin/env python3
"""
Vérifie qu'un endpoint se comporte comme un serveur de découverte OPC UA.

La découverte est un ensemble de **services**, pas des nœuds de l'espace
d'adressage : un LDS n'expose aucun NodeId standard de l'espace d'adressage
(``ns=0;i=11524`` et consorts appartiennent au GDS de la norme). Chercher
ces nœuds revient donc à conclure à tort que le serveur est défaillant.

Ce script interroge réellement les services et conclut sur le rôle du serveur :

* ``FindServers``       — listage du registre (serveur de découverte) ;
* ``GetEndpoints``      — toujours présent, y compris hors découverte ;
* ``FindServersOnNetwork`` — extension du projet SCIICAD, absente d'asyncua
  et de la plupart des piles ;
* identité et certificats, pour compléter le diagnostic.

Code de sortie non nul si l'endpoint n'est pas joignable, ou si le rôle
attendu (option ``--expect``) ne correspond pas.

    uv run tools/check_lds_gds.py --url opc.tcp://193.168.1.20:4840
    uv run tools/check_lds_gds.py --url opc.tcp://<ip>:4840 --expect gds
"""

import argparse
import asyncio
import sys
from urllib.parse import urlparse

from asyncua import Client, ua

# ApplicationType de la norme, pour un diagnostic lisible.
_TYPE_NAMES = {0: "Server", 1: "Client", 2: "ClientAndServer", 3: "DiscoveryServer"}


def parse_args(argv=None):
    """Analyse les arguments de la ligne de commande."""
    parser = argparse.ArgumentParser(
        description="Vérifie le rôle d'un endpoint OPC UA (LDS, GDS, serveur)",
    )
    parser.add_argument(
        "--url", required=True,
        help="URL du serveur à inspecter (ex: opc.tcp://193.168.1.20:4840)",
    )
    parser.add_argument(
        "--expect", choices=("discovery", "server"), default=None,
        help="rôle attendu ; 'discovery' exige FindServers (défaut : aucune exigence)",
    )
    parser.add_argument(
        "--timeout", type=float, default=10.0,
        help="délai maximal par appel, en secondes (défaut : 10)",
    )
    return parser.parse_args(argv)


def validate_url(value: str) -> str:
    """Vérifie la syntaxe de l'URL avant d'ouvrir une connexion."""
    parsed = urlparse(value)
    if parsed.scheme not in ("opc.tcp", "opc.wss", "opc.https"):
        raise argparse.ArgumentTypeError(
            f"URL invalide : {value!r} (schéma attendu : opc.tcp)"
        )
    if not parsed.hostname:
        raise argparse.ArgumentTypeError(f"URL sans hôte : {value!r}")
    return value


class Report:
    """Accumule les résultats et le verdict."""

    def __init__(self) -> None:
        self.failures = 0
        self.is_registry = False

    def check(self, label: str, ok: bool, detail: str = "") -> bool:
        print(f"  [{'OK   ' if ok else 'ECHEC'}] {label}{f' -- {detail}' if detail else ''}")
        if not ok:
            self.failures += 1
        return ok

    def info(self, label: str, detail: str) -> None:
        print(f"  [INFO ] {label}: {detail}")


async def probe(url: str, timeout: float, expected=None) -> int:
    """Interroge les services et renvoie un code de sortie."""
    parsed = urlparse(url)
    parsed_url = url
    report = Report()
    print(f"=== Inspection de {url} ===")
    print(f"  hôte {parsed.hostname}:{parsed.port or 4840}")

    client = Client(url=url)
    try:
        await asyncio.wait_for(client.connect(), timeout)
    except Exception as exc:
        report.check("connexion", False, f"{type(exc).__name__}: {exc}")
        return _finish(report, None)

    try:
        # --- identité ---------------------------------------------------
        # Les propriétés du nœud Server sont résolues par browse name :
        # leurs identifiants numériques varient selon la pile, alors que le
        # nom de browse est stable (cf. docs/depannage.md).
        identity = await asyncio.wait_for(_server_identity(client), timeout)
        report.info("applicationUri", identity.get("applicationUri", "(inconnue)"))
        report.info("productUri", identity.get("productUri", "(inconnu)"))
        namespaces = await asyncio.wait_for(client.get_namespace_array(), timeout)
        report.info("namespaces", str(namespaces))

        # --- GetEndpoints : présent sur tout serveur --------------------
        # On réutilise la session ouverte : connect_and_get_server_endpoints()
        # ouvrirait une seconde connexion et détruirait la précédente, ce qui
        # ferait échouer les appels suivants.
        try:
            endpoints = await asyncio.wait_for(client.get_endpoints(), timeout)
            modes = sorted({e.SecurityMode.name for e in endpoints})
            report.check(
                "GetEndpoints pris en charge", True,
                f"{len(endpoints)} endpoint(s), modes : {', '.join(modes)}",
            )
            for endpoint in endpoints:
                print(f"           {endpoint.EndpointUrl}")
        except Exception as exc:
            report.check("GetEndpoints pris en charge", False, type(exc).__name__)

        # --- FindServers : marqueur d'un serveur de découverte ----------
        servers = None
        try:
            servers = await asyncio.wait_for(client.find_servers(), timeout)
        except Exception as exc:
            report.check(
                "FindServers pris en charge", False,
                f"{type(exc).__name__} : pas un serveur de découverte",
            )

        if servers is not None:
            # Tout serveur OPC UA doit se décrire lui-même via FindServers
            # (clause 5.5.2.1). Ce n'est donc pas un critère de rôle : un
            # véritable serveur de découverte tient un *registre*, c'est-à-dire
            # des entrées qui ne le décrivent pas lui-même.
            #
            # « soi-même » est retenu par l'URI déclarée dans ServerArray OU,
            # à défaut, par le fait que l'entrée publie l'endpoint interrogé.
            # On ne se fie pas au seul ServerArray : une pile peut l'exposer
            # obsolète, et le diagnostic donnerait alors un faux verdict.
            own_uri = identity.get("applicationUri")
            probed = {url for url in (parsed_url,) }

            def is_self(entry) -> bool:
                if own_uri and entry.ApplicationUri == own_uri:
                    return True
                return bool(probed & set(entry.DiscoveryUrls or []))

            foreign = [s for s in servers if not is_self(s)]
            report.is_registry = bool(foreign)
            report.check(
                "FindServers pris en charge", True, f"{len(servers)} serveur(s)"
            )
            for server in sorted(servers, key=lambda s: s.ApplicationUri or ""):
                name = getattr(getattr(server, "ApplicationName", None), "Text", "")
                kind = _TYPE_NAMES.get(getattr(server, "ApplicationType_", None), "?")
                mark = " (soi-même)" if is_self(server) else ""
                print(f"           {server.ApplicationUri} [{kind}] {name}{mark}")
                for url_found in server.DiscoveryUrls or []:
                    print(f"             -> {url_found}")
            print(f"           registre : {len(foreign)} entrée(s) étrangère(s)")

        # --- FindServersOnNetwork : extension du projet -------------------
        try:
            result = await asyncio.wait_for(
                client.uaclient.find_servers_on_network(ua.FindServersOnNetworkParameters()),
                timeout,
            )
            records = result if isinstance(result, (list, tuple)) else [result]
            entries = [e for r in records for e in r.Servers]
            report.check(
                "FindServersOnNetwork pris en charge", True,
                f"{len(entries)} enregistrement(s) (extension SCIICAD, non standard)",
            )
            for entry in entries:
                print(f"           [{entry.RecordId}] {entry.ServerName} -> {entry.DiscoveryUrl}")
        except Exception as exc:
            report.info(
                "FindServersOnNetwork", f"non pris en charge ({type(exc).__name__})"
            )

    finally:
        try:
            await client.disconnect()
        except Exception:
            pass

    return _finish(report, expected)


async def _server_identity(client: Client) -> dict:
    """Lit l'identité du serveur sous le nœud ``Objects/Server``.

    Les propriétés sont cherchées par browse name parmi les enfants de
    ``Server`` : c'est la seule approche stable d'une pile à l'autre. Selon
    les serveurs, ``ProductUri`` peut être absent de cet ensemble ; c'est
    alors signalé plutôt que deviné.
    """
    identity: dict = {}
    try:
        server_node = client.get_node(ua.NodeId(ua.ObjectIds.Server))
        children = await server_node.get_children()
    except Exception:
        return identity

    for child in children:
        try:
            name = (await child.read_browse_name()).Name
        except Exception:
            continue
        key = name.lower()
        if key not in ("serverarray", "producturi"):
            continue
        try:
            # get_value() renvoie déjà la valeur déballée : un .Value
            # supplémentaire lèverait AttributeError sur une liste.
            value = await child.get_value()
        except Exception:
            continue
        if key == "serverarray":
            if isinstance(value, (list, tuple)) and value:
                identity["applicationUri"] = str(value[0])
        else:
            identity["productUri"] = str(value)
    return identity


def _finish(report: Report, expected) -> int:
    print("=== Verdict ===")
    role = (
        "serveur de découverte (tient un registre)"
        if report.is_registry
        else "serveur OPC UA ordinaire (se décrit lui-même)"
    )
    print(f"  Rôle détecté : {role}")
    if expected is not None:
        want_registry = expected == "discovery"
        ok = report.is_registry == want_registry
        print(
            "  Rôle attendu : "
            + ("serveur de découverte" if want_registry else "serveur ordinaire")
        )
        print(f"  [{'OK   ' if ok else 'ECHEC'}] conformité au rôle attendu")
        if not ok:
            report.failures += 1
    if report.failures:
        print(f"  {report.failures} vérification(s) en échec.")
        return 1
    print("  Toutes les vérifications sont passées.")
    return 0


async def main(argv=None) -> int:
    args = parse_args(argv)
    try:
        validate_url(args.url)
    except argparse.ArgumentTypeError as exc:
        print(f"  [ECHEC] {exc}")
        return 2
    return await probe(args.url, args.timeout, args.expect)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
