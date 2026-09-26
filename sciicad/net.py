"""Résolution réseau et construction d'endpoint OPC UA.

Point important : l'écoute et l'annonce sont découplées. Lier le serveur sur
l'adresse déduite du hostname fait échouer le démarrage sur
``OSError: could not bind on any address`` dès que le DNS renvoie une adresse
qui n'est pas une interface locale — situation fréquente en VM et en
conteneur. On écoute donc sur toutes les interfaces et on annonce un hôte
explicite, choisi par l'exploitant.
"""

from __future__ import annotations

import socket
from typing import Optional


def get_host_info() -> tuple[str, str]:
    """Retourne (hostname, adresse IP) de la machine.

    En cas d'échec de résolution, l'adresse de boucle locale est renvoyée :
    l'appelant n'a pas à traiter ce cas.
    """
    hostname = socket.gethostname()
    try:
        ip = socket.gethostbyname(hostname)
    except socket.gaierror:
        ip = "127.0.0.1"
    return hostname, ip


def resolve_endpoints(
    bind_address: str,
    port: int,
    advertise_host: Optional[str] = None,
) -> tuple[str, str, str]:
    """Retourne (adresse d'écoute, port, endpoint annoncé).

    ``advertise_host`` est l'hôte que les clients et le LDS utiliseront pour
    joindre le serveur. À défaut, l'IP déduite du hostname est utilisée.
    """
    _hostname, detected_ip = get_host_info()
    advertised = advertise_host or detected_ip
    endpoint = f"opc.tcp://{advertised}:{port}"
    return bind_address, port, endpoint
