"""Assemblage du Global Discovery Server.

Le GDS réutilise intégralement l'assemblage du LDS : mêmes services de la
Part 4, même persistance, même câblage. Il ne s'en distingue que par son rôle,
lu dans la configuration :

* l'endpoint annonce porte le chemin ``/GlobalDiscoveryServer`` ;
* le registre est en portée ``global``, donc rien n'y expire.

Aucune soustraction ici : c'est ce qui garantit que les deux rôles ne peuvent
pas diverger sur un service de découverte.
"""

from __future__ import annotations

from typing import Optional

from lds.server import DiscoveryServer

from .config import GDSConfig


class GlobalDiscoveryServer(DiscoveryServer):
    """GDS conforme OPC UA Part 4, portée globale.

    Portée ``global`` : une inscription reste en place jusqu'à son retrait
    explicite, par ``RegisterServer`` avec ``IsOnline = False`` (clause
    5.5.5.1) ou par :meth:`lds.registry.ServerRegistry.unregister`. Le
    registre restauré au démarrage fait foi — c'est ce qui distingue un GDS
    d'un LDS, dont les entrées restaurées sont soumises au TTL.
    """

    def __init__(self, config: Optional[GDSConfig] = None) -> None:
        super().__init__(config if config is not None else GDSConfig())
