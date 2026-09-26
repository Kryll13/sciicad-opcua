"""Global Discovery Server OPC UA (GDS) — projet SCIICAD.

Point d'entrée :

    python -m gds [--config gds_config.yaml] [--port 4840]
                  [--bind 0.0.0.0] [--advertise <hôte>] [--no-database]

Le GDS implémente les services de découverte de la Part 4 — ``FindServers``,
``FindServersOnNetwork``, ``RegisterServer`` et ``RegisterServer2`` — sur le
câblage déjà éprouvé par le LDS, en portée globale. Le code commun reste dans
``lds/`` : un GDS est un LDS dont les inscriptions n'expirent pas.
"""

from .config import GDS_ENDPOINT_PATH, GDSConfig
from .server import GlobalDiscoveryServer

__all__ = ["GDSConfig", "GDS_ENDPOINT_PATH", "GlobalDiscoveryServer"]
