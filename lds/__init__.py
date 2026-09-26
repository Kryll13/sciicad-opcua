"""Local Discovery Server (LDS) pour le projet SCIICAD.

Ce paquet implémente un LDS conforme à l'OPC UA Part 4, au-delà de ce que
fournit nativement asyncua :

* persistance SQLite du registre (survit au redémarrage du LDS) ;
* expiration des entrées dont le renouvellement périodique a cessé ;
* service ``FindServersOnNetwork``, absent du serveur asyncua.

Point d'entrée : ``python -m lds``.
"""

__all__ = ["__version__"]

__version__ = "1.0.0"
