"""Code partagé par les simulateurs PLC du projet SCIICAD.

Ce paquet regroupe ce qui est identique dans ``thermo-plc/`` et
``protect-plc/`` : résolution réseau, enregistrement auprès d'un serveur de
découverte, cycle d'arrêt, et validation des arguments de ligne de commande.

Le modèle d'adressage et la simulation restent propres à chaque simulateur.
"""

__all__ = ["__version__"]

__version__ = "1.0.0"
