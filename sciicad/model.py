"""Connaissances du modèle de données des simulateurs SCIICAD.

Le jeu de variables publiées par ``thermo-plc`` et ``protect-plc`` est
partagé par l'IHM et par les outils de diagnostic. Il était dupliqué dans
``ihm/ihm_client.py`` et ``tools/ihm_action.py`` : toute évolution du modèle
devait être répercutée à l'identique, sous peine d'affichage faux.
"""

from __future__ import annotations

# --- thermo-plc : Objects/Thermostat --------------------------------------

THERMOSTAT_OBJECT = "Thermostat"

#: Variables de ``Objects/Thermostat``, dans l'ordre d'affichage.
THERMOSTAT_VARIABLE_NAMES: tuple[str, ...] = (
    "Heating",
    "Temperature",
    "HighTempAlarm",
    "LowTempAlarm",
    "MaintenanceMode",
)

#: Variables modifiables par un client.
THERMOSTAT_WRITABLE: frozenset[str] = frozenset({"Heating", "MaintenanceMode"})

#: Unités, pour l'affichage des outils.
THERMOSTAT_UNITS: dict[str, str] = {"Temperature": "°C"}

#: Seuils d'alarme, dupliqués dans la simulation du PLC.
THERMOSTAT_HIGH_THRESHOLD = 25.0
THERMOSTAT_LOW_THRESHOLD = 15.0

# --- protect-plc : Objects/Protection -------------------------------------

PROTECTION_OBJECT = "Protection"

#: Variables de ``Objects/Protection``.
PROTECTION_VARIABLE_NAMES: tuple[str, ...] = ("MaintenanceMode",)


def thermostat_values() -> dict[str, object]:
    """Retourne un dictionnaire vide aux bonnes clés, pour l'affichage."""
    return {name: None for name in THERMOSTAT_VARIABLE_NAMES}
