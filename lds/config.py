"""Configuration du Local Discovery Server.

La configuration est chargée depuis un fichier YAML. Toutes les valeurs ont un
défaut utilisable, de sorte qu'un LDS peut démarrer sans fichier de config.
"""

from __future__ import annotations

import socket
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field, field_validator

DEFAULT_CONFIG_FILENAME = "lds_config.yaml"


def default_config_candidates() -> list[Path]:
    """Chemins cherchés quand ``--config`` n'est pas fourni.

    Le LDS est normalement lancé depuis la racine du dépôt (``uv run python -m
    lds``), mais le fichier de configuration vit dans ``lds/``. Sans cette
    double recherche, la configuration serait silencieusement ignorée et le
    serveur démarrerait avec les valeurs par défaut.
    """
    return [
        Path(DEFAULT_CONFIG_FILENAME),                 # répertoire courant
        Path(__file__).resolve().parent / DEFAULT_CONFIG_FILENAME,  # lds/
    ]


class ServerConfig(BaseModel):
    """Paramètres réseau et d'identité du LDS.

    ``bind_address`` est l'adresse sur laquelle le LDS écoute réellement ;
    ``advertise_host`` est le nom/IP annoncé aux clients. Les deux sont
    volontairement découplés : écouter sur ``0.0.0.0`` est robuste (le serveur
    démarre même si la résolution DNS est incohérente), tandis que l'hôte
    annoncé doit être joignable par les clients.
    """

    bind_address: str = "0.0.0.0"
    port: int = 4840
    advertise_host: Optional[str] = None
    application_name: str = "SCIICAD LDS"
    application_uri: str = "urn:SCIICAD:lds"
    product_uri: str = "urn:SCIICAD:product:lds"
    manufacturer_name: str = "SCIICAD"
    software_version: str = "1.0"

    @field_validator("port")
    @classmethod
    def _check_port(cls, value: int) -> int:
        if not 1 <= value <= 65535:
            raise ValueError(f"port hors plage: {value} (attendu 1..65535)")
        return value

    def resolve_advertise_host(self) -> str:
        """Retourne l'hôte à annoncer.

        Priorité à la valeur de configuration, sinon au hostname de la
        machine. L'adresse n'est volontairement pas résolue ici : la résolution
        est laissée au système, ce qui évite les erreurs « could not bind »
        dues à un enregistrement DNS obsolète.
        """
        return self.advertise_host or socket.gethostname()

    @property
    def endpoint_url(self) -> str:
        """URL d'endpoint annoncée aux clients."""
        return f"opc.tcp://{self.resolve_advertise_host()}:{self.port}"


class DiscoveryConfig(BaseModel):
    """Politique de gestion du registre des serveurs."""

    # La norme OPC UA impose un ré-enregistrement au moins toutes les 10
    # minutes. 300 s laisse une marge confortable : les PLC du projet
    # ré-enregistrent toutes les 60 s (register_to_discovery(url, 60)).
    entry_ttl_seconds: int = 300
    sweep_interval_seconds: int = 60
    find_servers_on_network: bool = True

    @field_validator("entry_ttl_seconds")
    @classmethod
    def _check_ttl(cls, value: int) -> int:
        if value < 10:
            raise ValueError("entry_ttl_seconds doit être >= 10")
        return value

    @field_validator("sweep_interval_seconds")
    @classmethod
    def _check_sweep(cls, value: int) -> int:
        if value < 1:
            raise ValueError("sweep_interval_seconds doit être >= 1")
        return value


class DatabaseConfig(BaseModel):
    """Persistance SQLite du registre."""

    enabled: bool = True
    path: str = "lds.db"
    event_log: bool = True


class LDSConfig(BaseModel):
    """Configuration complète du LDS."""

    server: ServerConfig = Field(default_factory=ServerConfig)
    discovery: DiscoveryConfig = Field(default_factory=DiscoveryConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)

    # Fichier réellement chargé, renseigné par load(). Champ privé exclu de la
    # validation : il ne fait pas partie de la configuration elle-même.
    _loaded_from: Optional[str] = None

    @property
    def source(self) -> str:
        """Indique l'origine de la configuration, pour le diagnostic."""
        return self._loaded_from or "valeurs par défaut (aucun fichier trouvé)"

    @classmethod
    def from_file(cls, path: str | Path) -> "LDSConfig":
        """Charge la configuration depuis un fichier YAML."""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"fichier de configuration introuvable: {path}")
        with open(path, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
        return cls(**data)

    @classmethod
    def load(cls, path: Optional[str | Path] = None) -> "LDSConfig":
        """Charge la configuration, ou retombe sur les défauts.

        Sans ``path``, les emplacements de :func:`default_config_candidates`
        sont essayés dans l'ordre. Un fichier absent n'est pas une erreur : le
        LDS doit pouvoir démarrer sur une VM vierge avec les valeurs par
        défaut.
        """
        candidates = [Path(path)] if path is not None else default_config_candidates()
        for candidate in candidates:
            if candidate.exists():
                config = cls.from_file(candidate)
                config._loaded_from = str(candidate)
                return config

        if path is not None:
            raise FileNotFoundError(
                f"fichier de configuration introuvable : {path} "
                f"(emplacements par défaut : "
                f"{', '.join(str(c) for c in default_config_candidates())})"
            )
        return cls()
