"""Configuration du Local Discovery Server.

La configuration est chargée depuis un fichier YAML. Toutes les valeurs ont un
défaut utilisable, de sorte qu'un LDS peut démarrer sans fichier de config.
"""

from __future__ import annotations

import socket
import sys
from pathlib import Path
from typing import ClassVar, Literal, Optional

import yaml
from pydantic import BaseModel, Field, field_validator

DEFAULT_CONFIG_FILENAME = "lds_config.yaml"

# Portée du registre, au sens de la Part 12.
#
# - "local"  : inscription valable jusqu'à expiration (LDS). Un serveur se
#              ré-enregistre périodiquement, faute de quoi l'entrée est
#              évacuée.
# - "global" : inscription conservée jusqu'à retrait explicite (GDS). Aucune
#              expiration, donc pas de renouvellement périodique imposé.
DiscoveryScope = Literal["local", "global"]


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

    # Chemin de l'endpoint annoncé. Vide pour un LDS, dont l'URL est
    # simplement opc.tcp://<hôte>:<port>. Un GDS annonce
    # /GlobalDiscoveryServer, ce que la Part 12 fixe pour ce rôle.
    endpoint_path: str = ""

    #: Certificat d'application du serveur.
    #:
    #: Son existence est une **obligation**, pas une option. La Part 12 fait du
    #: certificat du canal l'identite de l'application : « The Certificate used
    #: to create the SecureChannel is used to determine the identity of the OPC
    #: UA Application » (6.2), et RegisterApplication « shall be called from an
    #: authenticated SecureChannel » avec « MessageSecurityMode
    #: SignAndEncrypt » (6.5.6). Un serveur de decouverte qui n'annonce que
    #: NoSecurity ne peut donc **pas** satisfaire ce role.
    #:
    #: L'affirmation contraire — « la decouverte precede l'etablissement d'un
    #: canal securise » — etait un sophisme : la decouverte anonyme est bien
    #: possible en NoSecurity, mais ce n'est pas elle qui inscrit, et ce n'est
    #: pas elle qui gere les certificats. Ces deux operations exigent un canal
    #: authentifie, donc un certificat serveur.
    #:
    #: Vide, le serveur demarre quand meme et n'annonce que NoSecurity.
    #: L'avertissement le dit : c'est un mode degrade, pas un equivalent.
    certificate: Optional[str] = None

    #: Cle privee correspondant au certificat ci-dessus.
    private_key: Optional[str] = None

    #: Autoriser un canal Sign (signature sans chiffrement) en plus de
    #: SignAndEncrypt.
    #:
    #: Defaut **faux**, et ce n'est pas une preference de style : Sign protege
    #: l'integrite sans proteger la confidentialite. La Part 12 exige
    #: SignAndEncrypt pour les operations de gestion, et accepter Sign
    #: laisserait un client pretendre proteger une inscription tout en la
    #: laissant lisible. Un client qui veut la confidentialite choisit
    #: SignAndEncrypt, qui est toujours annonce.
    allow_sign_only: bool = False

    def certificate_paths(self) -> tuple[Optional[str], Optional[str]]:
        """Les deux chemins, ou ``(None, None)`` si le mode non securise."""
        if self.certificate and self.private_key:
            return self.certificate, self.private_key
        return None, None

    @field_validator("port")
    @classmethod
    def _check_port(cls, value: int) -> int:
        if not 1 <= value <= 65535:
            raise ValueError(f"port hors plage: {value} (attendu 1..65535)")
        return value

    @field_validator("endpoint_path")
    @classmethod
    def _check_path(cls, value: str) -> str:
        cleaned = value.strip().strip("/")
        if " " in cleaned or "://" in cleaned:
            raise ValueError(
                f"endpoint_path doit être un chemin simple, sans espace ni "
                f"schéma : {value!r}"
            )
        return cleaned

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
        base = f"opc.tcp://{self.resolve_advertise_host()}:{self.port}"
        return f"{base}/{self.endpoint_path}" if self.endpoint_path else base


class DiscoveryConfig(BaseModel):
    """Politique de gestion du registre des serveurs."""

    # La norme OPC UA impose un ré-enregistrement au moins toutes les 10
    # minutes. 300 s laisse une marge confortable : les PLC du projet
    # ré-enregistrent toutes les 60 s (register_to_discovery(url, 60)).
    entry_ttl_seconds: int = 300
    sweep_interval_seconds: int = 60
    find_servers_on_network: bool = True

    # "local" pour un LDS, "global" pour un GDS. Le champ ne change pas le
    # câblage des services, seulement la durée de vie des inscriptions.
    scope: DiscoveryScope = "local"

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
    """Configuration complète d'un serveur de découverte.

    Sert de base au LDS et au GDS, qui ne diffèrent que par les valeurs par
    défaut de :attr:`config_filename` et des champs_factory ci-dessous.
    """

    server: ServerConfig = Field(default_factory=ServerConfig)
    discovery: DiscoveryConfig = Field(default_factory=DiscoveryConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)

    # Fichier réellement chargé, renseigné par load(). Champ privé exclu de la
    # validation : il ne fait pas partie de la configuration elle-même.
    _loaded_from: Optional[str] = None

    #: Nom du fichier de configuration cherché quand --config est absent.
    config_filename: ClassVar[str] = DEFAULT_CONFIG_FILENAME

    @classmethod
    def package_dir(cls) -> Path:
        """Répertoire du paquet qui définit cette configuration.

        Résolu depuis le module de la classe, afin qu'une sous-classe située
        dans un autre dossier cherche son propre fichier sans avoir à le
        déclarer.
        """
        module = sys.modules.get(cls.__module__)
        filename = getattr(module, "__file__", None)
        return Path(filename).resolve().parent if filename else Path.cwd()

    @classmethod
    def config_candidates(cls) -> list[Path]:
        """Emplacements cherchés quand ``--config`` n'est pas fourni.

        Le répertoire courant d'abord, puis le dossier du paquet. Sans cette
        double recherche, la configuration serait silencieusement ignorée lors
        d'un lancement depuis la racine du dépôt, et le serveur démarrerait
        avec les valeurs par défaut.
        """
        return [Path(cls.config_filename), cls.package_dir() / cls.config_filename]

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

        Sans ``path``, les emplacements de :meth:`config_candidates` sont
        essayés dans l'ordre. Un fichier absent n'est pas une erreur : le
        serveur doit pouvoir démarrer sur une VM vierge avec les valeurs par
        défaut.
        """
        candidates = [Path(path)] if path is not None else cls.config_candidates()
        for candidate in candidates:
            if candidate.exists():
                config = cls.from_file(candidate)
                config._loaded_from = str(candidate)
                return config

        if path is not None:
            raise FileNotFoundError(
                f"fichier de configuration introuvable : {path} "
                f"(emplacements par défaut : "
                f"{', '.join(str(c) for c in cls.config_candidates())})"
            )
        return cls()
