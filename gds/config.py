"""Configuration du Global Discovery Server OPC UA.

Le GDS et le LDS partagent le même câblage de services ; ils ne diffèrent que
par la portée du registre et par les valeurs annoncées. La configuration
spécifique tient donc en quatre valeurs par défaut, et tout le reste est
hérité de :class:`lds.config.LDSConfig`.
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field, model_validator

from lds.config import DatabaseConfig, DiscoveryConfig, LDSConfig, ServerConfig

DEFAULT_CONFIG_FILENAME = "gds_config.yaml"

# Chemin d'endpoint fixé par la Part 12 pour ce rôle. Un client doit pouvoir
# distinguer un GDS d'un LDS sur la seule URL, y compris lorsqu'ils écoutent
# tous deux sur le port 4840.
GDS_ENDPOINT_PATH = "GlobalDiscoveryServer"


class GDSConfig(LDSConfig):
    """Configuration d'un GDS : portée globale, endpoint dédié.

    La portée ``global`` est la seule différence de fond avec le LDS : une
    inscription reste valide jusqu'à son retrait explicite, donc aucun
    renouvellement périodique n'est imposé aux serveurs inscrits.
    """

    config_filename: ClassVar[str] = DEFAULT_CONFIG_FILENAME

    server: ServerConfig = Field(
        default_factory=lambda: ServerConfig(
            application_name="SCIICAD GDS",
            application_uri="urn:SCIICAD:gds",
            product_uri="urn:SCIICAD:product:gds",
            # Injecté explicitement : ServerConfig est validé à la construction,
            # donc affecter endpoint_path après coup ne rejouerait pas le
            # validateur. Surtout, un fichier YAML qui remplacerait ce champ
            # par une chaîne vide produirait un endpoint indistinguable de
            # celui d'un LDS, ce que la Part 12 exclut pour ce rôle.
            endpoint_path=GDS_ENDPOINT_PATH,
        )
    )

    @model_validator(mode="after")
    def _force_gds_endpoint_path(self) -> "GDSConfig":
        """Garantit le chemin d'endpoint du GDS, même si le YAML le surcharge.

        La valeur par défaut ne suffit pas : un ``server.endpoint_path: ""``
        explicite dans le fichier de configuration produirait
        ``opc.tcp://hôte:4840``, c'est-à-dire l'URL exacte d'un LDS. Le
        client perdrait alors le seul moyen de distinguer les deux rôles.
        """
        if self.server.endpoint_path != GDS_ENDPOINT_PATH:
            self.server.endpoint_path = GDS_ENDPOINT_PATH
        return self
    discovery: DiscoveryConfig = Field(
        default_factory=lambda: DiscoveryConfig(scope="global")
    )
    database: DatabaseConfig = Field(default_factory=lambda: DatabaseConfig(path="gds.db"))
