"""Assemblage du Global Discovery Server.

Le GDS réutilise intégralement l'assemblage du LDS : mêmes services de la
Part 4, même persistance, même câblage. Il ne s'en distingue que par son rôle,
lu dans la configuration :

* l'endpoint annonce porte le chemin ``/GlobalDiscoveryServer`` ;
* le registre est en portée ``global``, donc rien n'y expire.

Aucune soustraction ici : c'est ce qui garantit que les deux rôles ne peuvent
pas diverger sur un service de découverte.

S'y ajoute la partie propre au GDS : les groupes de certificats et leurs listes
de confiance (Part 12 §7.8.2), puis l'objet ``ServerConfiguration`` et ses
méthodes de signature, qui font du GDS un *CertificateManager* (§7.1, §7.10).
"""

from __future__ import annotations

from typing import Optional

from asyncua import ua
from loguru import logger

from lds.server import DiscoveryServer

from .certificategroup import CertificateGroupNode
from .certstore import CertificateStore
from .config import GDSConfig
from .serverconfiguration import ServerConfigurationNode
from .trustlist import CertificateGroup


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
        #: Groupes de certificats publiés, par nom.
        self.certificate_groups: dict[str, CertificateGroupNode] = {}
        #: Magasin de certificats du rôle CertificateManager, ou ``None``.
        self.certificate_store: Optional[CertificateStore] = None
        #: Objet ``ServerConfiguration`` publié, ou ``None``.
        self.server_configuration: Optional[ServerConfigurationNode] = None

    async def setup(self) -> None:
        """Assemble le serveur, puis publie les objets de certificats."""
        await super().setup()
        await self._build_certificate_groups()
        await self._build_certificate_manager()

    async def _build_certificate_groups(self) -> None:
        """Publie les ``CertificateGroupType`` configurés.

        La construction a lieu après ``setup()`` mais avant ``start()`` : un
        groupe doit exister dans l'espace d'adressage avant que le serveur
        n'accepte une connexion, sinon un client qui découvre le GDS pourrait
        parcourir l'arborescence et trouver le dossier incomplet.
        """
        if self.server is None:
            return
        for name in self.config.certificates.groups:
            group = CertificateGroup(name=name)
            node = CertificateGroupNode(self.server, group)
            await node.build()
            self.certificate_groups[name] = node
        if self.certificate_groups:
            logger.info(
                f"Groupes de certificats publiés : {', '.join(self.certificate_groups)}"
            )

    def certificate_group(self, name: str) -> Optional[CertificateGroup]:
        """Retourne la liste de confiance d'un groupe, ou ``None``."""
        node = self.certificate_groups.get(name)
        return node.group if node is not None else None

    async def _build_certificate_manager(self) -> None:
        """Publie ``ServerConfiguration`` si la gestion est activée.

        Le magasin est construit *après* les groupes, car la validation d'un
        certificat s'appuie sur leurs listes de confiance : un magasin créé
        avant détiendrait des groupes vides et refuserait tout certificat signé
        par une autorité pourtant déclarée de confiance.
        """
        if self.server is None or not self.config.certificates.manage_certificates:
            return
        self.certificate_store = CertificateStore(
            groups={
                name: node.group for name, node in self.certificate_groups.items()
            },
            application_uri=self.config.server.application_uri,
            hostnames=self.config.certificates.hostnames,
            key_size=self.config.certificates.key_size,
        )
        self.server_configuration = ServerConfigurationNode(
            self.server, self.certificate_store, group_name=self._group_name
        )
        await self.server_configuration.build()

    def _group_name(self, nodeid: ua.NodeId) -> Optional[str]:
        """Résout le NodeId d'un groupe publié en nom de groupe.

        Un NodeId qui ne correspond à aucun groupe publié renvoie ``None``, et
        l'appelant refuse alors l'appel. Retomber sur le groupe par défaut
        déplacerait un certificat à l'insu du client.
        """
        for name, node in self.certificate_groups.items():
            if node.node is not None and node.node.nodeid == nodeid:
                return name
        return None

    def trust_list_summary(self) -> list[dict]:
        """Aperçu des listes de confiance, pour le diagnostic."""
        return [node.group.describe() for node in self.certificate_groups.values()]

    def certificate_summary(self) -> list[dict]:
        """Aperçu des certificats gérés, pour le diagnostic."""
        if self.certificate_store is None:
            return []
        return self.certificate_store.describe()

