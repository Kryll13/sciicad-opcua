"""Assemblage du Global Discovery Server.

Le GDS réutilise intégralement l'assemblage du LDS : mêmes services de la
Part 4, même persistance, même câblage. Il ne s'en distingue que par son rôle,
lu dans la configuration :

* l'endpoint annonce porte le chemin ``/GlobalDiscoveryServer`` ;
* le registre est en portée ``global``, donc rien n'y expire.

Aucune soustraction ici : c'est ce qui garantit que les deux rôles ne peuvent
pas diverger sur un service de découverte.

S'y ajoute la partie propre au GDS : les groupes de certificats et leurs listes
de confiance (Part 12 §7.8.2), que seul un GDS possède.
"""

from __future__ import annotations

from typing import Optional

from loguru import logger

from lds.server import DiscoveryServer

from .certificategroup import CertificateGroupNode
from .config import GDSConfig
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

    async def setup(self) -> None:
        """Assemble le serveur puis publie les groupes de certificats."""
        await super().setup()
        await self._build_certificate_groups()

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

    def trust_list_summary(self) -> list[dict]:
        """Aperçu des listes de confiance, pour le diagnostic."""
        return [node.group.describe() for node in self.certificate_groups.values()]

