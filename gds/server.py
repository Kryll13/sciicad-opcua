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

from .audit import AuditEmitter, build_for
from .certificategroup import (
    CertificateGroupNode,
    certificate_group_folder,
)
from .certstore import CertificateStore
from .config import GDSConfig
from .serverconfiguration import ServerConfigurationNode
from .trustlist import (
    CertificateGroup,
    load_issuer_crls,
    load_trusted_certificates,
)


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
        #: Émetteur d'audit OPC UA, ou ``None`` si désactivé ou indisponible.
        self.audit: Optional[AuditEmitter] = None

    async def setup(self) -> None:
        """Assemble le serveur, puis publie les objets de certificats."""
        await super().setup()
        if self.config.audit.enabled:
            self.audit = await build_for(self.server)
        await self._build_certificate_groups()
        await self._build_certificate_manager()

    async def _build_certificate_groups(self) -> None:
        """Rattache les ``CertificateGroupType`` configurés à leur dossier.

        Les groupes sont publiés sous ``ServerConfiguration.CertificateGroups``,
        à l'endroit que la norme leur réserve (§7.8.3.3) : le GDS réattache ses
        gestionnaires aux instances qu'asyncua a déjà créées, il n'en fabrique
        pas de nouvelles. Un groupe créé ailleurs laisserait l'instance
        normative sans gestionnaire — donc ``BadNothingToDo`` pour le client qui
        suit le chemin normal — tout en ajoutant un nœud imprévu.

        La construction a lieu après ``setup()`` mais avant ``start()`` : un
        groupe doit exister dans l'espace d'adressage avant que le serveur
        n'accepte une connexion, sinon un client qui découvre le GDS pourrait
        parcourir l'arborescence et trouver le dossier incomplet.

        Le dossier hôte est disponible dès ``Server.init()``, qui construit déjà
        ``ServerConfiguration`` et son ``CertificateGroups``. L'ordre des deux
        méthodes ci-dessous tient donc à une autre raison : le magasin de
        certificats valide les entrées à partir des listes de confiance des
        groupes, et le construit après.
        """
        if self.server is None:
            return
        folder = await certificate_group_folder(self.server)
        if folder is None:
            logger.error(
                "Dossier CertificateGroups introuvable : aucun groupe de "
                "certificats n'est publié"
            )
            return
        for name in self.config.certificates.groups:
            group = CertificateGroup(name=name)
            node = CertificateGroupNode(self.server, group, audit=self.audit)
            await node.build(parent=folder)
            self.certificate_groups[name] = node
        if self.certificate_groups:
            logger.info(
                f"Groupes de certificats publiés sous {folder.nodeid} "
                f"(CertificateGroupFolderType i={ua.ObjectIds.CertificateGroupFolderType})"
                f" : {', '.join(self.certificate_groups)}"
            )
        self._load_trust_anchors()

    def _load_trust_anchors(self) -> None:
        """Pose les ancrages de confiance configurés, Part 12 §7.1.

        L'amorçage est **hors bande** par construction : la norme écrit que le
        client doit avoir été configuré pour faire confiance au
        *CertificateManager* avant que l'onboarding ne commence, et ne définit
        aucun mécanisme en bande. Cette méthode est l'extrémité réseau de cette
        décision d'administrateur — elle s'exécute au démarrage, sans session et
        sans validation, ce qui est la seule façon qu'elle ait un sens.

        L'ordre est impératif : après ``_build_certificate_groups``, donc
        seulement si le groupe visé existe. Un ancrage destiné à un groupe
        absent est signalé et non redirigé vers un autre groupe — un
        certificat de confiance placé dans un groupe qui ne correspond pas à sa
        fonction accepterait silencieusement des présentations qui ne
        devraient pas l'être.
        """
        certificates = self.config.certificates
        anchors = certificates.trusted_certificates
        issuers_declared = certificates.issuer_certificates
        crls_declared = certificates.issuer_crls
        if not (anchors or issuers_declared or crls_declared):
            return
        target = certificates.trusted_certificates_group
        node = self.certificate_groups.get(target)
        if node is None:
            logger.error(
                f"Ancrages de confiance ignorés : le groupe {target!r} n'est pas "
                f"rattaché. Groupes disponibles : "
                f"{', '.join(self.certificate_groups) or '(aucun)'}. "
                f"Ajoutez {target!r} à certificates.groups, ou corrigez "
                f"certificates.trusted_certificates_group. Aucun certificat n'est "
                f"redirigé vers un autre groupe : un ancrage dans le mauvais "
                f"groupe accepterait des présentations qui ne doivent pas l'être."
            )
            return
        loaded = load_trusted_certificates(node.group, anchors)

        # Meme regle, deux autres listes : l'autorite et sa CRL sont hors bande
        # au meme titre que les ancres. Aucune des deux ne passe par la
        # validation, pour la meme raison qu'elles : ce sont des pieces de
        # contexte de confiance, pas des certificats presentes par le reseau.
        issuers = load_trusted_certificates(
            node.group, certificates.issuer_certificates, is_trusted=False
        )
        crls = load_issuer_crls(node.group, crls_declared)

        if anchors and not loaded:
            logger.error(
                f"Aucun ancrage de confiance n'a pu être chargé depuis "
                f"{len(anchors)} source(s) déclarée(s). Le GDS démarrera avec une "
                f"liste de confiance vide et refusera toute présentation de "
                f"certificat — c'est le comportement attendu quand la liste est "
                f"vide, mais ici c'est presque certainement une erreur de "
                f"déploiement."
            )

        if issuers_declared and not issuers:
            logger.error(
                f"Aucune autorité de certification n'a pu être chargée depuis "
                f"{len(issuers_declared)} source(s) déclarée(s). Tout certificat "
                f"signé par une autorité sera refusé : sa chaîne ne remontera "
                f"à aucun certificat de confiance."
            )

        # Une autorité sans CRL est le cas qui mérite le plus un avertissement,
        # parce que son symptôme est un refus partout et nulle part ailleurs :
        # la validation des autres listes passe, et rien n'indique la cause.
        if issuers and not crls:
            logger.warning(
                f"{len(issuers)} autorité(s) de certification chargée(s) mais "
                f"AUCUNE CRL. L'état de révocation des certificats qu'elles ont "
                f"signés sera INCONNU, et le défaut fermé de §7.8.2.10 les "
                f"refusera tous. Ce refus est correct — mais il n'a pas d'issue "
                f"tant que la CRL n'est pas distribuée. Déclarez "
                f"certificates.issuer_crls, ou posez "
                f"SuppressRevocationStatusUnknown si votre déploiement ne "
                f"diffuse volontairement pas de CRL."
            )

        logger.info(
            f"Contexte de confiance chargé dans {target!r} : "
            f"{len(node.group.trusted_certificates)} certificat(s) de confiance, "
            f"{len(node.group.issuer_certificates)} émetteur(s), "
            f"{len(node.group.issuer_crls)} CRL(s)"
            + (
                f" | ancres : {', '.join(loaded)}" if loaded else ""
            )
            + (f" | émetteurs : {', '.join(issuers)}" if issuers else "")
            + (f" | CRL : {', '.join(crls)}" if crls else "")
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
            self.server,
            self.certificate_store,
            group_name=self._group_name,
            group_nodeids={
                name: node.node.nodeid
                for name, node in self.certificate_groups.items()
                if node.node is not None
            },
            audit=self.audit,
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

