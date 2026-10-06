"""Configuration du Global Discovery Server OPC UA.

Le GDS et le LDS partagent le même câblage de services ; ils ne diffèrent que
par la portée du registre et par les valeurs annoncées. La configuration
spécifique tient donc en quatre valeurs par défaut, et tout le reste est
hérité de :class:`lds.config.LDSConfig`.
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import BaseModel, Field, model_validator

from lds.config import DatabaseConfig, DiscoveryConfig, LDSConfig, ServerConfig

DEFAULT_CONFIG_FILENAME = "gds_config.yaml"

# Chemin d'endpoint fixé par la Part 12 pour ce rôle. Un client doit pouvoir
# distinguer un GDS d'un LDS sur la seule URL, y compris lorsqu'ils écoutent
# tous deux sur le port 4840.
GDS_ENDPOINT_PATH = "GlobalDiscoveryServer"


class CertificatesConfig(BaseModel):
    """Certificats publiés et gérés par le GDS (Part 12 §7.8 et §7.10).

    Le GDS est une autorité de distribution : il expose des groupes de
    certificats, chacun portant sa propre liste de confiance (§7.8). Un groupe
    sans certificat est un groupe vide, ce qui est conforme : un GDS qui n'a
    encore distribué rien n'a rien à annoncer.

    Il tient en outre le rôle *CertificateManager* (§7.1) : préparer une demande
    de signature et installer le certificat signé (§7.10). Ce rôle est
    désactivable, car un déploiement peut vouloir distribuer des listes de
    confiance sans exposer la gestion de certificats.
    """

    #: Groupes rattachés au démarrage, parmi ceux que le dossier
    #: ``CertificateGroups`` contient déjà (§7.8.3.3).
    #:
    #: Les trois sont repris par défaut, et c'est délibéré. Un groupe laissé
    #: hors de cette liste reste dans l'espace d'adressage *sans*
    #: gestionnaire : un client qui le trouve répond ``BadNothingToDo``, ce qui
    #: est pire qu'un groupe vide mais câblé. Un groupe vide se constate et
    #: s'explique ; un nœud muet se découvre par l'échec d'un appel.
    groups: list[str] = Field(
        default_factory=lambda: [
            "DefaultApplicationGroup",
            "DefaultHttpsGroup",
            "DefaultUserTokenGroup",
        ]
    )

    #: Publier l'objet ``ServerConfiguration`` et ses méthodes de signature.
    manage_certificates: bool = True

    #: Taille des clés générées par ``CreateSigningRequest``, en bits.
    key_size: int = Field(default=2048, ge=2048, le=4096)

    #: Noms d'hôte ajoutés au SAN des demandes de signature, en plus de l'URI
    #: d'application. Vide par défaut : c'est à l'administrateur de dire quelle
    #: identité le GDS doit annoncer.
    hostnames: list[str] = Field(default_factory=list)

    #: Certificats de confiance amorcés **hors bande**, Part 12 §7.1.
    #:
    #: La norme ne définit aucun amorçage en bande. §7.1 l'exige au contraire :
    #: « Clients shall only connect to a CertificateManager which the Client has
    #: been configured to trust. This may require an out of band configuration
    #: step which is completed prior to starting the manual onboarding process. »
    #:
    #: C'est donc ici que se joue l'ancre de confiance du déploiement, et nulle
    #: part ailleurs. Un chemin, un dossier, ou les deux ; un dossier est
    #: développé sur ``*.pem``, ``*.der`` et ``*.crt``.
    #:
    #: Ces certificats entrent par :meth:`CertificateGroup.add`, **sans** passer
    #: la validation de :mod:`gds.certstore` — et c'est délibéré, pas une
    #: commodité. La validation certifie qu'un certificat présenté par le réseau
    #: est conforme ; ici, c'est l'administrateur qui décide, hors bande, que
    #: cette clé est de confiance. Faire passer l'ancre par la validation
    #: rendrait l'ancre de confiance dépendante d'elle-même. Concrètement, la
    #: validation échouerait d'ailleurs : avec le défaut fermé de §7.8.2.10, un
    #: certificat constructeur sans CRL associée a un état de révocation
    #: inconnu, donc refusé. Une ancre ne peut pas exiger la preuve de sa
    #: propre existence.
    #:
    #: Ces fichiers ne doivent contenir que des certificats publics. Une clé
    #: privée dans une liste de confiance est lisible par tout client autorisé à
    #: lire la liste.
    trusted_certificates: list[str] = Field(default_factory=list)

    #: Groupe destinataire des certificats amorcés ci-dessus. Les
    #: ``CertificateType`` d'un groupe désignent à quoi sert le certificat ;
    #: un certificat d'application n'a de sens que dans
    #: ``DefaultApplicationGroup``. Y mettre un certificat d'utilisateur ou de
    #: HTTPS rendrait la liste incohérente avec son propre ``CertificateTypes``.
    trusted_certificates_group: str = "DefaultApplicationGroup"

    #: Autorités de certification des certificats amorcés ci-dessus.
    #:
    #: C'est la seconde moitié du déploiement à autorité, et elle n'est pas
    #: facultative : sans elle, un certificat d'application doit être signé par
    #: un certificat de confiance du groupe — donc auto-signé. La Part 12
    #: décrit les deux configurations, mais dès qu'une autorité existe, elle se
    #: déclare ici.
    #:
    #: Le certificat de l'autorité n'est **pas** un certificat de confiance :
    #: il n'authentifie pas une application, il authentifie ce qui a signé une
    #: application. Le mettre dans ``trusted_certificates`` le ferait aussi, ce
    #: qui est plus permissif que nécessaire — il vaudrait validation directe de
    #: tout certificat qu'il a signé.
    issuer_certificates: list[str] = Field(default_factory=list)

    #: Listes de révocation des autorités ci-dessus.
    #:
    #: Leur présence n'est pas décorative : le défaut fermé de §7.8.2.10 refuse
    #: un certificat dont l'état de révocation est **inconnu**, et il est
    #: inconnu précisément en l'absence de CRL de l'émetteur. Déclarer une
    #: autorité sans sa CRL rend donc tous les certificats qu'elle a signés
    #: refusés — un refus correct, mais sans issue.
    #:
    #: Une CRL est **consultée par émetteur** : seules celles signées par
    #: l'autorité du certificat présenté sont appliquées. Une CRL sans
    #: signature correspondante est ignorée, avec un avertissement.
    issuer_crls: list[str] = Field(default_factory=list)


class AuditConfig(BaseModel):
    """Événements d'audit OPC UA, Part 12 §7.8.2.13 et §7.10.27.

    À ne pas confondre avec ``database.event_log``, qui écrit des lignes dans
    une table SQLite. Un événement d'audit est une notification OPC UA : il
    n'atteint que les clients abonnés, avec un ``EventType`` et des propriétés
    typées. Les deux mécanismes répondent à des questions différentes — « qu'a
    fait le serveur ? » et « qu'est-il arrivé à ce client ? » — et un seul des
    deux est interrogeable par un client OPC UA.
    """

    #: Publier ``TrustListUpdatedAuditEventType`` et
    #: ``CertificateUpdatedAuditEventType``. Actif par défaut : sans lui, une
    #: modification de la liste de confiance est silencieuse pour tout client.
    enabled: bool = True


class GDSConfig(LDSConfig):
    """Configuration d'un GDS : portée globale, endpoint dédié.

    La portée ``global`` est la seule différence de fond avec le LDS : une
    inscription reste valide jusqu'à son retrait explicite, donc aucun
    renouvellement périodique n'est imposé aux serveurs inscrits.
    """

    config_filename: ClassVar[str] = DEFAULT_CONFIG_FILENAME

    certificates: CertificatesConfig = Field(default_factory=CertificatesConfig)

    audit: AuditConfig = Field(default_factory=AuditConfig)

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
