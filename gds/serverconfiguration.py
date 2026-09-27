"""Publie l'objet ``ServerConfiguration`` du GDS, Part 12 §7.10.

§7.10.4 place ``ServerConfiguration`` sous le nœud ``Server``, avec le
browse name ``0:ServerConfiguration`` et le ``TypeDefinition``
``ServerConfigurationType`` : les deux sont normatifs, et asyncua fournit
déjà l'``ObjectType`` ainsi que ses sept méthodes, générés depuis les schémas
officiels. Il n'y a donc rien à construire — seulement à brancher.

Trois des sept méthodes sont ici câblées, celles qui forment le rôle
*CertificateManager* décrit en §7.1 :

* ``CreateSigningRequest`` (§7.10.10) — émet la PKCS #10 ;
* ``UpdateCertificate`` (§7.10.5) — valide et installe le certificat signé ;
* ``GetRejectedList`` (§7.10.12) — restitue les refus.

Les quatre autres — ``ApplyChanges``, ``CancelChanges``, ``ResetToServerDefaults``
et ``GetCertificates`` — restent des méthodes générées sans gestionnaire. Un
appel client y répond ``BadNothingToDo``, ce qui est la bonne réponse : elles
appartiennent au modèle *transactionnel* du §7.10, et ce GDS applique ses
changements immédiatement, sans transaction à approuver. Les câbler à vide
donnerait un ``Good`` trompeur, pire que le refus.

Les deux pièges d'asyncua déjà rencontrés dans
:mod:`gds.certificategroup` s'appliquent ici : ``call_method(méthode, *args)``
prend l'identifiant de la méthode en premier argument, et un gestionnaire doit
retourner une liste de ``Variant`` — un refus étant **retourné** sous forme de
``StatusCode``, jamais levé, sous peine d'être réencapsulé en
``BadUnexpectedError``.
"""

from __future__ import annotations

from typing import Callable, Optional

from asyncua import ua
from loguru import logger

from .certstore import CertificateError, CertificateStore

#: Browse name normatif de l'objet (§7.10.4).
BROWSE_NAME = "ServerConfiguration"

#: NodeId normatif de l'instance sous le nœud ``Server``. asyncua crée déjà cet
#: objet dans ``Server.init()`` ; cette valeur n'est qu'un repli pour un serveur
#: qui ne l'aurait pas fait.
INSTANCE_NODEID = 12637


class ServerConfigurationNode:
    """Objet ``ServerConfiguration`` et ses méthodes *CertificateManager*.

    :param server: serveur asyncua à compléter.
    :param store: magasin de certificats qui porte la logique.
    :param group_name: résout un ``CertificateGroupId`` reçu en nom de groupe.
        La norme dit que ce NodeId désigne l'objet groupe ; le GDS doit donc
        traduire cet identifiant d'instance en groupe qu'il connaît.
    """

    def __init__(
        self,
        server,
        store: CertificateStore,
        group_name: Optional[Callable[[ua.NodeId], Optional[str]]] = None,
    ) -> None:
        self.server = server
        self.store = store
        self._group_name = group_name
        self.node = None

    async def build(self) -> None:
        """Rattache nos gestionnaires à l'objet ``ServerConfiguration`` existant.

        asyncua crée déjà cet objet dans ``Server.init()``, au NodeId normatif
        i=12637 et avec ses méthodes aux NodeIds normatifs d'instance. Le
        réutiliser est donc non seulement possible mais préférable : un client
        qui parcourt l'espace d'adressage par browse name trouverait sinon
        *deux* objets ``ServerConfiguration``, dont le premier — celui d'asyncua
        — resterait sans gestionnaire et répondrait ``BadNothingToDo``. Le GDS
        paraîtrait alors ne rien savoir des certificats alors que la moitié de
        son objet est câblée.

        La création d'un doublon n'a lieu que si l'objet est absent, ce qui n'est
        pas le cas d'un serveur asyncua standard.
        """
        self.node = await self._child(self.server.nodes.server, BROWSE_NAME)
        if self.node is None:
            logger.warning(
                f"Objet {BROWSE_NAME} absent de l'espace d'adressage : il est "
                f"créé sous i={INSTANCE_NODEID}"
            )
            self.node = await self.server.nodes.server.add_object(
                ua.NodeId(INSTANCE_NODEID, 0),
                ua.QualifiedName(BROWSE_NAME, 0),
                objecttype=ua.ObjectIds.ServerConfigurationType,
            )
        logger.info(
            f"{BROWSE_NAME} câblé sous {self.node.nodeid} "
            f"(d'après ServerConfigurationType i={ua.ObjectIds.ServerConfigurationType})"
        )

        await self._bind(
            "CreateSigningRequest", self._create_signing_request,
            in_args=[
                self._arg("CertificateGroupId", ua.ObjectIds.NodeId),
                self._arg("CertificateTypeId", ua.ObjectIds.NodeId),
                self._arg("SubjectName", ua.ObjectIds.String),
                self._arg("RegeneratePrivateKey", ua.ObjectIds.Boolean),
                self._arg("Nonce", ua.ObjectIds.ByteString),
            ],
            out_args=[self._arg("CertificateRequest", ua.ObjectIds.ByteString)],
        )
        await self._bind(
            "UpdateCertificate", self._update_certificate,
            in_args=[
                self._arg("CertificateGroupId", ua.ObjectIds.NodeId),
                self._arg("CertificateTypeId", ua.ObjectIds.NodeId),
                self._arg("Certificate", ua.ObjectIds.ByteString),
                self._arg("IssuerCertificates", ua.ObjectIds.ByteString, rank=1),
                self._arg("PrivateKeyFormat", ua.ObjectIds.String),
                self._arg("PrivateKey", ua.ObjectIds.ByteString),
            ],
            out_args=[self._arg("ApplyChangesRequired", ua.ObjectIds.Boolean)],
        )
        await self._bind(
            "GetRejectedList", self._get_rejected_list,
            out_args=[self._arg("Certificates", ua.ObjectIds.ByteString, rank=1)],
        )

    # -- utilitaires -------------------------------------------------------

    @staticmethod
    def _arg(name: str, data_type: int, rank: int = ua.ValueRank.Scalar) -> ua.Argument:
        """Décrit un argument de méthode.

        ``Argument.DataType`` attend un ``NodeId`` et non l'entier nu : avec un
        entier, la sérialisation binaire de l'argument échoue et l'appel de
        méthode ne part jamais. Le corrigé est donc le ``NodeId``, malgré ce que
        suggère le nom du champ.
        """
        return ua.Argument(
            Name=name,
            DataType=ua.NodeId(data_type),
            ValueRank=rank,
            ArrayDimensions=None,
            Description=ua.LocalizedText(name),
        )

    async def _child(self, parent, name: str):
        """Retrouve un enfant par browse name.

        Les NodeIds d'instance sont alloués par asyncua et ne sont donc pas
        normatifs : le browse name est le seul moyen stable de les retrouver,
        exactement comme pour les méthodes d'un ``CertificateGroupType``.
        """
        for node in await parent.get_children():
            if (await node.read_browse_name()).Name == name:
                return node
        return None

    async def _bind(self, name: str, handler, in_args=(), out_args=None) -> None:
        """Recrée une méthode de l'instance avec notre gestionnaire.

        asyncua n'attache pas de gestionnaire à une méthode déjà créée : elle est
        supprimée puis recréée **sous le même NodeId**, pour qu'un client la
        retrouve à l'identique.
        """
        node = await self._child(self.node, name)
        if node is None:
            raise CertificateError(f"méthode {name} absente de ServerConfigurationType")

        nodeid = node.nodeid
        await node.delete()

        async def call(_parent, *inputs):
            try:
                return await handler(*(_plain(value) for value in inputs))
            except CertificateError as exc:
                # Le statut porté par l'exception est *retourné* : une exception
                # levée serait réencapsulée en BadUnexpectedError, et le client
                # perdrait la distinction entre un nonce trop court, un
                # certificat expiré et une chaîne de confiance absente.
                logger.warning(f"{name} refusé -> {ua.StatusCode(exc.status).name} : {exc}")
                return ua.StatusCode(exc.status)
            except Exception:  # pragma: no cover - garde-fou
                logger.exception(f"{name} : erreur inattendue")
                return ua.StatusCode(ua.StatusCodes.BadInternalError)

        await self.node.add_method(
            nodeid, ua.QualifiedName(name, 0), call, list(in_args), list(out_args or []), None
        )
        logger.debug(f"  {name} (i={nodeid.Identifier}) branché sur {BROWSE_NAME}")

    def _group_of(self, nodeid) -> str:
        """Résout un ``CertificateGroupId`` en nom de groupe.

        La norme précise qu'un NodeId nul désigne le ``DefaultApplicationGroup``
        (§7.10.5). Tout NodeId inconnu est refusé plutôt que retomber sur le
        groupe par défaut : écrire dans un groupe que le client n'a pas demandé
        déplacerait un certificat à l'insu de l'appelant.
        """
        if nodeid is None or (isinstance(nodeid, ua.NodeId) and nodeid.is_null()):
            return "DefaultApplicationGroup"
        if self._group_name is None:
            raise CertificateError(
                "aucun groupe de certificats n'est publié : impossible de "
                "résoudre le CertificateGroupId",
                ua.StatusCodes.BadNodeIdUnknown,
            )
        name = self._group_name(nodeid)
        if name is None:
            raise CertificateError(
                f"CertificateGroupId inconnu : {nodeid}",
                ua.StatusCodes.BadNodeIdUnknown,
            )
        return name

    # -- gestionnaires -----------------------------------------------------

    async def _create_signing_request(
        self, group_id, type_id, subject, regenerate, nonce
    ):
        request = self.store.create_signing_request(
            group=self._group_of(group_id),
            certificate_type=_type_nodeid(type_id),
            subject=_text(subject),
            regenerate=bool(regenerate),
            nonce=_bytes_of(nonce),
        )
        return [ua.Variant(request, ua.VariantType.ByteString)]

    async def _update_certificate(
        self, group_id, type_id, certificate, issuer_certificates, key_format, key
    ):
        apply_required = self.store.update_certificate(
            group=self._group_of(group_id),
            certificate_type=_type_nodeid(type_id),
            certificate=_bytes_of(certificate),
            issuer_certificates=[_bytes_of(item) for item in _sequence(issuer_certificates)],
            private_key_format=_text(key_format),
            private_key=_bytes_of(key),
        )
        return [ua.Variant(apply_required, ua.VariantType.Boolean)]

    async def _get_rejected_list(self):
        return [ua.Variant(self.store.rejected(), ua.VariantType.ByteString)]


def _plain(value):
    """Retire l'éventuel ``Variant`` enveloppant une valeur d'entrée."""
    return value.Value if isinstance(value, ua.Variant) else value


def _text(value) -> str:
    value = _plain(value)
    return "" if value is None else str(value)


def _bytes_of(value) -> bytes:
    """Normalise un ByteString reçu par le protocole."""
    value = _plain(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, str):
        import base64

        return base64.b64decode(value)
    return b"" if value is None else bytes(value)


def _sequence(value) -> list:
    """Normalise un tableau reçu par le protocole en liste Python."""
    value = _plain(value)
    if value is None:
        return []
    return list(value) if isinstance(value, (list, tuple)) else [value]


def _type_nodeid(value) -> Optional[ua.NodeId]:
    """Normalise un ``CertificateTypeId``, dont la valeur nulle est licite."""
    value = _plain(value)
    if isinstance(value, ua.NodeId):
        return None if value.is_null() else value
    if value is None:
        return None
    try:
        nodeid = ua.NodeId(int(value), 0)
    except (TypeError, ValueError):
        return None
    return None if nodeid.is_null() else nodeid
