"""Expose un :class:`~gds.trustlist.CertificateGroup` dans l'espace d'adressage.

La Part 12 n'a pas de *service* binaire pour la gestion des listes de
confiance : ce sont des méthodes d'un objet. Le GDS doit donc publier
l'arborescence, avec ses NodeIds normatifs, et brancher des gestionnaires qui
appellent la logique métier.

asyncua fournit déjà l'``ObjectType`` ``CertificateGroupType`` (i=12555) dans son
espace d'adressage standard, avec les dix méthodes ``TrustList``. Il suffit de
l'instancier : l'arborescence complète est générée, et les méthodes sont
déclarées ``Executable``. Elles n'ont cependant aucun gestionnaire, donc un
appel client renverrait ``BadNothingToDo`` ; ce module les recrée avec les
nôtres, sous le même NodeId.

Deux pièges d'asyncua 1.1.8, rencontrés et contournés ici. Tous deux sont
silencieux : l'appel échoue sans que l'erreur ne désigne la cause.

* ``call_method(méthode, *arguments)`` : le **premier** argument est
  l'identifiant de la méthode, pas un argument. Passer un ``bytes`` en premier
  l'interprète comme un ``MethodId``, et la sérialisation échoue sur
  « 'bytes' object has no attribute 'NodeIdType' ». L'appel se fait depuis le
  nœud parent : ``parent.call_method(methode, *args)``.
* Un gestionnaire doit **retourner** une liste de ``Variant`` : renvoyer une
  valeur nue casse la sérialisation de la réponse (« object of type 'int' has
  no len() »). Un refus doit être **retourné** sous forme de ``ua.StatusCode``,
  car toute exception levée est réencapsulée en ``BadUnexpectedError`` par
  ``address_space.py``, ce qui masque le motif réel.
"""

from __future__ import annotations

import base64
from typing import Optional

from asyncua import ua
from loguru import logger

from .trustlist import MODE_READ, CertificateGroup, TrustListError

#: NodeIds normatifs (OPC 10000-12 §7.8.2, schéma 1.05).
CERTIFICATE_GROUP_TYPE = ua.ObjectIds.CertificateGroupType          # 12555

#: Base de la plage privée des instances. Les NodeIds d'instance n'ont aucune
#: valeur normative : ils sont donc alloués loin des plages de la norme, pour
#: ne pas risquer d'en squatter une. 3 000 000 + hash du nom.
INSTANCE_BASE = 3_000_000
INSTANCE_SPAN = 900_000


class CertificateGroupNode:
    """Groupe de certificats publié dans l'espace d'adressage d'un serveur.

    :param server: serveur asyncua dont l'espace d'adressage est complété.
    :param group: la liste de confiance à publier ; une nouvelle est créée si
        aucun groupe n'est fourni.
    """

    def __init__(
        self, server, group: Optional[CertificateGroup] = None
    ) -> None:
        self.server = server
        self.group = group if group is not None else CertificateGroup()
        self.node = None
        self.trust_list = None

    async def build(self) -> None:
        """Instancie ``CertificateGroupType`` et branche les gestionnaires."""
        self.node = await self.server.nodes.server.add_object(
            ua.NodeId(self._instance_nodeid(), 0),
            ua.QualifiedName(self.group.name, 0),
            objecttype=CERTIFICATE_GROUP_TYPE,
        )
        logger.info(
            f"CertificateGroup « {self.group.name} » publié sous {self.node.nodeid} "
            f"(d'après CertificateGroupType i={ua.ObjectIds.CertificateGroupType})"
        )

        self.trust_list = await self._child(self.node, "TrustList")
        if self.trust_list is None:
            raise TrustListError(
                "l'instance de CertificateGroupType n'expose pas d'objet TrustList"
            )

        handle = self._arg("FileHandle", ua.ObjectIds.UInt32)

        await self._bind(
            self.trust_list, "Open", self._open,
            in_args=[self._arg("Mode", ua.ObjectIds.OpenFileMode)],
            out_args=[handle],
        )
        await self._bind(
            self.trust_list, "Close", self._close, in_args=[handle]
        )
        await self._bind(
            self.trust_list, "Read", self._read,
            in_args=[handle, self._arg("Length", ua.ObjectIds.UInt32)],
            out_args=[self._arg("Data", ua.ObjectIds.ByteString)],
        )
        await self._bind(
            self.trust_list, "Write", self._write,
            in_args=[handle, self._arg("Data", ua.ObjectIds.ByteString)],
        )
        await self._bind(
            self.trust_list, "GetPosition", self._get_position,
            in_args=[handle], out_args=[self._arg("Position", ua.ObjectIds.UInt32)],
        )
        await self._bind(
            self.trust_list, "SetPosition", self._set_position,
            in_args=[handle, self._arg("Position", ua.ObjectIds.UInt32)],
        )
        await self._bind(
            self.trust_list, "OpenWithMasks", self._open_with_masks,
            in_args=[self._arg("Masks", ua.ObjectIds.UInt32)],
            out_args=[handle],
        )
        await self._bind(
            self.trust_list, "CloseAndUpdate", self._close_and_update,
            in_args=[handle],
        )
        # Ces deux méthodes sont sur l'objet TrustList, au même titre que
        # Open et Read : elles s'appliquent à la liste de confiance elle-même.
        await self._bind(
            self.trust_list, "AddCertificate", self._add_certificate,
            in_args=[
                self._arg("Certificate", ua.ObjectIds.ByteString),
                self._arg("IsTrustedCertificate", ua.ObjectIds.Boolean),
            ],
        )
        await self._bind(
            self.trust_list, "RemoveCertificate", self._remove_certificate,
            in_args=[
                self._arg("Thumbprint", ua.ObjectIds.String),
                self._arg("IsTrustedCertificate", ua.ObjectIds.Boolean),
            ],
        )

    # -- utilitaires -------------------------------------------------------

    def _instance_nodeid(self) -> int:
        """NodeId de l'instance, dans une plage privée.

        ``hash`` n'est pas stable d'un processus à l'autre, ce qui est sans
        conséquence ici : l'instance n'est pas persistée, et aucun client
        ne doit mémoriser cet identifiant d'une session à l'autre. Le browse
        name reste le moyen stable de retrouver le groupe.
        """
        return INSTANCE_BASE + (hash(self.group.name) % INSTANCE_SPAN)

    @staticmethod
    def _arg(name: str, data_type: int) -> ua.Argument:
        """Décrit un argument de méthode.

        ``Argument.DataType`` attend un ``NodeId``, et non l'identifiant
        numérique nu : avec un entier, la sérialisation binaire de l'argument
        échoue sur ``'int' object has no attribute 'NodeIdType'``, et l'appel
        de méthode ne part jamais. Le corrigé est donc de passer le NodeId,
        malgré ce que suggère le nom du champ.
        """
        return ua.Argument(
            Name=name,
            DataType=ua.NodeId(data_type),
            ValueRank=ua.ValueRank.Scalar,
            ArrayDimensions=None,
            Description=ua.LocalizedText(name),
        )

    async def _child(self, parent, name: str):
        """Retrouve un enfant par browse name.

        Les NodeIds d'instance sont alloués par asyncua, ils ne sont donc pas
        normatifs : le browse name est le seul moyen stable de les retrouver.
        """
        for node in await parent.get_children():
            if (await node.read_browse_name()).Name == name:
                return node
        return None

    async def _bind(self, parent, name: str, handler, in_args, out_args=None) -> None:
        """Recrée une méthode de l'instance avec notre gestionnaire.

        asyncua ne permet pas d'attacher un gestionnaire à une méthode déjà
        créée : elle est supprimée puis recréée **sous le même NodeId**, de
        sorte qu'un client la retrouve à l'identique.
        """
        node = await self._child(parent, name)
        if node is None:
            raise TrustListError(f"méthode {name} absente de l'instance")

        nodeid = node.nodeid
        await node.delete()

        async def call(_parent, *inputs):
            try:
                return await handler(*(_plain(v) for v in inputs))
            except TrustListError as exc:
                # Le code est *retourné*, pas levé : asyncua enveloppe toute
                # exception en BadUnexpectedError (address_space.py, _call),
                # ce qui masquerait la cause réelle. Un StatusCode renvoyé
                # devient le résultat de l'appel, et le client reçoit enfin
                # BadNotWritable, ou BadInvalidArgument pour un handle inconnu.
                logger.warning(f"{name} refusé : {exc} -> {_status_name(exc)}")
                return ua.StatusCode(_status_for(exc))
            except Exception:  # pragma: no cover - garde-fou
                logger.exception(f"{name} : erreur inattendue")
                return ua.StatusCode(ua.StatusCodes.BadInternalError)

        await parent.add_method(
            nodeid, ua.QualifiedName(name, 0), call, in_args, out_args or [], None
        )
        logger.debug(
            f"  {name} (i={nodeid.Identifier}) branché sur {self.group.name}"
        )

    # -- gestionnaires -----------------------------------------------------

    # Les gestionnaires renvoient une liste de Variant, jamais une valeur
    # brute : asyncua affecte tel quel le retour à
    # CallMethodResult.OutputArguments, qui est un List[Variant]. Renvoyer un
    # `int` fait échouer la sérialisation de la réponse sur
    # « object of type 'int' has no len() », et le client reçoit un
    # BadInternalError sans indication de la cause.

    async def _open(self, mode):
        return [ua.Variant(self.group.open(int(mode)), ua.VariantType.UInt32)]

    async def _close(self, handle):
        self.group.close(int(handle))
        return []

    async def _read(self, handle, length):
        data = self.group.read(int(handle), int(length))
        return [ua.Variant(data, ua.VariantType.ByteString)]

    async def _write(self, handle, data):
        self.group.write(int(handle), _as_bytes(data))
        return []

    async def _get_position(self, handle):
        return [ua.Variant(self.group.get_position(int(handle)), ua.VariantType.UInt32)]

    async def _set_position(self, handle, position):
        self.group.set_position(int(handle), int(position))
        return []

    async def _open_with_masks(self, masks):
        # Un masque restreint est un droit de lecture : il ne doit jamais
        # donner accès en écriture.
        handle = self.group.open(MODE_READ, int(masks))
        return [ua.Variant(handle, ua.VariantType.UInt32)]

    async def _close_and_update(self, handle):
        self.group.close_and_update(int(handle))
        return []

    async def _add_certificate(self, certificate, is_trusted):
        self.group.add(_as_bytes(certificate), bool(is_trusted))
        return []

    async def _remove_certificate(self, thumbprint, is_trusted):
        self.group.remove(str(thumbprint), bool(is_trusted))
        return []


def _plain(value):
    """Retire l'éventuel ``Variant`` enveloppant une valeur d'entrée."""
    return value.Value if isinstance(value, ua.Variant) else value


def _as_bytes(value) -> bytes:
    """Normalise un ByteString reçu par le protocole."""
    value = _plain(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, str):
        return base64.b64decode(value)
    if value is None:
        return b""
    return bytes(value)


def _status_for(exc: TrustListError) -> int:
    """Traduit une erreur métier en ``StatusCode`` normatif.

    Un ``FileHandle`` inconnu donne ``BadInvalidArgument`` : il n'existe pas de
    code « handle invalide » en OPC UA, et l'inventer produirait un
    ``AttributeError`` au moment du journal, qui masquerait le vrai refus
    derrière un ``BadUnexpectedError``.
    """
    text = str(exc)
    if "lecture seule" in text:
        return ua.StatusCodes.BadNotWritable
    return ua.StatusCodes.BadInvalidArgument


def _status_name(exc: TrustListError) -> str:
    """Nom du code renvoyé, pour le journal de diagnostic."""
    return ua.StatusCode(_status_for(exc)).name
