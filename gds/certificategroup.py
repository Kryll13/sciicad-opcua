"""Expose un :class:`~gds.trustlist.CertificateGroup` dans l'espace d'adressage.

La Part 12 n'a pas de *service* binaire pour la gestion des listes de
confiance : ce sont des méthodes d'un objet. Le GDS doit donc publier
l'arborescence, avec ses NodeIds normatifs, et brancher des gestionnaires qui
appellent la logique métier.

Où publier, et pourquoi cela importe
-----------------------------------

Un ``CertificateGroupType`` ne vit pas n'importe où. §7.8.3.3 décrit le
dossier qui l'organise, et §7.9.2 place ce dossier sous l'objet de
configuration du serveur. L'arborescence normative est donc :

    Server
     └─ ServerConfiguration          (ServerConfigurationType, i=12637)
         └─ CertificateGroups        (CertificateGroupFolderType, i=14053)
             └─ DefaultApplicationGroup   (CertificateGroupType, i=14156)
                 └─ TrustList        (TrustListType, i=12642)

asyncua construit **déjà** les trois niveaux, avec les NodeIds d'instance
publiés par la norme. Créer un groupe ailleurs n'ajoute aucune capacité : cela
ajoute un nœud que la norme ne prévoit pas, en laisse un autre sans
gestionnaire, et fait qu'un client qui parcourt le chemin normatif aboutit à un
groupe mort renvoyant ``BadNothingToDo`` — le groupe réel, celui qu'il
voulait lire, restant hors d'atteinte. Ce module réattache donc ses
gestionnaires aux instances existantes au lieu d'en fabriquer.

C'est la même leçon que pour l'objet ``ServerConfiguration`` lui-même : la
première version de ce câblage en créait un second, invisible pour un test qui
visait le NodeId, fatal pour un client qui parcourt l'espace d'adressage.

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

#: NodeIds normatifs (OPC 10000-12 §7.8.3, sous-section TrustLists §7.8.2.1).
CERTIFICATE_GROUP_TYPE = ua.ObjectIds.CertificateGroupType          # 12555

#: Browse names normatifs du parcours §7.8.3.3 / §7.10.4.
GROUPS_FOLDER_NAME = "CertificateGroups"
SERVER_CONFIGURATION_NAME = "ServerConfiguration"

#: Plage privée, utilisée uniquement si un groupe doit être *créé* parce que
#: l'instance normative est absente. Les NodeIds d'instance publiés par la
#: norme ne sont pas squattés : les NodeIds d'un groupe ajouté par
#: l'administrateur restent hors des plages de la norme.
INSTANCE_BASE = 3_000_000
INSTANCE_SPAN = 900_000

#: Types de certificats admis par groupe, exigés par §7.8.3.1 : la propriété
#: ``CertificateTypes`` est *Mandatory* et « shall specify one or more subtypes
#: of ``ApplicationCertificateType`` » pour le groupe d'application.
#:
#: Les identifiants viennent du ``NodeIds.csv`` officiel de la Fondation OPC.
#: Pour le groupe de jetons d'utilisateur, l'identifiant normatif est
#: ``UserCertificateType`` (i=19323) : l'instantané d'asyncua est antérieur au
#: renommage, et garde l'ancien nom ``UserCredentialCertificateType`` (i=15181)
#: pour la même notion. C'est le numéro normatif qui est publié ici.
CERTIFICATE_TYPES: dict[str, tuple[int, ...]] = {
    "DefaultApplicationGroup": (
        ua.ObjectIds.RsaMinApplicationCertificateType,      # 12559
        ua.ObjectIds.RsaSha256ApplicationCertificateType,   # 12560
    ),
    "DefaultHttpsGroup": (ua.ObjectIds.HttpsCertificateType,),  # 12558
    "DefaultUserTokenGroup": (19323,),  # UserCertificateType
}

#: Propriétés obligatoires de l'objet ``TrustList`` : nom du nœud, champ
#: correspondant dans :meth:`gds.trustlist.CertificateGroup.state`, et type de
#: variante. Les DataTypes sont ceux de la Part 20 et de la Part 12 :
#: ``OpenCount`` est un ``UInt16``, ``LastUpdateTime`` un ``UtcTime``,
#: ``Size`` un ``UInt64`` — ce dernier n'y figure pas, sa valeur étant un statut
#: d'erreur, écrit une fois pour toutes par ``_declare_file_properties``.
_PROPERTIES: dict[str, tuple[str, ua.VariantType]] = {
    "Writable": ("writable", ua.VariantType.Boolean),
    "UserWritable": ("user_writable", ua.VariantType.Boolean),
    "OpenCount": ("open_count", ua.VariantType.UInt16),
    "LastUpdateTime": ("last_update_time", ua.VariantType.DateTime),
}


async def certificate_group_folder(server):
    """Retourne le dossier ``CertificateGroups``, en le créant s'il manque.

    Le dossier est normatif : il est le ``TypeDefinition`` du conteneur qui
    organise les groupes (§7.8.3.3). S'il est absent, le repli est de le créer
    sous ``ServerConfiguration`` — et non sous ``Server``, qui n'est pas la
    place prévue.
    """
    configuration = await _child_by_name(server.nodes.server, SERVER_CONFIGURATION_NAME)
    if configuration is None:
        logger.warning(
            f"{SERVER_CONFIGURATION_NAME} absent de l'espace d'adressage : "
            f"les groupes de certificats ne pourront pas être publiés à leur "
            f"place normative"
        )
        return None
    folder = await _child_by_name(configuration, GROUPS_FOLDER_NAME)
    if folder is None:
        logger.warning(
            f"{GROUPS_FOLDER_NAME} absent de {SERVER_CONFIGURATION_NAME} : il est créé"
        )
        folder = await configuration.add_object(
            ua.NodeId(ua.ObjectIds.ServerConfigurationType_CertificateGroups, 0),
            ua.QualifiedName(GROUPS_FOLDER_NAME, 0),
            objecttype=ua.ObjectIds.CertificateGroupFolderType,
        )
    return folder


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
        #: Nœuds des propriétés obligatoires de la ``TrustList``.
        self._property_nodes: dict = {}
        #: Dernière valeur écrite, pour n'écrire que ce qui change.
        self._published: dict = {}

    async def build(self, parent=None) -> None:
        """Rattache le groupe à son instance normative et branche les méthodes.

        :param parent: dossier ``CertificateGroups`` hôte. Résolu par défaut
            depuis ``ServerConfiguration`` (§7.8.3.3) ; le repli sur le nœud
            ``Server`` ne sert qu'à un serveur dépourvu de
            ``ServerConfiguration``, où la norme n'offre aucune autre place.
        """
        if parent is None:
            parent = await certificate_group_folder(self.server)
        if parent is None:
            logger.warning(
                f"Groupe « {self.group.name} » publié sous le nœud Server : "
                f"cette place n'est pas celle de la norme (§7.8.3.3)"
            )
            parent = self.server.nodes.server

        self.node = await _child_by_name(parent, self.group.name)
        if self.node is None:
            self.node = await parent.add_object(
                ua.NodeId(self._instance_nodeid(), 0),
                ua.QualifiedName(self.group.name, 0),
                objecttype=CERTIFICATE_GROUP_TYPE,
            )
            logger.info(
                f"CertificateGroup « {self.group.name} » créé sous {parent.nodeid}"
            )
        else:
            logger.info(
                f"CertificateGroup « {self.group.name} » rattaché à l'instance "
                f"normative {self.node.nodeid} sous {parent.nodeid}"
            )

        await self._declare_certificate_types()
        await self._bind_trust_list()

    async def _declare_certificate_types(self) -> None:
        """Renseigne ``CertificateTypes``, propriété obligatoire (§7.8.3.1).

        La norme dit ce que ces types signifient : « the set of permitted types
        is specified by the ``CertificateTypes`` Property belonging to the
        CertificateGroup » (§7.10.10). La laisser à ``None`` reviendrait à
        interdire tout certificat — un groupe qui refuse de tout, sans le dire.

        Les types déclarés sont ceux que le GDS sait réellement produire. Y
        annoncer une courbe ECC alors que la génération de clé est limitée à
        RSA ferait échouer ``CreateSigningRequest`` sur un type que le groupe
        autorise : la propriété doit décrire la capacité, pas l'envie.
        """
        types = CERTIFICATE_TYPES.get(self.group.name)
        if not types:
            logger.warning(
                f"Groupe « {self.group.name} » : aucun type de certificat "
                f"normatif connu, la propriété CertificateTypes reste vide"
            )
            return
        node = await _child_by_name(self.node, "CertificateTypes")
        if node is None:
            logger.warning(
                f"Groupe « {self.group.name} » : propriété CertificateTypes "
                f"absente de l'instance, elle n'est pas créée"
            )
            return
        await node.write_value(
            ua.Variant(
                [ua.NodeId(identifier, 0) for identifier in types],
                ua.VariantType.NodeId,
            )
        )
        logger.debug(
            f"  CertificateTypes = {', '.join('i=%d' % t for t in types)}"
        )

    async def _bind_trust_list(self) -> None:
        """Instancie la ``TrustList`` et branche méthodes et propriétés."""
        self.trust_list = await _child_by_name(self.node, "TrustList")
        if self.trust_list is None:
            raise TrustListError(
                "l'instance de CertificateGroupType n'expose pas d'objet TrustList"
            )

        await self._declare_file_properties()

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

    async def _declare_file_properties(self) -> None:
        """Renseigne les propriétés obligatoires de l'objet ``TrustList``.

        Cinq le sont, et aucune n'était écrite : un client qui lisait
        ``OpenCount`` obtenait ``None``, donc ne pouvait pas voir une ouverture
        abandonnée — exactement ce que la Part 20 expose cette propriété pour
        rendre visible. Le modèle Python était juste ; l'espace d'adressage, non.

        ``Size`` est le cas particulier. §7.8.2.1 : « The ``Size`` Property
        inherited from ``FileType`` has no meaning for TrustList and returns the
        error code defined in OPC 10000-20 », et la Part 20 tranche : « If the
        Server can not accurately determine the size of the file, the ``Size``
        Property shall be returned to a Client with a StatusCode of
        ``Bad_NotSupported`` ». La valeur est donc un statut d'erreur, pas un
        nombre — l'écrire à ``0`` ou à ``None`` mentirait sur la taille d'un
        contenu qui, lui, se lit très bien.
        """
        size = await _child_by_name(self.trust_list, "Size")
        if size is not None:
            # ua.DataValue est un dataclass figé : la valeur est remplacée, pas
            # modifiée sur place.
            current = await size.read_data_value()
            await size.write_attribute(
                ua.AttributeIds.Value,
                ua.DataValue(
                    Value=ua.Variant(None, current.Value.VariantType),
                    # Le champ s'appelle StatusCode_ : « StatusCode » est
                    # utilisé par asyncua pour l'attribut de même nom de la
                    # DataValue, et le constructeur a dû le renommer.
                    StatusCode_=ua.StatusCode(ua.StatusCodes.BadNotSupported),
                    SourceTimestamp=current.SourceTimestamp,
                    ServerTimestamp=current.ServerTimestamp,
                ),
            )
            logger.debug("  Size = BadNotSupported (§7.8.2.1)")

        state = self.group.state()
        for name in ("Writable", "UserWritable", "OpenCount", "LastUpdateTime"):
            node = await _child_by_name(self.trust_list, name)
            if node is None:
                logger.warning(
                    f"propriété {name} absente de l'instance TrustList, "
                    f"elle n'est pas créée"
                )
                continue
            self._property_nodes[name] = node
        await self._publish(force=True)

    async def _publish(self, force: bool = False) -> None:
        """Recopie l'état de la liste dans les propriétés de l'espace d'adressage.

        Appelé après chaque méthode servie. C'est suffisant pour que les
        propriétés soient exactes : une poignée n'existe que si un ``Open`` est
        passé par ce nœud, et cet appel met la propriété à jour. Aucune tâche de
        fond n'est nécessaire, et une rafraîchirait des valeurs déjà justes.

        Une note honnête sur ``UserWritable`` : la Part 20 veut qu'elle tienne
        compte des droits d'accès de l'utilisateur, et la Part 12 §7.2 exige
        qu'une écriture exige le rôle ``SecurityAdmin``. Le modèle de rôles n'est
        pas implanté — les rôles y sont définis en prose, sans NodeId, le modèle
        de la Part 5 étant propre à chaque application. La propriété vaut donc ce
        que vaut ``Writable``, et annoncer ``False`` serait faux.
        """
        state = self.group.state()
        for name, node in self._property_nodes.items():
            value = state[_PROPERTIES[name][0]]
            if not force and self._published.get(name) == value:
                continue
            _, variant_type = _PROPERTIES[name]
            await node.write_value(ua.Variant(value, variant_type))
            self._published[name] = value

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

        Délégué à :func:`_child_by_name`, pour qu'un même utilitaire serve à
        retrouver un groupe, un dossier ou une propriété.
        """
        return await _child_by_name(parent, name)


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
                result = await handler(*(_plain(v) for v in inputs))
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
            # Les propriétés obligatoires de l'objet TrustList sont republiées
            # ici, et non dans chaque gestionnaire : Open et Close modifient
            # OpenCount, CloseAndUpdate et AddCertificate modifient
            # LastUpdateTime — et il n'existe aucun autre chemin vers l'un ou
            # l'autre. Un refus, à l'inverse, ne change rien, donc ne publie
            # rien. Ainsi, un client qui relit une propriété juste après un
            # appel réussi la trouve juste, sans tâche de fond.
            await self._publish()
            return result

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


async def _child_by_name(parent, name: str):
    """Retrouve l'enfant direct portant ce browse name, ou ``None``.

    Le browse name est le seul moyen stable de retrouver un nœud ici : les
    NodeIds d'instance sont alloués par asyncua pour tout ce que la norme ne
    publie pas, et même normatifs ils ne sont pas garantis par la pile. Un
    appelant qui les mémoriserait entre deux sessions verrait son pointeur
    devenir caduc au redémarrage.
    """
    for node in await parent.get_children():
        if (await node.read_browse_name()).Name == name:
            return node
    return None


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
