"""Événements d'audit du GDS, Part 12 §7.8.2.13 et §7.10.27.

Deux `ObjectType` sont publiés par la Fondation OPC, donc atteignables sans rien
inventer :

* ``TrustListUpdatedAuditEventType`` (i=12561) — « raised when a TrustList is
  successfully changed », que ce soit par ``CloseAndUpdate``, ``AddCertificate``
  ou ``RemoveCertificate``. Sa propriété obligatoire ``TrustListId`` est le
  NodeId de l'objet ``TrustList`` modifié.
* ``CertificateUpdatedAuditEventType`` (i=12620) — « raised when a Certificate
  is actually changed as a result of a Method call », donc après un
  ``UpdateCertificate`` réussi. « No Event is raised if the Method call fails. »

Ce ne sont pas des journaux
----------------------------

Un événement OPC UA n'est pas une ligne de journal : c'est une notification que
le serveur pousse à un client abonné, avec un ``EventType`` et des propriétés
typées. Un client l'abonne, filtre sur l'``EventType`` et reçoit la trace
n'arrive que lorsqu'il le demande. C'est l'inverse du journal, qui part à tout le
monde et ne s'adresse à personne en particulier. Confondre les deux fait croire
qu'une traçabilité existe parce qu'il y a des lignes dans un fichier — c'est
précisément ce que fait le journal d'événements de la base SQLite, sous une
clé de configuration (`event_log`) qui ne parle que d'elle-même.

Un échec d'audit n'interrompt pas l'opération
---------------------------------------------

Émettre un événement est un effet de bord du service, jamais sa condition.
L'audit est au mieux : si la mécanique d'événements est indisponible, on
le journalise et le service se poursuit. L'inverse rendrait l'audit capable de
refuser une ``Write`` de liste de confiance — un empoisonnement de Availability
par la traçabilité, ce qui est pire que l'absence de trace.

Note d'implémentation
---------------------

``asyncua.common.events.get_event_obj_from_type_node`` construit l'événement
depuis l'espace d'adressage, mais pose ``EventType`` par affectation directe
plutôt que par ``add_property``. Le type de la donnée n'est donc pas
enregistré, et la sérialisation de la ``PublishResponse`` échoue sur
``'str' object has no attribute 'VariantType'`` : la notification est perdue,
sans message pour le client. Le défaut est isolé — un ``BaseEvent`` nu, construit
comme ci-dessous, est livré correctement.

Les événements sont donc bâtis à partir de ``BaseEvent`` avec des
``add_property`` explicites, la liste des propriétés étant celle que la norme
énumère pour chaque type. C'est plus long qu'un appel, et c'est vérifiable :
chaque propriété est nommée dans ce fichier, donc relisible face à la norme.
"""

from __future__ import annotations

from typing import Optional

from asyncua import ua
from asyncua.common.event_objects import BaseEvent
from loguru import logger

#: Propriétés que ``AuditUpdateMethodEventType`` (Part 5) ajoute à
#: ``AuditEventType``, et dont la Part 12 fait hériter les deux types
#: ci-dessus : ``MethodId``, ``StatusCodeId``, ``InputArguments`` et
#: ``OutputArguments``. Elles sont posées explicitement dans :meth:`_build`.
_AUDIT_PROPERTIES: tuple[tuple[str, ua.VariantType, object], ...] = (
    ("ActionTimeStamp", ua.VariantType.DateTime, None),
    ("Status", ua.VariantType.Boolean, True),
    ("ServerId", ua.VariantType.String, "SCIICAD GDS"),
    ("ClientAuditEntryId", ua.VariantType.String, ""),
    ("ClientUserId", ua.VariantType.String, ""),
)

#: Sévérité : ``AuditUpdateMethodEventType`` est un événement d'audit, donc de
#: sévérité Information (300) au sens de la Part 4. Les alarmes de la Part 12
#: utilisent 1000 ; ce n'est pas le cas ici.
_SEVERITY = 300


class AuditEmitter:
    """Émet les deux événements d'audit du GDS, et rien d'autre.

    :param server: serveur asyncua, dont le générateur d'événements est
        emprunté. Un émetteur sans serveur est inerte : toutes les émissions
        deviennent des no-op journalisés, ce qui permet de câbler les
        gestionnaires sans se soucier de l'ordre d'assemblage.
    """

    def __init__(self, server=None) -> None:
        self.server = server
        self._generator = None
        self._issued = 0
        self._failed = 0

    async def start(self) -> None:
        """Réserve le générateur d'événements du serveur."""
        if self.server is None:
            return
        try:
            self._generator = await self.server.get_event_generator()
        except Exception as exc:  # pragma: no cover - dépend de la pile
            logger.warning(f"Audit indisponible : {type(exc).__name__}: {exc}")
            self._generator = None

    @property
    def available(self) -> bool:
        return self._generator is not None

    def summary(self) -> dict:
        """Compteurs, pour le diagnostic."""
        return {
            "disponible": self.available,
            "emis": self._issued,
            "echecs": self._failed,
        }

    # -- émissions ---------------------------------------------------------

    async def trust_list_updated(
        self,
        trust_list: ua.NodeId,
        method: ua.NodeId,
        arguments: str = "",
        message: str = "Liste de confiance mise à jour",
    ) -> None:
        """``TrustListUpdatedAuditEventType``, §7.8.2.13."""
        await self._fire(
            ua.ObjectIds.TrustListUpdatedAuditEventType,   # 12561
            message,
            method,
            arguments,
            extra=(("TrustListId", ua.VariantType.NodeId, trust_list),),
        )

    async def certificate_updated(
        self,
        certificate_group: ua.NodeId,
        certificate_type: Optional[ua.NodeId],
        method: ua.NodeId,
        arguments: str = "",
        message: str = "Certificat installé",
    ) -> None:
        """``CertificateUpdatedAuditEventType``, §7.10.27."""
        await self._fire(
            ua.ObjectIds.CertificateUpdatedAuditEventType,  # 12620
            message,
            method,
            arguments,
            extra=(
                ("CertificateGroup", ua.VariantType.NodeId, certificate_group),
                (
                    "CertificateType",
                    ua.VariantType.NodeId,
                    certificate_type if certificate_type is not None else ua.NodeId(0, 0),
                ),
            ),
        )

    async def _fire(
        self,
        event_type: int,
        message: str,
        method: ua.NodeId,
        arguments: str,
        extra: tuple,
    ) -> None:
        if not self.available:
            return
        try:
            event = self._build(event_type, method, arguments, extra)
            event.Message = ua.LocalizedText(message)
            await self._generator.init(event, emitting_node=ua.ObjectIds.Server)
            await self._generator.trigger()
            self._issued += 1
            logger.debug(f"Événement d'audit émis : i={event_type}")
        except Exception as exc:
            # Best-effort : voir la note de module. On journalise, on ne lève pas.
            self._failed += 1
            logger.warning(
                f"Événement d'audit non émis (i={event_type}) : "
                f"{type(exc).__name__}: {exc}"
            )

    def _build(
        self,
        event_type: int,
        method: ua.NodeId,
        arguments: str,
        extra: tuple,
    ) -> BaseEvent:
        event = BaseEvent()
        # EventType passe par add_property : une affectation directe
        # n'enregistrerait pas le type, et la notification se perdrait à la
        # sérialisation sans que le client en sache rien.
        event.add_property("EventType", ua.NodeId(event_type, 0), ua.VariantType.NodeId)
        for name, variant_type, default in _AUDIT_PROPERTIES:
            event.add_property(name, default, variant_type)
        event.add_property("MethodId", method or ua.NodeId(0, 0), ua.VariantType.NodeId)
        event.add_property(
            "StatusCodeId", ua.StatusCode(ua.StatusCodes.Good), ua.VariantType.StatusCode
        )
        event.add_property("InputArguments", arguments, ua.VariantType.String)
        event.add_property("OutputArguments", "", ua.VariantType.String)
        for name, variant_type, value in extra:
            event.add_property(name, value, variant_type)
        event.Severity = _SEVERITY
        return event


async def build_for(server) -> AuditEmitter:
    """Crée un émetteur et le démarre, pour l'assemblage du serveur."""
    emitter = AuditEmitter(server)
    await emitter.start()
    if not emitter.available:
        logger.warning(
            "Audit Part 12 indisponible sur ce serveur : les changements de "
            "liste de confiance et de certificat ne produiront aucun "
            "événement OPC UA"
        )
    return emitter
