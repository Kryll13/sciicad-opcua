"""Validation des certificats présentés sur un canal sécurisé.

C'est ici que la Part 12 prend le certificat du canal pour l'**identité** de
l'application. §6.2, Table 2 :

    « The Certificate used to create the SecureChannel is used to determine the
    identity of the OPC UA Application. »

et §6.5.6 :

    « This Method shall be called from an authenticated SecureChannel and from
    a Client that has access to the DiscoveryAdmin Role or the
    ApplicationAdmin Privilege. »

Sans validation, un canal ``SignAndEncrypt`` prouve qu'un client détient une
clé privée — ce qui est vrai et insuffisant. Le canal chiffré empêche l'écoute ;
il n'empêche pas l'usurpation d'une identité de confiance. C'est cette
validation qui ferme l'écart, et elle porte sur le certificat présenté, pas
sur l'identité déclarée.

Où la confiance vient
---------------------

Du groupe de certificats du GDS, c'est-à-dire de ce que le déploiement a posé
hors bande (§7.1) et de ce que les clients peuvent ensuite modifier. La
validation ne consulte **jamais** le registre des serveurs inscrits : un serveur
enregistré est un serveur dont on connaît l'URL, ce n'est pas un serveur de
confiance. Confondre les deux ferait de toute inscription un droit d'accès, et
``RegisterServer`` est justement la porte que la Part 12 veut protéger.

Rejets
------

Un refus lève ``ServiceError``, la forme qu'attend
``set_certificate_validator`` et que la pile traduit en réponse porteuse du
``StatusCode``. Le statut est choisi au plus près du motif :
``BadCertificateUntrusted`` pour une chaîne qui ne remonte à rien de connu,
``BadCertificateInvalid`` pour un profil non conforme, ``BadCertificateRevoked``
pour une révocation. Les fusionner en un seul statut obligerait le client à
deviner, et il ne peut pas.
"""

from __future__ import annotations

from typing import Optional

from asyncua import ua
from asyncua.common.utils import ServiceError
from cryptography import x509
from loguru import logger


#: Motifs de refus, avec le statut qui va à chacun. Centralisés parce que la
#: même liste sert au refus et au message journalisé : deux listes divergeraient
#: au premier ajout, et le journal commencerait à annoncer une raison que le
#: client ne reçoit pas.
DENIALS = {
    "no_trust_anchor": (
        ua.StatusCodes.BadCertificateUntrusted,
        "aucune liste de confiance n'est chargée : rien ne peut être validé",
    ),
    "untrusted_chain": (
        ua.StatusCodes.BadCertificateUntrusted,
        "la signature ne remonte à aucun certificat de confiance",
    ),
    "not_in_trust_list": (
        ua.StatusCodes.BadCertificateUntrusted,
        "le certificat présenté ne figure dans aucune liste de confiance",
    ),
    "revoked": (
        ua.StatusCodes.BadCertificateRevoked,
        "le certificat est révoqué",
    ),
    "revocation_unknown": (
        ua.StatusCodes.BadCertificateRevoked,
        "l'état de révocation est inconnu et n'est pas supprimé",
    ),
    "profile": (
        ua.StatusCodes.BadCertificateInvalid,
        "le certificat ne respecte pas le profil de la Part 6 Table 50",
    ),
}


def _deny(reason: str, detail: str) -> ServiceError:
    """Construit le refus, journalisé puis levé.

    Journaliser **avant** de lever n'est pas un détail : un refus qui ne
    laisse rien dans le journal est un refus dont l'administrateur ne peut pas
    justifier l'absence, et l'absence est précisément ce qu'un attaquant
    recherche.

    Le type est ``ServiceError``, que la pile attrape explicitement pour en
    faire une réponse ``Call`` porteuse du ``StatusCode``. ``UaStatusCodeError``
    serait aussi attrapé — la pile liste les deux — mais il ne transporte que le
    code : le motif, qui est ce que l'administrateur doit pouvoir lire, resterait
    dans le journal local au lieu de revenir au client. Un client qui ne sait pas
    pourquoi il a été refusé ne peut rien corriger.
    """
    status, message = DENIALS[reason]
    logger.warning(f"Certificat client refusé ({reason}) : {detail} — {message}")
    error = ServiceError(status)
    # ``ServiceError`` ne porte que le code : ``str(error)`` vaut « UA Service
    # Error », quel que soit le motif. Un client qui reçoit cela ne peut ni
    # corriger son certificat ni expliquer l'échec à son exploitant. Le motif
    # est donc attaché à l'exception, et la pile le joint au journal serveur
    # lorsqu'elle la transforme en réponse.
    error.reason = reason
    error.message = f"{message} — {detail}"
    return error


class ChannelValidator:
    """Validateur de certificat pour ``set_certificate_validator``.

    Le validateur est **asynchrone** parce que l'appel de la pile l'est, et
    parce que la validation d'une chaîne peut, dans un déploiement clients,
    consulter un service de révocation. Le rendre synchrone obligerait à mentir
    sur l'une des deux.

    Un groupe de confiance est résolu au moment de la construction, pas à chaque
    appel : une liste de confiance est un **objet vivant** que les clients
    modifient par ``Write``, et le magasin se remplace au rechargement d'une
    configuration. Résoudre à chaque appel donnerait au validateur une image
    parfois périmée de la confiance, selon l'ordre de ces deux événements.
    """

    def __init__(self, store=None, group: str = "DefaultApplicationGroup") -> None:
        self._store = store
        self._group = group

    @property
    def group(self) -> str:
        return self._group

    def bind(self, store, group: str = "DefaultApplicationGroup") -> None:
        """Rattache le validateur au magasin construit au démarrage."""
        self._store = store
        self._group = group

    async def __call__(
        self,
        certificate: x509.Certificate,
        description: ua.ApplicationDescription,
    ) -> None:
        """Valide, ou lève. Ne rend rien en cas de succès."""
        if self._store is None:
            raise _deny(
                "no_trust_anchor",
                f"le validateur est actif avant qu'un magasin ne soit rattaché "
                f"(application « {description.ApplicationUri} »)",
            )

        try:
            self._store.validate_presented(self._group, certificate)
        except Exception as exc:
            reason = self._classify(exc)
            raise _deny(reason, f"{description.ApplicationUri} — {exc}") from exc

    @staticmethod
    def _classify(exc: Exception) -> str:
        """Traduit une exception du magasin en motif de refus.

        Le magasin parle en termes de validation ; le canal a besoin d'un
        motif de refus. La correspondance est explicite plutôt que déduite du
        texte, parce qu'une déduction par mot-clé cède dès qu'un message change
        — et un message qui change pour être plus clair casserait alors la
        sécurité au lieu de l'améliorer.
        """
        status = getattr(exc, "status", None)
        if status == ua.StatusCodes.BadCertificateRevoked:
            return "revoked" if "révoqué" in str(exc) else "revocation_unknown"
        if status in (
            ua.StatusCodes.BadCertificateUntrusted,
            ua.StatusCodes.BadSecurityChecksFailed,
        ):
            return "untrusted_chain"
        if status in (
            ua.StatusCodes.BadCertificateInvalid,
            ua.StatusCodes.BadCertificatePolicyCheckFailed,
        ):
            return "profile"
        return "untrusted_chain"