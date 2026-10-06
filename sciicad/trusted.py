"""Validation des certificats de serveur, côté client.

Le GDS valide ses clients depuis la phase 2 ; ce module ferme le dernier trou,
qui est dans l'autre sens. Un client se connecte aujourd'hui sans vérifier le
certificat qu'on lui présente : n'importe quel serveur — y compris un
intermédiaire coupe la vérification et répond à la place du serveur.

Le chiffrement ne suffit pas
---------------------------

``SignAndEncrypt`` prouve que le correspondant détient une clé privée. Cela
empêche l'**écoute**, pas l'**usurpation**. Un attaquant sur le chemin réseau
n'a pas besoin de casser la cryptographie : il intercepte ``OpenSecureChannel``,
présente son propre certificat et relaie. Chaque message reste parfaitement
chiffré et authentifié — pour l'attaquant. C'est un homme du milieu, et il n'a
rien besoin d'autre.

Le certificat du serveur ne suffit pas non plus
-----------------------------------------------

Vérifier que sa signature est valide ne prouve rien : un certifica auto-signé
est valide par définition et ne garantit aucune identité. Ce qui fait la preuve,
c'est la **correspondance** entre trois éléments :

1. le certificat présenté ;
2. l'URI d'application que le serveur **déclare** dans son
   ``ApplicationDescription`` ;
3. l'URI d'application qu'on **attendait**, dite hors bande.

Le troisième est ce qui manque partout ailleurs. Les deux premiers se vérifient
toujours ensemble ; sans le troisième, ils disent seulement que le serveur est
quelque chose.

C'est la même exigence que le GDS, et pour la même raison : §7.1 dit qu'un client
ne se connecte qu'à un serveur auquel il a été configuré faire confiance, et que
ce paramétrage est **hors bande**.

Hors bande, et c'est non négociable
-----------------------------------

Ce module ne va pas chercher la confiance sur le réseau. Une ancre téléchargée
depuis le serveur qu'elle doit authentifier ne prouve rien — elle prouve que le
serveur a bien Proteins être qui il dit, ce qui est précisément la question. La
confiance vient donc de la configuration, comme pour le GDS : c'est le même
problème d'amorçage, et il a la même solution.

Ce que la Part 12 apporte ici
-----------------------------

§7.1 ne dit pas seulement « hors bande » : elle donne la **place** où poser la
confiance. Le modèle *Push* du GDS (avec ``UpdateCertificate``, §7.10.5) fournit
exactement le mécanisme qu'un client doit pouvoir utiliser pour **remplacer** son
ancre sans réinstaller sa configuration. C'est ce qui rend le hors bande
gérable : une ancre posée une fois à l'installation, puis renouvelée par un canal
déjà validé.

Ce module ne l'implémente pas encore, et le dit. Il pose l'ancre et vérifie ; le
renouvellement par le GDS reste à faire, et il n'est pas bloquant tant que les
ancres sont valides.

    python tools/selftest_client_trust.py
"""

from __future__ import annotations

import asyncio
import base64
from pathlib import Path
from typing import Iterable, Optional, Sequence

from asyncua import ua
from asyncua.common.utils import ServiceError
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from loguru import logger


#: Motifs de refus, comme côté serveur : le statut va au plus près du motif.
DENIALS = {
    "empty_trust": (
        ua.StatusCodes.BadCertificateUntrusted,
        "aucune ancre de confiance configurée : on ne peut rien valider",
    ),
    "uri_mismatch": (
        ua.StatusCodes.BadCertificateUriInvalid,
        "l'URI déclarée ne correspond pas au certificat présenté",
    ),
    "expected_mismatch": (
        ua.StatusCodes.BadCertificateUriInvalid,
        "l'URI du serveur ne correspond pas à celle attendue",
    ),
    "untrusted": (
        ua.StatusCodes.BadCertificateUntrusted,
        "le certificat ne remonte à aucune ancre de confiance",
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

    Journalisé **avant** d'être levé, parce qu'un refus de connexion ne laisse
    chez le client qu'un ``UaStatusCodeError`` : si le motif n'est écrit nulle
    part, l'utilisateur voit une connexion qui échoue sans savoir pourquoi. Or
    « le certificat ne correspond pas » et « le certificat est révoqué » ne
    donnent pas la même conduite à tenir — et le second se corrige en renouvelant
    l'ancre, pas en changeant d'URL.
    """
    status, message = DENIALS[reason]
    logger.warning(f"C Certificat serveur refusé ({reason}) : {detail} — {message}")
    error = ServiceError(status)
    error.reason = reason
    error.message = f"{message} — {detail}"
    return error


class TrustStore:
    """Un ensemble d'ancres de confiance, propre au client.

    Volontairement distinct du ``CertificateGroup`` du GDS : celui-ci porte les
    listes d'un serveur distant et sait lire et écrire par morceaux ; celui-ci
    est un magasin **local**, en lecture seule, que l'administrateur remplit hors
    bande. Les confondre donnerait au client le pouvoir de modifier sa propre
    confiance — c'est-à-dire de se valider lui-même.
    """

    def __init__(self, anchors: Optional[Iterable[bytes]] = None) -> None:
        self._anchors: list[x509.Certificate] = []
        for der in anchors or ():
            self._add(der)

    def _add(self, der: bytes) -> None:
        try:
            certificate = x509.load_der_x509_certificate(der)
        except Exception:
            logger.warning("Ancre illisible, ignorée")
            return
        self._anchors.append(certificate)

    @classmethod
    def from_directory(cls, directory: str | Path) -> "TrustStore":
        """Charge les ancres d'un répertoire de certificats publics.

        Un dossier entier, plutôt qu'une liste : un déploiement ajoute une
        application en déposant son certificat, sans qu'une liste de
        configuration soit à réécrire.

        Un répertoire peut en contenir plusieurs : ancres d'applications **et**
        autorités. La distinction se fait sur ``cA``, pas sur le nom du fichier,
        et les deux classes sont conservées à part.

        Pourquoi il faut les deux
        ------------------------

        Dès qu'une autorité existe, un certificat d'application n'est pas
        auto-signé : sa chaîne passe par l'autorité. Un client qui ne connaît
        que la feuille **ne peut pas la valider** — il lui manque l'émetteur, et
        une signature ne se vérifie qu'avec la clé de celui qui a signé.

        C'est la hiérarchie de confiance, et c'est ce que ``pki/`` doit
        contenir : les certificats d'application dans ``pki/trusted/``, le
        certificat de l'autorité dans ``pki/ca/``. ``pki/trusted/`` seul est un
        déploiement incomplet dès la phase 1 — un défaut que seule l'étape
        suivante, où un client valide pour de vrai, pouvait révéler.
        """
        store = cls()
        path = Path(directory)
        if not path.is_dir():
            logger.warning(
                f"Ancres de confiance : {directory} est absent. Toute connexion "
                f"sécurisée sera refusée — c'est le comportement voulu quand on "
                f"n'a pas configuré d'ancre, mais presque toujours une erreur de "
                f"déploiement. Lancez tools/bootstrap_certificates.py."
            )
            return store
        for entry in sorted(path.rglob("*")):
            if not entry.is_file() or entry.suffix.lower() not in (".pem", ".der", ".crt"):
                continue
            try:
                certificate = x509.load_pem_x509_certificate(entry.read_bytes())
            except Exception:
                try:
                    certificate = x509.load_der_x509_certificate(entry.read_bytes())
                except Exception:
                    logger.warning(f"Ancre illisible, ignorée : {entry}")
                    continue
            store._anchors.append(certificate)
            kind = "autorité" if _is_authority(certificate) else "ancre"
            logger.info(f"Ancre de confiance chargée : {entry.name} ({kind})")
        return store

    def __len__(self) -> int:
        return len(self._anchors)

    @property
    def fingerprints(self) -> list[str]:
        return [self._thumbprint(a) for a in self._anchors]

    @staticmethod
    def _thumbprint(certificate: x509.Certificate) -> str:
        from gds.trustlist import thumbprint

        return thumbprint(certificate.public_bytes(serialization.Encoding.DER))

    def trusted_issuer_of(
        self, certificate: x509.Certificate
    ) -> Optional[x509.Certificate]:
        """L'ancre qui a signé ``certificate``, ou ``None``.

        Deux sources, comme côté serveur : le certificat peut être auto-signé et
        figurer dans les ancres, ou porter la signature d'une d'entre elles. Le
        deuxième cas est le fonctionnement normal dès qu'une autorité existe.

        Vérifie la signature, et non le seul nom de l'émetteur : c'est la même
        leçon que celle des CRL, appliquée au bon endroit. Apparier sur le nom
        seul laisserait une autorité sans rapport « signer » n'importe quel
        certificat.
        """
        from gds.certstore import _verify_signature

        for anchor in self._anchors:
            if _verify_signature(certificate, anchor.public_key()):
                return anchor
        return None

    def is_revoked(self, certificate: x509.Certificate, crls: dict) -> Optional[str]:
        """Cherche ``certificate`` dans les CRL fournies, par émetteur.

        ``crls`` associe l'empreinte d'une ancre à ses CRL. Le dictionnaire est
        fourni par l'appelant plutôt que stocké ici : les CRL se renouvelent,
        et un magasin de confiance qui les embarquerait donnerait l'impression
        d'une révocation à jour alors qu'elle est figée au moment de la
        construction.
        """
        issuer = self.trusted_issuer_of(certificate)
        if issuer is None:
            return None
        from gds.certstore import _crls_for, _is_revoked

        applicable = _crls_for(crls.get(self._thumbprint(issuer), []), issuer)
        for crl in applicable:
            if _is_revoked(crl, certificate):
                return f"série {certificate.serial_number:x}"
        return None


class ServerValidator:
    """Validateur de certificat pour ``Client.certificate_validator``.

    Asynchrone, parce que l'appel de la pile l'est et qu'une validation de
    chaîne peut consulun service de révocation.

    Trois contrôles, dont deux que l'on ne fait pas spontanément :

    * **chaîne** — le certificat remonte-t-il à une ancre ? Sans cela, un
      auto-signé passe ;
    * **cohérence** — l'URI déclarée figure-t-elle dans le SAN du certificat ?
      Sans cela, un serveur peut déclarer une identité et présenter le
      certificat d'une autre ;
    * **attente** — l'URI déclarée est-elle celle qu'on attendait ? C'est le
      contrôle qui manque d'ordinaire, et le seul qui prévient l'usurpation
      d'un serveur connu : les deux autres seraient satisfaits par le GDS
      lui-même s'il se faisait passer pour un autre serveur du déploiement.

    Le troisième est celui qu'un attaquant ne peut pas contourner sans possesses
    l another's key : il ne contrôle que ce qu'il présente, pas ce qu'on attend.
    """

    def __init__(
        self,
        store: TrustStore,
        expected_uri: Optional[str] = None,
        crls: Optional[dict] = None,
    ) -> None:
        self.store = store
        self.expected_uri = expected_uri
        self.crls = crls or {}

    async def __call__(
        self,
        certificate: x509.Certificate,
        description: ua.ApplicationDescription,
    ) -> None:
        """Valide, ou lève. Ne rend rien en cas de succès."""
        declared = description.ApplicationUri

        if not len(self.store):
            raise _deny(
                "empty_trust",
                f"serveur « {declared} » — aucune ancre configurée",
            )

        if self.expected_uri and declared != self.expected_uri:
            raise _deny(
                "expected_mismatch",
                f"serveur « {declared} », on attendait « {self.expected_uri} »",
            )

        # La cohérence entre le déclaré et le présenté. Une application qui
        # déclare une identité et présente le certificat d'une autre est soit
        # mal configurée, soit quelqu'un d'autre.
        if not _uri_in_san(certificate, declared):
            raise _deny(
                "uri_mismatch",
                f"le serveur déclare « {declared} », absent de son certificat",
            )

        issuer = self.store.trusted_issuer_of(certificate)
        if issuer is None:
            raise _deny(
                "untrusted",
                f"le certificat de « {declared} » ne remonte à aucune ancre "
                f"({len(self.store)} ancre(s) configurée(s))",
            )

        revoked = self.store.is_revoked(certificate, self.crls)
        if revoked:
            raise _deny("revoked", f"« {declared} » — {revoked}")

        from gds.certstore import _check_application_profile

        try:
            _check_application_profile(certificate)
        except Exception as exc:
            raise _deny("profile", f"« {declared} » — {exc}") from exc

        logger.info(
            f"Certificat serveur validé : « {declared} » remonte à "
            f"{issuer.subject.rfc4514_string()}"
        )


def _is_authority(certificate: x509.Certificate) -> bool:
    """Le certificat est-il une autorité de certification ?"""
    try:
        return certificate.extensions.get_extension_for_class(
            x509.BasicConstraints
        ).value.ca
    except x509.ExtensionNotFound:
        return False


def _uri_in_san(certificate: x509.Certificate, uri: str) -> bool:
    """L'URI figure-t-elle dans le SAN du certificat ?"""
    try:
        san = certificate.extensions.get_extension_for_class(
            x509.SubjectAlternativeName
        ).value
    except x509.ExtensionNotFound:
        return False
    return uri in san.get_values_for_type(x509.UniformResourceIdentifier)


def _as_list(value) -> list:
    """Normalise un chemin ou une liste de chemins."""
    if value is None:
        return []
    if isinstance(value, (str, Path)):
        return [value]
    return list(value)


async def secure_client(
    url: str,
    certificate: str | Path,
    private_key: str | Path,
    trust_directories: "str | Path | Sequence" = ("pki/trusted", "pki/ca"),
    expected_uri: Optional[str] = None,
    application_uri: Optional[str] = None,
):
    """Construit un client OPC UA qui valide le certificat du serveur.

    Le seul endroit du projet où un client sécurisé doit être fabriqué. Le
    garder unique est une exigence de sécurité autant que de style : un client
    construit ailleurs serait, par construction, celui qu'on oubliera de
    valider — et il fonctionnerait, ce qui le rendrait invisible.

    :raises ServiceError: si le serveur présente un certificat refusé. L'erreur
        remonte au client, qui ne se connecte pas.

    L'ancre vient des répertoires indiqués, **jamais du réseau** : une ancre
    téléchargée du serveur qu'elle authentifie ne prouve rien — elle prouve que
    le serveur sait se présenter, ce qui est précisément la question.

    Deux répertoires par défaut, parce qu'un seul ne suffit pas dès qu'une
    autorité existe : ``pki/trusted/`` porte les certificats d'application,
    ``pki/ca/`` le certificat de l'autorité. Sans ce second, aucune chaîne ne se
    vérifie — une signature ne se contrôle qu'avec la clé de celui qui a signé.
    """
    from asyncua import Client
    from asyncua.crypto import security_policies

    store = TrustStore()
    for directory in _as_list(trust_directories):
        # ``anchor``, et non ``certificate`` : ce dernier est un paramètre de
        # la fonction, et la boucle l'écrasait. L'appel à set_security recevait
        # alors un objet x509 au lieu d'un chemin — un défaut qui ne se montre
        # qu'à l'exécution, et qui aurait fait échouer tout client sécurisé.
        for anchor in TrustStore.from_directory(directory)._anchors:
            store._add(anchor.public_bytes(serialization.Encoding.DER))
    validator = ServerValidator(store, expected_uri=expected_uri)

    client = Client(url)
    if application_uri:
        client.application_uri = application_uri
    client.certificate_validator = validator
    await client.set_security(
        security_policies.SecurityPolicyBasic256Sha256,
        Path(certificate),
        Path(private_key),
        mode=ua.MessageSecurityMode.SignAndEncrypt,
    )
    logger.debug(
        f"Client sécurisé vers {url} : {len(store)} ancre(s) de confiance, "
        f"attente {expected_uri or '(aucune URI attendue)'}"
    )
    return client