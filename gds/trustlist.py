"""Listes de confiance d'un CertificateGroup, Part 12 §7.8.2.

Ce module implémente le modèle *fichier* de ``TrustListType``. La norme ne
transporte pas un ``TrustListDataType`` d'un appel à l'autre : le client ouvre
la liste, obtient un ``FileHandle``, puis lit ou écrit par morceaux, à une
position courante. ``TrustListDataType`` sert à décrire le *contenu*, pas à le
transporter.

    Open(OpenFileMode)          -> FileHandle
    Read(FileHandle, Length)    -> ByteString
    Write(FileHandle, Data)     -> void
    GetPosition/SetPosition     -> position courante
    OpenWithMasks(Masks)        -> FileHandle
    CloseAndUpdate(FileHandle)  -> void
    AddCertificate(Certificate:ByteString, IsTrustedCertificate:Boolean)
    RemoveCertificate(Thumbprint:String, IsTrustedCertificate:Boolean)

Deux règles de sûreté, non optionnelles :

* ``Open`` en lecture seule ne doit pas autoriser l'écriture, sinon un client
  qui ne veut que lire peut réécrire la liste de confiance. Le mode d'ouverture
  conditionne donc ``Write`` et ``AddCertificate``.
* ``OpenCount`` doit être décrémenté par ``Close``. Un client qui ouvre sans
  fermer bloquerait la liste pour tous les suivants ; la norme prévoit ce
  compteur précisément pour que cette fuite soit détectable.

Format de sérialisation
------------------------

La norme définit l'échange en CSV, dont la grammaire exacte n'est pas
reproductible ici sans le document. Plutôt que d'inventer un format et de
présenter une implementation faussement normative, le contenu est transporté
par la structure binaire ``TrustListDataType`` : elle est typée par la pile
(``ua.TrustListDataType``), son encodage est normatif, et un client qui sait
lire ``TrustListDataType`` sait lire cette liste. Le format est isolé dans
:func:`encode` et :func:`decode` pour être remplacé par le CSV dès que la
grammaire est disponible.
"""

from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Optional

from asyncua import ua
from asyncua.ua.ua_binary import from_binary, to_binary
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from loguru import logger

#: Masques de ``TrustListMasks`` (i=12552), qui désignent les quatre listes.
LIST_NAMES = {
    ua.TrustListMasks.TrustedCertificates: "trusted_certificates",
    ua.TrustListMasks.TrustedCrls: "trusted_crls",
    ua.TrustListMasks.IssuerCertificates: "issuer_certificates",
    ua.TrustListMasks.IssuerCrls: "issuer_crls",
}

#: Champ de ``TrustListDataType`` correspondant à chaque liste.
LIST_FIELDS = {
    "trusted_certificates": "TrustedCertificates",
    "trusted_crls": "TrustedCrls",
    "issuer_certificates": "IssuerCertificates",
    "issuer_crls": "IssuerCrls",
}

#: Ouverture en lecture seule / écriture, à partir de ``OpenFileMode``.
MODE_READ = ua.OpenFileMode.Read
MODE_WRITE = ua.OpenFileMode.Write

#: Plafond de taille d'une lecture, pour ne jamais renvoyer un message
#: dépassant les limites de transport d'asyncua (65535 octets utiles).
MAX_READ_CHUNK = 8192

#: Valeur initiale de ``LastUpdateTime`` exigée par §7.8.3.1 : « If a Server is
#: not able to determine the LastUpdateTime after an event such as a restart,
#: then the LastUpdateTime shall be DateTime.MinValue. » Cette liste vit en
#: mémoire et repart vide à chaque démarrage : son âge réel est inconnu, et
#: dater la liste de l'instant du démarrage affirmerait une mise à jour qui
#: n'a pas eu lieu.
DATE_MIN = datetime(1601, 1, 1, tzinfo=timezone.utc)

#: Valeur par défaut de ``DefaultValidationOptions``, §7.8.2.10 : « The default
#: value for this DataType only has the CheckRevocationStatusOffline bit set. »
#:
#: Ce défaut est **fermé**, et il faut le savoir : avec ce seul bit, un
#: certificat signé par un émetteur de confiance dont aucune CRL n'est connue a
#: un état de révocation *inconnu*, donc refusé — sauf si
#: ``SuppressRevocationStatusUnknown`` est posé. Publier la propriété rend donc
#: la distribution des CRL obligatoire, ce qui est le but, mais c'est un
#: changement de comportement notable pour un déploiement qui n'en diffuse pas.
DEFAULT_VALIDATION_OPTIONS = int(
    ua.TrustListValidationOptions.CheckRevocationStatusOffline
)


class TrustListError(ua.UaError):
    """Erreur fonctionnelle de la liste de confiance."""


def thumbprint(der: bytes) -> str:
    """Empreinte SHA-1 d'un certificat, en minuscules hexadécimales.

    C'est le format de ``RemoveCertificate.Thumbprint`` : la norme utilise
    SHA-1 pour identifier un certificat dans une liste, SHA-256 servant au
    signature du canal sécurisé.
    """
    return hashlib.sha1(der).hexdigest().lower()


#: Extensions reconnues pour un certificat hors bande. Le PEM est le format de
#:openssl par défaut et ce que produit ``crypto_opcua`` ; le DER et le ``.crt``
#: sont acceptés parce qu'un déploiement peut empêcher l'usage d'OpenSSL, et
#: refuser une ancre parce qu'elle est valide mais dans un autre suffixe serait
#: un échec de mise en service, pas un avertissement utile.
_CERTIFICATE_SUFFIXES = (".pem", ".der", ".crt", ".cer")


def load_issuer_crls(group: "CertificateGroup", paths: list[str]) -> list[str]:
    """Charge des CRL dans ``group.issuer_crls``, hors bande.

    Symétrique de :func:`load_trusted_certificates`, et soumis à la même
    question de périmètre : la CRL est une **pièce de contexte de confiance**.
    Elle entre sans passer la validation de :mod:`gds.certstore`, qui porte sur
    les certificats présentés, non sur les pièces de contexte que l'on distribue.

    Ce qui change par rapport au chargeur de certificats : la signature de la
    CRL est vérifiée contre la clé du certificat d'émetteur au moment de la
    **consultation**, pas du dépôt. C'est la seule défense possible, et c'est
    celle qui compte : une CRL peut être émise par une autorité à un instant
    où sa clé est légitime, et son authenticityité au moment de l'usage ne peut
    pas être celle de son dépôt. Une CRL hors signature est donc acceptée au
    dépôt puis ignorée à la consultation, avec un avertissement qui nomme
    l'émetteur — pas appliquée.

    Un PEM peut contenir une ou plusieurs CRL enchaînées ; chacune est
    conservée. Une CRL est un DER singleton : il n'y a pas d'y fractionner, et
    un fichier multi-CRL est traité comme une entrée par bloc.
    """
    loaded: list[str] = []
    files = _expand(paths, (".pem", ".der", ".crl", ".crt"))
    for path in files:
        try:
            payload = path.read_bytes()
        except OSError as exc:
            logger.warning(f"CRL illisible, ignorée : {path} ({exc})")
            continue
        blocks = _split_crls(payload)
        if not blocks:
            logger.warning(
                f"Aucune CRL exploitable dans {path} : ce n'est ni un PEM de "
                f"CRL ni un DER de CRL. Ignorée."
            )
            continue
        added = 0
        for der in blocks:
            try:
                if group.add_crl(der):
                    added += 1
            except TrustListError as exc:
                logger.warning(f"CRL refusée dans {path} : {exc}")
        if added:
            loaded.append(str(path))
            logger.info(f"{added} CRL(s) chargée(s) depuis {path}")
    return loaded


def _split_crls(payload: bytes) -> list[bytes]:
    """Blocs CRL DER contenus dans un PEM enchaîné ou un DER unique."""
    try:
        x509.load_der_x509_crl(payload)
        return [payload]
    except Exception:
        pass
    blocks: list[x509.CertificateRevocationList] = []
    marker = b"-----BEGIN X509 CRL-----"
    end = b"-----END X509 CRL-----"
    remainder = payload
    while marker in remainder:
        _, _, remainder = remainder.partition(marker)
        body, found, remainder = remainder.partition(end)
        if not found:
            break
        try:
            blocks.append(x509.load_pem_x509_crl(marker + body + end))
        except Exception:
            logger.warning("Bloc PEM illisible dans une CRL")
            continue
    # Le décodeur rend des objets CRL ; la liste de confiance stocke des DER.
    # La conversion se fait ici, à la frontière, plutôt que dans l'appelant
    # qui n'a aucune raison de connaître la forme interne.
    return [
        crl.public_bytes(serialization.Encoding.DER)
        for crl in blocks
    ]


def _expand(paths: list[str], suffixes: tuple[str, ...]) -> list[Path]:
    """Développe une liste de chemins en fichiers existants.

    Un chemin absent est un avertissement, jamais une erreur : une ancre qui
    manque doit se voir dans le journal sans empêcher un serveur de démarrer.
    """
    files: list[Path] = []
    for raw in paths:
        candidate = Path(raw)
        if candidate.is_dir():
            files += sorted(
                entry
                for entry in candidate.iterdir()
                if entry.is_file() and entry.suffix.lower() in suffixes
            )
        elif candidate.is_file():
            files.append(candidate)
        else:
            logger.warning(f"Source de confiance introuvable, ignorée : {raw!r}")
    return files


def _split_certificates(payload: bytes) -> list[bytes]:
    """Certificats DER contenus dans un PEM enchaîné ou un DER unique.

    On ne cherche pas le texte ``BEGIN CERTIFICATE`` mais on essaie le DER
    d'abord : c'est plus court, et un fichier DER n'a rien à chercher. Ensuite
    chaque bloc PEM est décodé puis **revalidé** par la pile — un bloc
    ``BEGIN CERTIFICATE`` suivi d'octets arbitraires produirait sinon des
    données non-certificats qui échoueraient plus tard, loin de sa source, et
    avec un message qui ne nommerait pas le fichier fautif.
    """
    try:
        x509.load_der_x509_certificate(payload)
        return [payload]
    except Exception:
        pass
    certificates: list[bytes] = []
    marker = b"-----BEGIN CERTIFICATE-----"
    end = b"-----END CERTIFICATE-----"
    remainder = payload
    while marker in remainder:
        head, _, remainder = remainder.partition(marker)
        body, found, remainder = remainder.partition(end)
        if not found:
            break
        try:
            certificates.append(
                x509.load_pem_x509_certificate(marker + body + end).public_bytes(
                    serialization.Encoding.DER
                )
            )
        except Exception:
            logger.warning("Bloc PEM illisible dans un certificat de confiance")
    return certificates


def load_trusted_certificates(
    group: "CertificateGroup",
    paths: list[str],
    is_trusted: bool = True,
) -> list[str]:
    """Charge des certificats de confiance dans ``group``, hors bande.

    Part 12 §7.1 : *« Clients shall only connect to a CertificateManager which
    the Client has been configured to trust. This may require an out of band
    configuration step which is completed prior to starting the manual
    onboarding process. »* La norme ne définit aucun amorçage en bande ; cette
    fonction est l'endroit où le déploiement pose son ancre de confiance.

    Un chemin peut désigner un fichier ou un dossier. Un dossier est développé
    sur les extensions de :data:`_CERTIFICATE_SUFFIXES`, ce qui permet de
    pointer un seul répertoire de certificats publics sans énumérer les quatre
    applications qu'il contient.

    Un fichier PEM peut contenir plusieurs certificats enchaînés, ce que produit
    ``cat`` et ce que produit ``openssl`` avec ``-bundle`` : les charger tous est
    le comportement attendu, et n'en charger qu'un laisserait une ancre
    silencieusement absente.

    Ce que fait cette fonction, et ce qu'elle ne fait pas
    -----------------------------------------------------

    Elle appelle :meth:`CertificateGroup.add`, et **n'exécute aucune validation**.
    Ce n'est pas un raccourci : la validation de :mod:`gds.certstore` certify
    qu'un certificat *présenté par le réseau* est conforme, alors qu'ici
    l'administrateur *décide* que cette clé est de confiance. Faire passer
    l'ancre par la validation la rendrait dépendante d'elle-même — et elle
    échouerait, puisque le défaut fermé de §7.8.2.10 refuse un certificat sans
    CRL, donc refuse précisément celui qui sert d'ancre. Une ancre ne peut pas
    exiger la preuve de sa propre existence.

    Rend la liste des sources effectivement chargées, et journalise
    l'inexistant, l'illisible et l'incompatible. Un fichier illisible est
    **avertissement et non erreur** : une ancre absente doit se voir dans le
    journal, pas empêcher un serveur de démarrer — sauf si l'administrateur
    dépendait d'elle, auquel cas le silence serait pire. C'est pourquoi le
    chemin d'erreur est explicite dans la valeur rendue.
    """
    loaded: list[str] = []
    wanted = is_trusted
    files = _expand(paths, _CERTIFICATE_SUFFIXES)

    for path in files:
        try:
            payload = path.read_bytes()
        except OSError as exc:
            logger.warning(f"Certificat de confiance illisible, ignoré : {path} ({exc})")
            continue

        certificates = _split_certificates(payload)
        if not certificates:
            logger.warning(
                f"Aucun certificat exploitable dans {path} : ce n'est ni un PEM "
                f"ni un DER. Ignoré."
            )
            continue

        added = 0
        for der in certificates:
            try:
                if group.add(der, is_trusted=wanted):
                    added += 1
            except TrustListError as exc:
                logger.warning(f"Certificat refusé dans {path} : {exc}")
        if added:
            loaded.append(str(path))
            logger.info(
                f"Ancre de confiance chargée depuis {path} : {added} certificat(s)"
            )
    return loaded


def encode(masks: int, lists: dict[str, list[bytes]]) -> bytes:
    """Sérialise les listes demandées en ``TrustListDataType`` binaire."""
    data = ua.TrustListDataType(
        SpecifiedLists=masks,
        TrustedCertificates=[],
        TrustedCrls=[],
        IssuerCertificates=[],
        IssuerCrls=[],
    )
    for mask, name in LIST_NAMES.items():
        if masks & mask:
            setattr(data, LIST_FIELDS[name], list(lists.get(name, [])))
    return to_binary(ua.TrustListDataType, data)


def decode(raw: bytes) -> tuple[int, dict[str, list[bytes]]]:
    """Relit un ``TrustListDataType`` binaire.

    Retourne ``(masques, listes)``. Les listes non couvertes par les masques
    restent vides : un client ne doit pas voir ce qu'il n'a pas demandé.
    """
    data = from_binary(ua.TrustListDataType, BytesIO(raw))
    masks = int(data.SpecifiedLists)
    lists = {
        name: list(getattr(data, field_name) or [])
        for name, field_name in LIST_FIELDS.items()
        if masks & next(m for m, n in LIST_NAMES.items() if n == name)
    }
    return masks, lists


@dataclass
class _Handle:
    """Ouverture en cours : contenu, position et droits."""

    buffer: bytes
    position: int = 0
    writable: bool = False
    masks: int = ua.TrustListMasks.All
    dirty: bool = False

    def size(self) -> int:
        return len(self.buffer)


@dataclass
class CertificateGroup:
    """Une liste de confiance, servie selon le modèle fichier de la Part 12.

    Un groupe correspond à un ``CertificateGroupType`` : il porte son nom, sa
    liste de confiance et l'instant de sa dernière mise à jour. Les
    certificats et CRL sont stockés par liste, et l'appartenance d'un
    certificat est déduite de son empreinte — pas d'une table d'index, ce qui
    rend ``AddCertificate`` idempotent.
    """

    name: str = "DefaultApplicationGroup"
    trusted_certificates: list[bytes] = field(default_factory=list)
    trusted_crls: list[bytes] = field(default_factory=list)
    issuer_certificates: list[bytes] = field(default_factory=list)
    issuer_crls: list[bytes] = field(default_factory=list)
    last_update_time: datetime = field(default_factory=lambda: DATE_MIN)

    #: Drapeaux de validation, §7.8.2.10. Un ensemble de bits, pas une
    #: énumération : la norme les définit en OptionSet.
    default_validation_options: int = DEFAULT_VALIDATION_OPTIONS

    # Verrou : les méthodes d'une liste de confiance sont appelables depuis
    # plusieurs sessions à la fois, et le modèle fichier expose une position
    # courante par ouverture.
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    _handles: dict[int, _Handle] = field(default_factory=dict, repr=False)
    _next_handle: int = field(default=1, repr=False)

    # -- état -------------------------------------------------------------

    def state(self) -> dict:
        """Propriétés de l'objet ``TrustList`` telles que la norme les définit.

        Une liste de confiance est un ``FileType`` (Part 20, Table *FileType*)
        auquel la Part 12 §7.8.3.1 ajoute ``LastUpdateTime``. Les cinq
        propriétés obligatoires sont ici, et nulle part ailleurs : c'est le
        seul endroit où l'on sait ce qu'elles valent.

        * ``size`` n'a pas de sens pour une liste de confiance — §7.8.2.1 le dit
          et renvoie à la Part 20, qui impose alors ``Bad_NotSupported``. La
          valeur n'est donc pas un nombre, et le câblage s'en charge ;
        * ``writable`` et ``user_writable`` valent ``True`` : la liste s'écrit
          par ``CloseAndUpdate`` et ``AddCertificate``. Elles ne peuvent pas
          être plus restrictives tant que le modèle de rôles du §7.2 n'est pas
          implanté — voir la note de ``_publish_properties``.
        * ``open_count`` est le nombre de poignées valides, que la Part 20
          définit comme « the number of currently valid file handles » ;
        * ``default_validation_options`` est le drapeau que la Part 12 §7.8.2.1
          nomme « the default options to use when validating Certificates with
          the TrustList ». Il est ici parce que c'est le seul endroit qui sait
          quelles listes la validation doit consulter.
        """
        with self._lock:
            return {
                "size": None,  # Bad_NotSupported, cf. §7.8.2.1
                "writable": True,
                "user_writable": True,
                "open_count": len(self._handles),
                "last_update_time": self.last_update_time,
                "default_validation_options": self.default_validation_options,
            }

    def lists(self, masks: int = ua.TrustListMasks.All) -> dict[str, list[bytes]]:
        """Retourne les listes couvertes par ``masks``."""
        out: dict[str, list[bytes]] = {}
        for mask, name in LIST_NAMES.items():
            if masks & mask:
                out[name] = list(getattr(self, name))
        return out

    def serialise(self, masks: int = ua.TrustListMasks.All) -> bytes:
        return encode(masks, self.lists(masks))

    def deserialise(self, raw: bytes) -> int:
        """Remplace le contenu par celui de ``raw`` et retourne les masques.

        Les listes non présentes dans ``raw`` sont vidées : le contenu
        transmis fait foi dans son ensemble, sinon un client ne pourrait pas
        retirer un certificat en réécrivant une liste complète.
        """
        masks, lists = decode(raw)
        for mask, name in LIST_NAMES.items():
            if masks & mask:
                setattr(self, name, list(lists.get(name, [])))
        self.last_update_time = datetime.now(timezone.utc)
        return masks

    # -- contenu ----------------------------------------------------------

    def _bucket(self, name: str) -> list[bytes]:
        """La liste nommée par ``name``, qui doit être l'une des cinq.

        Le routage était dupliqué dans ``add``, ``remove`` et ``contains`` ;
        l'ajouter comme liste d'arguments possibles le rendrait faux au
        troisième appel. Le membre est vérifié ici une fois pour toutes, et
        ``LIST_FIELDS`` fait autorité : une cinquième liste normative y
       apparaîtrait automatiquement, sans qu'une liste codée en dur décide à la place
        de la norme de ce qui existe.
        """
        bucket = getattr(self, name, None)
        if not isinstance(bucket, list) or name not in LIST_FIELDS:
            raise TrustListError(f"liste de confiance inconnue : {name!r}")
        return bucket

    def add(self, certificate: bytes, is_trusted: bool = True) -> bool:
        """Ajoute un certificat DER. Retourne ``True`` s'il était absent.

        Idempotent : ré-ajouter un certificat déjà présent n'est pas une
        erreur et ne crée pas de doublon.
        """
        if not certificate:
            raise TrustListError("certificat vide")
        name = "trusted_certificates" if is_trusted else "issuer_certificates"
        current = self._bucket(name)
        if any(thumbprint(item) == thumbprint(certificate) for item in current):
            return False
        current.append(certificate)
        self.last_update_time = datetime.now(timezone.utc)
        return True

    def add_crl(self, crl: bytes) -> bool:
        """Ajoute une CRL DER à ``issuer_crls``.

        Équivalent en mémoire de ce que fait ``Write`` pour un client autorisé
        à diffuser des CRL, et il n'existait aucun chemin hors bande pour cela.
        Les tests existants atteignaient la liste par attribut, ce qui est
        précisément le genre d'accès qui masque une liste non câblée.

        La CRL est **parcourue** avant d'être acceptée. Un certificat glissé
        ici serait accepté puis silencieusement ignoré à la consultation, avec
        un journal qui dirait « CRL illisible » sans dire d'où elle vient ; le
        refuser à l'entrée nomme le fichier fautif.
        """
        if not crl:
            raise TrustListError("CRL vide")
        try:
            x509.load_der_x509_crl(crl)
        except Exception as exc:
            raise TrustListError(f"contenu illisible en CRL DER : {exc}") from exc
        current = self._bucket("issuer_crls")
        if any(thumbprint(item) == thumbprint(crl) for item in current):
            return False
        current.append(crl)
        self.last_update_time = datetime.now(timezone.utc)
        return True

    def remove(self, thumb: str, is_trusted: bool = True) -> bool:
        """Retire un certificat par empreinte. Retourne ``True`` s'il existait."""
        name = "trusted_certificates" if is_trusted else "issuer_certificates"
        current = self._bucket(name)
        wanted = thumb.strip().lower()
        for index, item in enumerate(current):
            if thumbprint(item) == wanted:
                del current[index]
                self.last_update_time = datetime.now(timezone.utc)
                return True
        return False

    def remove_crl(self, thumb: str) -> bool:
        """Retire une CRL par empreinte. Retourne ``True`` si elle existait."""
        current = self._bucket("issuer_crls")
        wanted = thumb.strip().lower()
        for index, item in enumerate(current):
            if thumbprint(item) == wanted:
                del current[index]
                self.last_update_time = datetime.now(timezone.utc)
                return True
        return False

    def contains(self, certificate: bytes, is_trusted: bool = True) -> bool:
        name = "trusted_certificates" if is_trusted else "issuer_certificates"
        wanted = thumbprint(certificate)
        return any(thumbprint(item) == wanted for item in self._bucket(name))

    def contains_crl(self, crl: bytes) -> bool:
        wanted = thumbprint(crl)
        return any(thumbprint(item) == wanted for item in self._bucket("issuer_crls"))

    def count(self, masks: int = ua.TrustListMasks.All) -> int:
        return sum(len(v) for v in self.lists(masks).values())

    # -- modèle fichier ----------------------------------------------------

    @staticmethod
    def _normalise_masks(masks: int) -> int:
        """Valide un masque de listes, ou retombe sur toutes.

        Seul le masque vide est refusé. Un masque nommant une liste unique
        (``TrustedCrls`` seul) est parfaitement valide : le confondre avec une
        valeur invalide reviendrait à élargir silencieusement la demande du
        client, qui verrait alors des listes qu'il n'a pas demandées.
        """
        try:
            value = int(masks)
        except (TypeError, ValueError):
            logger.warning(f"Masque illisible ({masks!r}) : ouverture de toutes les listes")
            return ua.TrustListMasks.All
        if value == int(ua.TrustListMasks.None_):
            logger.warning("Masque vide : ouverture de toutes les listes")
            return ua.TrustListMasks.All
        # Ne conserver que les bits définis par la norme, sans modifier le
        # masque si les quatre sont valides.
        return value & int(ua.TrustListMasks.All)

    def open(self, mode: int = MODE_READ, masks: int = ua.TrustListMasks.All) -> int:
        """Ouvre la liste et retourne un ``FileHandle``.

        Le contenu est figé dans le tampon de l'ouverture : deux clients
        ouverts simultanément ne se perturbent pas. C'est ce que permet le
        modèle fichier, et la raison pour laquelle ``CloseAndUpdate`` existe
        pour publier une modification.
        """
        with self._lock:
            masks = self._normalise_masks(masks)
            handle = self._next_handle
            self._next_handle += 1
            self._handles[handle] = _Handle(
                buffer=self.serialise(masks),
                writable=bool(mode & (MODE_WRITE | ua.OpenFileMode.Append)),
                masks=masks,
            )
            logger.debug(
                f"TrustList {self.name} ouverte (handle={handle}, "
                f"{len(self._handles)} ouverture(s), écriture={self._handles[handle].writable})"
            )
            return handle

    def close(self, handle: int) -> bool:
        with self._lock:
            if handle not in self._handles:
                return False
            del self._handles[handle]
            return True

    def _get(self, handle: int) -> _Handle:
        entry = self._handles.get(handle)
        if entry is None:
            raise TrustListError(f"FileHandle inconnu ou déjà fermé : {handle}")
        return entry

    def read(self, handle: int, length: int) -> bytes:
        """Lit au plus ``length`` octets depuis la position courante."""
        with self._lock:
            entry = self._get(handle)
            if length < 0:
                raise TrustListError(f"longueur négative : {length}")
            chunk = entry.buffer[entry.position : entry.position + length]
            entry.position += len(chunk)
            return chunk

    def write(self, handle: int, data: bytes) -> int:
        """Écrit ``data`` à la position courante. Retourne le nombre d'octets.

        L'écriture **peut agrandir le fichier**, et c'est nécessaire : c'est le
        seul chemin normatif pour ajouter une CRL ou un certificat à une liste.
        La Part 12 ne définit aucune méthode ``AddCrl`` — le contenu transite par
        ``Write`` puis ``CloseAndUpdate``. Une écriture bornée à la taille
        d'origine rendrait les listes immuables, donc la révocation
        indiffusable et la propriété ``DefaultValidationOptions`` inopérante.

        Agrandir suppose une ouverture en écriture : en lecture seule, le refus
        intervient avant toute question de taille, pour que le motif reste
        « lecture seule » et non « hors du fichier ». C'est le même correctif
        que le client doit apporter, pas un autre.
        """
        with self._lock:
            entry = self._get(handle)
            if not entry.writable:
                raise TrustListError("ouverture en lecture seule")
            end = entry.position + len(data)
            if end > entry.size():
                # Agrandissement : le tampon est étendu. Les listes
                # sous-jacentes ne sont reconstruites qu'à la fermeture, donc
                # un contenu intermédiaire incomplet n'est jamais observé.
                entry.buffer = entry.buffer[: entry.position] + bytes(data)
            else:
                entry.buffer = (
                    entry.buffer[: entry.position]
                    + bytes(data)
                    + entry.buffer[end:]
                )
            entry.position = end
            entry.dirty = True
            return len(data)

    def get_position(self, handle: int) -> int:
        with self._lock:
            return self._get(handle).position

    def set_position(self, handle: int, position: int) -> None:
        with self._lock:
            entry = self._get(handle)
            if position < 0 or position > entry.size():
                raise TrustListError(
                    f"position {position} hors plage (0..{entry.size()})"
                )
            entry.position = position

    def close_and_update(self, handle: int) -> int:
        """Ferme l'ouverture et publie son contenu si elle a été modifiée.

        Sans modification, l'ouverture est simplement fermée : inutile de
        réécrire une liste inchangée, ce qui ferait bouger
        ``LastUpdateTime`` pour rien.
        """
        with self._lock:
            entry = self._get(handle)
            masks = entry.dirty
            if entry.dirty:
                try:
                    masks = self.deserialise(entry.buffer)
                except Exception as exc:
                    logger.error(f"Mise à jour de la liste de confiance refusée : {exc}")
                    del self._handles[handle]
                    raise TrustListError(f"contenu illisible : {exc}") from exc
            del self._handles[handle]
            return masks

    # -- introspection ------------------------------------------------------

    def open_count(self) -> int:
        """Nombre d'ouvertures en cours.

        La norme expose ce compteur pour que la fuite d'une ouverture — un
        client qui ouvre et disparaît — soit visible par l'administrateur.
        """
        with self._lock:
            return len(self._handles)

    def describe(self) -> dict:
        with self._lock:
            return {
                "name": self.name,
                "trusted_certificates": len(self.trusted_certificates),
                "trusted_crls": len(self.trusted_crls),
                "issuer_certificates": len(self.issuer_certificates),
                "issuer_crls": len(self.issuer_crls),
                "open_count": len(self._handles),
                "last_update": self.last_update_time.isoformat(timespec="seconds"),
            }

    def close_all(self) -> None:
        """Referme toutes les ouvertures. Utilisé à l'arrêt."""
        with self._lock:
            if self._handles:
                logger.debug(
                    f"{len(self._handles)} ouverture(s) abandonnée(s) sur {self.name}"
                )
            self._handles.clear()
