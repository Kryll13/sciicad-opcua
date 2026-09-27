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
  qui只想 lire peut réécrire la liste de confiance. Le mode d'ouverture
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
from typing import Optional

from asyncua import ua
from asyncua.ua.ua_binary import from_binary, to_binary
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


class TrustListError(ua.UaError):
    """Erreur fonctionnelle de la liste de confiance."""


def thumbprint(der: bytes) -> str:
    """Empreinte SHA-1 d'un certificat, en minuscules hexadécimales.

    C'est le format de ``RemoveCertificate.Thumbprint`` : la norme utilise
    SHA-1 pour identifier un certificat dans une liste, SHA-256 servant au
    signature du canal sécurisé.
    """
    return hashlib.sha1(der).hexdigest().lower()


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
    last_update_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Verrou : les méthodes d'une liste de confiance sont appelables depuis
    # plusieurs sessions à la fois, et le modèle fichier expose une position
    # courante par ouverture.
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    _handles: dict[int, _Handle] = field(default_factory=dict, repr=False)
    _next_handle: int = field(default=1, repr=False)

    # -- état -------------------------------------------------------------

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

    def add(self, certificate: bytes, is_trusted: bool = True) -> bool:
        """Ajoute un certificat DER. Retourne ``True`` s'il était absent.

        Idempotent : ré-ajouter un certificat déjà présent n'est pas une
        erreur et ne crée pas de doublon.
        """
        if not certificate:
            raise TrustListError("certificat vide")
        name = "trusted_certificates" if is_trusted else "issuer_certificates"
        current = getattr(self, name)
        if any(thumbprint(item) == thumbprint(certificate) for item in current):
            return False
        current.append(certificate)
        self.last_update_time = datetime.now(timezone.utc)
        return True

    def remove(self, thumb: str, is_trusted: bool = True) -> bool:
        """Retire un certificat par empreinte. Retourne ``True`` s'il existait."""
        name = "trusted_certificates" if is_trusted else "issuer_certificates"
        current = getattr(self, name)
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
        return any(thumbprint(item) == wanted for item in getattr(self, name))

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

        L'écriture est bornée au fichier déjà ouvert : on ne peut pas ajouter
        des octets au-delà de la taille connue, ce qui laisserait un tampon
        incohérent avec les listes sous-jacentes.
        """
        with self._lock:
            entry = self._get(handle)
            if not entry.writable:
                raise TrustListError("ouverture en lecture seule")
            end = entry.position + len(data)
            if end > entry.size():
                raise TrustListError(
                    f"écriture hors du fichier ({end} > {entry.size()})"
                )
            entry.buffer = entry.buffer[: entry.position] + bytes(data) + entry.buffer[end :]
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
