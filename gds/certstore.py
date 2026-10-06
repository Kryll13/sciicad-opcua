"""Certificats applicatifs du GDS : rôle *CertificateManager*, Part 12 §7.10.

Le GDS n'est pas une autorité de certification, et ce module ne prétend pas en
être une. Il tient le rôle décrit en §7.1 : il prépare une demande de signature,
conserve la clé privée, puis reçoit le certificat signé et l'installe. La
signature est faite par une autorité d'enregistrement extérieure — ce que
confirme §7.10.5, qui décrit le certificat reçu comme étant *signé* et non
produit par le serveur. Le GDS n'a donc jamais besoin d'une clé de CA, et n'en
génère pas.

Trois méthodes normatives sont implémentées :

* ``CreateSigningRequest`` (§7.10.10) produit une PKCS #10 DER et retient la clé ;
* ``UpdateCertificate`` (§7.10.5) valide puis installe le certificat signé ;
* ``GetRejectedList`` (§7.10.12) restitue ce qui a été refusé.

Le magasin est indexé par couple (groupe, type de certificat), ce qui est
exactement la granularité qu'exige ``CertificateGroupType`` : un même GDS peut
gérer un certificat d'application et un certificat HTTPS, dans des groupes
différents, sans qu'ils se recouvrent.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable, Optional

from asyncua import ua
from cryptography import x509
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from loguru import logger

from .trustlist import (
    DEFAULT_VALIDATION_OPTIONS,
    CertificateGroup,
    thumbprint,
)

#: Attributs de nom acceptés par le format normatif du ``SubjectName``
#: (§7.9.4). La norme énumère exactement ceux-là ; tout autre préfixe est
#: refusé plutôt que deviné, car un attribut inventé produirait un certificat
#: que l'autorité d'enregistrement rejeterait à son tour.
SUBJECT_ATTRIBUTES = {
    "CN": NameOID.COMMON_NAME,
    "O": NameOID.ORGANIZATION_NAME,
    "OU": NameOID.ORGANIZATIONAL_UNIT_NAME,
    "DC": NameOID.DOMAIN_COMPONENT,
    "L": NameOID.LOCALITY_NAME,
    "S": NameOID.STATE_OR_PROVINCE_NAME,
    "C": NameOID.COUNTRY_NAME,
}

#: Longueur minimale du ``Nonce`` (§7.10.10) : « It shall be at least 32 bytes
#: long ». En dessous, la méthode doit échouer avec ``Bad_InvalidArgument``.
MIN_NONCE_LENGTH = 32

#: Types de certificats que ce magasin sait produire lui-même. Les autres types
#: normatifs (HTTPS, TLS, ECC) sont acceptés en *réception* — la validation ne
#: dépend pas du type — mais la génération de clé est limitée à ce que la
#: bibliothèque sait faire de façon éprouvée.
RSA_CERTIFICATE_TYPES = frozenset(
    {
        ua.ObjectIds.ApplicationCertificateType,
        ua.ObjectIds.RsaMinApplicationCertificateType,
        ua.ObjectIds.RsaSha256ApplicationCertificateType,
    }
)

#: Tailles de clé admises. En dessous de 2048 bits, un certificat
#: applicatif est refusé par la plupart des clients ; au-dessus de 4096, la
#: génération devient coûteuse sans gain réel pour un certificat serveur.
MIN_KEY_SIZE = 2048
MAX_KEY_SIZE = 4096


class CertificateError(ua.UaError):
    """Refus fonctionnel, portant le ``StatusCode`` normatif à renvoyer.

    Le code est transporté par l'exception elle-même plutôt que déduit du texte
    plus loin. Reconnaître le motif dans un message — « lecture seule »,
    « FileHandle inconnu » — marche jusqu'au jour où le message est reformulé,
    et le jour venu le refus part en ``BadInvalidArgument`` sans que rien ne
    signale pourquoi. Un test qui passe au vert ne prouve plus que le client
    reçoit le bon code.
    """

    def __init__(
        self,
        message: str,
        status: int = ua.StatusCodes.BadInvalidArgument,
        untrusted: bool = False,
    ) -> None:
        super().__init__(message)
        self.status = status
        #: Vrai si le refus tient à la seule absence de confiance, et non à une
        #: erreur de validation. §7.8.3.2 réserve la liste des rejets aux
        #: certificats « that have no unsuppressed validation errors but are
        #: not trusted » : un certificat expiré, mal adressé ou illisible est
        #: refusé, mais n'a rien à faire sur cette liste.
        self.untrusted = untrusted


def _invalid(message: str) -> CertificateError:
    return CertificateError(message, ua.StatusCodes.BadInvalidArgument)


def _bad_certificate(message: str) -> CertificateError:
    return CertificateError(message, ua.StatusCodes.BadCertificateInvalid)


def _untrusted(message: str) -> CertificateError:
    """Refus faute de confiance : le seul cas qui appartienne à la liste."""
    return CertificateError(
        message, ua.StatusCodes.BadCertificateUntrusted, untrusted=True
    )


def parse_subject(subject: str) -> x509.Name:
    """Convertit un ``SubjectName`` normatif (§7.9.4) en ``x509.Name``.

    Le format est une suite de paires ``CLE=VALEUR`` séparées par ``/``, la clé
    appartenant à :data:`SUBJECT_ATTRIBUTES`. La norme autorise ``/`` et ``=``
    dans une valeur à condition de la mettre entre guillemets, d'où l'analyse
    par machine à états plutôt qu'un simple ``split``.

    >>> parse_subject("CN=gds/SCIICAD/O=SCIICAD").get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
    'gds'
    """
    if not subject or not subject.strip():
        raise _invalid("SubjectName vide")

    parts: list[str] = []
    current: list[str] = []
    in_quotes = False
    for char in subject:
        if char == '"':
            in_quotes = not in_quotes
            continue
        if char == "/" and not in_quotes:
            parts.append("".join(current))
            current = []
            continue
        current.append(char)
    if in_quotes:
        raise _invalid(f"guillemet non fermé dans le SubjectName : {subject!r}")
    parts.append("".join(current))

    attributes: list[x509.NameAttribute] = []
    for part in parts:
        if not part.strip():
            continue
        key, sep, value = part.partition("=")
        if not sep:
            raise _invalid(f"paire SubjectName sans '=' : {part!r}")
        key = key.strip().upper()
        if key not in SUBJECT_ATTRIBUTES:
            raise _invalid(
                f"attribut de SubjectName non normatif : {key!r} "
                f"(acceptés : {', '.join(sorted(SUBJECT_ATTRIBUTES))})"
            )
        attributes.append(
            x509.NameAttribute(SUBJECT_ATTRIBUTES[key], value.strip())
        )
    if not attributes:
        raise _invalid("SubjectName sans aucun attribut")
    return x509.Name(attributes)


def build_signing_request(
    key,
    subject: x509.Name,
    application_uri: str,
    hostnames: Iterable[str] = (),
) -> bytes:
    """Construit la PKCS #10 DER d'un certificat applicatif.

    §7.10.10 impose que la demande contienne « all fields required by
    OPC 10000-6 such as the subjectAltName ». L'URI d'application est donc
    obligatoire dans le SAN : c'est elle que le client rapproche de l'identité
    annoncée par le serveur, sans elle le certificat est inexploitable.
    """
    names = [x509.UniformResourceIdentifier(application_uri)]
    names += [x509.DNSName(name) for name in dict.fromkeys(hostnames) if name]

    builder = (
        x509.CertificateSigningRequestBuilder()
        .subject_name(subject)
        .add_extension(
            x509.BasicConstraints(ca=False, path_length=None), critical=True
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=True,
                key_encipherment=True,
                data_encipherment=True,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage(
                [ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH]
            ),
            critical=False,
        )
        .add_extension(x509.SubjectAlternativeName(names), critical=False)
    )
    return builder.sign(key, hashes.SHA256()).public_bytes(serialization.Encoding.DER)


def _load_pkcs12_key(payload: bytes):
    """Lit la clé privée d'un PKCS #12, quelle que soit la version de la pile.

    ``serialization.load_der_pkcs12_private_key`` a disparu de
    ``cryptography`` : depuis la 46.0 l'API est
    ``serialization.pkcs12.load_key_and_certificates``, qui rend un tuple
    *éventuellement vide*. L'appel de l'ancienne forme échoue par
    ``AttributeError`` — donc **tout** PKCS #12 était refusé, avec un message
    qui parlait de format illisible alors que le format était parfait et
    l'appel nonexistent.

    La distinction est maintained parce qu'elle ne se voit pas : une
    ``AttributeError`` est un défaut de code, pas un problème de données, et
    les deux se présentent par la même exception si on les laisse fusionner.
    """
    from cryptography.hazmat.primitives.serialization import pkcs12

    result = pkcs12.load_key_and_certificates(payload, None)
    key = result[0] if isinstance(result, tuple) else result
    if key is None:
        raise ValueError("le PKCS #12 ne contient aucune clé privée")
    return key


def _verify_raw(signature: bytes, tbs: bytes, hash_algorithm, public_key) -> bool:
    """Vérifie une signature against une clé publique, quel que soit le porteur.

    ``tbs`` est le bloc à signer : ``tbs_certificate_bytes`` pour un
    certificat, ``tbs_certlist_bytes`` pour une CRL. Les deux portent la même
    structure, ce qui rend la vérification commune — et il est utile qu'elle le
    soit, parce qu'une CRL non vérifiée est le trou de ce module.

    Deux remplissages RSA sont essayés parce que rien dans le bloc ne dit
    lequel a été utilisé : PKCS #1 v1.5 d'abord, puis PSS. Les deux algorithmes
    de hachage sont déduits de la signature elle-même.
    """
    if hash_algorithm is None:
        return False
    if isinstance(public_key, rsa.RSAPublicKey):
        for pad in (
            padding.PKCS1v15(),
            padding.PSS(
                mgf=padding.MGF1(hash_algorithm),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
        ):
            try:
                public_key.verify(signature, tbs, pad, hash_algorithm)
                return True
            except InvalidSignature:
                continue
        return False
    if isinstance(public_key, ec.EllipticCurvePublicKey):
        try:
            public_key.verify(signature, tbs, ec.ECDSA(hash_algorithm))
            return True
        except InvalidSignature:
            return False
    return False


def _verify_signature(certificate: x509.Certificate, issuer_public_key) -> bool:
    """Vérifie la signature d'un certificat avec une clé publique d'émetteur."""
    return _verify_raw(
        certificate.signature,
        certificate.tbs_certificate_bytes,
        certificate.signature_hash_algorithm,
        issuer_public_key,
    )


def _check_application_profile(certificate: x509.Certificate) -> None:
    """Vérifie le profil d'un certificat d'application, Part 6 Table 50.

    Cette vérification existait déjà, mais en version faible : elle exigeait
    ``digitalSignature`` et ``keyEncipherment``, deux bits parmi ceux que la
    norme impose. La Table 50 est plus exigeante, et plus subtile, car elle
    **distingue le type de clé** :

    * « For RSA keys, the keyUsage shall include digitalSignature,
      nonRepudiation, keyEncipherment and dataEncipherment. » — quatre bits.
    * « For ECC keys, the keyUsage shall include digitalSignature. » — un seul.
    * « Self-signed Certificates shall also include keyCertSign. »
    * « For RSA profiles, the extendedKeyUsage shall specify serverAuth for
      Servers. »

    Exiger les quatre bits à une clé ECC serait un refus à tort : la norme les
    exclut explicitement, et le chiffrement ECC est une voie aujourd'hui
    publiée — ``EccApplicationCertificateType`` i=23537 est dans le NodeIds
    officiel. Inversement, n'exiger que deux bits laissait passer un certificat
    RSA dont il manque la moitié des usages — ce qui est précisément ce que
    produisait ``crypto_opcua.py`` avant que la Table 50 ne soit lue.

    Le message nomme les bits manquants. « KeyUsage insuffisant » ne dit pas
    lequel, et un opérateur ne peut pas corriger ce qu'on ne lui nomme pas.
    """
    try:
        constraints = certificate.extensions.get_extension_for_class(
            x509.BasicConstraints
        ).value
    except x509.ExtensionNotFound as exc:
        raise _bad_certificate("extension BasicConstraints absente") from exc
    if constraints.ca:
        # La Table 50 autorise cA=TRUE « to ensure backward interoperability »
        # quand la vérification de révocation est active, et dit d'écrire un
        # avertissement. Elle ne l'autorise pas quand elle est inactive.
        if not _has(DEFAULT_VALIDATION_OPTIONS, "CheckRevocationStatusOffline"):
            raise _bad_certificate(
                "le drapeau cA est positionné alors que la vérification de "
                "révocation est inactive : la Table 50 ne l'admet pas dans ce cas"
            )
        logger.warning(
            "Certificat d'application avec cA=TRUE, accepté par compatibilité "
            "descendante (Table 50) : la validation de révocation est active."
        )

    try:
        key_usage = certificate.extensions.get_extension_for_class(x509.KeyUsage).value
    except x509.ExtensionNotFound as exc:
        raise _bad_certificate("extension KeyUsage absente") from exc

    is_rsa = isinstance(certificate.public_key(), rsa.RSAPublicKey)
    if is_rsa:
        required = {
            "digitalSignature": key_usage.digital_signature,
            "nonRepudiation": key_usage.content_commitment,
            "keyEncipherment": key_usage.key_encipherment,
            "dataEncipherment": key_usage.data_encipherment,
        }
        absent = [name for name, present in required.items() if not present]
        if absent:
            raise _bad_certificate(
                f"KeyUsage incomplet pour un certificat RSA (Table 50) : "
                f"{', '.join(absent)} absent(s)"
            )
    elif not key_usage.digital_signature:
        raise _bad_certificate(
            "KeyUsage incomplet pour un certificat non-RSA (Table 50) : "
            "digitalSignature absent"
        )

    if certificate.issuer == certificate.subject and not key_usage.key_cert_sign:
        raise _bad_certificate(
            "certificat auto-signé sans keyCertSign dans KeyUsage, que la "
            "Table 50 exige pour tout certificat auto-signé"
        )

    if is_rsa:
        try:
            eku = certificate.extensions.get_extension_for_class(
                x509.ExtendedKeyUsage
            ).value
        except x509.ExtensionNotFound as exc:
            raise _bad_certificate(
                "extension ExtendedKeyUsage absente : la Table 50 impose "
                "serverAuth pour un profil RSA servant de serveur"
            ) from exc
        if ExtendedKeyUsageOID.SERVER_AUTH not in eku:
            raise _bad_certificate(
                "ExtendedKeyUsage ne contient pas serverAuth, que la Table 50 "
                "impose pour un profil RSA"
            )


def _has(flags: int, flag_name: str) -> bool:
    """Vrai si le drapeau nommé est posé dans l'ensemble de bits.

    Le nom est résolu à partir de l'OptionSet de la norme plutôt que codé en
    dur : une valeur littérale serait juste jusqu'à ce que la pile change, et
    le dopage d'un bit faux — ``SuppressCertificateExpired`` valant 1 comme
    ``SuppressHostNameInvalid`` vaut 2 — produirait une validation qui ignore
    exactement ce que l'administrateur a demandé de surveiller.
    """
    flag = getattr(ua.TrustListValidationOptions, flag_name, None)
    if flag is None:  # pragma: no cover - garde-fou
        raise _invalid(f"drapeau de validation inconnu : {flag_name!r}")
    return bool(int(flags) & int(flag))


def _crls_for(crls: list[bytes], issuer: x509.Certificate) -> list[x509.Certificate]:
    """CRL de la liste qui sont **signées par** ``issuer``.

    L'appariement se fait sur le **nom** de l'émetteur *et* sur la **signature**.
    Le nom seul ne suffit pas, et cette insuffisance était un trou réel : une
    liste de confiance s'alimente par ``Write`` (le seul chemin normatif de
    diffusion des CRL), donc quiconque est autorisé à écrire dans
    ``issuer_crls`` peut y déposer une CRL portant le nom de la CA de confiance
    — et la faire appliquer. La signature de cette CRL ne correspondant à rien
    de vérifiable, elle était malgré tout utilisée pour révoquer des
    certificats.

    Ce n'est pas une hypothèse : le contrôle négatif de
    ``tools/selftest_revocation.py`` reproduit exactement ce cas, avec une CRL
    portant le bon nom et une clé étrangère. Le contrôle était vert parce que
    cette CRL était ignorée — mais pour la mauvaise raison : elle était ignorée
    parce qu'aucune CRL n'était applicable du tout, non parce que sa signature
    avait été vérifiée. Il suffisait d'ajouter la CRL authentique pour que la
    version non vérifiée commence à révoquer.

    Le tri se fait donc par signature vérifiée, et une CRL qui ne la valide pas
    est écartée avec un avertissement qui nomme l'émetteur attendu. Elle
    n'apparaît donc pas dans les CRL applicables, ce qui la conduit au chemin
    « état de révocation inconnu » — où le comportement est déjà défini, et
    où ``SuppressRevocationStatusUnknown`` joue son rôle. Une CRL étrangère
    ne peut ainsi ni révoquer (sa signature ne vaut rien) ni blanchir (les
    révocations sont une union, pas une intersection).
    """
    applicable: list[x509.Certificate] = []
    for raw in crls:
        try:
            crl = x509.load_der_x509_crl(raw)
        except Exception:
            logger.warning("CRL illisible dans issuer_crls, ignorée")
            continue
        if crl.issuer != issuer.subject:
            continue
        if not _verify_raw(
            crl.signature,
            crl.tbs_certlist_bytes,
            crl.signature_hash_algorithm,
            issuer.public_key(),
        ):
            logger.warning(
                f"CRL ignorée : elle porte le nom d'émetteur "
                f"{issuer.subject.rfc4514_string()!r} mais sa signature ne se "
                f"vérifie pas avec la clé de ce certificat. Une CRL non signée "
                f"par l'émetteur de confiance ne peut ni révoquer ni blanchir."
            )
            continue
        applicable.append(crl)
    return applicable


def _is_revoked(crl: x509.Certificate, certificate: x509.Certificate) -> bool:
    """Vrai si la CRL liste le numéro de série du certificat.

    Une interrogation qui échoue **ne vaut pas** « non révoqué ». Le
    commentaire précédent affirmait le contraire du code, qui rendait
    ``False`` : une CRL à moitié lisible valait autorisation de suite. Ici
    l'échec remonte, et le refus qui en découle est un refus par prudence —
    l'état de révocation est inconnu, ce qui est précisément la situation que
    §7.8.2.10 traite et que ``SuppressRevocationStatusUnknown`` permet de
    laisser passer.
    """
    try:
        return (
            crl.get_revoked_certificate_by_serial_number(certificate.serial_number)
            is not None
        )
    except Exception as exc:
        raise CertificateError(
            f"CRL illisible pour le certificat de série "
            f"{certificate.serial_number:x} : l'état de révocation est "
            f"inconnu ({exc})",
            ua.StatusCodes.BadCertificateRevoked,
        ) from exc


def _public_key_of(key) -> bytes:
    """Clé publique SPKI encodée, que ``key`` soit privé ou déjà public.

    Le magasin manipule les deux : une clé privée conservée pour la
    signature, et la clé publique d'un certificat présenté. Comparer les deux
    impose de les ramener à la même forme, ce qu'une clé privée ne fait pas
    d'elle-même.
    """
    public = key.public_key() if hasattr(key, "public_key") else key
    return public.public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )


@dataclass
class CertificateEntry:
    """Un certificat applicatif et la clé privée qui lui appartient."""

    group: str
    certificate_type: Optional[ua.NodeId] = None
    subject: str = ""
    private_key: object = None
    certificate: Optional[x509.Certificate] = None
    #: Empreinte SHA-1 du dernier certificat accepté, pour l'introspection.
    thumbprint: str = ""
    updated_at: Optional[datetime] = None
    #: Empreinte du ``Nonce`` reçu lors de la demande en cours, lorsqu'elle est
    #: encore en attente de certificat signé.
    pending_nonce: str = ""

    @property
    def installed(self) -> bool:
        return self.certificate is not None

    def describe(self) -> dict:
        return {
            "groupe": self.group,
            "type": _type_number(self.certificate_type),
            "sujet": self.subject,
            "installe": self.installed,
            "empreinte": self.thumbprint,
            "demande_en_attente": bool(self.pending_nonce),
            "mis_a_jour": self.updated_at.isoformat(timespec="seconds")
            if self.updated_at
            else None,
        }


class CertificateStore:
    """Magasin de certificats applicatifs du GDS, Part 12 §7.10.

    :param groups: listes de confiance, indexées par nom de groupe. Le magasin
        s'en sert pour valider un certificat signé : la norme exige que les
        certificats d'émetteur soient *déjà* dans la liste de confiance du
        groupe (§7.10.5).
    :param application_uri: URI d'application inscrite dans le SAN.
    :param hostnames: noms d'hôte ajoutés au SAN de la demande.
    """

    def __init__(
        self,
        groups: Optional[dict[str, CertificateGroup]] = None,
        application_uri: str = "",
        hostnames: Iterable[str] = (),
        key_size: int = MIN_KEY_SIZE,
    ) -> None:
        if not MIN_KEY_SIZE <= key_size <= MAX_KEY_SIZE:
            raise _invalid(
                f"taille de clé hors plage : {key_size} "
                f"(attendu {MIN_KEY_SIZE}..{MAX_KEY_SIZE})"
            )
        self.groups = groups if groups is not None else {}
        self.application_uri = application_uri
        self.hostnames = [name for name in dict.fromkeys(hostnames) if name]
        self.key_size = key_size
        self._lock = threading.RLock()
        self._entries: dict[tuple[str, str], CertificateEntry] = {}
        #: Certificats refusés, du plus ancien au plus récent.
        self._rejected: list[bytes] = []

    # -- accès -------------------------------------------------------------

    def _key(self, group: str, certificate_type: Optional[ua.NodeId]) -> tuple[str, str]:
        return (group, "" if certificate_type is None else str(certificate_type))

    def entry(self, group: str, certificate_type: Optional[ua.NodeId] = None) -> CertificateEntry:
        """Retourne l'entrée du couple (groupe, type), en la créant si besoin."""
        with self._lock:
            key = self._key(group, certificate_type)
            found = self._entries.get(key)
            if found is None:
                found = CertificateEntry(group=group, certificate_type=certificate_type)
                self._entries[key] = found
            return found

    def rejected(self) -> list[bytes]:
        """Certificats *valides mais non approuvés*, du plus ancien au récent.

        C'est le contenu que ``GetRejectedList`` restitue (§7.8.3.2). La liste
        est sans limite de taille ni de durée, comme la norme le permet : un
        serveur peut en supprimer des entrées si le message ne tient pas dans
        la taille maximale, mais rien ici n'impose de le faire.
        """
        with self._lock:
            return list(self._rejected)

    def clear_rejected(self) -> int:
        with self._lock:
            count = len(self._rejected)
            self._rejected.clear()
            return count

    def _reject(self, der: bytes, reason: str) -> None:
        """Verse un certificat dans la liste des rejets, sans doublon.

        Appelé uniquement pour un refus tenant à la confiance : c'est
        l'appelant, qui connaît la nature du refus, qui décide.
        """
        logger.warning(f"Certificat non approuvé : {reason}")
        with self._lock:
            if der and all(previous != der for previous in self._rejected):
                self._rejected.append(der)

    # -- §7.10.10 CreateSigningRequest --------------------------------------

    def create_signing_request(
        self,
        group: str,
        certificate_type: Optional[ua.NodeId] = None,
        subject: str = "",
        regenerate: bool = False,
        nonce: bytes = b"",
    ) -> bytes:
        """Produit une PKCS #10 DER, Part 12 §7.10.10.

        ``RegeneratePrivateKey`` pilote le cycle de vie de la clé privée, qui
        est conservée jusqu'à ce que le certificat signé arrive par
        ``UpdateCertificate``. Une clé existante est réutilisée si
        ``regenerate`` est faux, conformément à la norme ; c'est ce qui permet
        de renouveler un certificat sans changer de clé.

        Le ``Nonce`` est une contribution d'entropie du client, et la norme en
        exige au moins 32 octets. Elle est contrôlée puis **liée** à la demande
        en cours : la bibliothèque de chiffrement ne permet pas d'orienter la
        génération d'une clé RSA depuis une graine, et l'entropie de la clé
        provient donc du générateur du système — au moins aussi forte. Ce que la
        norme vise, à savoir qu'une entropie extérieure a bien été apportée par
        l'appelant, est obtenu en refusant une demande sans nonce.
        """
        if regenerate and len(nonce) < MIN_NONCE_LENGTH:
            raise _invalid(
                f"Nonce de {len(nonce)} octets, la norme en exige "
                f"{MIN_NONCE_LENGTH} quand RegeneratePrivateKey est vrai"
            )

        type_id = _as_type_nodeid(certificate_type)
        if type_id is not None and _type_number(type_id) not in RSA_CERTIFICATE_TYPES:
            raise _invalid(
                f"type de certificat {_type_number(type_id)} non pris en charge pour "
                f"la génération d'une clé"
            )

        with self._lock:
            entry = self.entry(group, type_id)
            if subject:
                entry.subject = subject
            if not entry.subject:
                entry.subject = f"CN={_default_common_name(self.application_uri)}"
            if regenerate or entry.private_key is None:
                entry.private_key = rsa.generate_private_key(
                    public_exponent=65537, key_size=self.key_size
                )
            entry.pending_nonce = thumbprint(nonce) if nonce else ""

            request = build_signing_request(
                entry.private_key,
                parse_subject(entry.subject),
                self.application_uri,
                self.hostnames,
            )
        logger.info(
            f"Demande de signature émise pour {group} "
            f"(type={_type_number(type_id) if type_id is not None else 'défaut'}, "
            f"sujet={entry.subject!r}, clé régénérée={regenerate})"
        )
        return request

    # -- §7.10.5 UpdateCertificate ------------------------------------------

    def update_certificate(
        self,
        group: str,
        certificate_type: Optional[ua.NodeId] = None,
        certificate: bytes = b"",
        issuer_certificates: Iterable[bytes] = (),
        private_key_format: str = "",
        private_key: bytes = b"",
    ) -> bool:
        """Valide puis installe un certificat signé, Part 12 §7.10.5.

        Retourne ``ApplyChangesRequired``. La valeur ``False`` signifie que le
        GDS a satisfaction la requête immédiatement : il n'ouvre pas de
        transaction, contrairement à un serveur soumis au modèle *Push* avec
        approbation administrative. La norme ne l'oblige pas : elle définit
        cette valeur pour signaler « rien à faire de plus ».

        La validation suit le processus de la Part 4 : période de validité,
        contraintes de base, usage de la clé, présence de l'URI d'application,
        et surtout la signature, qui doit remonter à un certificat de confiance
        du groupe.

        Seul un certificat *valide mais non approuvé* est versé à la liste des
        rejets (§7.8.3.2). Un certificat expiré ou mal adressé est une erreur de
        validation, pas un rejet d'approbation : le client a mieux à faire que
        de le retrouver dans une liste de candidats à approuver. Il est donc
        refusé, avec son code, sans être enregistré.
        """
        if not certificate:
            raise _invalid("certificat vide")
        type_id = _as_type_nodeid(certificate_type)
        try:
            parsed = x509.load_der_x509_certificate(certificate)
        except Exception as exc:
            raise _bad_certificate(f"certificat DER illisible : {exc}") from exc

        try:
            self._validate(parsed, group)
        except CertificateError as exc:
            if exc.untrusted:
                self._reject(certificate, str(exc))
            raise

        key = self._install_key(group, type_id, private_key_format, private_key)
        if key is None:
            key = self.entry(group, type_id).private_key
        if key is None:
            raise _bad_certificate(
                "aucune clé privée connue : la clé doit avoir été créée par "
                "CreateSigningRequest ou fournie avec le certificat"
            )
        if _public_key_of(key) != _public_key_of(parsed.public_key()):
            raise _bad_certificate(
                "la clé privée fournie ne correspond pas à la clé publique du "
                "certificat"
            )

        with self._lock:
            entry = self.entry(group, type_id)
            entry.certificate = parsed
            entry.private_key = key
            entry.thumbprint = thumbprint(certificate)
            entry.updated_at = datetime.now(timezone.utc)
            entry.pending_nonce = ""

        # La norme est explicite : la validation suppose que les certificats
        # d'émetteur figurent déjà dans la liste de confiance du groupe. Les
        # chaînes fournies sont donc conservées, ce qui rend la confiance
        # reproductible pour les stations qui viendront lire la liste.
        chain = self.groups.get(group)
        for issuer in issuer_certificates:
            if chain is not None and issuer:
                chain.add(issuer, is_trusted=False)

        logger.info(
            f"Certificat installé pour {group} "
            f"(empreinte {entry.thumbprint}, sujet {parsed.subject.rfc4514_string()!r})"
        )
        return False

    def _install_key(
        self,
        group: str,
        type_id: Optional[ua.NodeId],
        private_key_format: str,
        private_key: bytes,
    ):
        """Charge une clé privée fournie avec le certificat, si elle existe."""
        if not private_key:
            return None
        if private_key_format and "PKCS12" not in private_key_format.upper() \
                and "PFX" not in private_key_format.upper():
            logger.warning(
                f"Format de clé privée non supporté : {private_key_format!r} "
                f"(attendu PKCS #12)"
            )
            return None
        try:
            key = _load_pkcs12_key(private_key)
        except Exception as exc:
            raise _invalid(f"clé privée PKCS #12 illisible : {exc}") from exc
        with self._lock:
            entry = self.entry(group, type_id)
            entry.private_key = key
        return key

    def _validate(
        self,
        certificate: x509.Certificate,
        group: str,
    ) -> None:
        """Applique le processus de validation de la Part 4.

        Les drapeaux de ``DefaultValidationOptions`` (§7.8.2.10) ne sont pas
        décoratifs : ils décident de quelles erreurs sont **insupprimables**.
        ``SuppressCertificateExpired`` transforme une erreur de temps en
        simple avertissement, ``SuppressHostNameInvalid`` fait de même pour
        l'URI d'application, et les drapeaux de révocation décident si la CRL
        d'un émetteur doit être consultée. Ignorer ce drapeau reviendrait à
        valider plus strictement que l'administrateur ne l'a demandé.
        """
        flags = self._validation_options(group)
        now = datetime.now(timezone.utc)

        if not _has(flags, "SuppressCertificateExpired"):
            not_before = certificate.not_valid_before_utc
            not_after = certificate.not_valid_after_utc
            if now < not_before:
                raise CertificateError(
                    f"certificat pas encore valide "
                    f"(à partir de {not_before.isoformat()})",
                    ua.StatusCodes.BadCertificateTimeInvalid,
                )
            if now > not_after:
                raise CertificateError(
                    f"certificat expiré (le {not_after.isoformat()})",
                    ua.StatusCodes.BadCertificateTimeInvalid,
                )

        try:
            constraints = certificate.extensions.get_extension_for_class(
                x509.BasicConstraints
            ).value
            if constraints.ca:
                raise _bad_certificate("un certificat applicatif ne peut pas être une CA")
        except x509.ExtensionNotFound as exc:
            raise _bad_certificate("extension BasicConstraints absente") from exc

        _check_application_profile(certificate)

        if not _has(flags, "SuppressHostNameInvalid") and not self._has_application_uri(
            certificate
        ):
            raise CertificateError(
                f"l'URI d'application {self.application_uri!r} est absente du SAN",
                ua.StatusCodes.BadCertificateUriInvalid,
            )

        issuer = self._trusted_issuer_of(certificate, group)
        if issuer is None:
            raise _untrusted(
                f"la signature ne remonte à aucun certificat de confiance du "
                f"groupe {group!r} (ni comme certificats de confiance, ni comme "
                f"émetteurs)"
            )
        self._check_revocation(certificate, issuer, group, flags)


    def _has_application_uri(self, certificate: x509.Certificate) -> bool:
        try:
            san = certificate.extensions.get_extension_for_class(
                x509.SubjectAlternativeName
            ).value
        except x509.ExtensionNotFound:
            return False
        return any(
            isinstance(entry, x509.UniformResourceIdentifier)
            and entry.value == self.application_uri
            for entry in san
        )

    def _chains_to_trusted_issuer(self, certificate: x509.Certificate, group: str) -> bool:
        """Vrai si la signature du certificat remonte à un certificat de confiance."""
        return self._trusted_issuer_of(certificate, group) is not None

    def _trusted_issuer_of(
        self, certificate: x509.Certificate, group: str
    ) -> Optional[x509.Certificate]:
        """Rend le certificat de confiance qui a signé ``certificate``, ou ``None``.

        Deux sources sont admises, conformément à la Part 4 : le certificat peut
        être auto-signé et figurer dans les certificats de confiance du groupe, ou
        être signé par un certificat d'émetteur du même groupe. Ce second cas est
        le fonctionnement normal d'un déploiement à autorité de certification.

        Rendre l'émetteur plutôt qu'un booléen est ce qui permet ensuite de lui
        associer sa CRL : la révocation se consulte « par émetteur », pas en
        balayant toutes les CRL du groupe au hasard.
        """
        chain = self.groups.get(group)
        candidates: list[bytes] = []
        if chain is not None:
            candidates += list(chain.trusted_certificates)
            candidates += list(chain.issuer_certificates)
        if not candidates:
            # Sans liste de confiance, aucune validation de chaîne n'est possible.
            # Refuser est le seul comportement sûr : accepter ferait installer un
            # certificat dont personne n'a vérifié l'origine.
            return None
        for raw in candidates:
            try:
                issuer = x509.load_der_x509_certificate(raw)
            except Exception:
                continue
            if _verify_signature(certificate, issuer.public_key()):
                return issuer
        return None

    def _validation_options(self, group: str) -> int:
        """Drapeaux de validation du groupe, §7.8.2.10.

        Un groupe absent, ou dépourvu de drapeaux, retombe sur la valeur par
        défaut de la norme : le bit ``CheckRevocationStatusOffline``. Le
        comportement par défaut est donc celui qu'un déploiement attend, sans
        qu'aucune configuration ne soit requise.
        """
        chain = self.groups.get(group)
        if chain is None:
            return DEFAULT_VALIDATION_OPTIONS
        return int(getattr(chain, "default_validation_options", DEFAULT_VALIDATION_OPTIONS))

    def _check_revocation(
        self,
        certificate: x509.Certificate,
        issuer: x509.Certificate,
        group: str,
        flags: int,
    ) -> None:
        """Confronte le certificat à la CRL de son émetteur.

        §7.8.2.10 énumère sept drapeaux ; deux damping la révocation hors ligne
        et quatre la neutralisent. ``CheckRevocationStatusOnline`` n'est pas
        implanté : interroger un OCSP depuis un serveur de découverte sortirait
        du périmètre, et une révocation en ligne qui échoue silencieusement est
        pire qu'une absence de vérification. C'est dit explicitement plutôt que
        passé sous silence, car un administrateur qui pose ce bit croirait le
        contraire.

        Une CRL absente pour un émetteur signifie un état de révocation
        *inconnu* : la Part 4 impose alors l'échec, sauf si
        ``SuppressRevocationStatusUnknown`` est posé. C'est ce qui rend la
        distribution des CRL obligatoire dès lors que la propriété est
        exposée — un comportement fermé, et voulu.
        """
        if not _has(flags, "CheckRevocationStatusOffline"):
            return

        chain = self.groups.get(group)
        crls = list(getattr(chain, "issuer_crls", []) or []) if chain else []
        applicable = _crls_for(crls, issuer)

        if not applicable:
            if _has(flags, "SuppressRevocationStatusUnknown"):
                logger.debug(
                    f"État de révocation inconnu pour {certificate.subject.rfc4514_string()!r} "
                    f"(aucune CRL de l'émetteur) : erreur supprimée"
                )
                return
            raise CertificateError(
                f"aucune CRL connue pour l'émetteur "
                f"{issuer.subject.rfc4514_string()!r} : l'état de révocation est "
                f"inconnu et l'erreur n'est pas supprimée "
                f"(SuppressRevocationStatusUnknown)",
                ua.StatusCodes.BadCertificateRevoked,
            )

        for crl in applicable:
            if _is_revoked(crl, certificate):
                raise CertificateError(
                    f"certificat révoqué (série {certificate.serial_number:x}, "
                    f"CRL de {crl.issuer.rfc4514_string()!r})",
                    ua.StatusCodes.BadCertificateRevoked,
                )

        if _has(flags, "SuppressIssuerRevocationStatusUnknown"):
            # Les drapeaux « Issuer… » visent l'état de révocation des
            # certificats d'émetteur eux-mêmes, pas celui du certificat présenté.
            # Le vérifier ici reviendrait à appliquer le mauvais drapeau à la
            # mauvaise chose ; la distinction est conservée, pas confondue.
            logger.debug("SuppressIssuerRevocationStatusUnknown sans effet ici")


    # -- introspection ------------------------------------------------------

    def describe(self) -> list[dict]:
        with self._lock:
            return [entry.describe() for entry in self._entries.values()]

    def certificate_der(self, group: str, certificate_type: Optional[ua.NodeId] = None) -> bytes:
        """Certificat installé, en DER, ou vide."""
        entry = self.entry(group, certificate_type)
        if entry.certificate is None:
            return b""
        return entry.certificate.public_bytes(serialization.Encoding.DER)


def _type_number(nodeid: Optional[ua.NodeId]) -> Optional[int]:
    """Identifiant numérique d'un ``NodeId``, ou ``None`` s'il est nul.

    ``int(NodeId)`` échoue : ``NodeId`` n'est pas un entier, il encapsule un
    identifiant *et* un index d'espace de noms. Faut-il encore l'attribut
    ``Identifier``, dont la présence varie selon la version d'asyncua — d'où
    cette fonction unique, plutôt qu'un accès dispersé qui divergerait.
    """
    if nodeid is None or getattr(nodeid, "is_null", bool)():
        return None
    return int(nodeid.Identifier)


def _as_type_nodeid(value) -> Optional[ua.NodeId]:
    """Normalise un ``CertificateTypeId`` reçu par le protocole."""
    if value is None:
        return None
    if isinstance(value, ua.NodeId):
        return None if value.is_null() else value
    return None


def _default_common_name(application_uri: str) -> str:
    """Nomcommun par défaut, dérivé de l'URI d'application.

    §7.10.6 autorise le serveur à choisir un sujet par défaut pour un
    certificat applicatif, à partir de son identité d'application. Le fragment
    après le dernier ``:`` de l'URI est le nom de l'application tel qu'il a été
    déclaré, donc le choix le plus parlant.
    """
    tail = application_uri.rsplit(":", 1)[-1] if application_uri else ""
    return tail or "SCIICAD"
