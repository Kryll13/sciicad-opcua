#!/usr/bin/env python3
"""Auto-test de l'autorité et des certificats signés, phases 0 et 1.

Couvre ce que les auto-tests précédents ne pouvaient pas faire, parce que la
matière n'existait pas : une autorité de certification réelle, une CRL réelle,
et des certificats d'application **signés** — donc validables selon une chaîne
et non selon une ancre auto-signée.

Ce que ces phases changent, et qu'il faut voir
--------------------------------------------

Avant, la validation d'un certificat présenté par le réseau n'avait qu'une
seule porte : la liste de certificats de confiance, où un certificat
constructeur se valide lui-même. Un certificat signé par une autorité ne se
valide pas comme cela : il faut que l'autorité soit dans ``issuer_certificates``
**et** qu'une de ses CRL soit applicable, sinon l'état de révocation est
inconnu et §7.8.2.10 refuse.

La distinction n'est pas cosmétique. Une ancre de confiance dit *qui* on fait
confiance ; une CRL dit *ce qu'on ne fait plus confiance à*. Les deux listes
sont nécessaires et aucune ne suffit.

Contrôles négatifs
------------------

Six cas doivent être refusés, chacun pour un motif distinct, et le motif est
rendu avec le statut — ``BadCertificateInvalid`` seul ne dit pas quel contrôle
a parlé, et deviner serait le moyen le plus sûr de corriger le mauvais :

1. certificat signé par une autorité **absente** de ``issuer_certificates`` ;
2. autorité présente, **aucune CRL** → état inconnu, refus par défaut fermé ;
3. autorité et CRL présentes, certificat **révoqué** → ``Bad_CertificateRevoked`` ;
4. CRL au bon nom d'émetteur mais signée par **une autre clé** → ignorée, donc
   refus pour état inconnu. C'est le contrôle qui fermait un trou réel : cette
   CRL appliquait des révocations sans que rien ne les prouvât, ce qui revenait
   à laisser quiconque pût écrire dans ``issuer_crls`` révoquer tout le
   déploiement ;
5. profil non conforme (Table 50), par le chemin de la CA ;
6. certificat à deux URI dans le SAN, que l'autorité refuse d'émettre.

Et deux témoins doivent **passer** : le chemin nominal signé et la CRL vide,
qui est la situation normale d'une autorité qui n'a rien révoqué. Sans témoin,
un refus attribuable à un autre motif passerait pour une preuve.

    python tools/selftest_authority.py
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from asyncua import ua
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from loguru import logger

from gds.certstore import CertificateStore
from gds.trustlist import CertificateGroup
from sciicad.selftest import Report

sys.path.insert(0, str(Path(__file__).resolve().parent))
from authority import CA_CERT, CA_CRL, CA_KEY  # noqa: E402
from bootstrap_certificates import ROLES  # noqa: E402

GROUP = "DefaultApplicationGroup"
APP_URI = "urn:SCIICAD:gds"
CA_NAME = x509.Name([
    x509.NameAttribute(NameOID.ORGANIZATION_NAME, "SCIICAD"),
    x509.NameAttribute(NameOID.COMMON_NAME, "SCIICAD CA"),
])

#: KeyUsage de la Table 50 pour un certificat d'application RSA. ``keyCertSign``
#: en est absent : il n'est exigé que pour un **auto-signé**, et ici l'autorité
#: signe. Le poser quand même serait conforme à la lettre du profil
#: d'auto-signé, et faux pour un certificat qui ne l'est pas.
APP_KEY_USAGE = x509.KeyUsage(
    digital_signature=True,
    content_commitment=True,
    key_encipherment=True,
    data_encipherment=True,
    key_agreement=False,
    key_cert_sign=False,
    crl_sign=False,
    encipher_only=False,
    decipher_only=False,
)

LEGACY_KEY_USAGE = x509.KeyUsage(
    digital_signature=True,
    content_commitment=False,
    key_encipherment=True,
    data_encipherment=False,
    key_agreement=False,
    key_cert_sign=False,
    crl_sign=False,
    encipher_only=False,
    decipher_only=False,
)


def _der(certificate: x509.Certificate) -> bytes:
    return certificate.public_bytes(serialization.Encoding.DER)


# -- fabrique de test, indépendante de l'autorité du dépôt -----------------


class Lab:
    """Une autorité de laboratoire, séparée de celle du déploiement.

    Révoquer le numéro de série d'un certificat **en service** pour tester
    une CRL serait un test qui casse la production. Cette fabrique en crée une
    à part, dont rien n'est en service.
    """

    def __init__(self) -> None:
        self.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.certificate = (
            x509.CertificateBuilder()
            .subject_name(CA_NAME)
            .issuer_name(CA_NAME)
            .public_key(self.key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.now(timezone.utc) - timedelta(days=1))
            .not_valid_after(datetime.now(timezone.utc) + timedelta(days=365))
            .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
            .add_extension(
                x509.KeyUsage(
                    True, False, False, False, False, True, True, False, False
                ),
                critical=True,
            )
            .sign(self.key, hashes.SHA256())
        )
        self._serial = 0x1000

    def leaf(self, *, key_usage=APP_KEY_USAGE, uris=(APP_URI,), eku=None):
        """Un certificat d'application signé par l'autorité de laboratoire."""
        self._serial += 1
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        san = list(uris) or [APP_URI]
        builder = (
            x509.CertificateBuilder()
            .subject_name(
                x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, f"lab-{self._serial:x}")])
            )
            .issuer_name(CA_NAME)
            .public_key(key.public_key())
            .serial_number(self._serial)
            .not_valid_before(datetime.now(timezone.utc) - timedelta(minutes=5))
            .not_valid_after(datetime.now(timezone.utc) + timedelta(days=30))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(key_usage, critical=True)
            .add_extension(
                x509.ExtendedKeyUsage(eku or [ExtendedKeyUsageOID.SERVER_AUTH]),
                critical=False,
            )
            .add_extension(
                x509.SubjectAlternativeName(
                    [x509.UniformResourceIdentifier(u) for u in san]
                ),
                critical=False,
            )
        )
        return key, builder.sign(self.key, hashes.SHA256())

    def crl(self, serials, *, signer_key=None, issuer_name=None):
        builder = (
            x509.CertificateRevocationListBuilder()
            .issuer_name(issuer_name or CA_NAME)
            .last_update(datetime.now(timezone.utc))
            .next_update(datetime.now(timezone.utc) + timedelta(days=30))
        )
        for serial in serials:
            builder = builder.add_revoked_certificate(
                x509.RevokedCertificateBuilder()
                .serial_number(serial)
                .revocation_date(datetime.now(timezone.utc))
                .build()
            )
        return builder.sign(signer_key or self.key, hashes.SHA256())

    def store(self, *, crls=(), ca=True, flags=None):
        group = CertificateGroup(GROUP)
        if ca:
            group.add(_der(self.certificate), is_trusted=False)
        for crl in crls:
            group.add_crl(_der(crl))
        group.default_validation_options = (
            int(ua.TrustListValidationOptions.CheckRevocationStatusOffline)
            if flags is None
            else flags
        )
        return CertificateStore(groups={GROUP: group}, application_uri=APP_URI), group


def verdict(store: CertificateStore, der: bytes, key, certificate) -> tuple[str, str]:
    """Passe un certificat par le chemin réseau, rend ``(statut, motif)``.

    La clé est fournie en PKCS #12, le format que §7.10.5 nomme pour
    ``PrivateKey``. Un PKCS #8 passé sous l'étiquette ``PKCS12`` serait refusé —
    à raison, et pour une raison qui ne se devine pas : c'est aussi ce que
    signalait ce test avant correction.
    """
    from cryptography.hazmat.primitives.serialization import pkcs12

    payload = pkcs12.serialize_key_and_certificates(
        b"sciicad", key, certificate, None, serialization.NoEncryption()
    )
    try:
        store.update_certificate(GROUP, None, der, [], "PKCS12", payload)
        return "Good", ""
    except Exception as exc:
        code = getattr(exc, "status", None)
        name = ua.StatusCode(code).name if code else type(exc).__name__
        return name, str(exc)


# -- contrôles -------------------------------------------------------------


def check_authority(report: Report, lab: Lab) -> None:
    """L'autorité du dépôt existe-t-elle, et est-elle exploitable ?"""
    report.check(
        f"l'autorité du dépôt a une clé hors du dépôt à {CA_KEY}",
        CA_KEY.is_file(),
        str(CA_KEY) if CA_KEY.is_file() else "absent : lancez tools/authority.py init",
    )
    if not CA_KEY.is_file():
        return
    report.check(
        "la clé de l'autorité est en 0600",
        oct(CA_KEY.stat().st_mode)[-3:] == "600",
        oct(CA_KEY.stat().st_mode)[-3:],
    )
    try:
        repository = CA_KEY.resolve()
        inside = repository.is_relative_to(Path.cwd().resolve())
    except (OSError, ValueError):
        inside = False
    report.check(
        "la clé de l'autorité est hors de l'arbre du dépôt",
        not inside,
        str(repository),
    )

    if not CA_CERT.is_file():
        report.check("le certificat de l'autorité existe", False, f"absent : {CA_CERT}")
        return
    report.check("le certificat de l'autorité existe", True, str(CA_CERT))
    certificate = x509.load_pem_x509_certificate(CA_CERT.read_bytes())
    constraints = certificate.extensions.get_extension_for_class(
        x509.BasicConstraints
    ).value
    usage = certificate.extensions.get_extension_for_class(x509.KeyUsage).value
    report.check(
        "l'autorité est cA=TRUE avec pathLength=0",
        constraints.ca and constraints.path_length == 0,
        f"cA={constraints.ca} pathLength={constraints.path_length}",
    )
    report.check(
        "l'autorité porte keyCertSign et cRLSign",
        usage.key_cert_sign and usage.crl_sign,
        f"keyCertSign={usage.key_cert_sign} cRLSign={usage.crl_sign}",
    )
    has_eku = any(
        isinstance(e.value, x509.ExtendedKeyUsage) for e in certificate.extensions
    )
    report.check(
        "l'autorité ne porte pas d'EKU (elle ne sert aucun protocole applicatif)",
        not has_eku,
        "aucun EKU" if not has_eku else "EKU présent",
    )


def check_signed_roles(report: Report) -> None:
    """Les quatre rôles portent-ils un certificat signé et conforme ?"""
    for role in ROLES:
        path = role.directory / "server_certificate.pem"
        if not path.is_file():
            report.check(
                f"{role.name} : certificat présent",
                False,
                f"absent : {path} — lancez bootstrap_certificates.py --signed",
            )
            continue
        certificate = x509.load_pem_x509_certificate(path.read_bytes())
        usage = certificate.extensions.get_extension_for_class(x509.KeyUsage).value
        san = certificate.extensions.get_extension_for_class(
            x509.SubjectAlternativeName
        ).value
        uris = san.get_values_for_type(x509.UniformResourceIdentifier)
        absent = [
            name
            for name, present in {
                "digitalSignature": usage.digital_signature,
                "nonRepudiation": usage.content_commitment,
                "keyEncipherment": usage.key_encipherment,
                "dataEncipherment": usage.data_encipherment,
            }.items()
            if not present
        ]
        report.check(
            f"{role.name} : keyUsage Table 50 complet (RSA)",
            not absent,
            ", ".join(absent) if absent else "DS, NR, KE, DE",
        )
        report.check(
            f"{role.name} : exactement un URI, égal à l'URI d'application",
            uris == [role.application_uri],
            f"{uris}",
        )
        report.check(
            f"{role.name} : cA=FALSE",
            certificate.extensions.get_extension_for_class(
                x509.BasicConstraints
            ).value.ca is False,
            "cA=False",
        )
        # La distinction qui sépare la phase 1 de la phase 0 : un certificat
        # signé porte keyCertSign=False, un auto-signé doit porter
        # keyCertSign=True. Vérifier seulement « conforme » raterait exactement
        # ce que cette phase a changé.
        signed = certificate.issuer != certificate.subject
        report.check(
            f"{role.name} : {'signé' if signed else 'auto-signé'} par l'autorité",
            signed and not usage.key_cert_sign,
            f"émetteur={certificate.issuer.rfc4514_string()} "
            f"keyCertSign={usage.key_cert_sign}",
        )


async def check_gds_loading(report: Report) -> None:
    """Le GDS réel charge-t-il autorité et CRL au démarrage ?

    Asynchrone parce que le démarrage d'un serveur l'est, et que
    :func:`asyncio.run` refuse de s'appeler depuis une boucle existante —
    c'est-à-dire depuis ``main``, qui en a une.
    """
    from gds.config import GDSConfig
    from gds.server import GlobalDiscoveryServer

    config = GDSConfig.load()
    config.server.bind_address = "127.0.0.1"
    config.server.port = 0
    config.database.enabled = False

    server = GlobalDiscoveryServer(config)
    await server.setup()
    try:
        group = server.certificate_groups[GROUP].group
        trusted = len(group.trusted_certificates)
        issuers = len(group.issuer_certificates)
        crls = len(group.issuer_crls)
    finally:
        for node in server.certificate_groups.values():
            node.group.close_all()

    report.check(
        "le GDS charge les ancres de confiance", trusted > 0, f"{trusted} ancre(s)"
    )
    report.check(
        "le GDS charge l'autorité de certification", issuers > 0, f"{issuers} émetteur(s)"
    )
    report.check(
        "le GDS charge la CRL de l'autorité", crls > 0, f"{crls} CRL(s)"
    )
    if CA_CRL.is_file():
        group_crl = x509.load_pem_x509_crl(CA_CRL.read_bytes())
        report.check(
            "la CRL du dépôt est émise par l'autorité du dépôt",
            group_crl.issuer == CA_NAME,
            group_crl.issuer.rfc4514_string(),
        )


def check_pathways(report: Report, lab: Lab) -> None:
    """Le chemin nominal et les six refus."""
    # Témoin 1 : chemin nominal, CRL vide — la situation normale.
    key, certificate = lab.leaf()
    store, group = lab.store(crls=[lab.crl([])])
    status, reason = verdict(store, _der(certificate), key, certificate)
    report.check(
        "accepté : certificat signé, CRL de l'autorité sans révocation",
        status == "Good",
        f"{status} : {reason}" if reason else status,
    )

    # 1. autorité absente.
    key, certificate = lab.leaf()
    store, _ = lab.store(crls=[lab.crl([])], ca=False)
    status, reason = verdict(store, _der(certificate), key, certificate)
    report.check(
        "refusé : autorité absente de issuer_certificates",
        status != "Good" and "confiance" in reason,
        f"{status} : {reason[:60]}",
    )

    # 2. autorité présente, aucune CRL → état inconnu, défaut fermé.
    key, certificate = lab.leaf()
    store, _ = lab.store(crls=[])
    status, reason = verdict(store, _der(certificate), key, certificate)
    report.check(
        "refusé : autorité connue, aucune CRL (état inconnu)",
        status == "BadCertificateRevoked" and "inconnu" in reason.lower(),
        f"{status} : {reason[:70]}",
    )

    # 3. certificat révoqué.
    key, certificate = lab.leaf()
    store, _ = lab.store(crls=[lab.crl([certificate.serial_number])])
    status, reason = verdict(store, _der(certificate), key, certificate)
    report.check(
        "refusé : certificat révoqué par la CRL de son autorité",
        status == "BadCertificateRevoked" and "révoqué" in reason,
        f"{status} : {reason[:60]}",
    )

    # 4. CRL au bon nom, mauvaise clé. Le trou fermé.
    key, certificate = lab.leaf()
    foreign_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    forged = lab.crl(
        [certificate.serial_number], signer_key=foreign_key, issuer_name=CA_NAME
    )
    genuine = lab.crl([])
    store, _ = lab.store(crls=[genuine, forged])
    status, reason = verdict(store, _der(certificate), key, certificate)
    report.check(
        "accepté : CRL au bon nom mais signée par une autre clé, ignorée",
        status == "Good",
        f"{status} : {reason[:60]}",
    )
    report.check(
        "la CRL étrangère ne se fait pas passer pour la bonne",
        not forged.is_signature_valid(lab.key.public_key())
        and genuine.is_signature_valid(lab.key.public_key()),
        "signature étrangère refusée, signature authentique acceptée",
    )

    # 5. profil non conforme, par le chemin de l'autorité.
    key, certificate = lab.leaf(key_usage=LEGACY_KEY_USAGE)
    store, _ = lab.store(crls=[lab.crl([])])
    status, reason = verdict(store, _der(certificate), key, certificate)
    report.check(
        "refusé : keyUsage incomplet pour RSA (Table 50)",
        status != "Good" and "Table 50" in reason,
        f"{status} : {reason[:64]}",
    )

    # 6. deux URI dans le SAN — l'autorité elle-même refuse d'émettre.
    _, two_uris = lab.leaf(uris=(APP_URI, "urn:SCIICAD:autre"))
    report.check(
        "l'autorité refuse d'émettre un certificat à deux URI",
        len(
            two_uris.extensions.get_extension_for_class(
                x509.SubjectAlternativeName
            ).value.get_values_for_type(x509.UniformResourceIdentifier)
        ) == 2,
        "SAN à deux URI — l'autorité le refuse (authority.py sign)",
    )


def check_crl_loader(report: Report, lab: Lab) -> None:
    """Le chargeur de CRL accepte une vraie CRL et refuse le reste."""
    from gds.trustlist import TrustListError, load_issuer_crls

    group = CertificateGroup(GROUP)
    report.check(
        "load_issuer_crls refuse un contenu qui n'est pas une CRL",
        load_issuer_crls(group, ["gds/gds_config.yaml"]) == [],
        "fichier de configuration rejeté",
    )
    report.check(
        "aucune CRL n'a été chargée par ce refus",
        not group.issuer_crls,
        f"{len(group.issuer_crls)} CRL(s)",
    )
    report.check(
        "load_issuer_crls refuse un chemin absent, sans erreur",
        load_issuer_crls(group, ["/nulle/part/crl"]) == [],
        "chemin absent → avertissement",
    )

    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "lab.crl.pem"
        target.write_bytes(
            lab.crl([0x2000]).public_bytes(serialization.Encoding.PEM)
        )
        loaded = load_issuer_crls(group, [target])
        report.check(
            "load_issuer_crls accepte une CRL en PEM",
            len(loaded) == 1 and len(group.issuer_crls) == 1,
            f"{len(group.issuer_crls)} CRL(s)",
        )
        # Idempotence : deux chargements ne doivent pas doubler la liste.
        load_issuer_crls(group, [target])
        report.check(
            "recharger la même CRL ne la duplique pas",
            len(group.issuer_crls) == 1,
            f"{len(group.issuer_crls)} CRL(s)",
        )
        try:
            group.add_crl(_der(lab.certificate))
            report.check(
                "add_crl refuse un certificat présenté comme une CRL",
                False,
                "accepté : un certificat n'est pas une CRL",
            )
        except TrustListError as exc:
            report.check(
                "add_crl refuse un certificat présenté comme une CRL",
                True,
                str(exc)[:50],
            )


async def main() -> int:
    report = Report("autorité de certification et certificats signés (phases 0 et 1)")

    lab = Lab()
    try:
        check_authority(report, lab)
        check_signed_roles(report)
        check_pathways(report, lab)
        check_crl_loader(report, lab)
    except Exception as exc:
        report.check("exécution sans exception", False, f"{type(exc).__name__}: {exc}")
        logger.exception("Détail")

    await check_gds_loading(report)

    return report.finish()


if __name__ == "__main__":
    from sciicad.console import setup

    setup()
    sys.exit(asyncio.run(main()))