#!/usr/bin/env python3
"""Auto-test de la révocation du GDS, Part 12 §7.8.2.10.

``DefaultValidationOptions`` est l'unique endroit où la norme dit quelles
options appliquer « when validating Certificates with the TrustList ». Ses sept
drapeaux ne sont pas décoratifs : ils décident quelles erreurs sont
insupprimables, et la valeur par défaut de §7.8.2.10 — le seul bit
``CheckRevocationStatusOffline`` — est **fermée**. Un certificat dont l'état de
révocation est inconnu est donc refusé.

Ce que vérifie cet auto-test, et pourquoi :

* la propriété est publiée, au DataType normatif, avec la valeur de la norme ;
* une CRL peut être diffusée par le **seul chemin normatif** qu'est la Part 12 :
  ``Open`` en écriture, ``Write``, ``CloseAndUpdate`` — il n'existe aucun
  ``AddCrl`` ;
* sans CRL d'émetteur, un certificat signé par une autorité de confiance est
  refusé : l'état de révocation est inconnu, et l'erreur n'est pas supprimée ;
* avec une CRL ne listant pas le certificat, il est accepté ;
* avec une CRL le listant, il est refusé ``Bad_CertificateRevoked`` ;
* ``SuppressRevocationStatusUnknown`` laisse passer le cas « aucune CRL », ce qui
  est le levier prévu pour un déploiement qui ne diffuse pas de révocation ;
* ``SuppressCertificateExpired`` rend l'expiration non bloquante ;
* une CRL émise par une **autre** autorité n'a aucun effet : c'est ce qui
  empêche une CRL sans lien de révoquer un certificat sans rapport.

    python tools/selftest_revocation.py
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from asyncua import Server, ua
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from loguru import logger

from gds.certstore import CertificateStore
from gds.certificategroup import CertificateGroupNode
from gds.serverconfiguration import ServerConfigurationNode
from gds.trustlist import DEFAULT_VALIDATION_OPTIONS, CertificateGroup, encode
from sciicad.selftest import Report, free_port

GROUP = "DefaultApplicationGroup"
APP_URI = "urn:SCIICAD:gds-revocation"
NULL = ua.NodeId(0, 0)
EMPTY = ua.Variant([], ua.VariantType.ByteString)


def make_ca(name: str) -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "SCIICAD"),
        x509.NameAttribute(NameOID.COMMON_NAME, name),
    ])
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject).issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True, content_commitment=False,
                key_encipherment=False, data_encipherment=False,
                key_agreement=False, key_cert_sign=True, crl_sign=True,
                encipher_only=False, decipher_only=False,
            ), critical=True,
        )
        .sign(key, hashes.SHA256())
    )
    return key, certificate


def issue(
    ca_key,
    ca_certificate,
    csr_der: bytes,
    lifetime_days: int = 30,
    issued_days_ago: int = 0,
) -> x509.Certificate:
    """Signe une PKCS #10 et rend le certificat (pas son DER).

    ``issued_days_ago`` antédate l'émission plutôt que de raccourcir la durée :
    une durée négative est rejetée par la bibliothèque, et un certificat
    « expiré » s'obtient en le crucifiant il y a longtemps, non en lui donnant
    une fin avant son début.
    """
    csr = x509.load_der_x509_csr(csr_der)
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=issued_days_ago)
    builder = (
        x509.CertificateBuilder()
        .subject_name(csr.subject)
        .issuer_name(ca_certificate.subject)
        .public_key(csr.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(start)
        .not_valid_after(start + timedelta(days=lifetime_days))
    )
    for extension in csr.extensions:
        if isinstance(extension.value, x509.BasicConstraints):
            continue
        builder = builder.add_extension(extension.value, extension.critical)
    builder = builder.add_extension(
        x509.BasicConstraints(ca=False, path_length=None), critical=True
    )
    return builder.sign(ca_key, hashes.SHA256())


def make_crl(
    ca_key, ca_certificate, revoked_serials: tuple[int, ...] = ()
) -> bytes:
    """Fabrique une CRL DER émise par l'autorité donnée.

    Une CRL vide vaut dire « rien n'est révoqué » : c'est un contenu valide et
    distinct de « aucune CRL connue », qui ne dit rien du tout. Les deux cas
    sont covariés, et c'est précisément la distinction que la validation doit
    faire.
    """
    now = datetime.now(timezone.utc)
    builder = (
        x509.CertificateRevocationListBuilder()
        .issuer_name(ca_certificate.subject)
        .last_update(now - timedelta(minutes=5))
        .next_update(now + timedelta(days=30))
    )
    for serial in revoked_serials:
        builder = builder.add_revoked_certificate(
            x509.RevokedCertificateBuilder()
            .serial_number(serial)
            .revocation_date(now - timedelta(minutes=1))
            .build()
        )
    return builder.sign(ca_key, hashes.SHA256()).public_bytes(
        serialization.Encoding.DER
    )


def install_crl(group: CertificateGroup, crl: bytes) -> None:
    """Diffuse une CRL par le chemin normatif : ``Write`` puis ``CloseAndUpdate``.

    Le contenu est réécrit en entier, CRL comprise — c'est ce que fait un client
    conforme, la Part 12 ne fournissant pas de méthode d'ajout. La fonction
    passe par la logique métier de la liste, donc par exactement le même code
    que le réseau.
    """
    lists = group.lists()
    lists["issuer_crls"] = [*lists["issuer_crls"], crl]
    handle = group.open(1 | 2)          # Read | Write
    try:
        payload = encode(ua.TrustListMasks.All, lists)
        # SetPosition 0, puis on écrit la totalité, en agrandissant si besoin.
        group.set_position(handle, 0)
        group.write(handle, payload)
    finally:
        group.close_and_update(handle)


async def run(report: Report, url: str, group_node, manager_node, store, group) -> None:
    from asyncua import Client

    ca_key, ca_certificate = make_ca("SCIICAD CRL CA")
    ca_der = ca_certificate.public_bytes(serialization.Encoding.DER)
    group.add(ca_der, is_trusted=False)
    report.check(
        "l'autorité de certification est dans la liste d'émetteurs",
        ca_der in group.issuer_certificates,
        f"{len(group.issuer_certificates)} émetteur(s)",
    )
    report.check(
        "aucune CRL n'est encore diffusée",
        not group.issuer_crls,
        f"{len(group.issuer_crls)} CRL(s)",
    )

    # -- la propriété est-elle exposée, et à la bonne valeur ? ---------------
    async with Client(url) as client:
        trust_list = client.get_node(group_node.trust_list.nodeid)
        options_node = None
        for child in await trust_list.get_children():
            if (await child.read_browse_name()).Name == "DefaultValidationOptions":
                options_node = child
                break
        report.check(
            "DefaultValidationOptions est publiée",
            options_node is not None,
            f"i={options_node.nodeid.Identifier}" if options_node else "absente",
        )
        if options_node is not None:
            data_type = await options_node.read_data_type()
            report.check(
                "son DataType est TrustListValidationOptions (i=23564)",
                data_type.Identifier == ua.ObjectIds.TrustListValidationOptions,
                f"i={data_type.Identifier}",
            )
            report.check(
                "sa valeur est celle de §7.8.2.10",
                await options_node.read_value() == DEFAULT_VALIDATION_OPTIONS,
                f"{await options_node.read_value()} "
                f"(attendu {DEFAULT_VALIDATION_OPTIONS})",
            )

    configuration = None

    # -- sans CRL : état de révocation inconnu, donc refus ------------------
    csr = store.create_signing_request(GROUP, None, "CN=sans-crl", True, b"a" * 32)
    certificate = issue(ca_key, ca_certificate, csr)
    der = certificate.public_bytes(serialization.Encoding.DER)
    try:
        store.update_certificate(GROUP, None, der, [], "", b"")
        status = "Good"
    except Exception as exc:
        status = ua.StatusCode(getattr(exc, "status", 0)).name or type(exc).__name__
    report.check(
        "sans CRL, un certificat d'une autorité de confiance est refusé",
        status == "BadCertificateRevoked",
        status,
    )

    # -- avec une CRL vide : rien n'est révoqué, donc accepté ----------------
    install_crl(group, make_crl(ca_key, ca_certificate))
    report.check(
        "la CRL est parvenue dans la liste par Write/CloseAndUpdate",
        len(group.issuer_crls) == 1,
        f"{len(group.issuer_crls)} CRL(s), "
        f"{len(group.issuer_crls[0])} octets",
    )
    status = _install(store, der)
    report.check(
        "une CRL vide n'interdit pas le certificat",
        status == "Good",
        status,
    )

    # -- avec une CRL qui révoque ce certificat ------------------------------
    other_key, other_ca = make_ca("SCIICAD Autre CA")
    csr = store.create_signing_request(GROUP, None, "CN=revoque", True, b"b" * 32)
    revoked = issue(ca_key, ca_certificate, csr)
    install_crl(group, make_crl(ca_key, ca_certificate, (revoked.serial_number,)))
    status = _install(store, revoked.public_bytes(serialization.Encoding.DER))
    report.check(
        "un certificat listé par la CRL est refusé BadCertificateRevoked",
        status == "BadCertificateRevoked",
        status,
    )

    # -- une CRL d'une autre autorité n'a aucun effet -----------------------
    csr = store.create_signing_request(GROUP, None, "CN=autre-ca", True, b"c" * 32)
    signed_by_other = issue(ca_key, ca_certificate, csr)
    install_crl(group, make_crl(other_key, other_ca, (signed_by_other.serial_number,)))
    status = _install(store, signed_by_other.public_bytes(serialization.Encoding.DER))
    report.check(
        "une CRL émise par une autre autorité est sans effet",
        status == "Good",
        status,
    )

    # -- les drapeaux de suppression ----------------------------------------
    group.default_validation_options = (
        DEFAULT_VALIDATION_OPTIONS
        | int(ua.TrustListValidationOptions.SuppressRevocationStatusUnknown)
    )
    group.issuer_crls.clear()
    csr = store.create_signing_request(GROUP, None, "CN=supprime", True, b"d" * 32)
    unknown = issue(ca_key, ca_certificate, csr)
    status = _install(store, unknown.public_bytes(serialization.Encoding.DER))
    report.check(
        "SuppressRevocationStatusUnknown laisse passer l'absence de CRL",
        status == "Good",
        status,
    )

    group.default_validation_options = (
        int(ua.TrustListValidationOptions.SuppressCertificateExpired)
    )
    csr = store.create_signing_request(GROUP, None, "CN=expire", True, b"e" * 32)
    expired = issue(ca_key, ca_certificate, csr, issued_days_ago=90)
    status = _install(store, expired.public_bytes(serialization.Encoding.DER))
    report.check(
        "SuppressCertificateExpired rend l'expiration non bloquante",
        status == "Good",
        status,
    )

    group.default_validation_options = DEFAULT_VALIDATION_OPTIONS
    csr = store.create_signing_request(GROUP, None, "CN=expire2", True, b"f" * 32)
    expired2 = issue(ca_key, ca_certificate, csr, issued_days_ago=90)
    install_crl(group, make_crl(ca_key, ca_certificate))
    status = _install(store, expired2.public_bytes(serialization.Encoding.DER))
    report.check(
        "sans le drapeau, un certificat expiré reste refusé",
        status == "BadCertificateTimeInvalid",
        status,
    )

    # -- CheckRevocationStatusOnline n'est pas implanté, et c'est dit --------
    report.check(
        "la révocation en ligne n'est pas annoncée comme active",
        not _has(DEFAULT_VALIDATION_OPTIONS, "CheckRevocationStatusOnline"),
        "bit non posé par défaut, et non honoré si posé",
    )


def _install(store: CertificateStore, der: bytes) -> str:
    """Tente une installation et rend le nom du refus, ou ``Good``."""
    try:
        store.update_certificate(GROUP, None, der, [], "", b"")
    except Exception as exc:
        return getattr(exc, "status", None) and ua.StatusCode(
            getattr(exc, "status")
        ).name or type(exc).__name__
    return "Good"


def _has(flags: int, name: str) -> bool:
    return bool(int(flags) & int(getattr(ua.TrustListValidationOptions, name)))


async def main() -> int:
    report = Report("révocation du GDS (Part 12 §7.8.2.10)")

    port = free_port()
    server = Server()
    await server.init()
    server.socket_address = ("127.0.0.1", port)
    server.set_endpoint(f"opc.tcp://127.0.0.1:{port}")
    server.set_security_policy([ua.SecurityPolicyType.NoSecurity])

    group = CertificateGroup(name=GROUP)
    store = CertificateStore(groups={GROUP: group}, application_uri=APP_URI)
    group_node = CertificateGroupNode(server, group)
    await group_node.build()
    manager_node = ServerConfigurationNode(
        server, store, group_name=lambda _nodeid: GROUP
    )
    await manager_node.build()

    try:
        await server.start()
        await run(
            report, server.endpoint.geturl(), group_node, manager_node, store, group
        )
    except Exception as exc:
        report.check(
            "auto-test exécuté sans exception", False, f"{type(exc).__name__}: {exc}"
        )
        logger.exception("Détail")
    finally:
        group.close_all()
        await server.stop()

    return report.finish()


if __name__ == "__main__":
    from sciicad.console import setup

    setup()
    sys.exit(asyncio.run(main()))
