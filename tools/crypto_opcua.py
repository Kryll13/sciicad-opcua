#!/usr/bin/env python3
"""
Génère le certificat applicatif et la clé privée d'un serveur OPC UA.

Produit les deux fichiers attendus par ``thermo-plc/plc_server.py`` et
``protect-plc/plc_server.py`` pour activer ``Basic256Sha256_SignAndEncrypt``.

    uv run tools/crypto_opcua.py \\
        --hostname thermo-plc \\
        --application-uri urn:SCIICAD:thermo-plc \\
        --output-dir thermo-plc

Les deux fichiers sont ignorés par git. La clé privée est écrite avec des
permissions restreintes (0600) ; les fichiers préexistants sont écrasés sans
confirmation, donc vérifiez le chemin avant de lancer la commande.
"""

import argparse
import ipaddress
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from sciicad.console import banner, logger, setup

# Tailles de clé acceptables : en dessous de 2048 bits, la sécurité est
# jugée insuffisante pour un déploiement ; les valeurs folles font échouer
# la génération avec une erreur opaque de la biblicryptographie.
MIN_KEY_SIZE = 2048
MAX_KEY_SIZE = 4096


def build_names(hostname: str, application_uri: str) -> x509.Name:
    """Construit le Distinguished Name du certificat."""
    return x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "FR"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "SCIICAD"),
        x509.NameAttribute(NameOID.COMMON_NAME, f"OPC UA Server - {hostname}"),
    ])


def build_san(hostname: str, application_uri: str) -> list:
    """Construit les noms alternatifs du certificat.

    L'URI d'application est le point contrôle : c'est elle que le client
    rapproche de l'identité annoncée par le serveur.
    """
    entries = [x509.UniformResourceIdentifier(application_uri), x509.DNSName(hostname)]
    if hostname != socket.gethostname():
        entries.append(x509.DNSName(socket.gethostname()))
    for candidate in {hostname, socket.gethostname()}:
        try:
            entries.append(x509.IPAddress(ipaddress.ip_address(candidate)))
        except ValueError:
            continue  # un nom d'hôte n'est pas une adresse : normal
    entries.append(x509.IPAddress(ipaddress.ip_address("127.0.0.1")))
    return entries


def generate(
    hostname: str,
    output_dir: str = ".",
    key_size: int = 2048,
    validity_days: int = 365,
    application_uri: str = "",
) -> tuple[Path, Path]:
    """Génère et écrit le couple certificat / clé privée.

    Retourne ``(chemin_certificat, chemin_cle)``.
    """
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)

    subject = build_names(hostname, application_uri)
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=key_size)
    now = datetime.now(timezone.utc)

    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)  # auto-signé
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + timedelta(days=validity_days))
        .add_extension(
            x509.BasicConstraints(ca=False, path_length=None), critical=True
        )
        .add_extension(
            # Table 50 de la Part 6, « keyUsage » : « For RSA keys, the
            # keyUsage shall include digitalSignature, nonRepudiation,
            # keyEncipherment and dataEncipherment », et « Self-signed
            # Certificates shall also include keyCertSign ».
            #
            # Les trois bits manquants ne sont pas un détail : un validateur
            # conforme rejette le certificat, et le message qu'il rend parle
            # d'une contrainte d'usage — jamais de l'absence de chaîne. C'est
            # le genre de faute qui ne se voit qu'en production.
            #
            # keyCertSign sur un certificat d'application auto-signé est
            # paradoxal en apparence, mais exigé : c'est ce qui permet à ce
            # certificat d'être sa propre ancre, ce qu'est nécessairement un
            # certificat constructeur. La Table 50 le dit sans ambiguïté.
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=True,   # nonRepudiation
                key_encipherment=True,
                data_encipherment=True,
                key_agreement=False,
                key_cert_sign=True,        # auto-signé : ancre de lui-même
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            # Table 50 : « For RSA profiles, the extendedKeyUsage shall specify
            # serverAuth for Servers ». Le clientAuth pour un serveur est un
            # « should », et il n'est pas posé ici : un certificat d'application
            # n'est pas utilisé comme jeton d'identité utilisateur, ce qui est
            # le rôle du DefaultUserTokenGroup.
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False
        )
        .add_extension(
            x509.SubjectAlternativeName(build_san(hostname, application_uri)),
            critical=False,
        )
    )
    certificate = builder.sign(private_key, hashes.SHA256())

    key_path = directory / "server_private_key.pem"
    cert_path = directory / "server_certificate.pem"

    # Permissions restreintes avant écriture : la clé est écrite sans
    # chiffrement, elle ne doit donc pas être lisible par tous.
    key_path.touch(mode=0o600, exist_ok=True)
    key_path.chmod(0o600)
    key_path.write_bytes(
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))

    return cert_path, key_path


def generate_csr(
    hostname: str,
    output_dir: str = ".",
    key_size: int = 2048,
    application_uri: str = "",
):
    """Génère une clé privée et la demande de signature correspondante.

    La clé **reste ici** : elle n'est jamais confiée à l'autorité. C'est le
    sens de ``CreateSigningRequest`` (§7.10.4), dont cette fonction est
    l'équivalent hors bande pour une application que le GDS ne gère pas. La clé
    ne voyage que dans la demande, et la demande ne prouve que la possession.

    Ce qui change par rapport à :func:`generate` : aucun certificat n'est
    produit. Il n'y a donc pas de ``server_certificate.pem`` écrit, et c'est
    voulu — écrire un auto-signé pour le remplacer ensuite ferait subsister un
    instant où le serveur annonce une identité qu'aucune autorité ne soutient.

    Retourne ``(chemin_cle, chemin_demande)``.
    """
    if not application_uri:
        raise ValueError(
            "une demande de signature exige l'URI d'application : c'est elle "
            "que le certificat portera dans son SAN, et elle ne se devine pas"
        )
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)

    subject = build_names(hostname, application_uri)
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=key_size)
    request = (
        x509.CertificateSigningRequestBuilder()
        .subject_name(subject)
        .add_extension(
            # Le SAN voyage dans la demande et n'est pas posé par l'autorité :
            # une autorité qui réécrirait l'identité de celui qu'elle certifie
            # certifierait autre chose que ce qui a été demandé.
            x509.SubjectAlternativeName(build_san(hostname, application_uri)),
            critical=False,
        )
        .sign(private_key, hashes.SHA256())
    )

    key_path = directory / "server_private_key.pem"
    csr_path = directory / "server_certificate.csr"

    key_path.touch(mode=0o600, exist_ok=True)
    key_path.chmod(0o600)
    key_path.write_bytes(
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    csr_path.write_bytes(request.public_bytes(serialization.Encoding.PEM))
    return key_path, csr_path


def parse_args(argv=None):
    """Analyse les arguments de la ligne de commande."""
    parser = argparse.ArgumentParser(
        description="Génère un certificat applicatif OPC UA et sa clé privée",
    )
    parser.add_argument(
        "--hostname", default=socket.gethostname(),
        help="nom d'hôte du serveur (défaut : hostname courant)",
    )
    parser.add_argument(
        "--output-dir", default=".",
        help="répertoire de sortie, créé s'il n'existe pas (défaut : .)",
    )
    parser.add_argument(
        "--key-size", type=int, default=2048,
        help=f"taille de la clé RSA, {MIN_KEY_SIZE}..{MAX_KEY_SIZE} (défaut : 2048)",
    )
    parser.add_argument(
        "--validity-days", type=int, default=365,
        help="durée de validité, > 0 (défaut : 365)",
    )
    parser.add_argument(
        "--application-uri", default="",
        help="URI d'application inscrite dans le SAN (ex : urn:SCIICAD:thermo-plc)",
    )
    parser.add_argument(
        "--csr", action="store_true",
        help=(
            "Produire une demande de signature au lieu d'un certificat. La clé "
            "privée reste locale et aucun certificat n'est écrit : "
            "l'autorité signe ensuite la demande. C'est le chemin d'un "
            "déploiement à autorité de certification."
        ),
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    setup()
    args = parse_args(argv)

    if not MIN_KEY_SIZE <= args.key_size <= MAX_KEY_SIZE:
        logger.error(
            f"--key-size hors plage : {args.key_size} "
            f"(attendu {MIN_KEY_SIZE}..{MAX_KEY_SIZE})"
        )
        return 2
    if args.validity_days <= 0:
        logger.error(f"--validity-days doit être positif : {args.validity_days}")
        return 2

    if args.csr:
        if args.validity_days <= 0:
            logger.error(
                f"--validity-days doit être positif : {args.validity_days}"
            )
            return 2
        if not args.application_uri:
            logger.error(
                "--csr exige --application-uri : c'est elle que le certificat "
                "portera dans son SAN, et elle ne se devine pas."
            )
            return 2
        key_path, csr_path = generate_csr(
            hostname=args.hostname,
            output_dir=args.output_dir,
            key_size=args.key_size,
            application_uri=args.application_uri,
        )
        logger.info(banner("Demande de signature générée", 50))
        logger.info(f"  Clé privée  : {key_path} (non chiffrée, permissions 0600)")
        logger.info(f"  Demande     : {csr_path}")
        logger.info(f"  URI appli.  : {args.application_uri}")
        logger.info("")
        logger.info("La clé n'est sortie de cette machine à aucun moment. Signez la")
        logger.info("demande hors ligne, puis placez le certificat à sa place :")
        logger.info(f"  python tools/authority.py sign {csr_path}")
        return 0

    cert_path, key_path = generate(
        hostname=args.hostname,
        output_dir=args.output_dir,
        key_size=args.key_size,
        validity_days=args.validity_days,
        application_uri=args.application_uri,
    )

    logger.info(banner("Certificat OPC UA généré", 50))
    logger.info(f"  Certificat  : {cert_path}")
    logger.info(f"  Clé privée  : {key_path} (non chiffrée, permissions 0600)")
    logger.info(f"  Hostname    : {args.hostname}")
    if args.application_uri:
        logger.info(f"  URI appli.  : {args.application_uri}")
    logger.info(f"  Validité    : {args.validity_days} jours")
    logger.info("")
    logger.info("Redémarrez le serveur pour qu'il charge ces fichiers.")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
