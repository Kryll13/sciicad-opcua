#!/usr/bin/env python3
"""
Outil de génération de certificat et clé privée pour serveur OPC UA.

Utilisation:
    uv run crypto_opcua.py [--hostname HOSTNAME] [--output-dir DIR]

Dépendances:
    uv add cryptography

Le certificat généré est compatible avec Basic256Sha256_SignAndEncrypt.
"""

import argparse
import ipaddress
import socket
from datetime import datetime, timedelta

from cryptography import x509
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend


def generate_opcua_certificate(
    hostname: str,
    output_dir: str = ".",
    key_size: int = 2048,
    validity_days: int = 365,
    application_uri: str = None,
) -> tuple[str, str]:
    """
    Génère un certificat OPC UA et une clé privée.

    Args:
        hostname: Nom d'hôte du serveur
        output_dir: Répertoire de sortie pour les fichiers
        key_size: Taille de la clé RSA
        validity_days: Validité du certificat en jours
        application_uri: URI d'application OPC UA (ex: urn:SCIICAD:hostname)

    Returns:
        Tuple (chemin_certificat, chemin_clé_privée)
    """
    # Utiliser l'URI d'application par défaut si non spécifiée
    if application_uri is None:
        application_uri = f"urn:SCIICAD:{hostname}"
    # Générer la clé privée RSA
    print(f"Génération de la clé RSA ({key_size} bits)...")
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=key_size,
        backend=default_backend(),
    )

    # Créer le certificat
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "FR"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "France"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, "Industrial"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "SCIICAD"),
        x509.NameAttribute(NameOID.COMMON_NAME, f"OPC UA Server - {hostname}"),
    ])

    # Essayer d'obtenir l'adresse IP
    try:
        ip_address = socket.gethostbyname(hostname)
    except socket.gaierror:
        ip_address = "127.0.0.1"

    # Créer le certificat auto-signé
    print("Création du certificat...")
    cert_builder = x509.CertificateBuilder()
    cert_builder = cert_builder.subject_name(subject)
    cert_builder = cert_builder.issuer_name(issuer)
    cert_builder = cert_builder.public_key(private_key.public_key())
    cert_builder = cert_builder.serial_number(x509.random_serial_number())
    cert_builder = cert_builder.not_valid_before(datetime.utcnow())
    cert_builder = cert_builder.not_valid_after(datetime.utcnow() + timedelta(days=validity_days))

    # Ajouter les extensions OPC UA
    # Subject Alternative Name (requis pour OPC UA)
    # Inclut l'URI d'application, le DNS et les IPs
    cert_builder = cert_builder.add_extension(
        x509.SubjectAlternativeName([
            x509.UniformResourceIdentifier(application_uri),
            x509.DNSName(hostname),
            x509.DNSName(socket.gethostname()),
            x509.IPAddress(ipaddress.ip_address(ip_address)),
            x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
        ]),
        critical=False,
    )

    # Key Usage
    cert_builder = cert_builder.add_extension(
        x509.KeyUsage(
            digital_signature=True,
            key_encipherment=True,
            content_commitment=True,
            data_encipherment=False,
            key_agreement=False,
            key_cert_sign=False,
            crl_sign=False,
            encipher_only=False,
            decipher_only=False,
        ),
        critical=True,
    )

    # Extended Key Usage (serveur OPC UA)
    cert_builder = cert_builder.add_extension(
        x509.ExtendedKeyUsage([
            ExtendedKeyUsageOID.SERVER_AUTH,
        ]),
        critical=False,
    )

    # Ajouter Basic Constraints (indique que ce n'est pas une CA)
    cert_builder = cert_builder.add_extension(
        x509.BasicConstraints(ca=False, path_length=None),
        critical=True,
    )

    # Signer le certificat avec la clé privée
    certificate = cert_builder.sign(private_key, hashes.SHA256(), default_backend())

    # Sauvegarder la clé privée
    key_path = f"{output_dir}/server_private_key.pem"
    print(f"Sauvegarde de la clé privée: {key_path}")
    with open(key_path, "wb") as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ))

    # Sauvegarder le certificat
    cert_path = f"{output_dir}/server_certificate.pem"
    print(f"Sauvegarde du certificat: {cert_path}")
    with open(cert_path, "wb") as f:
        f.write(certificate.public_bytes(serialization.Encoding.PEM))

    print("\n=== Certificat OPC UA généré avec succès ===")
    print(f"Certificat: {cert_path}")
    print(f"Clé privée: {key_path}")
    print(f"Hostname: {hostname}")
    print(f"IP: {ip_address}")
    print(f"Validité: {validity_days} jours")

    return cert_path, key_path


def main():
    parser = argparse.ArgumentParser(
        description="Génère un certificat et une clé privée pour serveur OPC UA"
    )
    parser.add_argument(
        "--hostname",
        default=socket.gethostname(),
        help="Nom d'hôte du serveur (défaut: hostname courant)",
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Répertoire de sortie (défaut: répertoire courant)",
    )
    parser.add_argument(
        "--key-size",
        type=int,
        default=2048,
        help="Taille de la clé RSA (défaut: 2048)",
    )
    parser.add_argument(
        "--validity-days",
        type=int,
        default=365,
        help="Validité du certificat en jours (défaut: 365)",
    )
    parser.add_argument(
        "--application-uri",
        default=None,
        help="URI d'application OPC UA (défaut: urn:SCIICAD:hostname)",
    )

    args = parser.parse_args()

    generate_opcua_certificate(
        hostname=args.hostname,
        output_dir=args.output_dir,
        key_size=args.key_size,
        validity_days=args.validity_days,
        application_uri=args.application_uri,
    )


if __name__ == "__main__":
    main()
