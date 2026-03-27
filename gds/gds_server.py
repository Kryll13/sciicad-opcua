#!/usr/bin/env python3
"""
Global Discovery Server OPC-UA - Single File Implementation

Ce fichier contient tout ce qui est nécessaire pour exécuter un serveur
GlobalDiscoveryServer OPC-UA.
"""

# ============================================================================
# IMPORTS
# ============================================================================

import asyncio
import base64
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from uuid import uuid4

# Third-party imports
from loguru import logger
from asyncua import Server, ua
from asyncua.common import ua_utils
from pydantic import BaseModel, Field
import yaml
from sqlalchemy import create_engine, select, Column, Integer, String, DateTime, Boolean, Text, ForeignKey, Table, Enum
from sqlalchemy.orm import declarative_base, relationship, Session, sessionmaker
from sqlalchemy.pool import StaticPool
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend


# ============================================================================
# DATABASE MODELS
# ============================================================================

Base = declarative_base()


class RoleEnum(str, Enum):
    """Énumération des rôles disponibles"""
    AUTHENTICATED_USER = "authenticated_user"
    SECURITY_ADMIN = "security_admin"
    CONFIGURE_ADMIN = "configure_admin"
    DISCOVERY_ADMIN = "discovery_admin"
    CERTIFICATE_AUTHORITY_ADMIN = "certificate_authority_admin"


# Association table pour Many-to-Many entre User et Role
user_roles = Table(
    'user_roles',
    Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id')),
    Column('role', String(50), ForeignKey('roles.name'))
)


class Role(Base):
    """Modèle pour les rôles"""
    __tablename__ = 'roles'
    
    name = Column(String(50), primary_key=True)
    description = Column(String(255))
    users = relationship('User', secondary=user_roles, back_populates='roles')
    
    def __repr__(self):
        return f"<Role {self.name}>"


class User(Base):
    """Modèle pour les utilisateurs"""
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    username = Column(String(100), unique=True, nullable=False, index=True)
    password_hash = Column(String(256), nullable=False)
    email = Column(String(255), unique=True, nullable=True)
    is_active = Column(Boolean, default=True)
    is_locked = Column(Boolean, default=False)
    failed_login_attempts = Column(Integer, default=0)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    last_login = Column(DateTime, nullable=True)
    roles = relationship('Role', secondary=user_roles, back_populates='users')
    
    def __repr__(self):
        return f"<User {self.username}>"


class Application(Base):
    """Modèle pour les applications OPC-UA enregistrées"""
    __tablename__ = 'applications'
    
    id = Column(Integer, primary_key=True)
    application_id = Column(String(36), unique=True, nullable=False, index=True)
    application_name = Column(String(255), nullable=False)
    application_type = Column(String(50), nullable=False)
    application_uri = Column(String(500), unique=True, nullable=False)
    product_uri = Column(String(500), nullable=True)
    gateway_server_uri = Column(String(500), nullable=True)
    is_discoverable = Column(Boolean, default=True)
    server_capabilities = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    created_by = Column(Integer, ForeignKey('users.id'), nullable=True)
    
    endpoints = relationship('ServerEndpoint', back_populates='application', cascade='all, delete-orphan')
    names = relationship('ApplicationName', back_populates='application', cascade='all, delete-orphan')
    certificate_requests = relationship('CertificateRequest', back_populates='application', cascade='all, delete-orphan')
    
    def __repr__(self):
        return f"<Application {self.application_name}>"


class ServerEndpoint(Base):
    """Modèle pour les endpoints de serveur"""
    __tablename__ = 'server_endpoints'
    
    id = Column(Integer, primary_key=True)
    application_id = Column(Integer, ForeignKey('applications.id'), nullable=False)
    endpoint_url = Column(String(500), nullable=False)
    security_mode = Column(String(50), default='None')
    security_policy_uri = Column(String(500), nullable=True)
    transport_profile_uri = Column(String(500), nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    
    application = relationship('Application', back_populates='endpoints')
    
    def __repr__(self):
        return f"<ServerEndpoint {self.endpoint_url}>"


class ApplicationName(Base):
    """Modèle pour les noms d'applications localisés"""
    __tablename__ = 'application_names'
    
    id = Column(Integer, primary_key=True)
    application_id = Column(Integer, ForeignKey('applications.id'), nullable=False)
    locale = Column(String(10), default='en-US')
    text = Column(String(255), nullable=False)
    
    application = relationship('Application', back_populates='names')
    
    def __repr__(self):
        return f"<ApplicationName {self.locale}/{self.text}>"


class CertificateRequest(Base):
    """Modèle pour les demandes de certificats"""
    __tablename__ = 'certificate_requests'
    
    id = Column(Integer, primary_key=True)
    request_id = Column(String(36), unique=True, nullable=False, index=True)
    application_id = Column(Integer, ForeignKey('applications.id'), nullable=False)
    request_status = Column(String(50), default='Pending')
    status_message = Column(Text, nullable=True)
    certificate_group_id = Column(String(100), nullable=True)
    certificate_type_id = Column(String(100), nullable=True)
    subject_name = Column(String(500), nullable=True)
    domain_names = Column(Text, nullable=True)
    ip_addresses = Column(Text, nullable=True)
    csr_data = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    certificate_data = Column(Text, nullable=True)
    private_key_data = Column(Text, nullable=True)
    
    application = relationship('Application', back_populates='certificate_requests')
    
    def __repr__(self):
        return f"<CertificateRequest {self.request_id} - {self.request_status}>"


class Certificate(Base):
    """Modèle pour les certificats stockés"""
    __tablename__ = 'certificates'
    
    id = Column(Integer, primary_key=True)
    thumbprint = Column(String(40), unique=True, nullable=False, index=True)
    subject_name = Column(String(500), nullable=False)
    issuer_name = Column(String(500), nullable=True)
    serial_number = Column(String(50), nullable=False)
    valid_from = Column(DateTime, nullable=False)
    valid_until = Column(DateTime, nullable=False)
    certificate_data = Column(Text, nullable=False)
    is_ca = Column(Boolean, default=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    
    def __repr__(self):
        return f"<Certificate {self.thumbprint}>"


class AuditLog(Base):
    """Modèle pour les logs d'audit"""
    __tablename__ = 'audit_logs'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=True)
    action = Column(String(100), nullable=False)
    resource_type = Column(String(50), nullable=False)
    resource_id = Column(String(100), nullable=True)
    details = Column(Text, nullable=True)
    result = Column(String(20), default='Success')
    error_message = Column(Text, nullable=True)
    ip_address = Column(String(50), nullable=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)
    
    def __repr__(self):
        return f"<AuditLog {self.action} - {self.timestamp}>"


class CertificateChangeType(str, Enum):
    """Types de changements de certificats"""
    ADDED = "Added"
    REMOVED = "Removed"
    EXPIRED = "Expired"
    REVOKED = "Revoked"


class CertificateSubscription(Base):
    """Modèle pour les abonnements aux changements de certificats (Pull)"""
    __tablename__ = 'certificate_subscriptions'
    
    id = Column(Integer, primary_key=True)
    subscription_id = Column(String(36), unique=True, nullable=False, index=True)
    certificate_group_id = Column(String(100), nullable=True)
    certificate_type_id = Column(String(100), nullable=True)
    last_change_number = Column(Integer, default=0)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    last_polled_at = Column(DateTime, nullable=True)
    
    def __repr__(self):
        return f"<CertificateSubscription {self.subscription_id}>"


class CertificateChange(Base):
    """Modèle pour les changements de certificats (Pull)"""
    __tablename__ = 'certificate_changes'
    
    id = Column(Integer, primary_key=True)
    subscription_id = Column(String(36), nullable=False, index=True)
    change_number = Column(Integer, unique=True, nullable=False, autoincrement=True)
    change_type = Column(String(20), nullable=False)
    certificate_group_id = Column(String(100), nullable=True)
    certificate_type_id = Column(String(100), nullable=True)
    certificate_thumbprint = Column(String(40), nullable=True)
    certificate_data = Column(Text, nullable=True)
    application_uri = Column(String(255), nullable=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)
    
    def __repr__(self):
        return f"<CertificateChange {self.change_number} - {self.change_type}>"


# ============================================================================
# CONFIGURATION
# ============================================================================

class DatabaseConfig(BaseModel):
    """Configuration de la base de données"""
    type: str = Field(default="sqlite", description="Type: sqlite, postgresql, mysql")
    url: str = Field(default="sqlite:///gds.db", description="URL de connexion")
    host: Optional[str] = None
    port: Optional[int] = None
    database: str = Field(default="gds")
    user: Optional[str] = None
    password: Optional[str] = None
    
    def get_connection_url(self) -> str:
        """Construit l'URL de connexion"""
        if self.url:
            return self.url
        
        if self.type == "sqlite":
            return f"sqlite:///{self.database}.db"
        elif self.type == "postgresql":
            return (f"postgresql://{self.user}:{self.password}@"
                   f"{self.host}:{self.port}/{self.database}")
        elif self.type == "mysql":
            return (f"mysql+pymysql://{self.user}:{self.password}@"
                   f"{self.host}:{self.port}/{self.database}")
        else:
            raise ValueError(f"Type de base de données inconnu: {self.type}")


class ServerConfig(BaseModel):
    """Configuration du serveur OPC-UA"""
    endpoint: str = Field(default="opc.tcp://localhost:4840/GlobalDiscoveryServer")
    application_name: str = Field(default="Global Discovery Server")
    application_uri: str = Field(default="urn:localhost:GlobalDiscoveryServer")
    product_uri: str = Field(default="urn:OPC Foundation:GlobalDiscoveryServer")
    max_connections: int = Field(default=100)
    timeout: int = Field(default=60)
    debug: bool = Field(default=False)


class SecurityConfig(BaseModel):
    """Configuration de sécurité"""
    cert_store_path: Optional[str] = Field(default=None)
    key_size: int = Field(default=2048)
    certificate_validity_days: int = Field(default=365)
    require_authentication: bool = Field(default=True)
    session_timeout_hours: int = Field(default=24)
    password_min_length: int = Field(default=8)
    password_require_uppercase: bool = Field(default=True)
    password_require_numbers: bool = Field(default=True)
    password_require_special: bool = Field(default=False)


class LoggingConfig(BaseModel):
    """Configuration du logging"""
    level: str = Field(default="INFO")
    format: str = Field(default="{time} | {level: <8} | {name}:{function}:{line} - {message}")
    file: Optional[str] = Field(default=None)
    file_rotation: str = Field(default="500 MB")
    file_retention: int = Field(default=10)


class GDSConfig(BaseModel):
    """Configuration principale du serveur GDS"""
    server: ServerConfig = Field(default_factory=ServerConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    
    @classmethod
    def from_file(cls, config_path: str) -> "GDSConfig":
        """Charge la configuration depuis un fichier YAML."""
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Fichier de configuration introuvable: {config_path}")
        
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        return cls(**data)
    
    def to_file(self, config_path: str):
        """Sauvegarde la configuration dans un fichier YAML."""
        path = Path(config_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(path, 'w', encoding='utf-8') as f:
            yaml.dump(self.model_dump(), f, default_flow_style=False)
    
    def to_dict(self) -> Dict[str, Any]:
        """Converti la configuration en dictionnaire"""
        return self.model_dump()


def load_config(config_path: Optional[str] = None) -> GDSConfig:
    """Charge la configuration avec fallback aux valeurs par défaut."""
    if config_path and Path(config_path).exists():
        return GDSConfig.from_file(config_path)
    return GDSConfig()


# ============================================================================
# SECURITY - PASSWORD MANAGER
# ============================================================================

class PasswordManager:
    """Gestionnaire des mots de passe avec Scrypt"""
    
    @staticmethod
    def hash_password(password: str) -> str:
        """Hash un mot de passe avec Scrypt."""
        from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
        
        salt = os.urandom(16)
        
        kdf = Scrypt(
            salt=salt,
            length=32,
            n=2**14,
            r=8,
            p=1,
            backend=default_backend()
        )
        
        key = kdf.derive(password.encode())
        hashed = base64.b64encode(salt + key).decode('utf-8')
        
        return hashed
    
    @staticmethod
    def verify_password(password: str, password_hash: str) -> bool:
        """Vérifie un mot de passe contre son hash."""
        from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
        
        try:
            decoded = base64.b64decode(password_hash.encode('utf-8'))
            salt = decoded[:16]
            stored_hash = decoded[16:]
            
            kdf = Scrypt(
                salt=salt,
                length=32,
                n=2**14,
                r=8,
                p=1,
                backend=default_backend()
            )
            
            computed_hash = kdf.derive(password.encode())
            return computed_hash == stored_hash
        except Exception as e:
            logger.warning(f"Erreur lors de la vérification du mot de passe: {e}")
            return False


# ============================================================================
# SECURITY - CERTIFICATE MANAGER
# ============================================================================

class CertificateManager:
    """Gestionnaire des certificats X.509"""
    
    def __init__(self, cert_store_path: Optional[str] = None):
        """Initialise le gestionnaire de certificats."""
        if cert_store_path is None:
            home = Path.home()
            cert_store_path = home / ".opc-foundation" / "certificate-stores"
        
        self.cert_store_path = Path(cert_store_path)
        self.cert_store_path.mkdir(parents=True, exist_ok=True)
        
        self.trusted_path = self.cert_store_path / "trusted"
        self.issuer_path = self.cert_store_path / "issuer"
        self.rejected_path = self.cert_store_path / "rejected"
        self.private_key_path = self.cert_store_path / "private"
        
        for path in [self.trusted_path, self.issuer_path, self.rejected_path, self.private_key_path]:
            path.mkdir(exist_ok=True, mode=0o700)
    
    @staticmethod
    def _parse_distinguished_name(dn_string: str) -> x509.Name:
        """Parse une chaîne Distinguished Name."""
        attrs = []
        parts = dn_string.split("/")
        
        oid_map = {
            "CN": NameOID.COMMON_NAME,
            "O": NameOID.ORGANIZATION_NAME,
            "C": NameOID.COUNTRY_NAME,
            "ST": NameOID.STATE_OR_PROVINCE_NAME,
            "L": NameOID.LOCALITY_NAME,
        }
        
        for part in parts:
            if "=" in part:
                key, value = part.split("=", 1)
                key = key.strip().upper()
                value = value.strip()
                if key in oid_map:
                    attrs.append(x509.NameAttribute(oid_map[key], value))
        
        if not any(a.oid == NameOID.COUNTRY_NAME for a in attrs):
            attrs.append(x509.NameAttribute(NameOID.COUNTRY_NAME, "US"))
        
        return x509.Name(attrs)
    
    def generate_self_signed_certificate(
        self,
        subject_name: str,
        hostname: str = "localhost",
        valid_days: int = 365,
        key_size: int = 2048,
        application_uri: Optional[str] = None
    ) -> Tuple[bytes, bytes]:
        """Génère un certificat autosigné."""
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=key_size,
            backend=default_backend()
        )
        
        subject = self._parse_distinguished_name(subject_name)
        issuer = subject
        
        cert_builder = x509.CertificateBuilder()
        cert_builder = cert_builder.subject_name(subject)
        cert_builder = cert_builder.issuer_name(issuer)
        cert_builder = cert_builder.public_key(private_key.public_key())
        cert_builder = cert_builder.serial_number(x509.random_serial_number())
        cert_builder = cert_builder.not_valid_before(datetime.now(timezone.utc))
        cert_builder = cert_builder.not_valid_after(
            datetime.now(timezone.utc) + timedelta(days=valid_days)
        )
        
        cert_builder = cert_builder.add_extension(
            x509.BasicConstraints(ca=False, path_length=None),
            critical=True,
        )
        
        cert_builder = cert_builder.add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        
        san_list = [x509.DNSName(hostname)]
        if hostname != "localhost":
            san_list.append(x509.DNSName("localhost"))
        
        if application_uri:
            san_list.append(x509.UniformResourceIdentifier(application_uri))
        
        try:
            from ipaddress import ip_address
            san_list.append(x509.IPAddress(ip_address("127.0.0.1")))
        except:
            pass
        
        cert_builder = cert_builder.add_extension(
            x509.SubjectAlternativeName(san_list),
            critical=False,
        )
        
        certificate = cert_builder.sign(
            private_key,
            hashes.SHA256(),
            backend=default_backend()
        )
        
        cert_der = certificate.public_bytes(serialization.Encoding.DER)
        key_der = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        return cert_der, key_der
    
    @staticmethod
    def get_certificate_thumbprint(certificate_der: bytes) -> str:
        """Calcule le thumbprint SHA1 d'un certificat."""
        cert = x509.load_der_x509_certificate(
            certificate_der,
            backend=default_backend()
        )
        thumbprint = hashlib.sha1(cert.public_bytes(serialization.Encoding.DER)).hexdigest().upper()
        return thumbprint
    
    def generate_ca_certificate(
        self,
        subject_name: str,
        organization_name: str = "GDS CA",
        valid_days: int = 3650,
        key_size: int = 4096
    ) -> Tuple[bytes, bytes]:
        """Génère un certificat CA autosigné."""
        ca_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=key_size,
            backend=default_backend()
        )
        
        subject = self._parse_distinguished_name(subject_name)
        
        cert_builder = x509.CertificateBuilder()
        cert_builder = cert_builder.subject_name(subject)
        cert_builder = cert_builder.issuer_name(subject)
        cert_builder = cert_builder.public_key(ca_key.public_key())
        cert_builder = cert_builder.serial_number(x509.random_serial_number())
        cert_builder = cert_builder.not_valid_before(datetime.now(timezone.utc))
        cert_builder = cert_builder.not_valid_after(
            datetime.now(timezone.utc) + timedelta(days=valid_days)
        )
        
        cert_builder = cert_builder.add_extension(
            x509.BasicConstraints(ca=True, path_length=1),
            critical=True,
        )
        
        cert_builder = cert_builder.add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        
        cert_builder = cert_builder.add_extension(
            x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()),
            critical=False,
        )
        
        ca_cert = cert_builder.sign(
            ca_key,
            hashes.SHA256(),
            backend=default_backend()
        )
        
        cert_der = ca_cert.public_bytes(serialization.Encoding.DER)
        key_der = ca_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.BestAvailableEncryption(b"gds-ca-secret")
        )
        
        logger.info(f"Certificat CA généré: {subject.rfc4514_string()}")
        
        return cert_der, key_der
    
    def sign_csr(
        self,
        csr_pem: bytes,
        ca_cert_der: bytes,
        ca_key_pem: bytes,
        validity_days: int = 365,
        serial_number: Optional[int] = None
    ) -> bytes:
        """Signe une demande de certificat (CSR) avec le certificat CA."""
        csr = x509.load_pem_x509_csr(csr_pem, backend=default_backend())
        
        ca_key = serialization.load_pem_private_key(
            ca_key_pem,
            password=b"gds-ca-secret",
            backend=default_backend()
        )
        
        ca_cert = x509.load_der_x509_certificate(ca_cert_der, backend=default_backend())
        
        cert_builder = x509.CertificateBuilder()
        cert_builder = cert_builder.subject_name(csr.subject)
        cert_builder = cert_builder.issuer_name(ca_cert.issuer)
        cert_builder = cert_builder.public_key(csr.public_key())
        cert_builder = cert_builder.serial_number(serial_number or x509.random_serial_number())
        cert_builder = cert_builder.not_valid_before(datetime.now(timezone.utc))
        cert_builder = cert_builder.not_valid_after(
            datetime.now(timezone.utc) + timedelta(days=valid_days)
        )
        
        for ext in csr.extensions:
            if isinstance(ext, x509.UninitializedExtension):
                continue
            cert_builder = cert_builder.add_extension(ext.value, critical=ext.critical)
        
        try:
            csr.extensions.get_extension_for_class(x509.BasicConstraints)
        except x509.ExtensionNotFound:
            cert_builder = cert_builder.add_extension(
                x509.BasicConstraints(ca=False, path_length=None),
                critical=True,
            )
        
        certificate = cert_builder.sign(
            ca_key,
            hashes.SHA256(),
            backend=default_backend()
        )
        
        return certificate.public_bytes(serialization.Encoding.DER)
    
    def save_ca_certificate(
        self,
        ca_cert_der: bytes,
        ca_key_pem: bytes,
        filename: str = "gds_ca"
    ) -> Tuple[str, str]:
        """Sauvegarde le certificat CA et sa clé privée."""
        ca_cert = x509.load_der_x509_certificate(ca_cert_der, backend=default_backend())
        ca_cert_pem = ca_cert.public_bytes(serialization.Encoding.PEM)
        
        cert_path = self.issuer_path / f"{filename}.pem"
        cert_path.write_bytes(ca_cert_pem)
        
        key_path = self.private_key_path / f"{filename}.key"
        key_path.write_bytes(ca_key_pem)
        key_path.chmod(0o600)
        
        logger.info(f"Certificat CA sauvegardé: {cert_path}")
        
        return str(cert_path), str(key_path)
    
    def load_ca_certificate(self) -> Tuple[Optional[bytes], Optional[bytes]]:
        """Charge le certificat CA et sa clé privée."""
        cert_path = self.issuer_path / "gds_ca.pem"
        key_path = self.private_key_path / "gds_ca.key"
        
        if not cert_path.exists() or not key_path.exists():
            return None, None
        
        ca_cert_pem = cert_path.read_bytes()
        ca_key_pem = key_path.read_bytes()
        
        ca_cert = x509.load_pem_x509_certificate(ca_cert_pem, backend=default_backend())
        ca_cert_der = ca_cert.public_bytes(serialization.Encoding.DER)
        
        return ca_cert_der, ca_key_pem


# ============================================================================
# DATABASE MANAGERS
# ============================================================================

class DatabaseManager:
    """Gestionnaire principal de la base de données"""
    
    def __init__(self, db_url: str = "sqlite:///gds.db"):
        """Initialise le gestionnaire de base de données."""
        self.db_url = db_url
        
        if db_url.startswith("sqlite"):
            engine = create_engine(
                db_url,
                connect_args={"check_same_thread": False},
                poolclass=StaticPool
            )
        else:
            engine = create_engine(db_url, echo=False, pool_pre_ping=True)
        
        self.engine = engine
        self.SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
        
        Base.metadata.create_all(bind=engine)
        
        logger.info(f"Database initialized: {db_url}")
    
    def get_session(self) -> Session:
        """Obtient une nouvelle session de base de données"""
        return self.SessionLocal()
    
    def close(self):
        """Ferme les connexions de la base de données"""
        if hasattr(self, 'engine'):
            self.engine.dispose()


class UserManager:
    """Gestionnaire des utilisateurs"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def create_user(self, username: str, password_hash: str, role: str = "User") -> User:
        """Crée un nouvel utilisateur"""
        session = self.db_manager.get_session()
        try:
            existing = session.query(User).filter_by(username=username).first()
            if existing:
                return existing
            
            user = User(
                username=username,
                password_hash=password_hash
            )
            session.add(user)
            session.flush()
            
            if role:
                role_obj = session.query(Role).filter_by(name=role).first()
                if not role_obj:
                    role_obj = Role(name=role)
                    session.add(role_obj)
                user.roles.append(role_obj)
            
            session.commit()
            return user
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def get_user(self, username: str) -> Optional[User]:
        """Récupère un utilisateur par son nom d'utilisateur"""
        session = self.db_manager.get_session()
        try:
            return session.query(User).filter_by(username=username).first()
        finally:
            session.close()


class ApplicationManager:
    """Gestionnaire des applications OPC-UA"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def register_application(
        self,
        application_uri: str,
        application_name: str,
        application_type: str = "Server",
        product_uri: str = "",
        discovery_urls: Optional[List[str]] = None,
        capabilities: Optional[List[str]] = None
    ) -> Application:
        """Enregistre une nouvelle application"""
        session = self.db_manager.get_session()
        try:
            existing = session.query(Application).filter_by(application_uri=application_uri).first()
            if existing:
                return existing
            
            app = Application(
                application_id=str(uuid4()),
                application_uri=application_uri,
                application_name=application_name,
                application_type=application_type,
                product_uri=product_uri,
                server_capabilities=",".join(capabilities) if capabilities else None
            )
            session.add(app)
            session.flush()
            
            if discovery_urls:
                for url in discovery_urls:
                    endpoint = ServerEndpoint(
                        application_id=app.id,
                        url=url,
                        is_server=application_type in ["Server", "ClientAndServer"]
                    )
                    session.add(endpoint)
            
            app_name = ApplicationName(
                application_id=app.id,
                locale="en",
                text=application_name
            )
            session.add(app_name)
            
            session.commit()
            return app
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def get_application_by_uri(self, application_uri: str) -> Optional[Application]:
        """Récupère une application par son URI"""
        session = self.db_manager.get_session()
        try:
            return session.query(Application).filter_by(application_uri=application_uri).first()
        finally:
            session.close()
    
    def get_application_by_guid(self, application_guid: str) -> Optional[Application]:
        """Récupère une application par son GUID"""
        session = self.db_manager.get_session()
        try:
            return session.query(Application).filter_by(application_id=application_guid).first()
        finally:
            session.close()
    
    def unregister_application(self, application_guid: str) -> bool:
        """Désenregistre une application par son application_id"""
        session = self.db_manager.get_session()
        try:
            app = session.query(Application).filter_by(application_id=application_guid).first()
            if app:
                session.query(ServerEndpoint).filter_by(application_id=app.id).delete()
                session.query(ApplicationName).filter_by(application_id=app.id).delete()
                session.delete(app)
                session.commit()
                logger.info(f"Application désenregistrée: {application_guid}")
                return True
            return False
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def get_all_applications(self, discoverable_only: bool = False) -> List[Application]:
        """Récupère toutes les applications"""
        session = self.db_manager.get_session()
        try:
            query = session.query(Application)
            return query.all()
        finally:
            session.close()
    
    def list_applications(self) -> List[Application]:
        """Liste toutes les applications"""
        session = self.db_manager.get_session()
        try:
            return session.query(Application).all()
        finally:
            session.close()


class DBCertificateManager:
    """Gestionnaire des certificats en base de données"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def create_certificate_request(
        self,
        request_id: str,
        app_id: str,
        certificate_group_id: str,
        certificate_type_id: str,
        subject_name: str,
        domain_names: str,
        csr_data: str,
        private_key_data: str
    ) -> CertificateRequest:
        """Crée une demande de certificat avec CSR et clé privée"""
        session = self.db_manager.get_session()
        try:
            app = session.query(Application).filter_by(application_id=app_id).first()
            if not app:
                raise ValueError(f"Application with GUID {app_id} not found")
            
            cert_req = CertificateRequest(
                request_id=request_id,
                application_id=app.id,
                certificate_group_id=certificate_group_id or None,
                certificate_type_id=certificate_type_id or None,
                subject_name=subject_name,
                domain_names=domain_names,
                csr_data=csr_data,
                private_key_data=private_key_data,
                request_status="Pending"
            )
            session.add(cert_req)
            session.commit()
            return cert_req
        finally:
            session.close()
    
    def get_certificate_request(self, request_id: str) -> Optional[CertificateRequest]:
        """Récupère une demande de certificat"""
        session = self.db_manager.get_session()
        try:
            return session.query(CertificateRequest).filter_by(request_id=request_id).first()
        finally:
            session.close()
    
    def update_certificate_request_status(
        self,
        request_id: str,
        status: str,
        certificate_data: Optional[str] = None,
        status_message: Optional[str] = None
    ) -> bool:
        """Met à jour le statut d'une demande de certificat"""
        session = self.db_manager.get_session()
        try:
            req = session.query(CertificateRequest).filter_by(request_id=request_id).first()
            if not req:
                return False
            
            req.request_status = status
            if certificate_data:
                req.certificate_data = certificate_data
            if status_message:
                req.status_message = status_message
            
            session.commit()
            return True
        finally:
            session.close()
    
    def create_certificate_subscription(
        self,
        subscription_id: str,
        certificate_group_id: Optional[str] = None,
        certificate_type_id: Optional[str] = None
    ) -> CertificateSubscription:
        """Crée un abonnement pour le suivi des changements de certificats"""
        session = self.db_manager.get_session()
        try:
            existing = session.query(CertificateSubscription).filter_by(subscription_id=subscription_id).first()
            if existing:
                return existing
            
            subscription = CertificateSubscription(
                subscription_id=subscription_id,
                certificate_group_id=certificate_group_id,
                certificate_type_id=certificate_type_id,
                last_change_number=0
            )
            session.add(subscription)
            session.commit()
            return subscription
        finally:
            session.close()
    
    def get_certificate_changes(
        self,
        subscription_id: str,
        from_change_number: int = 0,
        max_changes: int = 100
    ) -> tuple:
        """Récupère les changements de certificats depuis un numéro de changement."""
        session = self.db_manager.get_session()
        try:
            subscription = session.query(CertificateSubscription).filter_by(subscription_id=subscription_id).first()
            if not subscription:
                return [], 0
            
            changes = session.query(CertificateChange).filter(
                CertificateChange.subscription_id == subscription_id,
                CertificateChange.change_number > from_change_number
            ).order_by(CertificateChange.change_number.asc()).limit(max_changes + 1).all()
            
            changes_data = []
            for change in changes:
                changes_data.append({
                    "change_number": change.change_number,
                    "change_type": change.change_type.value if hasattr(change.change_type, 'value') else str(change.change_type),
                    "certificate_group_id": change.certificate_group_id,
                    "certificate_type_id": change.certificate_type_id,
                    "timestamp": change.timestamp.isoformat() if change.timestamp else None
                })
            
            more_changes = len(changes) > max_changes
            if more_changes:
                changes_data = changes_data[:max_changes]
            
            last_number = subscription.last_change_number
            
            return changes_data, last_number
        finally:
            session.close()
    
    def get_certificate_groups(self) -> list:
        """Récupère la liste des groupes de certificats"""
        return [
            {
                "node_id": "ns=0;i=25",
                "display_name": "Application Certificate",
                "description": "Default application certificate group",
                "certificate_type": "ApplicationCertificate",
                "is_trust_list": False
            },
            {
                "node_id": "ns=0;i=26",
                "display_name": "HTTPS Certificate",
                "description": "Default HTTPS certificate group",
                "certificate_type": "HttpsCertificate",
                "is_trust_list": False
            },
            {
                "node_id": "ns=0;i=27",
                "display_name": "Certificate Authority",
                "description": "Certificate authority for signing",
                "certificate_type": "CertificateAuthorityCertificate",
                "is_trust_list": False
            }
        ]
    
    def get_trust_lists(self, certificate_group_id: Optional[str] = None) -> list:
        """Récupère les listes de confiance pour un groupe de certificats"""
        trust_lists = []
        group_str = str(certificate_group_id) if certificate_group_id else ""
        show_all = not group_str or group_str in ('0', 'None', 'ns=0;i=0', 'NodeId(0)')
        
        if show_all or group_str == "ns=0;i=25":
            trust_lists.append({
                "node_id": "ns=0;i=35",
                "display_name": "Default Application Trust List",
                "description": "Default trust list for application certificates",
                "location_id": "ns=0;i=25",
                "specified_list_id": "",
                "untrusted": False
            })
        
        if show_all or group_str == "ns=0;i=26":
            trust_lists.append({
                "node_id": "ns=0;i=36",
                "display_name": "Default HTTPS Trust List",
                "description": "Default trust list for HTTPS certificates",
                "location_id": "ns=0;i=26",
                "specified_list_id": "",
                "untrusted": False
            })
        
        if show_all or group_str == "ns=0;i=27":
            trust_lists.append({
                "node_id": "ns=0;i=37",
                "display_name": "Default CA Trust List",
                "description": "Default trust list for CA certificates",
                "location_id": "ns=0;i=27",
                "specified_list_id": "",
                "untrusted": False
            })
        
        return trust_lists


class AuditManager:
    """Gestionnaire des logs d'audit"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def log_action(
        self,
        action: str,
        resource_type: str,
        user_id: Optional[int] = None,
        resource_id: Optional[str] = None,
        details: Optional[dict] = None,
        result: str = "Success",
        error_message: Optional[str] = None,
        ip_address: Optional[str] = None
    ) -> AuditLog:
        """Enregistre une action dans l'audit"""
        session = self.db_manager.get_session()
        try:
            audit = AuditLog(
                user_id=user_id,
                action=action,
                resource_type=resource_type,
                resource_id=resource_id,
                details=json.dumps(details) if details else None,
                result=result,
                error_message=error_message,
                ip_address=ip_address
            )
            session.add(audit)
            session.commit()
            return audit
        finally:
            session.close()


# ============================================================================
# GLOBAL DISCOVERY SERVER
# ============================================================================

class GlobalDiscoveryServer:
    """Serveur Global Discovery Server OPC-UA"""
    
    def __init__(self, config: GDSConfig):
        """Initialise le serveur GDS."""
        self.config = config
        self.server: Optional[Server] = None
        
        # Gestionnaires
        db_url = config.database.get_connection_url()
        self.db_manager = DatabaseManager(db_url)
        self.user_manager = UserManager(self.db_manager)
        self.app_manager = ApplicationManager(self.db_manager)
        self.cert_manager = CertificateManager(config.security.cert_store_path)
        self.db_cert_manager = DBCertificateManager(self.db_manager)
        self.audit_manager = AuditManager(self.db_manager)
        
        # Cache des applications
        self.app_cache: Dict[str, Dict[str, Any]] = {}
        
        # Stockage du certificat CA
        self._ca_cert_der: Optional[bytes] = None
        self._ca_key_pem: Optional[bytes] = None
    
    @staticmethod
    def _parse_distinguished_name(dn_string: str):
        """Parse une chaîne Distinguished Name."""
        from cryptography.x509.oid import NameOID
        
        attrs = []
        parts = dn_string.split("/")
        
        oid_map = {
            "CN": NameOID.COMMON_NAME,
            "O": NameOID.ORGANIZATION_NAME,
            "C": NameOID.COUNTRY_NAME,
            "ST": NameOID.STATE_OR_PROVINCE_NAME,
            "L": NameOID.LOCALITY_NAME,
        }
        
        for part in parts:
            if "=" in part:
                key, value = part.split("=", 1)
                key = key.strip().upper()
                value = value.strip()
                if key in oid_map:
                    attrs.append(x509.NameAttribute(oid_map[key], value))
        
        if not any(a.oid == NameOID.COUNTRY_NAME for a in attrs):
            attrs.append(x509.NameAttribute(NameOID.COUNTRY_NAME, "US"))
        
        return x509.Name(attrs)
    
    async def start(self):
        """Démarre le serveur"""
        try:
            logger.info("Démarrage du serveur Global Discovery Server...")
            
            # Initialiser le certificat CA
            self._initialize_ca_certificate()
            
            # Créer une instance du serveur AsyncUA
            self.server = Server()
            
            # Initialiser le serveur AsyncUA
            await self.server.init()
            
            # Configurer le serveur
            await self._configure_server()
            
            # Initialiser la base de données
            await self._initialize_database()
            
            # Créer les nœuds OPC-UA
            logger.info("Création des nœuds OPC-UA...")
            await self._create_nodes()
            
            # Démarrer le serveur
            logger.info("Démarrage du serveur OPC-UA...")
            await self.server.start()
            logger.info(f"Serveur GDS écoute sur {self.config.server.endpoint}")
            
            # Garder le serveur en exécution
            while True:
                await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Erreur lors du démarrage du serveur: {e}")
            raise
        finally:
            try:
                if self.server:
                    await self.server.stop()
            except Exception:
                pass
            self.db_manager.close()
    
    def _initialize_ca_certificate(self):
        """Initialise le certificat CA (charge ou génère)."""
        self._ca_cert_der, self._ca_key_pem = self.cert_manager.load_ca_certificate()
        
        if self._ca_cert_der and self._ca_key_pem:
            logger.info("Certificat CA chargé depuis le stockage")
        else:
            logger.info("Génération d'un nouveau certificat CA...")
            self._ca_cert_der, self._ca_key_pem = self.cert_manager.generate_ca_certificate(
                subject_name="CN=GDS Global Discovery Server CA",
                organization_name="OPC Foundation GDS",
                valid_days=3650,
                key_size=4096
            )
            
            self.cert_manager.save_ca_certificate(
                self._ca_cert_der,
                self._ca_key_pem,
                filename="gds_ca"
            )
        
        logger.info(f"Certificat CA prêt: {len(self._ca_cert_der)} bytes")
    
    async def _configure_server(self):
        """Configure les paramètres du serveur"""
        self.server.set_endpoint(self.config.server.endpoint)
        self.server.set_server_name(self.config.server.application_name)
        
        await self.server.set_application_uri(self.config.server.application_uri)
        self.server.product_uri = self.config.server.product_uri
        
        import socket
        system_hostname = socket.gethostname()
        
        from urllib.parse import urlparse
        endpoint_parsed = urlparse(self.config.server.endpoint)
        endpoint_hostname = endpoint_parsed.hostname or "localhost"
        
        hostname = system_hostname if system_hostname != "localhost" else endpoint_hostname
        
        try:
            cert_der, key_der = self.cert_manager.generate_self_signed_certificate(
                subject_name=f"CN={self.config.server.application_name}",
                hostname=hostname,
                application_uri=self.config.server.application_uri,
                valid_days=self.config.security.certificate_validity_days
            )
            
            import tempfile
            import os
            with tempfile.NamedTemporaryFile(delete=False, suffix=".der", mode='wb') as cert_file:
                cert_file.write(cert_der)
                cert_path = cert_file.name
            
            with tempfile.NamedTemporaryFile(delete=False, suffix=".der", mode='wb') as key_file:
                key_file.write(key_der)
                key_path = key_file.name
            
            try:
                await self.server.load_certificate(cert_path)
                await self.server.load_private_key(key_path)
                logger.info("Certificat serveur configuré")
            except Exception as cert_err:
                logger.warning(f"Certificat partiellement configuré: {cert_err}")
            finally:
                try:
                    os.unlink(cert_path)
                    os.unlink(key_path)
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"Erreur lors de la génération du certificat: {e}")
    
    async def _initialize_database(self):
        """Initialise la base de données avec les utilisateurs par défaut"""
        session = self.db_manager.get_session()
        try:
            users_count = session.query(User).count()
            if users_count == 0:
                logger.info("Création des utilisateurs par défaut...")
                
                default_users = {
                    "sysadmin": ["security_admin", "configure_admin", "discovery_admin", "certificate_authority_admin"],
                    "appadmin": ["authenticated_user", "certificate_authority_admin", "discovery_admin"],
                    "appuser": ["authenticated_user"],
                    "DiscoveryAdmin": ["authenticated_user", "discovery_admin"],
                    "CertificateAuthorityAdmin": ["authenticated_user", "certificate_authority_admin"],
                }
                
                for username, roles in default_users.items():
                    password_hash = PasswordManager.hash_password("demo")
                    self.user_manager.create_user(
                        username=username,
                        password_hash=password_hash,
                        role=roles[0] if roles else "User"
                    )
                    logger.info(f"Utilisateur créé: {username}")
        finally:
            session.close()
    
    async def _create_nodes(self):
        """Crée les nœuds OPC-UA du serveur GDS"""
        try:
            objects = self.server.get_objects_node()
            
            new_node_id = ua.NodeId(Identifier=1002, NamespaceIndex=0)
            qualified_name = ua.QualifiedName("GlobalDiscoveryServer", 0)
            
            gds_folder = await objects.add_folder(new_node_id, qualified_name)
            
            logger.info(f"Dossier GDS créé: {gds_folder.nodeid}")
            
            browse_name = await gds_folder.read_browse_name()
            logger.info(f"GDS folder browse name: {browse_name.Name} (namespace={browse_name.NamespaceIndex})")
            
            await self._create_methods(gds_folder)
            
        except Exception as e:
            logger.error(f"Erreur lors de la création des nœuds GDS: {e}")
            raise
    
    async def _create_methods(self, parent_node):
        """Crée les méthodes OPC-UA du serveur GDS"""
        
        # === RegisterApplication ===
        async def register_application(parent, application_record):
            """Enregistre une nouvelle application."""
            app_id = None
            app_name = "Unknown"
            app_uri = ""
            
            try:
                record_str = None
                if isinstance(application_record, str):
                    record_str = application_record
                elif hasattr(application_record, 'Value'):
                    record_str = application_record.Value
                elif hasattr(application_record, 'value'):
                    record_str = application_record.value
                else:
                    try:
                        record_str = str(application_record)
                    except:
                        raise ua.UaStatusCodeError(ua.StatusCodes.BadApplicationRecordInvalid)
                
                app_record_data = json.loads(record_str)
                
                # Validate ApplicationUri
                app_uri = app_record_data.get('ApplicationUri', '')
                if not app_uri or not isinstance(app_uri, str):
                    raise ua.UaStatusCodeError(ua.StatusCodes.BadApplicationUriInvalid)
                
                if not ('://' in app_uri or app_uri.startswith('urn:')):
                    raise ua.UaStatusCodeError(ua.StatusCodes.BadApplicationUriInvalid)
                
                # Validate ApplicationName
                app_names = app_record_data.get('ApplicationNames', [])
                if not app_names or not isinstance(app_names, list):
                    raise ua.UaStatusCodeError(ua.StatusCodes.BadApplicationNameInvalid)
                
                first_name_entry = app_names[0] if app_names else {}
                app_name = first_name_entry.get('Text', '')
                if not app_name:
                    raise ua.UaStatusCodeError(ua.StatusCodes.BadApplicationNameInvalid)
                
                # Validate ApplicationType
                app_type = app_record_data.get('ApplicationType', 0)
                valid_types = {0, 1, 2, 3}
                
                if isinstance(app_type, str):
                    type_map = {
                        "Server": 0, "Client": 1, 
                        "ClientAndServer": 2, "DiscoveryServer": 3
                    }
                    app_type = type_map.get(app_type, -1)
                
                if app_type not in valid_types:
                    raise ua.UaStatusCodeError(ua.StatusCodes.BadApplicationTypeInvalid)
                
                type_map = {0: "Server", 1: "Client", 2: "ClientAndServer", 3: "DiscoveryServer"}
                app_type_str = type_map.get(app_type, "Server")
                
                # Check for duplicate
                existing = self.app_manager.get_application_by_uri(app_uri)
                if existing:
                    return [ua.Variant(existing.application_id, ua.VariantType.String)]
                
                # Register the application
                app_id = str(uuid4())
                
                product_uri = app_record_data.get('ProductUri', '')
                gateway_uri = app_record_data.get('GatewayServerUri', '')
                is_discoverable = app_record_data.get('IsDiscoverable', True)
                server_caps = app_record_data.get('ServerCapabilities', [])
                
                self.app_manager.register_application(
                    application_uri=app_uri,
                    application_name=app_name,
                    application_type=app_type_str,
                    product_uri=product_uri,
                    discovery_urls=None,
                    capabilities=server_caps
                )
                
                logger.info(f"Application enregistrée: {app_name} ({app_id})")
                return [ua.Variant(app_id, ua.VariantType.String)]
            
            except ua.UaStatusCodeError:
                raise
            except json.JSONDecodeError as e:
                raise ua.UaStatusCodeError(ua.StatusCodes.BadApplicationRecordInvalid)
            except Exception as e:
                logger.error(f"RegisterApplication - Error: {type(e).__name__}: {e}")
                try:
                    self.audit_manager.log_action(
                        action="RegisterApplication",
                        resource_type="Application",
                        result="Failure",
                        error_message=f"{type(e).__name__}: {e}"
                    )
                except Exception:
                    pass
                raise ua.UaStatusCodeError(ua.StatusCodes.BadUnexpectedError)
        
        # === UnregisterApplication ===
        async def unregister_application(parent, app_id):
            """Désenregistre une application."""
            if isinstance(app_id, ua.uatypes.Variant):
                app_id = app_id.Value
            
            try:
                existing = None
                try:
                    apps = self.app_manager.get_all_applications(discoverable_only=False)
                    existing = next((a for a in apps if a.application_id == app_id), None)
                except Exception:
                    pass
                
                if not existing:
                    raise ua.UaStatusCodeError(ua.StatusCodes.BadNodeIdUnknown)
                
                result = self.app_manager.unregister_application(app_id)
                
                self.audit_manager.log_action(
                    action="UnregisterApplication",
                    resource_type="Application",
                    resource_id=app_id,
                    result="Success",
                    details=json.dumps({"deleted": result})
                )
                
                logger.info(f"Application désenregistrée: {app_id}")
                return None
            
            except ua.UaStatusCodeError:
                raise
            except Exception as e:
                logger.exception(f"UnregisterApplication - Error: {e}")
                raise ua.UaStatusCodeError(ua.StatusCodes.BadUnexpectedError)
        
        # === QueryApplications ===
        async def query_applications(parent, start_index, max_records, application_filter=None):
            """Interroge les applications enregistrées."""
            try:
                start_idx = int(start_index.Value) if hasattr(start_index, 'Value') else int(start_index)
                max_recs = int(max_records.Value) if hasattr(max_records, 'Value') else int(max_records)
                
                filter_uri = None
                filter_name = None
                filter_type = None
                filter_product_uri = None
                
                if application_filter is not None:
                    if hasattr(application_filter, 'Value'):
                        filter_data = application_filter.Value
                    elif hasattr(application_filter, 'value'):
                        filter_data = application_filter.value
                    else:
                        filter_data = application_filter
                    
                    filter_dict = {}
                    if isinstance(filter_data, str) and filter_data.strip():
                        try:
                            filter_dict = json.loads(filter_data)
                        except json.JSONDecodeError:
                            pass
                    elif isinstance(filter_data, dict):
                        filter_dict = filter_data
                    
                    filter_uri = filter_dict.get('applicationUri')
                    filter_name = filter_dict.get('applicationName')
                    filter_type = filter_dict.get('applicationType')
                    filter_product_uri = filter_dict.get('productUri')
                
                apps = self.app_manager.get_all_applications(discoverable_only=True)
                
                filtered_apps = []
                for app in apps:
                    if filter_uri and app.application_uri != filter_uri:
                        continue
                    if filter_name and filter_name.lower() not in app.application_name.lower():
                        continue
                    if filter_type is not None:
                        if isinstance(filter_type, int):
                            type_map = {"Server": 0, "Client": 1, "ClientAndServer": 2, "DiscoveryServer": 3}
                            expected_type = type_map.get(app.application_type, -1)
                            if filter_type != expected_type:
                                continue
                        else:
                            if str(filter_type) != app.application_type:
                                continue
                    if filter_product_uri and app.product_uri != filter_product_uri:
                        continue
                    filtered_apps.append(app)
                
                paginated_apps = filtered_apps[start_idx:start_idx + max_recs]
                
                result = []
                for app in paginated_apps:
                    caps = []
                    if app.server_capabilities:
                        try:
                            caps = json.loads(app.server_capabilities)
                        except:
                            pass
                    
                    result.append({
                        "ApplicationId": app.application_id,
                        "ApplicationUri": app.application_uri,
                        "ApplicationType": app.application_type,
                        "ApplicationNames": [{
                            "Locale": "en",
                            "Text": app.application_name
                        }],
                        "ProductUri": app.product_uri or "",
                        "GatewayServerUri": app.gateway_server_uri or "",
                        "IsDiscoverable": app.is_discoverable,
                        "ServerCapabilities": caps
                    })
                
                return [ua.Variant(json.dumps(result), ua.VariantType.String)]
            
            except Exception as e:
                logger.error(f"QueryApplications - Error: {e}")
                raise ua.UaStatusCodeError(ua.StatusCodes.BadUnexpectedError)
        
        # === Certificate Management Methods ===
        
        async def get_certificate(parent, certificate_group_id, certificate_type_id):
            """Récupère le certificat CA."""
            try:
                group_id = str(certificate_group_id.Value) if hasattr(certificate_group_id, 'Value') else str(certificate_group_id)
                type_id = str(certificate_type_id.Value) if hasattr(certificate_type_id, 'Value') else str(certificate_type_id)
                
                if self._ca_cert_der:
                    return [ua.Variant(self._ca_cert_der, ua.VariantType.ByteString)]
                else:
                    return [ua.Variant(b'', ua.VariantType.ByteString)]
            
            except Exception as e:
                logger.error(f"GetCertificate - Error: {e}")
                raise ua.UaStatusCodeError(ua.StatusCodes.BadUnexpectedError)
        
        async def create_certificate_request(
            parent, application_id, certificate_group_id, certificate_type_id,
            subject_name, domain_names, alternative_names, request_type
        ):
            """Crée une demande de certificat (CSR)."""
            try:
                app_id = str(application_id.Value) if hasattr(application_id, 'Value') else str(application_id)
                group_id = str(certificate_group_id.Value) if hasattr(certificate_group_id, 'Value') else str(certificate_group_id)
                type_id = str(certificate_type_id.Value) if hasattr(certificate_type_id, 'Value') else str(certificate_type_id)
                subject = str(subject_name.Value) if hasattr(subject_name, 'Value') else str(subject_name)
                
                domains = []
                if hasattr(domain_names, 'Value'):
                    domains = domain_names.Value if isinstance(domain_names.Value, list) else [domain_names.Value]
                elif hasattr(domain_names, 'value'):
                    domains = domain_names.value if isinstance(domain_names.value, list) else [domain_names.value]
                elif domain_names:
                    domains = [domain_names]
                
                app = None
                if app_id.startswith('urn:') or ('.' in app_id and '-' in app_id):
                    app = self.app_manager.get_application_by_guid(app_id)
                    if not app:
                        app = self.app_manager.get_application_by_uri(app_id)
                elif app_id.startswith('urn:'):
                    app = self.app_manager.get_application_by_uri(app_id)
                else:
                    app = self.app_manager.get_application_by_guid(app_id)
                
                if not app:
                    raise ua.UaStatusCodeError(ua.StatusCodes.BadUnexpectedError)
                
                private_key = rsa.generate_private_key(
                    public_exponent=65537,
                    key_size=2048,
                    backend=default_backend()
                )
                
                subject_attrs = self._parse_distinguished_name(subject)
                
                csr_builder = x509.CertificateSigningRequestBuilder()
                csr_builder = csr_builder.subject_name(subject_attrs)
                
                san_list = []
                for domain in domains:
                    san_list.append(x509.DNSName(domain))
                
                if san_list:
                    csr_builder = csr_builder.add_extension(
                        x509.SubjectAlternativeName(san_list),
                        critical=False
                    )
                
                csr = csr_builder.sign(private_key, hashes.SHA256(), default_backend())
                
                csr_pem = csr.public_bytes(serialization.Encoding.PEM)
                
                private_key_pem = private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.TraditionalOpenSSL,
                    encryption_algorithm=serialization.BestAvailableEncryption(b"demo")
                )
                
                session = self.db_manager.get_session()
                try:
                    count = session.query(CertificateRequest).count()
                    request_id = str(count + 1)
                finally:
                    session.close()
                
                self.db_cert_manager.create_certificate_request(
                    request_id=request_id,
                    app_id=app_id,
                    certificate_group_id=group_id,
                    certificate_type_id=type_id,
                    subject_name=subject,
                    domain_names=json.dumps(domains),
                    csr_data=base64.b64encode(csr_pem).decode('utf-8'),
                    private_key_data=base64.b64encode(private_key_pem).decode('utf-8')
                )
                
                return [ua.Variant(request_id, ua.VariantType.String)]
            
            except Exception as e:
                logger.error(f"CreateCertificateRequest - Error: {e}")
                raise ua.UaStatusCodeError(ua.StatusCodes.BadUnexpectedError)
        
        async def get_certificate_status(parent, request_id):
            """Récupère le statut d'une demande de certificat."""
            try:
                req_id = str(request_id.Value) if hasattr(request_id, 'Value') else str(request_id)
                
                request = self.db_cert_manager.get_certificate_request(req_id)
                
                if not request:
                    raise ua.UaStatusCodeError(ua.StatusCodes.BadRequestTypeInvalid)
                
                status = request.request_status
                
                return [ua.Variant(status, ua.VariantType.String)]
            
            except ua.UaStatusCodeError:
                raise
            except Exception as e:
                logger.error(f"GetCertificateStatus - Error: {e}")
                raise ua.UaStatusCodeError(ua.StatusCodes.BadUnexpectedError)
        
        async def approve_certificate_request(parent, request_id, certificate=None, certificate_type=None, private_key_format=None):
            """Approuve une demande de certificat."""
            try:
                req_id = str(request_id.Value) if hasattr(request_id, 'Value') else str(request_id)
                
                cert_bytes = None
                if certificate is not None and str(certificate).strip():
                    if hasattr(certificate, 'Value'):
                        cert_bytes = certificate.Value
                    elif hasattr(certificate, 'value'):
                        cert_bytes = certificate.value
                    else:
                        cert_bytes = certificate
                
                request = self.db_cert_manager.get_certificate_request(req_id)
                
                if not request:
                    raise ua.UaStatusCodeError(ua.StatusCodes.BadRequestTypeInvalid)
                
                if request.request_status == "Issued":
                    return [ua.Variant(True, ua.VariantType.Boolean)]
                
                if cert_bytes is None:
                    if not self._ca_cert_der or not self._ca_key_pem:
                        raise ua.UaStatusCodeError(ua.StatusCodes.BadUnexpectedError)
                    
                    csr_pem = base64.b64decode(request.csr_data.encode('utf-8'))
                    
                    cert_bytes = self.cert_manager.sign_csr(
                        csr_pem=csr_pem,
                        ca_cert_der=self._ca_cert_der,
                        ca_key_pem=self._ca_key_pem,
                        validity_days=365
                    )
                    
                    logger.info(f"ApproveCertificateRequest - Auto-signed certificate for request: {req_id}")
                
                if isinstance(cert_bytes, str):
                    cert_bytes = cert_bytes.encode('utf-8')
                
                self.db_cert_manager.update_certificate_request_status(
                    req_id, "Issued", certificate_data=base64.b64encode(cert_bytes).decode('utf-8')
                )
                
                return [ua.Variant(True, ua.VariantType.Boolean)]
            
            except ua.UaStatusCodeError:
                raise
            except Exception as e:
                logger.error(f"ApproveCertificateRequest - Error: {e}")
                raise ua.UaStatusCodeError(ua.StatusCodes.BadUnexpectedError)
        
        # === Pull Certificate Management Methods ===
        
        async def start_get_certificate_changes(parent, certificate_group_id, certificate_type_id):
            """Démarre le suivi des changements de certificats."""
            try:
                group_id = str(certificate_group_id.Value) if hasattr(certificate_group_id, 'Value') else str(certificate_group_id)
                type_id = str(certificate_type_id.Value) if hasattr(certificate_type_id, 'Value') else str(certificate_type_id)
                
                subscription_id = str(uuid4())
                
                self.db_cert_manager.create_certificate_subscription(
                    subscription_id=subscription_id,
                    certificate_group_id=group_id if group_id else None,
                    certificate_type_id=type_id if type_id else None
                )
                
                return [ua.Variant(subscription_id, ua.VariantType.String)]
            
            except Exception as e:
                logger.error(f"StartGetCertificateChanges - Error: {e}")
                raise ua.UaStatusCodeError(ua.StatusCodes.BadUnexpectedError)
        
        async def get_certificate_changes(parent, subscription_id):
            """Récupère les changements de certificats."""
            try:
                sub_id = str(subscription_id.Value) if hasattr(subscription_id, 'Value') else str(subscription_id)
                
                changes, last_number = self.db_cert_manager.get_certificate_changes(sub_id)
                
                more_changes = len(changes) > 0
                
                result = {
                    "changes": changes,
                    "moreChanges": more_changes
                }
                
                return [
                    ua.Variant(json.dumps(result), ua.VariantType.String),
                    ua.Variant(more_changes, ua.VariantType.Boolean)
                ]
            
            except Exception as e:
                logger.error(f"GetCertificateChanges - Error: {e}")
                raise ua.UaStatusCodeError(ua.StatusCodes.BadUnexpectedError)
        
        async def get_certificate_groups(parent):
            """Récupère la liste des groupes de certificats disponibles."""
            try:
                groups = self.db_cert_manager.get_certificate_groups()
                
                result = []
                for group in groups:
                    group_json = json.dumps(group)
                    result.append(ua.Variant(group_json, ua.VariantType.String))
                
                return result
            
            except Exception as e:
                logger.error(f"GetCertificateGroups - Error: {e}")
                raise ua.UaStatusCodeError(ua.StatusCodes.BadUnexpectedError)
        
        async def get_trust_lists(parent, certificate_group_id):
            """Récupère les listes de confiance pour un groupe de certificats."""
            try:
                if hasattr(certificate_group_id, 'Value'):
                    node_id = certificate_group_id.Value
                    if hasattr(node_id, 'Identifier'):
                        group_id = str(node_id.Identifier)
                    else:
                        group_id = str(node_id)
                else:
                    if hasattr(certificate_group_id, 'Identifier'):
                        group_id = str(certificate_group_id.Identifier)
                    else:
                        group_id = str(certificate_group_id)
                
                trust_lists = self.db_cert_manager.get_trust_lists(group_id if group_id and group_id not in ('0', 'None') else None)
                
                result = []
                for tl in trust_lists:
                    tl_json = json.dumps(tl)
                    result.append(ua.Variant(tl_json, ua.VariantType.String))
                
                return result
            
            except Exception as e:
                logger.error(f"GetTrustLists - Error: {e}")
                raise ua.UaStatusCodeError(ua.StatusCodes.BadUnexpectedError)
        
        # === Discovery Services ===
        
        async def find_servers(parent, server_uri=None, locale_ids=None):
            """Retourne la liste des serveurs enregistrés."""
            try:
                servers = self.app_manager.list_applications()
                
                result = []
                for app in servers:
                    if server_uri and app.application_uri != server_uri:
                        continue
                    
                    server_info = {
                        "ApplicationUri": app.application_uri,
                        "ProductUri": app.product_uri or "",
                        "ApplicationNames": [
                            {"Locale": "en-US", "Text": app.application_name}
                        ],
                        "ApplicationType": app.application_type,
                        "GatewayServerUri": "",
                        "DiscoveryUrls": [],
                        "ServerCertificate": b"",
                        "SemaphoreFilePath": ""
                    }
                    result.append(server_info)
                
                return [ua.Variant(json.dumps(result), ua.VariantType.String)]
            
            except Exception as e:
                logger.error(f"FindServers - Error: {e}")
                raise ua.UaStatusCodeError(ua.StatusCodes.BadUnexpectedError)
        
        async def find_servers_on_network(parent, starting_node_id=None, max_records_to_return=100):
            """Retourne la liste des serveurs découverts sur le réseau."""
            try:
                servers = self.app_manager.list_applications()
                
                result = []
                for i, app in enumerate(servers[:max_records_to_return]):
                    server_record = {
                        "RecordId": i + 1,
                        "ServerName": app.application_name,
                        "DiscoveryUrl": f"opc.tcp://localhost:4840/{app.application_uri}",
                        "ServerCapabilities": app.server_capabilities or "",
                        "ServerVersion": 1,
                        "ProductUri": app.product_uri or "",
                        "ApplicationUri": app.application_uri,
                        "ApplicationType": app.application_type,
                        "GatewayServerUri": "",
                        "IsOnline": True
                    }
                    result.append(server_record)
                
                return [ua.Variant(json.dumps(result), ua.VariantType.String)]
            
            except Exception as e:
                logger.error(f"FindServersOnNetwork - Error: {e}")
                raise ua.UaStatusCodeError(ua.StatusCodes.BadUnexpectedError)
        
        async def register_server(parent, server_description):
            """Enregistre un serveur au GDS."""
            try:
                if hasattr(server_description, 'Value'):
                    desc = json.loads(server_description.Value)
                elif hasattr(server_description, 'value'):
                    desc = json.loads(server_description.value)
                else:
                    desc = json.loads(str(server_description))
                
                application_uri = desc.get("ApplicationUri", "")
                application_name = desc.get("ApplicationName", "")
                application_type = desc.get("ApplicationType", "Server")
                discovery_urls = desc.get("DiscoveryUrls", [])
                
                if not application_uri:
                    raise ValueError("ApplicationUri is required")
                
                self.app_manager.register_application(
                    application_uri=application_uri,
                    application_name=application_name or application_uri,
                    application_type=application_type,
                    discovery_urls=discovery_urls
                )
                
                return [ua.Variant(True, ua.VariantType.Boolean)]
            
            except Exception as e:
                logger.error(f"RegisterServer - Error: {e}")
                raise ua.UaStatusCodeError(ua.StatusCodes.BadUnexpectedError)
        
        async def register_server2(parent, server_description, semaphore_file_path=None, is_online_session=True):
            """Enregistre un serveur au GDS avec des informations supplémentaires."""
            try:
                result = await register_server(parent, server_description)
                return result
            
            except Exception as e:
                logger.error(f"RegisterServer2 - Error: {e}")
                raise ua.UaStatusCodeError(ua.StatusCodes.BadUnexpectedError)
        
        # Ajouter les méthodes au serveur
        try:
            # Application Registration Methods
            await parent_node.add_method(
                ua.NodeId(1003, 0),
                ua.QualifiedName("RegisterApplication", 0),
                register_application,
                [ua.VariantType.String],
                [ua.VariantType.String]
            )
            
            await parent_node.add_method(
                ua.NodeId(1004, 0),
                ua.QualifiedName("UnregisterApplication", 0),
                unregister_application,
                [ua.VariantType.String]
            )
            
            await parent_node.add_method(
                ua.NodeId(1005, 0),
                ua.QualifiedName("QueryApplications", 0),
                query_applications,
                [ua.VariantType.Int32, ua.VariantType.Int32, ua.VariantType.String],
                [ua.VariantType.String]
            )
            
            # Certificate Management Methods
            await parent_node.add_method(
                ua.NodeId(1010, 0),
                ua.QualifiedName("GetCertificate", 0),
                get_certificate,
                [ua.VariantType.String, ua.VariantType.String],
                [ua.VariantType.ByteString]
            )
            
            await parent_node.add_method(
                ua.NodeId(1011, 0),
                ua.QualifiedName("CreateCertificateRequest", 0),
                create_certificate_request,
                [ua.VariantType.String, ua.VariantType.String, ua.VariantType.String, 
                 ua.VariantType.String, ua.VariantType.String, ua.VariantType.String,
                 ua.VariantType.String, ua.VariantType.String],
                [ua.VariantType.String]
            )
            
            await parent_node.add_method(
                ua.NodeId(1012, 0),
                ua.QualifiedName("GetCertificateStatus", 0),
                get_certificate_status,
                [ua.VariantType.String],
                [ua.VariantType.String]
            )
            
            await parent_node.add_method(
                ua.NodeId(1013, 0),
                ua.QualifiedName("ApproveCertificateRequest", 0),
                approve_certificate_request,
                [ua.VariantType.String, ua.VariantType.ByteString],
                [ua.VariantType.Boolean]
            )
            
            # Pull Certificate Management Methods
            await parent_node.add_method(
                ua.NodeId(1020, 0),
                ua.QualifiedName("StartGetCertificateChanges", 0),
                start_get_certificate_changes,
                [ua.VariantType.NodeId, ua.VariantType.NodeId],
                [ua.VariantType.String]
            )
            
            await parent_node.add_method(
                ua.NodeId(1021, 0),
                ua.QualifiedName("GetCertificateChanges", 0),
                get_certificate_changes,
                [ua.VariantType.String],
                [ua.VariantType.String, ua.VariantType.Boolean]
            )
            
            await parent_node.add_method(
                ua.NodeId(1022, 0),
                ua.QualifiedName("GetCertificateGroups", 0),
                get_certificate_groups,
                [],
                [ua.VariantType.String]
            )
            
            await parent_node.add_method(
                ua.NodeId(1023, 0),
                ua.QualifiedName("GetTrustLists", 0),
                get_trust_lists,
                [ua.VariantType.NodeId],
                [ua.VariantType.String]
            )
            
            # Discovery Services
            await parent_node.add_method(
                ua.NodeId(1030, 0),
                ua.QualifiedName("FindServers", 0),
                find_servers,
                [ua.VariantType.String, ua.VariantType.String],
                [ua.VariantType.String]
            )
            
            await parent_node.add_method(
                ua.NodeId(1031, 0),
                ua.QualifiedName("FindServersOnNetwork", 0),
                find_servers_on_network,
                [ua.VariantType.String, ua.VariantType.Int32],
                [ua.VariantType.String]
            )
            
            await parent_node.add_method(
                ua.NodeId(1032, 0),
                ua.QualifiedName("RegisterServer", 0),
                register_server,
                [ua.VariantType.String],
                [ua.VariantType.Boolean]
            )
            
            await parent_node.add_method(
                ua.NodeId(1033, 0),
                ua.QualifiedName("RegisterServer2", 0),
                register_server2,
                [ua.VariantType.String, ua.VariantType.String, ua.VariantType.Boolean],
                [ua.VariantType.Boolean]
            )
            
            logger.info("Méthodes GDS créées: RegisterApplication, UnregisterApplication, QueryApplications")
            logger.info("  + Méthodes Certificate Management: GetCertificate, CreateCertificateRequest, GetCertificateStatus, ApproveCertificateRequest")
            logger.info("  + Méthodes Pull Certificate Management: StartGetCertificateChanges, GetCertificateChanges, GetCertificateGroups, GetTrustLists")
            logger.info("  + Méthodes Discovery Services: FindServers, FindServersOnNetwork, RegisterServer, RegisterServer2")
        except Exception as e:
            logger.error(f"Erreur lors de l'ajout des méthodes: {e}")
            raise
    
    async def stop(self):
        """Arrête le serveur"""
        if self.server:
            logger.info("Arrêt du serveur GDS...")
            try:
                await self.server.stop()
            except (AttributeError, TypeError):
                pass
            self.db_manager.close()
            logger.info("Serveur arrêté")


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

async def main():
    """Point d'entrée principal"""
    # Charger la configuration
    config = load_config("gds_config.yaml")
    
    # Créer et démarrer le serveur
    server = GlobalDiscoveryServer(config)
    
    try:
        await server.start()
    except KeyboardInterrupt:
        logger.info("Arrêt demandé par l'utilisateur")
    finally:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
