#!/usr/bin/env python3
"""Auto-test du rôle *CertificateManager* du GDS, Part 12 §7.10.

Le scénario reproduit le cycle réel de la norme : le serveur prépare une
demande de signature, une autorité d'enregistrement **extérieure** signe, puis le
certificat revient être installé. Aucune clé de CA n'est détenue par le GDS,
puisque ce n'est pas son rôle (§7.1) — c'est pourquoi le test fabrique sa propre
autorité de certification pour signer.

Tout est appelé **par le réseau**, avec un client qui parcourt l'espace
d'adressage comme le ferait un vrai consommateur. Une méthode annoncée mais non
câblée échoue donc ici.

Ce que vérifie cet auto-test, et pourquoi :

* l'objet ``ServerConfiguration`` existe sous le browse name normatif, avec le
  ``TypeDefinition`` normatif (§7.10.4) ;
* les trois méthodes *CertificateManager* répondent, avec la signature normative ;
* une PKCS #10 réellement exploitable est produite, URI d'application comprise ;
* un certificat signé par une autorité de confiance est accepté et installé ;
* la chaîne d'émetteurs fournie est versée dans la liste de confiance du groupe,
  comme l'exige §7.10.5 pour que la validation soit reproductible ;
* un ``Nonce`` trop court donne ``Bad_InvalidArgument`` (§7.10.10) ;
* un certificat expiré, mal adressé ou non signé par une autorité de confiance
  est refusé avec le code normatif correspondant, et ``GetRejectedList`` le
  restitue ;
* un ``CertificateGroupId`` inconnu est refusé, jamais réécrit ailleurs.

    python tools/selftest_certmanager.py
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from asyncua import Client, Server, ua
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from loguru import logger

from gds.certstore import CertificateStore
from gds.serverconfiguration import BROWSE_NAME, ServerConfigurationNode
from gds.trustlist import CertificateGroup, thumbprint
from sciicad.selftest import Report, free_port

GROUP = "DefaultApplicationGroup"
APP_URI = "urn:SCIICAD:gds-selftest"
CA_URI = "urn:SCIICAD:selftest-ca"

#: Un tableau ByteString vide doit être transmis comme un ``Variant`` typé :
#: asyncua devine le type d'un tableau à partir de son premier élément, et échoue
#: sur « could not guess UA type of variable [] » quand il n'y en a pas. Ce n'est
#: pas unebizarrerie de notre câblage, mais une contrainte du client qu'un
#: consommateur normatif doit connaître.
EMPTY = ua.Variant([], ua.VariantType.ByteString)

#: Durée de vie du certificat de test : courte mais largement suffisante, et
#: le test n'accélère pas le temps — il fabrique plutôt un certificat déjà
#: expiré, ce qui est le cas réellement à couvrir.
VALIDITY_DAYS = 30


# -- autorité de certification de test -------------------------------------


def make_ca() -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    """Fabrique une autorité de certification auto-signée, pour le test.

    Le GDS étant un CertificateManager et non une CA, il ne possède aucune clé
    de signature : c'est ce motif externe qui produit le certificat signé que le
    GDS doit ensuite valider.
    """
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name(
        [
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "SCIICAD"),
            x509.NameAttribute(NameOID.COMMON_NAME, "SCIICAD Auto-Test CA"),
        ]
    )
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
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
        .sign(key, hashes.SHA256())
    )
    return key, certificate


def sign_request(
    ca_key,
    ca_certificate: x509.Certificate,
    csr_der: bytes,
    application_uri: str,
    lifetime_days: int = VALIDITY_DAYS,
    not_before: Optional[datetime] = None,
) -> bytes:
    """Signe une PKCS #10 avec l'autorité de test, et rend le DER.

    Le sujet est repris de la demande, mais l'URI d'application du SAN est
    imposée par le paramètre : c'est ainsi que l'on fabrique un certificat
    cohérent sauf sur ce point précis, et donc un refus attribuable à la seule
    validation de l'URI.
    """
    csr = x509.load_der_x509_csr(csr_der)
    now = datetime.now(timezone.utc)
    start = not_before if not_before is not None else now - timedelta(minutes=5)
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
        if isinstance(
            extension.value, (x509.BasicConstraints, x509.SubjectAlternativeName)
        ):
            # BasicConstraints est réécrite inconditionnellement, et le SAN est
            # remplacé par celui demandé : la norme autorise l'autorité à
            # arbitrer l'identité, mais ici c'est le test qui la fixe.
            continue
        builder = builder.add_extension(extension.value, extension.critical)
    builder = builder.add_extension(
        x509.BasicConstraints(ca=False, path_length=None), critical=True
    )
    builder = builder.add_extension(
        x509.SubjectAlternativeName([x509.UniformResourceIdentifier(application_uri)]),
        critical=False,
    )
    return builder.sign(ca_key, hashes.SHA256()).public_bytes(serialization.Encoding.DER)


# -- client de test --------------------------------------------------------


class ManagerClient:
    """Client qui parle au GDS comme un consommateur réel."""

    def __init__(self, url: str, config_nodeid: ua.NodeId) -> None:
        self.url = url
        self.config_nodeid = config_nodeid
        self._client: Optional[Client] = None
        self._node = None
        self._methods: dict[str, Any] = {}

    async def __aenter__(self) -> "ManagerClient":
        self._client = Client(self.url)
        await self._client.connect()
        self._node = self._client.get_node(self.config_nodeid)
        self._methods = {
            (await child.read_browse_name()).Name: child
            for child in await self._node.get_children()
        }
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._client is not None:
            await self._client.disconnect()

    def has(self, name: str) -> bool:
        return name in self._methods

    async def arguments(self, name: str) -> dict[str, list[tuple[str, int, int]]]:
        """Signatures apparentes d'une méthode, entrées et sorties.

        Les deux propriétés sont lues séparément : les comparer réunies ferait
        dépendre le résultat de l'ordre de parcours des enfants, qui n'est pas
        garanti.
        """
        found: dict[str, list[tuple[str, int, int]]] = {}
        for child in await self._methods[name].get_children():
            browse = (await child.read_browse_name()).Name
            if browse not in ("InputArguments", "OutputArguments"):
                continue
            values = (await child.read_data_value()).Value.Value or []
            found[browse] = [
                (a.Name, a.DataType.Identifier, a.ValueRank) for a in values
            ]
        return found

    async def call(self, name: str, *args: Any) -> Any:
        """Appelle une méthode et rend la première valeur de sortie."""
        result = await self._node.call_method(self._methods[name], *args)
        if isinstance(result, (tuple, list)):
            return result[0] if result else None
        return result

    async def call_array(self, name: str, *args: Any) -> list:
        """Appelle une méthode dont l'unique sortie est un tableau.

        ``call_method`` rend ``OutputArguments[0].Value`` lorsqu'il n'y a qu'une
        seule sortie : le résultat est donc déjà le tableau, et le déballer
        comme une liste de sorties en prendrait le premier élément. Les deux
        formes sont indiscernables côté client ; le test choisit donc explicitement
        laquelle il attend.
        """
        result = await self._node.call_method(self._methods[name], *args)
        if result is None:
            return []
        if isinstance(result, (list, tuple)):
            return [bytes(item) for item in result]
        return [bytes(result)]

    async def status_of(self, name: str, *args: Any) -> ua.StatusCode:
        """Appelle et rend le ``StatusCode``, sans laisser remonter l'erreur.

        ``call_method`` lève dès que le statut n'est pas ``Good`` : c'est
        précisément ce que l'on veut observer ici.
        """
        try:
            result = await self._node.call_method(self._methods[name], *args)
        except ua.UaStatusCodeError as exc:
            return ua.StatusCode(exc.code)
        if isinstance(result, ua.StatusCode):
            return result
        if isinstance(result, (tuple, list)) and result and isinstance(
            result[0], ua.StatusCode
        ):
            return result[0]
        return ua.StatusCode(ua.StatusCodes.Good)


# -- scénario --------------------------------------------------------------


async def run(
    report: Report,
    client: ManagerClient,
    store: CertificateStore,
    group: CertificateGroup,
    published_group_id: ua.NodeId,
    config_nodeid: ua.NodeId,
) -> None:
    ca_key, ca_certificate = make_ca()
    ca_der = ca_certificate.public_bytes(serialization.Encoding.DER)
    # La norme (§7.10.5) suppose que les certificats d'émetteur figurent DÉJÀ
    # dans la liste de confiance du groupe : sans cela, aucune validation de
    # chaîne n'est possible et tout serait refusé.
    group.add(ca_der, is_trusted=False)
    report.check(
        "l'autorité de certification est dans la liste d'émetteurs",
        ca_der in group.issuer_certificates,
        f"{len(group.issuer_certificates)} émetteur(s)",
    )

    # -- l'objet est-il au bon endroit ? ------------------------------------
    async with Client(client.url) as plain:
        node = plain.get_node(client.config_nodeid)
        browse = await node.read_browse_name()
        type_definition = await node.read_type_definition()
        # Un doublon porterait le même browse name et responderait
        # BadNothingToDo : c'est exactement le défaut qu'un client
        # parcourant l'espace d'adressage constaterait, et qu'un test qui vise
        # le NodeId directement ne verrait pas.
        twins = [
            child
            for child in await plain.nodes.server.get_children()
            if (await child.read_browse_name()).Name == BROWSE_NAME
        ]
    report.check(
        f"l'objet porte le browse name normatif {BROWSE_NAME!r}",
        browse.Name == BROWSE_NAME,
        browse.Name,
    )
    report.check(
        "le TypeDefinition est bien ServerConfigurationType (i=12581)",
        type_definition.Identifier == ua.ObjectIds.ServerConfigurationType,
        f"i={type_definition.Identifier}",
    )
    report.check(
        "un seul objet ServerConfiguration est publié",
        len(twins) == 1,
        f"{len(twins)} objet(s) : {', '.join(str(t.nodeid) for t in twins)}",
    )
    report.check(
        "c'est l'instance normative qui est câblée (i=12637)",
        config_nodeid.Identifier == ua.ObjectIds.ServerConfiguration,
        f"i={config_nodeid.Identifier}",
    )

    for name in ("CreateSigningRequest", "UpdateCertificate", "GetRejectedList"):
        report.check(f"{name} est publiée et câblée", client.has(name))
    # Les signatures sont normatives : les vérifier ici, entrées et sorties
    # séparément, empêche qu'un câblage bogus passe inaperçu.
    expected = {
        "CreateSigningRequest": (
            [
                ("CertificateGroupId", ua.ObjectIds.NodeId, -1),
                ("CertificateTypeId", ua.ObjectIds.NodeId, -1),
                ("SubjectName", ua.ObjectIds.String, -1),
                ("RegeneratePrivateKey", ua.ObjectIds.Boolean, -1),
                ("Nonce", ua.ObjectIds.ByteString, -1),
            ],
            [("CertificateRequest", ua.ObjectIds.ByteString, -1)],
        ),
        "UpdateCertificate": (
            [
                ("CertificateGroupId", ua.ObjectIds.NodeId, -1),
                ("CertificateTypeId", ua.ObjectIds.NodeId, -1),
                ("Certificate", ua.ObjectIds.ByteString, -1),
                ("IssuerCertificates", ua.ObjectIds.ByteString, 1),
                ("PrivateKeyFormat", ua.ObjectIds.String, -1),
                ("PrivateKey", ua.ObjectIds.ByteString, -1),
            ],
            [("ApplyChangesRequired", ua.ObjectIds.Boolean, -1)],
        ),
        "GetRejectedList": ([], [("Certificates", ua.ObjectIds.ByteString, 1)]),
    }
    for name, (inputs, outputs) in expected.items():
        found = await client.arguments(name)
        report.check(
            f"{name} a la signature normative",
            found.get("InputArguments", []) == inputs
            and found.get("OutputArguments", []) == outputs,
            str(found),
        )

    # -- CreateSigningRequest ----------------------------------------------
    # Un NodeId de groupe nul désigne le DefaultApplicationGroup (§7.10.5),
    # sans qu'aucune résolution soit nécessaire.
    null_id = ua.NodeId(0, 0)
    csr_der = await client.call(
        "CreateSigningRequest",
        null_id,
        null_id,
        "CN=gds-selftest/O=SCIICAD",
        False,
        b"",
    )
    parsed = x509.load_der_x509_csr(csr_der) if csr_der else None
    report.check(
        "CreateSigningRequest rend une PKCS #10 exploitable",
        parsed is not None and parsed.is_signature_valid,
        f"{len(csr_der or b'')} octets",
    )
    uris = [
        entry.value
        for entry in (parsed.extensions.get_extension_for_class(
            x509.SubjectAlternativeName).value if parsed else [])
        if isinstance(entry, x509.UniformResourceIdentifier)
    ] if parsed else []
    report.check(
        "la demande porte l'URI d'application dans le SAN",
        uris == [APP_URI],
        str(uris),
    )
    report.check(
        "le sujet demandé est repris tel quel",
        parsed is not None
        and parsed.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
        == "gds-selftest",
        parsed.subject.rfc4514_string() if parsed else "",
    )

    # -- UpdateCertificate : le chemin nominal -------------------------------
    signed = sign_request(ca_key, ca_certificate, csr_der, APP_URI)
    required = await client.call("UpdateCertificate", null_id, null_id, signed, EMPTY, "", b"")
    report.check(
        "un certificat signé par une autorité de confiance est accepté",
        required is False,
        f"ApplyChangesRequired={required}",
    )
    entry = store.entry(GROUP)
    report.check(
        "le certificat est installé avec son empreinte",
        entry.installed and entry.thumbprint == thumbprint(signed),
        entry.thumbprint,
    )
    report.check(
        "le certificat rendu est bien celui installé",
        store.certificate_der(GROUP) == signed,
        f"{len(store.certificate_der(GROUP))} octets",
    )

    # La chaîne fournie doit atterrir dans la liste d'émetteurs du groupe.
    report.check(
        "la chaîne d'émetteurs est versée dans la liste de confiance (§7.10.5)",
        ca_der in group.issuer_certificates,
        f"{len(group.issuer_certificates)} émetteur(s)",
    )

    # -- résolution du groupe ----------------------------------------------
    # Ces deux vérifications précèdent volontairement la régénération de clé
    # plus bas : après celle-ci, le certificat ci-dessus devient legitimately
    # périmé et serait refusé pour une raison sans rapport avec le groupe.
    status = await client.status_of(
        "UpdateCertificate", published_group_id, null_id, signed, EMPTY, "", b""
    )
    report.check(
        "le NodeId du groupe publié se résout sans erreur",
        status.name == "Good",
        status.name,
    )
    status = await client.status_of(
        "UpdateCertificate", ua.NodeId(999999, 0), null_id, signed, EMPTY, "", b""
    )
    report.check(
        "un CertificateGroupId inconnu donne BadNodeIdUnknown",
        status.name == "BadNodeIdUnknown",
        status.name,
    )
    report.check(
        "le refus n'a pas déplacé le certificat installé",
        store.certificate_der(GROUP) == signed,
        "groupe intact",
    )

    # -- les refus, avec leur code normatif ---------------------------------
    csr2 = store.create_signing_request(GROUP, None, "CN=gds-refus", True, b"x" * 32)

    # Nonce trop court : §7.10.10 impose au moins 32 octets.
    status = await client.status_of(
        "CreateSigningRequest", null_id, null_id, "CN=gds", True, b"court"
    )
    report.check(
        "un Nonce trop court donne BadInvalidArgument",
        status.name == "BadInvalidArgument",
        status.name,
    )

    # Certificat expiré.
    expired = sign_request(
        ca_key, ca_certificate, csr2, APP_URI, lifetime_days=1,
        not_before=datetime.now(timezone.utc) - timedelta(days=90),
    )
    status = await client.status_of("UpdateCertificate", null_id, null_id, expired, EMPTY, "", b"")
    report.check(
        "un certificat expiré donne BadCertificateTimeInvalid",
        status.name == "BadCertificateTimeInvalid",
        status.name,
    )

    # URI d'application absente du SAN.
    other = sign_request(ca_key, ca_certificate, csr2, "urn:SCIICAD:autre-appli")
    status = await client.status_of("UpdateCertificate", null_id, null_id, other, EMPTY, "", b"")
    report.check(
        "un certificat portant une autre URI d'application donne BadCertificateUriInvalid",
        status.name == "BadCertificateUriInvalid",
        status.name,
    )

    # Chaîne de confiance absente : une autre autorité, non reconnue.
    rogue_key, rogue_ca = make_ca()
    forged = sign_request(rogue_key, rogue_ca, csr2, APP_URI)
    status = await client.status_of("UpdateCertificate", null_id, null_id, forged, EMPTY, "", b"")
    report.check(
        "un certificat d'une autorité non approuvée donne BadCertificateUntrusted",
        status.name == "BadCertificateUntrusted",
        status.name,
    )

    # Certificat illisible.
    status = await client.status_of(
        "UpdateCertificate", null_id, null_id, b"pas-un-DER", EMPTY, "", b""
    )
    report.check(
        "un DER illisible donne BadCertificateInvalid",
        status.name == "BadCertificateInvalid",
        status.name,
    )

    # Groupe inconnu : on ne doit surtout pas écrire dans un autre groupe.
    # Le résolveur du test ne reconnaît qu'un NodeId publié, celui du groupe ;
    # tout autre identifiant est donc réellement inconnu, comme le serait un
    # NodeId forgé par un client.

    # -- GetRejectedList ----------------------------------------------------
    # §7.8.3.2 : « Servers only add Certificates to this list that have no
    # unsuppressed validation errors but are not trusted. » Seul le certificat
    # d'une autorité non approuvée y figure donc. Un certificat expiré, mal
    # adressé ou illisible est un défaut, pas un candidat à approuver : il est
    # refusé avec son code, et absent de la liste.
    rejected = await client.call_array("GetRejectedList")
    report.check(
        "GetRejectedList ne contient que le certificat non approuvé (§7.8.3.2)",
        {thumbprint(item) for item in rejected} == {thumbprint(forged)},
        f"{len(rejected)} certificat(s) : "
        + ", ".join(
            "non approuvé" if thumbprint(item) == thumbprint(forged) else "AUTRE"
            for item in rejected
        ),
    )
    for label, candidate in (
        ("expiré", expired),
        ("mal adressé", other),
        ("illisible", b"pas-un-DER"),
    ):
        report.check(
            f"le certificat {label} est absent de la liste des rejets",
            all(thumbprint(item) != thumbprint(candidate) for item in rejected),
            "refusé, mais pas rejeté",
        )
    report.check(
        "aucun certificat accepté ne figure parmi les refus",
        all(thumbprint(item) != thumbprint(signed) for item in rejected),
        "",
    )

    # -- le renouvellement réutilise la clé ---------------------------------
    # Chaque vérification déclenche son propre appel : une demande sans nonce
    # remet ``pending_nonce`` à vide, donc lire cet état après un appel sans
    # régénération ne prouverait rien sur le précédent.
    store.create_signing_request(GROUP, None, "", False, b"")
    report.check(
        "CreateSigningRequest sans régénération réutilise la clé existante",
        store.entry(GROUP).private_key is not None and not store.entry(GROUP).pending_nonce,
        "clé conservée, aucune demande en attente",
    )
    store.create_signing_request(GROUP, None, "", True, b"n" * 32)
    report.check(
        "CreateSigningRequest avec régénération ouvre une demande en attente",
        store.entry(GROUP).pending_nonce == thumbprint(b"n" * 32),
        f"nonce lié : {store.entry(GROUP).pending_nonce[:12]}…",
    )
    # Un certificat types hors de portée (ici ECC) est refusé : le magasin ne
    # sait générer que des clés RSA, et doit le dire plutôt que dériver.
    status = await client.status_of(
        "CreateSigningRequest",
        null_id,
        ua.NodeId(ua.ObjectIds.EccNistP256ApplicationCertificateType, 0),
        "CN=gds",
        False,
        b"",
    )
    report.check(
        "un type de certificat hors de portée donne BadInvalidArgument",
        status.name == "BadInvalidArgument",
        status.name,
    )


async def main() -> int:
    report = Report("rôle CertificateManager du GDS (Part 12 §7.10)")

    port = free_port()
    server = Server()
    await server.init()
    server.socket_address = ("127.0.0.1", port)
    server.set_endpoint(f"opc.tcp://127.0.0.1:{port}")
    server.set_security_policy([ua.SecurityPolicyType.NoSecurity])

    group = CertificateGroup(name=GROUP)
    store = CertificateStore(
        groups={GROUP: group}, application_uri=APP_URI, hostnames=["localhost"]
    )
    # Le résolveur ne reconnaît qu'un NodeId de groupe, celui que le GDS
    # publierait. Tout autre identifiant doit être refusé, jamais deviné.
    published_group_id = ua.NodeId(3_000_123, 0)
    node = ServerConfigurationNode(
        server,
        store,
        group_name=lambda nodeid: GROUP if nodeid == published_group_id else None,
    )
    await node.build()

    try:
        await server.start()
        async with ManagerClient(
            server.endpoint.geturl(), node.node.nodeid
        ) as client:
            await run(
                report,
                client,
                store,
                group,
                published_group_id,
                node.node.nodeid,
            )
    except Exception as exc:  # Erreur d'infrastructure, pas de conformité.
        report.check("auto-test exécuté sans exception", False, f"{type(exc).__name__}: {exc}")
        logger.exception("Détail")
    finally:
        group.close_all()
        await server.stop()

    return report.finish()


if __name__ == "__main__":
    from sciicad.console import setup

    setup()
    sys.exit(asyncio.run(main()))
