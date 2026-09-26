# Sécurité OPC UA

## Politiques de sécurité

Les simulateurs PLC (`thermo-plc/`, `protect-plc/`) exposent deux politiques :

| Policy                   | Mode           | Usage                          |
|--------------------------|----------------|--------------------------------|
| `http://…/SecurityPolicy#None` | `None`    | accès anonyme, non sécurisé    |
| `Basic256Sha256_SignAndEncrypt` | `SignAndEncrypt` | accès authentifié chiffré |

Le LDS (`lds/`) n'expose que `NoSecurity` — c'est la norme pour un serveur de
découverte.

### Méthodes d'authentification

Endpoints du PLC (via `GetEndpoints`) :

- `anonymous` — anonymous (policy None) ;
- `username` — utilisateur/mot de passe (Basic256Sha256) ;
- `certificate` — certificat client (Basic256Sha256).

Les clients asyncua actuels du dépôt utilisent l'accès anonyme
(`MessageSecurityMode.None`), ce qui suffit pour la simulation.

## Certificats serveur

Pour activer `Basic256Sha256_SignAndEncrypt`, chaque serveur charge :

- `thermo-plc/server_certificate.pem`
- `thermo-plc/server_private_key.pem`
- (idem pour `protect-plc/`)

Génération avec `uv run tools/crypto_opcua.py` :

```bash
uv run tools/crypto_opcua.py \
    --hostname thermo-plc \
    --application-uri urn:SCIICAD:thermo-plc \
    --output-dir thermo-plc
```

Le répertoire de sortie est créé s'il n'existe pas. **La clé privée est écrite
non chiffrée**, avec des permissions `0600` : ne jamais la committer, ni la
copier sur un support partagé. Les deux fichiers sont ignorés par `.gitignore`.

`--key-size` est borné à 2048..4096 bits et `--validity-days` doit être
positif ; toute autre valeur est refusée.

Le certificat inclut les extensions OPC UA requises : Subject Alternative
Name (URI d'application, DNS, IP, 127.0.0.1), KeyUsage, ExtendedKeyUsage
(serverAuth), BasicConstraints (non-CA).

> Si les fichiers sont absents, le serveur démarre quand même (warning) avec
> `NoSecurity` uniquement. Vérifier ces fichiers sur un PLC distant pour
> activer le chiffrement.

## Global Discovery Server (`gds/gds_server.py`)

Le GDS centralise la gestion des certificats (OPC UA Part 12) :

- il joue le rôle d'**autorité de certification** (CA) : émission,
  révocation, listes de confiance ;
- les applications s'enregistrent (`RegisterApplication`) et peuvent demander
  des certificats via CSR (`CreateCertificateRequest`), approbation
  (`ApproveCertificateRequest`), statut (`GetCertificateStatus`) ;
- stockage des certificats : `security.cert_store_path` dans
  `gds/gds_config.yaml` (par défaut `~/.opc-foundation/certificate-stores`) ;
- politique : authentification requise, longueur/politique de mot de passe,
  durée de session définissables en config.

Configuration clé (`gds_config.yaml`) :

```yaml
security:
  require_authentication: true
  key_size: 2048
  certificate_validity_days: 365
  session_timeout_hours: 24
```

Rôles gérés en base : `authenticated_user`, `security_admin`,
`configure_admin`, `discovery_admin`, `certificate_authority_admin`.

## Recommandations

- Ne jamais committer `server_private_key.pem` (ignorés via `.gitignore` /
  fichiers à générer localement).
- Pour un client sécurisé asyncua :

```python
from asyncua import Client

client = Client("opc.tcp://<host>:4840")
await client.set_security_string(
    "Basic256Sha256,SignAndEncrypt,client_cert.pem,client_key.pem"
)
await client.connect()
```

- Vérifier l'endpoint choisi avec `tools/analyze.py` et `tools/test_gds.py`
  (phases 2 et 3) après toute modification des certificats.