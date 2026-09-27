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

## Global Discovery Server — couche certificats, non exposée

`gds/gds_server.py` contient une couche de gestion des certificats Part 11/12 :
autorité de certification (émission, révocation, listes de confiance),
enregistrement des applications, demandes de certificats par CSR, approbation,
groupes de confiance, audit, et gestion des rôles en base
(`authenticated_user`, `security_admin`, `configure_admin`, `discovery_admin`,
`certificate_authority_admin`).

**L'émission de certificats n'est pas encore atteignable.** Les services de
`gds_server.py` sont déclarés comme nœuds `Method` à NodeIds non normatifs, avec
des entrées et sorties en `String` : aucun client OPC UA normatif ne les appelle.
Ce n'est pas une réserve de forme, c'est un fait mesuré — un client standard
interrogeant ce serveur n'atteint aucun de ces gestionnaires.

**En revanche, la gestion des listes de confiance est exposée conformément.**
Elle suit le modèle fichier de la Part 12 §7.8.2, sous les NodeIds normatifs du
`CertificateGroupType` (i=12555 et suivants) : voir
[`serveurs.md`](serveurs.md#groupes-de-certificats-part-12-78). Un GDS publie
donc déjà ses groupes de certificats, et un client peut lire et modifier leurs
listes de confiance par `Open`/`Read`/`Write`/`AddCertificate`.

**Le rôle de *CertificateManager* l'est aussi** (§7.10, modèle *Push*) : l'objet
`ServerConfiguration` est publié au NodeId normatif i=12637, et
`CreateSigningRequest`, `UpdateCertificate` et `GetRejectedList` y sont câblées.
Voir [`serveurs.md`](serveurs.md#role-certificatemanager-part-12-710).

> **Le GDS n'est pas une autorité de certification et n'en tient pas le rôle.**
> Il prépare une demande de signature et installe le certificat *signé par une
> autorité extérieure* ; il ne détient aucune clé de CA et ne peut donc pas
> signer lui-même. C'est ce que prescrit §7.10.5, qui décrit le certificat reçu
> comme signé et non produit par le serveur.

La validation d'un certificat entrant applique le processus de la Part 4 et
n'accepte que si la chaîne de signature remonte à un certificat de confiance du
groupe — la liste `issuer_certificates` doit donc contenir l'autorité de
signature **avant** l'appel. À défaut, tout est refusé : c'est le seul
comportement sûr, accepter reviendrait à installer un certificat dont personne
n'a vérifié l'origine.

Le modèle *Pull* d'autorité de certification (§7.9, `CertificateDirectoryType`)
n'est **pas** implémenté, et ne peut pas l'êtreconformément : ses NodeIds ne
sont pas publiés par la Fondation OPC. Voir
[`serveurs.md`](serveurs.md#gdsgds_serverpy-prototype-non-expose).

Ce qui reste à faire : la distribution des CRL, et le modèle transactionnel du
§7.10.

Configuration prévue (`gds/gds_config.yaml` du prototype) :

```yaml
security:
  require_authentication: true
  key_size: 2048
  certificate_validity_days: 365
  session_timeout_hours: 24
```

> Le `gds_config.yaml` effectivement livré décrit le **GDS de découverte**, et
> non cette couche : `scope: global`, registre SQLite, pas de `security:`.
> Les deux fichiers portent le même nom ; le point d'entrée `python -m gds` ne
> lit pas les clés de certificat.

### Portée de la découverte

Le GDS de découverte reste en `NoSecurity` uniquement, comme le LDS : la
découverte précède l'établissement d'un canal sécurisé, et la Part 4 impose
que ces services n'exigent pas la sécurité des messages. La gestion des
certificats est donc une couche distincte, au-dessus.

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