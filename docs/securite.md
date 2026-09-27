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

> **Le contrôle d'accès du §7.2 n'est pas implanté.** Les Tables 18 à 20
> définissent en prose les rôles `CertificateAuthorityAdmin`,
> `RegistrationAuthorityAdmin` et `SecurityAdmin`, sans NodeId : le modèle de
> rôles est celui de la Part 5, où chaque application définit les siens. Ce
> n'est donc pas une référence manquante mais un choix de déploiement. Tant
> qu'il n'est pas fait, toute session — y compris anonyme — peut écrire dans
> les listes de confiance et appeler `UpdateCertificate`. Voir
> [`serveurs.md`](serveurs.md#controle-dacces-non-implante-et-ce-nest-pas-une-reference-manquante).

La validation d'un certificat entrant applique le processus de la Part 4 et
n'accepte que si la chaîne de signature remonte à un certificat de confiance du
groupe — la liste `issuer_certificates` doit donc contenir l'autorité de
signature **avant** l'appel. À défaut, tout est refusé : c'est le seul
comportement sûr, accepter reviendrait à installer un certificat dont personne
n'a vérifié l'origine.

`GetRejectedList` ne liste pas tous les refus. §7.8.3.2 réserve cette liste aux
certificats « that have no unsuppressed validation errors but are not trusted » :
un certificat expiré, mal adressé ou illisible est refusé avec son code, mais
n'y figure pas. Y verser des défauts de validation brouillerait la liste, qui
sert à présenter des candidats à approuver, pas un journal d'erreurs.

Le modèle *Pull* d'autorité de certification (§7.9, `CertificateDirectoryType`)
n'est **pas** implémenté, et ne peut pas l'être conformément : ses NodeIds ne
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

## Journalisation

**loguru est la seule bibliothèque de journalisation du projet.** Aucune ligne de
`print()` ne sert de journal : la sortie standard est réservée à l'affichage
interactif — `ihm/ihm_client.py` rafraîchit une ligne pour un humain.

Deux `import logging` subsistent, dans `sciicad/console.py` et `lds/services.py`.
Ils ne sont pas des exceptions à la règle mais son **mécanisme** : ils
redirigent les journaux d'asyncua vers loguru, de sorte qu'une seule
configuration de sortie et un seul format s'appliquent à tout. Les retirer
ferait revenir les journaux de la pile vers le module `logging` de la
bibliothèque standard, et le GDS émettrait alors deux flux de formats
différents. Ce ne sont pas des lignes à « nettoyer ».

## Audit OPC UA et journal d'événements : deux choses distinctes

La configuration du GDS porte deux clés qui se ressemblent et ne répondent pas
à la même question.

| Clé                  | Mécanisme                         | Qui le voit                    |
|----------------------|-----------------------------------|--------------------------------|
| `database.event_log` | lignes dans une table SQLite     | l'administrateur, sur la machine |
| `audit.enabled`      | notification OPC UA, avec `EventType` | un client OPC UA **abonné**   |

Un journal d'événements répond à « qu'a fait le serveur ? ». Un événement
d'audit répond à « qu'est-il arrivé à ce certificat, à cette liste de
confiance ? », et ne parvient qu'aux clients qui se sont abonnementés. Confondre
les deux laisse croire qu'une traçabilité existe parce qu'il y a des lignes dans
un fichier : c'est faux pour tout client OPC UA, et c'est ce qui rendait le
premier état des lieux de cette couche trompeur.

Le GDS émet deux `ObjectType` d'audit, tous deux à leurs NodeIds publiés :

| `EventType`                             | NodeId | Émis quand                              |
|------------------------------------------|--------|-----------------------------------------|
| `TrustListUpdatedAuditEventType`        | 12561  | la liste de confiance a réellement changé |
| `CertificateUpdatedAuditEventType`      | 12620  | un certificat a été installé            |

§7.8.2.13 et §7.10.27 sont explicites sur un point que le test vérifie : un
`AddCertificate` **idempotent**, ou un `UpdateCertificate` **refusé**, ne
produisent aucun événement. Le premier réussit sans rien modifier, le second
échoue — « No Event is raised if the Method call fails. »

Une émission d'audit qui échoue est journalisée et n'interrompt pas l'opération.
C'est un effet de bord, jamais une condition : l'inverse rendrait l'audit capable
de refuser une écriture de liste de confiance, ce qui est pire que l'absence de
trace.

> Un défaut d'asyncua 1.1.8 a été contourné, sans modification de la pile :
> `get_event_obj_from_type_node` pose `EventType` par affectation directe au
> lieu de `add_property`, le type de la donnée n'est donc pas enregistré, et la
> notification se perd à la sérialisation — sans message pour le client. Le
> défaut est isolé : un `BaseEvent` nu, construit explicitement, est livré
> correctement. Voir la note de `gds/audit.py`.

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