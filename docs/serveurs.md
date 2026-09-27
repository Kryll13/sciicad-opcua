# Composants serveurs

Chaque composant est un dossier autonome. Tous partagent le port OPC UA 4840
et s'appuient sur `asyncua`.

## LDS — Local Discovery Server (`lds/`)

Paquet Python (`uv run python -m lds`), application `urn:SCIICAD:lds`.

| Fichier          | Rôle                                                     |
|------------------|----------------------------------------------------------|
| `__main__.py`    | point d'entrée en ligne de commande                      |
| `config.py`      | configuration YAML (`lds_config.yaml`)                   |
| `server.py`      | assemblage, cycle de vie, tâche de balayage              |
| `registry.py`    | état du registre, expiration, `FindServersOnNetwork`     |
| `store.py`       | persistance SQLite du registre                           |
| `services.py`    | services ajoutés au serveur asyncua                      |

- Endpoint : écoute sur `bind_address` (défaut `0.0.0.0:4840`), annonce
  `advertise_host` (défaut : hostname). Les deux sont découplés, ce qui évite
  les échecs « could not bind on any address » quand la résolution DNS du
  hostname ne correspond à aucune interface locale.
- `server.discovery_server_flag = True` active `FindServers`, `GetEndpoints`,
  `RegisterServer` et `RegisterServer2`, fournis nativement par asyncua.
- `FindServersOnNetwork` **n'est pas** routé par asyncua : il est ajouté par
  `services.py`, qui enveloppe `UaProcessor._process_message`. C'est le seul
  service de la Part 4 qui manquait.
- Politique de sécurité : `NoSecurity` uniquement, comme la norme le prévoit
  pour un serveur de découverte.

Lancement (détail dans [`demarrage.md`](demarrage.md)) :

```bash
uv run python -m lds
uv run python -m lds --port 4840 --advertise 193.168.1.20 \
                   --database /var/lib/sciicad/lds.db
```

Auto-test (registre, persistance, expiration, pagination, sans toucher au LDS
de production) :

```bash
uv run tools/selftest_lds.py
```

### Persistance et expiration

Le registre est persisté dans SQLite (`lds.db` par défaut) et survit au
redémarrage du LDS. Au redémarrage, les entrées sont rechargées puis
soumises au TTL : elles ne sont pas « ressuscitées ».

**La norme OPC UA ne définit pas de service de désenregistrement** d'un serveur
auprès d'un LDS (`UnregisterServer` n'existe pas dans la spécification). Le
mécanisme normal est le renouvellement : un serveur se réenregistre au plus
tard toutes les 10 minutes, et le LDS évince les entrées qui cessent de
renouveler. D'où `discovery.entry_ttl_seconds` (300 s par défaut, soit 5× le
rythme de 60 s utilisé par les PLC du projet).

Conséquence pratique : un arrêt brutal d'un PLC laisse son entrée visible
jusqu'à l'expiration, puis elle disparaît. Un redémarrage du LDS vide
immédiatement le registre (les serveurs se réenregistrent ensuite d'eux-mêmes).

## GDS — Global Discovery Server (`gds/`)

Paquet Python (`uv run python -m gds`), application `urn:SCIICAD:gds`. Le GDS
réutilise l'assemblage du LDS : mêmes services de la Part 4, même persistance,
aucune duplication.

| Fichier              | Rôle                                                          |
|----------------------|---------------------------------------------------------------|
| `__main__.py`        | point d'entrée en ligne de commande                           |
| `config.py`          | configuration YAML, valeurs par défaut du rôle GDS           |
| `server.py`          | `GlobalDiscoveryServer`, spécialisation de `DiscoveryServer`   |
| `trustlist.py`       | liste de confiance : 4 listes, modèle fichier, empreintes      |
| `certificategroup.py`| publication du `CertificateGroupType` et câblage des méthodes   |
| `gds_config.yaml`    | configuration                                                |

### Ce qui distingue le GDS du LDS

Un seul champ : `discovery.scope`.

| Portée  | Rôle | Expiration | Renouvellement | Registre restauré |
|---------|------|------------|----------------|-------------------|
| `local` | LDS  | oui, 300 s | imposé (~60 s) | soumis au TTL |
| `global`| GDS  | **aucune** | **non imposé** | **fait foi** |

L'endpoint porte en plus le chemin `/GlobalDiscoveryServer`, fixé par la Part 12
et imposé par `gds/config.py` : un client distingue ainsi les deux rôles sur la
seule URL, y compris s'ils écoutent tous deux sur 4840.

Lancement :

```bash
uv run python -m gds --bind 0.0.0.0 --advertise <hôte> --database gds.db
```

`--ttl` n'est pas proposé : en portée globale il n'aurait aucun effet.

### Groupes de certificats (Part 12 §7.8)

C'est la partie qui distingue un GDS d'un LDS. Les groupes vivent à l'endroit que
la norme leur réserve (§7.8.3.3), sous l'objet de configuration du serveur :

```
Server
 └─ ServerConfiguration                  i=12637  ServerConfigurationType
     └─ CertificateGroups                i=14053  CertificateGroupFolderType
         ├─ DefaultApplicationGroup      i=14156  CertificateGroupType
         │   ├─ TrustList                i=12642  TrustListType
         │   └─ CertificateTypes         i=14161
         ├─ DefaultHttpsGroup            i=14088
         └─ DefaultUserTokenGroup        i=14122
```

asyncua construit déjà toute cette arborescence, **avec les NodeIds d'instance
publiés par la norme**. Le GDS s'y rattache ; il ne crée pas de groupe. La
première version du code en créait un, directement sous le nœud `Server` : le
groupe norms restait alors sans gestionnaire, et un client qui suivait le chemin
normal obtenait `BadNothingToDo` — pendant que le groupe réellement servi,
lui, était hors d'atteinte. C'est invisible pour un test qui vise un NodeId ;
`tools/selftest_gds.py` vérifie désormais le chemin parcouru, et l'absence de
tout groupe hors du dossier.

Les trois groupes sont rattachés par défaut (`certificates.groups`), et c'est
délibéré : un groupe laissé hors de la liste reste dans l'espace d'adressage
sans gestionnaire. Un groupe vide mais câblé se constate et s'explique ; un nœud
muet se découvre par l'échec d'un appel.

La propriété `CertificateTypes` est **obligatoire** (§7.8.3.1) et décrit ce que
le groupe admet : `RsaMinApplicationCertificateType` et
`RsaSha256ApplicationCertificateType` pour le groupe d'application,
`HttpsCertificateType` pour celui de HTTPS, `UserCertificateType` (i=19323) pour
celui des jetons d'utilisateur. Elle ne déclare que ce que le GDS sait produire :
annoncer une courbe ECC alors que la génération de clé est limitée à RSA
ferait échouer `CreateSigningRequest` sur un type que le groupe autorise.

La liste de confiance suit le **modèle fichier** de la norme, et non un échange
direct de `TrustListDataType` : le client ouvre la liste, obtient un
`FileHandle`, puis lit ou écrit par morceaux à une position courante. Le contenu
transité est un `TrustListDataType` binaire, dont la structure est normativisée
par la pile.

| Méthode                  | Effet                                                    |
|--------------------------|----------------------------------------------------------|
| `Open`                   | ouvre et retourne un `FileHandle` ; incrémente `OpenCount` |
| `Read` / `Write`         | lecture / écriture à la position courante                |
| `GetPosition` / `SetPosition` | déplacement dans le fichier                        |
| `OpenWithMasks`          | ouvre en ne chargeant que les listes demandées           |
| `CloseAndUpdate`         | publie le contenu si l'ouverture a été modifiée         |
| `AddCertificate`         | ajoute un certificat DER, par empreinte                 |
| `RemoveCertificate`      | retire par empreinte SHA-1                               |

Cinq propriétés sont **obligatoires** : `Size`, `Writable`, `UserWritable` et
`OpenCount` viennent de `FileType` (Part 20), `LastUpdateTime` est ajoutée par
§7.8.3.1. Elles reflètent l'état réel et sont republiées après chaque méthode
servie — une poignée n'existant que si un `Open` est passé par ce nœud, la
valeur est donc exacte sans tâche de fond.

| Propriété        | Valeur                                    |
|------------------|-------------------------------------------|
| `Size`           | `Bad_NotSupported` — §7.8.2.1 renvoie à la Part 20, qui l'impose quand la taille n'a pas de sens. Un `0` ou un `None` mentirait sur un contenu qui, lui, se lit très bien. |
| `Writable`       | `True`                                    |
| `UserWritable`   | `True` — ne peut pas être plus restrictif tant que le §7.2 n'est pas implanté (voir plus bas) |
| `OpenCount`      | nombre de poignées valides                |
| `LastUpdateTime` | `DateTime.MinValue` tant que la liste n'a pas bougé — §7.8.3.1 l'exige après un redémarrage, cette liste vivant en mémoire et repartant vide |

`UpdateFrequency`, `ActivityTimeout` et `DefaultValidationOptions` sont
optionnelles et ne sont pas publiées. `DefaultValidationOptions` est pourtant la
pièce qui manque le plus : ses sept bits (`SuppressCertificateExpired`,
`CheckRevocationStatusOnline`…) sont le levier prévu pour la révocation.

Deux règles de sûreté, vérifiées par `tools/selftest_trustlist.py` :

- `Open` en lecture seule refuse `Write` avec `BadNotWritable`, et
  `OpenWithMasks` n'ouvre jamais en écriture. Un client qui veut lire ne peut
  donc pas réécrire la liste de confiance.
- `OpenCount` retombe à zéro après `Close`. La norme expose ce compteur pour
  qu'une ouverture abandonnée — un client qui disparaît sans fermer — soit
  visible de l'administrateur.

`AddCertificate` est idempotent : l'appartenance est déduite de l'empreinte du
certificat, pas d'une table d'index, donc ré-ajouter ne crée pas de doublon.

### Rôle CertificateManager (Part 12 §7.10)

Le GDS tient en outre le rôle de *CertificateManager* (§7.1) : il prépare une
demande de signature et installe le certificat signé. Il **n'est pas** une autorité
de certification et ne détient aucune clé de CA — la signature est faite par une
autorité d'enregistrement extérieure, comme le suppose §7.10.5 en décrivant le
certificat reçu comme étant signé et non produit par le serveur.

L'objet `ServerConfiguration` est déjà publié par asyncua au NodeId normatif
i=12637 ; le GDS s'y **rattache** plutôt que d'en créer un second, faute de quoi
un client parcourant l'espace d'adressage trouverait deux objets de même browse
name, dont le premier répondrait `BadNothingToDo`.

| Méthode                        | Effet                                                     |
|--------------------------------|-----------------------------------------------------------|
| `CreateSigningRequest`         | produit une PKCS #10 DER et retient la clé privée         |
| `UpdateCertificate`            | valide puis installe le certificat signé                   |
| `GetRejectedList`              | restitue les certificats **valides mais non approuvés**   |

La validation suit le processus de la Part 4 — validité, `BasicConstraints`,
`KeyUsage`, URI d'application dans le SAN, et surtout chaîne de signature — et
chaque refus porte son `StatusCode` normatif :

| Cas                                   | `StatusCode`              | Versé à `GetRejectedList` |
|---------------------------------------|---------------------------|---------------------------|
| `Nonce` de moins de 32 octets         | `BadInvalidArgument`      | non                       |
| certificat expiré ou pas encore valide | `BadCertificateTimeInvalid` | non                    |
| URI d'application absente du SAN       | `BadCertificateUriInvalid` | non                       |
| autorité de signature non approuvée    | `BadCertificateUntrusted` | **oui**                   |
| DER illisible, clé privée incohérente | `BadCertificateInvalid`   | non                       |
| `CertificateGroupId` inconnu          | `BadNodeIdUnknown`        | non                       |

La dernière colonne est la sémantique de §7.8.3.2 : *« Servers only add
Certificates to this list that have no unsuppressed validation errors but are
not trusted. »* Seul un certificat **valide mais non approuvé** y figure. Un
certificat expiré ou mal adressé est un défaut, pas un candidat à approuver : le
client a mieux à faire que de le retrouver dans une liste à approuver. Il est
donc refusé, avec son code, sans être enregistré.

La chaîne d'émetteurs fournie avec `UpdateCertificate` est versée dans la liste
`issuer_certificates` du groupe, comme l'exige §7.10.5 : la validation suppose que
les certificats d'émetteur figurent **déjà** dans la liste de confiance, faute de
quoi elle ne pourrait aboutir.

Un `CertificateGroupId` nul désigne le `DefaultApplicationGroup`. Tout autre
NodeId qui ne correspond à aucun groupe publié est **refusé** et non redirigé
vers le groupe par défaut : un certificat déplacé à l'insu du client serait
piresque.

Désactivable par `certificates.manage_certificates: false`, pour un déploiement
qui distribue des listes de confiance sans gérer de certificats.

`ApplyChanges`, `CancelChanges`, `ResetToServerDefaults` et `GetCertificates`
restent des méthodes générées **sans gestionnaire**, et répondent donc
`BadNothingToDo`. Elles appartiennent au modèle transactionnel du §7.10 ;
ce GDS applique ses changements immédiatement. Les câbler à vide donnerait un
`Good` trompeur — pire que le refus.

Vérifié par `tools/selftest_certmanager.py` (34 vérifications) et, pour
l'emplacement des groupes, par `tools/selftest_gds.py` (35 vérifications).

### `gds/gds_server.py` — prototype non exposé

Ce fichier (2126 lignes) reste dans le dépôt mais **n'est plus le point
d'entrée**, et ses services ne sont pas exploitables tels quels. Il exposait
« FindServers », « FindServersOnNetwork », « RegisterServer » et
« RegisterServer2 » comme des nœuds `Method` à NodeIds inventés (1030-1033) avec
des entrées et sorties en `String` : aucun client OPC UA normatif ne les
appelle, un `FindServers` standard ne renvoyait que le GDS lui-même, et un
`FindServersOnNetwork` se faisait refuser par `BadUserAccessDenied`.

Sa couche certificats (registre SQLAlchemy, CA, groupes de confiance, audit)
n'a jamais été atteignable non plus. Elle reste la base de la Part 11/12 : en
revanche, la gestion des listes de confiance est désormais exposée
conformément, via `gds/trustlist.py` et `gds/certificategroup.py`, et le rôle de
*CertificateManager* via `gds/certstore.py` et `gds/serverconfiguration.py`.

Ce qui reste hors de portée, et pour une raison qui n'est pas un choix : la
 Part 12 définit en §7.9 un modèle *Pull* d'autorité de certification, autour
d'un `CertificateDirectoryType` et de ses neuf méthodes
(`StartSigningRequest`, `FinishRequest`, `GetTrustList`…). **Ces NodeIds ne sont
pas publiés** — ils sont absents du `NodeIds.csv` de la Fondation OPC elle-même,
pas seulement d'asyncua. Les implémenter demanderait donc d'inventer des NodeIds,
c'est-à-dire de reproduire exactement le défaut reproché plus haut à ce
prototype. Le modèle *Push* du §7.10, lui, est intégralement normé et c'est lui
qui est implémenté.

Reste donc à faire, si le besoin se présente : la distribution des CRL, et le
modèle transactionnel du §7.10 (`ApplyChanges`, `ResetToServerDefaults`).

Le §7.10 est lui-même **partiellement inimplementable** :
`CreateSelfSignedCertificate` (§7.10.6) et `DeleteCertificate` (§7.10.7) sont
absents du `NodeIds.csv` officiel aussi bien que d'asyncua — même cas que le
§7.9, pour la même raison. Sept des onze méthodes de `ServerConfigurationType`
sont donc atteignables ; quatre restent hors de portée normative.

### Contrôle d'accès — non implanté, et ce n'est pas une référence manquante

Le §7.2 exige qu'une écriture exige un rôle — `CertificateAuthorityAdmin`,
`RegistrationAuthorityAdmin`, `SecurityAdmin` — et §7.10.5 comme §7.8.3.2
répètent que ces méthodes « shall be called from an encrypted SecureChannel and
from a Client that has access to the SecurityAdmin Role ». **Rien de tout cela
n'est en place** : le GDS tourne en `NoSecurity` et n'a aucun modèle de rôles.

Ce n'est pas une information à aller chercher. Les Tables 18, 19 et 20 du §7.2
définissent ces rôles **en prose**, sans NodeId, et `SecurityAdmin` est absent du
`NodeIds.csv` : le modèle de rôles est celui de la Part 5, où chaque
application définit les siens — le nœud `RoleType` n'a pour enfants que
`ApplicationsExclude` et `EndpointsExclude`. Décider quels rôles ce GDS offre,
et les faire vérifier, est un choix de déploiement.

Tant que ce n'est pas fait, `UserWritable` vaut ce que vaut `Writable` :
l'annoncer `False` serait faux.

## thermo-plc — PLC Thermostat (`thermo-plc/`)

`plc_server.py` — simulation de régulation thermique.

- Application : `urn:SCIICAD:thermo-plc` ; serveur = « SCIICAD PLC Thermostat Server ».
- Endpoint : `opc.tcp://<ip>:<port>` (`--port`, défaut 4840).
- Enregistrement LDS : `--lds opc.tcp://<lds>:4840` (défaut `lds:4840`),
  renouvellement toutes les 60 s, retrait de l'entrée à l'arrêt
  (`Ctrl+C` ou `SIGTERM`) via `sciicad.discovery.LdsRegistrar`.
- Sécurité : `NoSecurity` + `Basic256Sha256_SignAndEncrypt` (certificat
  `thermo-plc/server_certificate.pem` + clé `server_private_key.pem` si
  présents — voir [`docs/securite.md`](securite.md)).

### Espace d'adressage

```
Objects
└── Thermostat (Object)
    ├── Heating         (Boolean, lecture/écriture)  — commande chauffage
    ├── Temperature     (Float, lecture seulement)    — °C
    ├── HighTempAlarm   (Boolean, lecture seulement)  — T > 25 °C
    ├── LowTempAlarm    (Boolean, lecture seulement)  — T < 15 °C
    └── MaintenanceMode (Boolean, lecture/écriture)   — mode maintenance
```

> Adressage réel observé : namespace `urn:SCIICAD:thermo-plc` à l'index **1**,
> identifiants **2001–2006**. Résoudre par browse name — cf.
> [`docs/architecture.md`](architecture.md).

Simulation : boucle asynchrone (~1 s). Si `Heating=ON`, la température monte
aléatoirement (+0.1 à +0.5 °C) sinon elle descend (−0.1 à −0.3 °C), bornée à
[10, 30] °C. Les alarmes sont recalculées à chaque itération. En mode
maintenance, la simulation est en pause.

## protect-plc — PLC Protection (`protect-plc/`)

`plc_server.py` — simulation du système de protection d'une installation.

- Application : `urn:SCIICAD:protect-plc` ; serveur = « SCIICAD PLC Protect Server ».
- Endpoint : `opc.tcp://<ip>:<port>`, enregistrement LDS (`--lds`), sécurité
  identiques au thermo-plc.
- **Statut : stub.** Le simulateur ne publie que l'état du mode maintenance et
  ne modélise aucun processus de protection (seuils, alarmes, déclenchements).

### Espace d'adressage

```
Objects
└── Protection (Object)
    └── MaintenanceMode (Boolean, lecture/écriture)
```

Lorsque `MaintenanceMode=ON`, la boucle de protection est suspendue.

## Code partagé — `sciicad/`

Paquet installé par `uv sync` (voir `[build-system]` dans `pyproject.toml`),
donc importable depuis n'importe quel répertoire. Il regroupe ce qui était
dupliqué entre les deux simulateurs.

| Module               | Contenu                                                        |
|----------------------|----------------------------------------------------------------|
| `sciicad/net.py`     | `get_host_info`, `resolve_endpoints` (écoute / annonce découplées) |
| `sciicad/discovery.py` | `LdsRegistrar` : enregistrement, renouvellement, retrait `IsOnline=False` |
| `sciicad/lifecycle.py` | `install_signal_handlers`, `run_until_stopped`, `withdraw_from_lds` |
| `sciicad/cli.py`     | validation `--port` / `--lds`, `add_plc_arguments`              |

Chaque simulateur ne conserve que ce qui lui est propre : son modèle
d'adressage, sa simulation et son `main()`.

## IHM (`ihm/`)

`ihm_client.py` — client IHM d'affichage continu (rafraîchissement 500 ms).

- Connexion : `--host` (défaut `thermo-plc`) / `--port` (défaut 4840).
- Affiche `T`, état chauffage, maintenance et alarmes ; quitte sur `Ctrl+C`
  comme sur `SIGTERM`.
- Résout le nœud `Thermostat` et ses variables par **browse name**
  (indépendant du namespace), via `sciicad/nodes.py`.
- **Code de sortie non nul** si la connexion échoue.

```bash
uv run ihm/ihm_client.py --host 193.168.1.90 --port 4840
```

> L'affichage suppose un terminal : il utilise `\r` sans saut de ligne.
> Redirigé vers un fichier, la sortie est bufferisée — utiliser
> `PYTHONUNBUFFERED=1` pour la journaliser.

## Containerisation

**État actuel : incomplet.** Seul le `Dockerfile` du LDS a été remis à jour ;
les quatre autres sont encore cassés. Aucun build n'a pu être exécuté (Docker
absent de l'environnement de développement), donc même le Dockerfile du LDS
n'est **pas vérifié**.

### `lds/Dockerfile` — à jour, non vérifié

Base `python:3.13-slim`, dépendances résolues par `uv` à partir de
`pyproject.toml` + `uv.lock`. Le contexte de build est la **racine** du dépôt,
car `pyproject.toml` s'y trouve :

```bash
docker build -t sciicad-lds -f lds/Dockerfile .
docker run --rm -p 4840:4840 -v lds-data:/data sciicad-lds
```

Le registre est monté sur `/data` (`VOLUME`), sinon la persistance serait
perdue au recréation du conteneur.

### `gds/`, `ihm/`, `thermo-plc/`, `protect-plc/` — cassés

Ces quatre `Dockerfile` font `COPY requirements.txt .` alors qu'**aucun
`requirements.txt` n'existe** dans le dépôt : `docker build` échoue
immédiatement. Ils présentent en outre d'autres défauts :

| Défaut                                              | Fichiers concernés              |
|-----------------------------------------------------|----------------------------------|
| `requirements.txt` inexistant                        | les 4                           |
| base `python:3.11-slim` contre `requires-python >=3.13` | les 4                       |
| `uv` installé puis jamais utilisé                    | les 4                           |
| toolchain `gcc`/`g++` conservé dans l'image finale   | les 4                           |
| aucun `USER` : exécution en root                     | les 4                           |
| chemin de certificat relatif non résolvable en conteneur (`thermo-plc/server_certificate.pem` alors que seul le script est copié) | `thermo-plc`, `protect-plc` |

Remise à niveau conseillée, sur le modèle du `Dockerfile` du LDS : base
`python:3.13-slim`, `uv sync --frozen --no-install-project --no-dev` depuis la
racine, `COPY` du seul composant, et un `USER` non privilégié.

### Compilation des images

Aucune image ne peut être construite avant correction des quatre
`Dockerfile`. En attendant, le déploiement s'appuie sur `uv` directement (voir
[`demarrage.md`](demarrage.md)).
