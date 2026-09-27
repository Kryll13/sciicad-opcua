# Outils `tools/`

Scripts utilitaires du projet. Ils s'exécutent via `uv run tools/<script>.py`
et s'appuient sur le paquet `sciicad/`, importable depuis n'importe quel
répertoire.

`tools/` ne contient que des scripts d'appel. Tout le code partagé — lecture
de l'espace d'adressage, identité d'application, configuration console,
connaissances du modèle, banc de test — vit dans `sciicad/`.

## Code partagé : `sciicad/`

| Module              | Rôle                                                              |
|---------------------|-------------------------------------------------------------------|
| `console.py`        | configuration de la sortie console d'un outil ; réduction du bruit des bibliothèques tierces |
| `nodes.py`          | lecture de l'espace d'adressage : `find_node_by_name`, `find_by_path`, `read_access`, `list_children`, `server_array` |
| `model.py`          | variables publiées par les simulateurs, seuils, unités            |
| `net.py`            | détection d'adresse, découplage écoute / annonce                  |
| `discovery.py`      | `LdsRegistrar` : enregistrement, renouvellement, retrait           |
| `lifecycle.py`      | signaux, arrêt coopératif, retrait du LDS                          |
| `identity.py`       | alignement de `ServerArray` (asyncua ne le fait pas)              |
| `cli.py`            | validation des arguments, groupes d'options PLC                    |
| `selftest.py`       | `Report`, `LdsHarness`, `temp_database`, `wait_for`               |

> `opcua_utils.py` a été supprimé : il mélangeait réseau, logging, analyse
> d'arguments et fabriques de nœuds, et configurait la racine des logs **au
> moment de l'import**, ce qui fuyait dans tout le processus. Son contenu
> utile est réparti ci-dessus.

## Outils de contrôle

### `ihm_action.py` — contrôle du thermostat

Interroge et commande le PLC thermostat (`Heating`, `MaintenanceMode`).

```bash
uv run tools/ihm_action.py --ip 193.168.1.90 --port 4840                 # état
uv run tools/ihm_action.py --ip 193.168.1.90 --heat on                   # chauffer
uv run tools/ihm_action.py --ip 193.168.1.90 --heat off
uv run tools/ihm_action.py --ip 193.168.1.90 --maintenance on|off
```

Nœuds résolus par **browse name** (`Thermostat`, `Heating`, …) — robuste
indépendamment de l'index de namespace.

### `analyze.py` — analyse d'un serveur

Parcourt l'espace d'adressage et affiche les variables, leur mode d'accès
(lecture/lecture-écriture) et leur valeur courante.

```bash
uv run tools/analyze.py -u opc.tcp://193.168.1.90:4840
```

## Outils de découverte / diagnostic

### `test_lds_discovery.py`

Interroge un LDS : serveurs enregistrés, endpoints, politiques de sécurité.

```bash
uv run tools/test_lds_discovery.py --url opc.tcp://193.168.1.20:4840
```

### `test_discovery.py` — chaîne de découverte de bout en bout

Vérifie successivement : les services de découverte du LDS
(`FindServers`, `FindServersOnNetwork`), la résolution de l'endpoint d'un PLC
**depuis le registre**, puis la lecture de son espace d'adressage. Aucune
hypothèse n'est faite sur le modèle de données du PLC : l'espace d'adressage
est parcouru et affiché tel quel.

```bash
uv run tools/test_discovery.py --lds-url opc.tcp://193.168.1.20:4840
uv run tools/test_discovery.py --lds-url opc.tcp://<lds>:4840 \
    --plc-uri urn:SCIICAD:protect-plc      # un autre PLC
uv run tools/test_discovery.py --lds-url opc.tcp://<lds>:4840 --skip-plc
```

Options : `--plc-uri`, `--skip-plc`, `--wait`, `--timeout`, `--depth`.
**Code de sortie non nul** si le LDS ne répond pas, si le PLC attendu est
absent du registre, ou si la lecture échoue.

### `check_lds_gds.py` — rôle d'un endpoint

Identifie un endpoint en interrogeant réellement les services de découverte,
et non en cherchant des nœuds : la découverte est un ensemble de *services*,
pas des nœuds de l'espace d'adressage.

> Une version antérieure cherchait `ns=0;i=11524` et consorts. Ce sont des
> NodeIds du **GDS** de la norme : un LDS ne les expose pas, et l'outil
> concluait à tort à une défaillance. Ces NodeIds ont été supprimés.

```bash
uv run tools/check_lds_gds.py --url opc.tcp://193.168.1.20:4840 --expect discovery
uv run tools/check_lds_gds.py --url opc.tcp://<ip>:4840 --expect server
```

Rapporte : identité (`ServerArray`), namespaces, `GetEndpoints`,
`FindServers` avec le contenu du registre, et `FindServersOnNetwork`
(extension du projet, absente des piles standard).

**Discrimination LDS / serveur ordinaire** : tout serveur doit se décrire
lui-même via `FindServers` (clause 5.5.2.1) — un PLC répond donc lui aussi.
Le critère retenu est le **registre** : un serveur de découverte renvoie des
entrées qui ne le décrivent pas lui-même. `ServerArray` n'est pas utilisé seul
pour ce critère, car une pile peut l'exposer obsolète.

**Code de sortie non nul** si l'endpoint est injoignable ou si le rôle
observé ne correspond pas à `--expect`.

## Certificats

### `crypto_opcua.py`

Génère un certificat auto-signé OPC UA et sa clé privée (compatibles
`Basic256Sha256_SignAndEncrypt`).

```bash
uv run tools/crypto_opcua.py --hostname thermo-plc --output-dir thermo-plc
```

Génère `server_certificate.pem` + `server_private_key.pem` (SAN : URI
d'application, DNS, IP, 127.0.0.1).

```bash
uv run tools/crypto_opcua.py \
    --hostname thermo-plc \
    --application-uri urn:SCIICAD:thermo-plc \
    --output-dir thermo-plc
```

Le répertoire de sortie est créé s'il n'existe pas, et la clé privée est écrite
en `0600` (elle est **non chiffrée**). `--key-size` est borné à 2048..4096 et
`--validity-days` doit être positif ; toute valeur hors plage est refusée avec
un code de sortie non nul. Les deux fichiers sont ignorés par git.

## Tests GDS

### `test_gds.py`

Tests autonomes du GDS, en 3 phases :

- phase 1 : méthodes cœur (applications : register/query/unregister) ;
- phase 2 : gestion des certificats (groupes, CA, CSR, statut, approbation) ;
- phase 3 : méthodes "pull" (trust lists, changements de certificats).

```bash
uv run tools/test_gds.py --url opc.tcp://<ip>:4840
uv run tools/test_gds.py --url opc.tcp://<ip>:4840 --phase 2   # une seule phase
uv run tools/test_gds.py --url opc.tcp://<ip>:4840 --username admin --password ...
```

L'URL n'est plus codée en dur dans le source. Les phases sont isolées : l'échec
de l'une n'empêche pas les suivantes, et le bilan indique le nombre de phases
en échec. **Code de sortie non nul** si le GDS est injoignable ou si une phase
échoue.

> Corrections appliquées : la phase 2 lisait `request_id` alors que la méthode
> renvoyait `requestId`, ce qui rendait ses étapes 4 à 6 silencieusement
> inopérantes. Cinq méthodes de la classe de base n'étaient jamais appelées et
> ont été retirées.

### `selftest_lds.py` — auto-test du LDS

Vérifie le LDS sur des ports éphémères, sans toucher au LDS de production :
enregistrement et persistance, restauration après redémarrage, expiration
d'une entrée dont le renouvellement a cessé, séparation écoute/annonce, et
pagination de `FindServersOnNetwork`. Code de sortie non nul en cas d'échec.

```bash
uv run tools/selftest_lds.py
```

### `selftest_thermo_lds.py` — enregistrement / retrait du PLC

Vérifie la procédure d'enregistrement du PLC auprès du LDS : enregistrement,
renouvellement périodique, retrait par `IsOnline = False`, retry progressif si
le LDS est indisponible, et absence de résurrection après redémarrage du LDS.

```bash
uv run tools/selftest_thermo_lds.py
```

### `selftest_gds.py` — conformité du GDS aux services de la Part 4

Interroge le GDS **par les NodeIds normatifs**, depuis un client OPC UA
ordinaire, et échoue si une réponse change de forme. C'est la garantie qui
manquait : l'ancien GDS annonçait ces services dans son journal sans répondre à
rien.

```bash
uv run tools/selftest_gds.py
```

Vérifié notamment : `FindServersOnNetwork` répond sans session (NodeId 12208) —
sans le patch, la requête tombe dans la branche « pas de session » d'asyncua et
reçoit `BadUserAccessDenied` —, `RegisterServer` rend un serveur découvrable
avec un `RecordId` croissant, le retrait par `IsOnline = False` évacue l'entrée
de la mémoire *et* de la base, et le registre restauré au démarrage fait foi
sans renouvellement.

### `selftest_trustlist.py` — liste de confiance du GDS (Part 12 §7.8)

Interroge un `CertificateGroupType` **par le réseau**, avec un client qui
parcourt l'espace d'adressage comme le ferait un vrai consommateur. Une méthode
annoncée mais non câblée échoue donc ici.

```bash
uv run tools/selftest_trustlist.py
```

Vérifié notamment : les dix méthodes `TrustList` répondent, `AddCertificate` est
idempotent, la lecture par blocs donne le même contenu que la lecture en un
bloc, un `Open` en lecture seule refuse `Write` avec `BadNotWritable`,
`OpenWithMasks` ne livre que les listes demandées, et un `FileHandle` inconnu
donne `BadInvalidArgument`.

### `selftest_thermo_lifecycle.py` — cycle de vie réel (SIGTERM)

Lance un PLC dans un vrai sous-processus face à un LDS, puis envoie `SIGTERM`
(ce que fera systemd) et vérifie que l'entrée disparaît de la découverte
**immédiatement**, sans attendre l'expiration.

```bash
uv run tools/selftest_thermo_lifecycle.py              # les deux PLC
uv run tools/selftest_thermo_lifecycle.py protect-plc  # un seul
```

## Fichiers supprimés

Ces trois scripts ont été retirés : leurs rôles sont couverts, et ils
étaient sources de pièges.

| Fichier supprimé  | Remplacé par                       | Motif                                                                 |
|-------------------|------------------------------------|-----------------------------------------------------------------------|
| `lds_minimal.py`  | `lds/` (paquet)                    | n'avait ni persistance ni expiration ; se substituait au LDS de production en liant `0.0.0.0` |
| `plc_basic.py`    | `protect-plc/plc_server.py`         | ~85 % identique à `plc_2_lds.py`, et **même `applicationUri`** : les lancer ensemble faisait collision dans le registre |
| `plc_2_lds.py`    | `protect-plc/plc_server.py`         | idem                                                                 |
