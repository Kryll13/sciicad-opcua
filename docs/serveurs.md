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

| Fichier          | Rôle                                                    |
|------------------|---------------------------------------------------------|
| `__main__.py`    | point d'entrée en ligne de commande                     |
| `config.py`      | configuration YAML, valeurs par défaut du rôle GDS     |
| `server.py`      | `GlobalDiscoveryServer`, spécialisation de `DiscoveryServer` |
| `gds_config.yaml`| configuration                                          |

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

### `gds/gds_server.py` — prototype de la couche certificats

Ce fichier (2126 lignes) reste dans le dépôt mais **n'est plus le point
d'entrée**, et ses services ne sont pas exploitables tels quels. Il exposait
« FindServers », « FindServersOnNetwork », « RegisterServer » et
« RegisterServer2 » comme des nœuds `Method` à NodeIds inventés (1030-1033) avec
des entrées et sorties en `String` : aucun client OPC UA normatif ne les
appelle, un `FindServers` standard ne renvoyait que le GDS lui-même, et un
`FindServersOnNetwork` se faisait refuser par `BadUserAccessDenied`.

Sa couche certificats (registre SQLAlchemy, CA, groupes de confiance, audit)
n'a jamais été atteignable non plus. Elle reste la base de la Part 11/12, mais
son exposition normative reste à faire.

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
