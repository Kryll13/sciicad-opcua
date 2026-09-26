# OPCUA for SCIICAD

Simulateurs industriels OPC UA pour le projet SCIICAD.

Ce dépôt contient des serveurs OPC UA simulant des équipements industriels
(thermostat, système de protection), un serveur de découverte local (LDS),
un Global Discovery Server (GDS) ainsi que des clients et outils IHM.

Le LDS et le GDS implémentent tous deux les services de découverte de la Part 4
— `FindServers`, `FindServersOnNetwork`, `RegisterServer`, `RegisterServer2` —
qui manquent à la pile `asyncua`. Ils partagent le même code d'assemblage ; seul
le rôle diffère, par la portée du registre.

## Architecture

```
                     ┌─────────────────────────┐
   192.168/193.x ───▶│  LDS :4840              │  Découverte locale
   réseau local      │  (Local Discovery Server)│
                     └────────────┬────────────┘
                                  │ enregistrement
                    ┌─────────────┼──────────────────┐
                    │             │                  │
            ┌───────▼─────┐ ┌─────▼──────┐   ┌──────▼────────┐
            │ thermo-plc  │ │ protect-plc│   │  GDS :4840    │
            │ :4840       │ │ :4840      │   │ (Global       │
            │ thermostat  │ │ protection │   │  Discovery)   │
            └─────────────┘ └─────────────┘   └───────────────┘
                    │             │
                    └──────┬──────┘   LDS et GDS partagent le même code
                           ▼          d'assemblage : seule la portée du
                  ┌────────────────┐  registre diffère (« local » avec
                  │  IHM / clients │  expiration, « global » sans).
                  │ ihm_action.py  │  interroge/commande les PLC
                  │ ihm_client.py  │
                  └────────────────┘
```

Les simulateurs s'enregistrent auprès du LDS (`register_to_discovery`) au
démarrage et **se réenregistrent périodiquement** (toutes les 60 s par défaut).
Ce renouvellement est imposé par la portée « local » : une inscription non
renouvelée est évacuée après le TTL. Auprès d'un GDS, la portée « global »
conserve l'inscription jusqu'au retrait explicite, sans renouvellement.
L'IHM et les outils se connectent directement aux serveurs concernés.

> **À noter** : la norme OPC UA ne définit aucun service de désenregistrement
> d'un serveur auprès d'un LDS. Une entrée disparaît parce que le serveur
> cesse de se réenregistrer et que le LDS l'évince, pas parce qu'il se
> désinscrit. Voir [`docs/serveurs.md`](docs/serveurs.md#lds--local-discovery-server-lds).

## Composants

| Dossier        | Rôle                                              | Endpoint par défaut        |
|----------------|---------------------------------------------------|----------------------------|
| `lds/`         | Serveur de découverte local (LDS)                 | `opc.tcp://<ip>:4840`      |
| `gds/`         | Global Discovery Server (GDS), portée globale  | `:4840/GlobalDiscoveryServer` |
| `thermo-plc/`  | Simulateur PLC thermostat                          | `opc.tcp://<ip>:4840`      |
| `protect-plc/` | Simulateur PLC système de protection              | `opc.tcp://<ip>:4840`      |
| `ihm/`         | Client IHM (affichage continu du thermostat)      | `opc.tcp://thermo-plc:4840` |
| `sciicad/`     | Code partagé : identité, découverte, CLI, console  | —                          |
| `tools/`       | Scripts utilitaires (diagnostic, contrôle, tests) | —                          |

### Organisation du code

`lds/`, `thermo-plc/` et `protect-plc/` sont des composants autonomes.
`gds/` réutilise l'assemblage de `lds/` : les deux rôles ne diffèrent que par
la portée du registre et le chemin de l'endpoint, et n'ont donc aucun service
de découverte dupliqué. Tout ce qui est commun à plusieurs d'entre eux vit
dans le paquet `sciicad/` : identité d'application, enregistrement auprès d'un LDS, cycle
d'arrêt, arguments de ligne de commande, lecture d'espace d'adressage,
configuration console et banc de test.

Ce paquet est déclaré dans `pyproject.toml` (`[build-system]`) et installé par
`uv sync` : il est donc importable depuis n'importe quel répertoire, ce qui
permet aux scripts de `tools/` de l'utiliser sans manipuler `sys.path`.

`tools/` ne contient que des scripts d'appel.

## Installation

Prérequis : Python 3.13 et [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

Dépendances principales (définies dans `pyproject.toml`) :
`asyncua`, `cryptography`, `loguru`, `pydantic`, `pyyaml`, `sqlalchemy`.

`uv sync` installe ces dépendances **et** le projet lui-même, ce qui rend les
paquets `sciicad` et `lds` importables.

## Démarrage rapide

Le guide complet est [`docs/demarrage.md`](docs/demarrage.md).

```bash
uv sync
```

```bash
# 1. Serveur de découverte (une fois)
uv run python -m lds --advertise 193.168.1.20 --database /var/lib/sciicad/lds.db

# 2. Simulateur de régulation thermique
uv run thermo-plc/plc_server.py --lds opc.tcp://193.168.1.20:4840 --advertise 193.168.1.90

# 3. Simulateur de système de protection
uv run protect-plc/plc_server.py --lds opc.tcp://193.168.1.20:4840 --advertise 193.168.1.91
```

Puis interroger le thermostat :

```bash
uv run tools/ihm_action.py --ip 193.168.1.90 --port 4840                 # état
uv run tools/ihm_action.py --ip 193.168.1.90 --heat on                   # chauffer
uv run tools/ihm_action.py --ip 193.168.1.90 --heat off --maintenance on # maintenance
```

## Documentation technique

- [`docs/demarrage.md`](docs/demarrage.md) — **commandes de démarrage** (LDS, GDS, thermo-plc, protect-plc)
- [`docs/architecture.md`](docs/architecture.md) — topologie opcua, flux, adressage
- [`docs/adressage.md`](docs/adressage.md) — plan d'adressage et de nommage des équipements
- [`docs/serveurs.md`](docs/serveurs.md) — LDS, GDS, thermo-plc, protect-plc, IHM
- [`docs/outils.md`](docs/outils.md) — inventaire et usage des outils `tools/`
- [`docs/securite.md`](docs/securite.md) — certificats, Basic256Sha256, GDS
- [`docs/depannage.md`](docs/depannage.md) — problèmes connus et résolution

## Docker

**Incomplet.** Le `Dockerfile` de `lds/` a été remis à jour (base
`python:3.13-slim`, dépendances via `uv`), contexte de build à la racine :

```bash
docker build -t sciicad-lds -f lds/Dockerfile .
```

Les `Dockerfile` de `gds/`, `ihm/`, `thermo-plc/` et `protect-plc/` sont
**cassés** : ils copient un `requirements.txt` qui n'existe pas dans le dépôt,
le `docker build` échoue. Le détail figure dans
[`docs/serveurs.md`](docs/serveurs.md#containerisation).

Aucun build n'a pu être exécuté lors de la rédaction (Docker indisponible) :
même le Dockerfile du LDS reste **non vérifié**.

Licence : voir [LICENSE](LICENSE).
