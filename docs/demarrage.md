# Démarrage des composants

Ce document donne les commandes exactes pour lancer le serveur de découverte
et les deux simulateurs, avec `uv`. Toutes ont été exécutées et vérifiées.

> **Prérequis** : Python 3.13 (géré par `.python-version`) et
> [uv](https://docs.astral.sh/uv/).

## Mise en place

```bash
git clone <url-du-dépôt> sciicad-opcua
cd sciicad-opcua
uv sync
```

`uv sync` installe les dépendances **et** le projet lui-même, ce qui rend les
paquets `lds` et `sciicad` importables depuis n'importe quel répertoire. Sans
cela, `from sciicad... import ...` échoue dans les simulateurs.

---

## 1. Le LDS — serveur de découverte local

```bash
uv run python -m lds
```

Sans `--config`, le fichier de configuration est cherché dans le répertoire
courant, puis dans `lds/`. L'origine retenue et les valeurs effectives sont
affichées au démarrage : une configuration ignorée ne peut donc pas passer
inaperçue. Un `--config` explicite et absent est une erreur (code 2).

Démarrage avec des options explicites (recommandé sur une VM) :

```bash
uv run python -m lds \
  --bind 0.0.0.0 \
  --advertise 193.168.1.20 \
  --database /var/lib/sciicad/lds.db
```

| Option            | Défaut          | Rôle                                                     |
|-------------------|-----------------|----------------------------------------------------------|
| `--config`        | `lds_config.yaml` | fichier de configuration YAML                          |
| `--port`          | `4840`          | port d'écoute                                            |
| `--bind`          | `0.0.0.0`       | adresse d'écoute (toutes interfaces)                     |
| `--advertise`     | hostname        | hôte annoncé aux clients — **à renseigner en VM**        |
| `--ttl`           | `300`           | durée de vie d'une entrée sans renouvellement (secondes) |
| `--database`      | `lds.db`        | chemin de la base SQLite                                 |
| `--no-database`   | —               | registre en mémoire seule, sans persistance              |

**`--advertise` est l'option qui compte en déploiement.** Le LDS annonce par
défaut le hostname de la machine : si les clients ne le résolvent pas, la
découverte renvoie une URL injoignable. Renseignez l'IP joignable.

**Sur la base de données** : placez-la hors du dépôt (`/var/lib/...`) pour
qu'elle survive à un redéploiement. `lds.db` est ignoré par git.

Vérification :

```bash
uv run tools/test_lds_discovery.py --url opc.tcp://193.168.1.20:4840
```

---

## 2. THERMO-PLC — simulateur de régulation thermique

```bash
uv run thermo-plc/plc_server.py --lds opc.tcp://193.168.1.20:4840
```

En VM, en annonçant une IP joignable :

```bash
uv run thermo-plc/plc_server.py \
  --lds opc.tcp://193.168.1.20:4840 \
  --advertise 193.168.1.90
```

Espace d'adressage publié :

| Nœud                                | Type    | Accès       |
|-------------------------------------|---------|-------------|
| `Thermostat.Heating`                | Boolean | lecture/écriture |
| `Thermostat.Temperature`            | Float   | lecture seule   |
| `Thermostat.HighTempAlarm`          | Boolean | lecture seule   |
| `Thermostat.LowTempAlarm`           | Boolean | lecture seule   |
| `Thermostat.MaintenanceMode`        | Boolean | lecture/écriture |

---

## 3. PROTECT-PLC — simulateur de système de protection

```bash
uv run protect-plc/plc_server.py --lds opc.tcp://193.168.1.20:4840
```

Espace d'adressage publié :

| Nœud                          | Type    | Accès            |
|-------------------------------|---------|------------------|
| `Protection.MaintenanceMode`  | Boolean | lecture/écriture |

> Ce simulateur est un **stub** : il ne publie que l'état du mode
> maintenance et ne modélise aucun processus de protection.

---

## 4. Le GDS — serveur de découverte global

```bash
uv run python -m gds --bind 0.0.0.0 --advertise <hôte> --database gds.db
```

Le GDS implémente les mêmes services de découverte que le LDS. Il n'en diffère
que par la portée du registre :

| Portée   | Expiration | Renouvellement | Registre restauré |
|----------|------------|----------------|-------------------|
| `local`  | 300 s      | imposé (~60 s) | soumis au TTL   |
| `global` | **aucune** | **non imposé** | **fait foi**     |

Un serveur inscrit auprès d'un GDS n'a donc **pas** à se réenregistrer : son
inscription vaut jusqu'à son retrait explicite. C'est utile pour un
équipement piloté par une application de supervision, qui ne peut pas se
réinscrire toutes les 60 s.

L'endpoint porte le chemin `/GlobalDiscoveryServer`, imposé par la Part 12 :

```
opc.tcp://<hôte>:4840/GlobalDiscoveryServer
```

Ce chemin est ce qui permet à un client de distinguer un GDS d'un LDS quand les
deux écoutent sur 4840. Il est imposé par `gds/config.py` : un fichier de
configuration ne peut pas le remplacer.

| Option          | Défaut                | Rôle                                       |
|-----------------|-----------------------|--------------------------------------------|
| `--port`        | `4840`                | port OPC UA                                |
| `--bind`        | `0.0.0.0`             | adresse d'écoute                           |
| `--advertise`   | hostname de la machine| hôte annoncé aux clients                   |
| `--database`    | `gds.db`              | fichier SQLite du registre                 |
| `--no-database` | —                     | registre en mémoire seule                  |

> **`--ttl` n'existe pas pour le GDS.** En portée globale il n'aurait aucun
> effet : l'exposer laisserait croire qu'il pilote quelque chose.

Un serveur inscrit auprès d'un GDS n'a pas à se réenregistrer, mais un serveur
**retiré** doit le signaler : `RegisterServer` avec `IsOnline = False`
(clause 5.5.5.1), ce que fait `sciicad.lifecycle.withdraw_from_lds` à l'arrêt.

### Vider un registre global

Aucune entrée n'expire. Pour repartir d'un registre vide, arrêter le GDS et
supprimer `gds.db`. Un redémarrage seul ne suffit pas : c'est précisément ce qui
distingue le GDS du LDS.

---

## Options communes aux deux simulateurs

| Option          | Défaut                   | Rôle                                                    |
|-----------------|--------------------------|---------------------------------------------------------|
| `--port`        | `4840`                   | port OPC UA                                             |
| `--lds URL`     | `opc.tcp://lds:4840`     | LDS d'enregistrement ; `none` ou vide pour désactiver  |
| `--bind`        | `0.0.0.0`                | adresse d'écoute                                        |
| `--advertise`   | IP détectée              | hôte annoncé aux clients et au LDS                      |

`--port` et `--lds` sont validés : un port hors plage ou une URL sans schéma
`opc.tcp://` est refusé au lancement, avec un code de sortie non nul.

**`--bind` et `--advertise` sont découplés volontairement.** Le serveur écoute
sur `--bind` et annonce `--advertise`. Lier l'adresse déduite du hostname fait
échouer le démarrage dès que le DNS renvoie une adresse absente des interfaces
locales — ce qui arrive régulièrement en VM et en conteneur.

---

## Ordre de démarrage

Le LDS n'a pas besoin d'être démarré en premier : les simulateurs retentent
d'enregistrement avec un délai qui double à chaque échec (1 s, 2 s, 4 s…),
jusqu'à la période nominale de 60 s. Un PLC démarré avant le LDS finit donc
par s'y enregistrer tout seul.

En pratique, sur une VM :

1. le LDS (une fois) ;
2. les simulateurs, avec `--lds` pointant vers lui.

Un GDS, s'il est déployé, s'enregistre lui-même auprès du LDS de chaque
sous-réseau : c'est ainsi qu'un client le trouve via `FindServersOnNetwork` sur
son LDS local.

## Cycle de vie et arrêt

Un simulateur :

- s'enregistre **après** être en écoute, jamais avant ;
- se réenregistre toutes les 60 s (imposé par la portée « local ») ;
- retire son entrée du LDS à l'arrêt, sur `Ctrl+C` comme sur `SIGTERM`.

`SIGTERM` est traité explicitement : c'est ce qu'envoient `systemd` et
`docker stop`. Le retrait est acquitté en quelques dizaines de millisecondes.

Si le LDS est injoignable au moment de l'arrêt, l'entrée reste visible jusqu'à
l'expiration (`--ttl`, 300 s par défaut) : c'est un délai de grâce, pas une
fuite.

**Auprès d'un GDS, le délai de grâce n'existe pas.** Le retrait reste
recommandé, mais un échec n'a aucune conséquence : l'inscription est de toute
façon conservée jusqu'au retrait. Un arrêt brutal (coupure, `SIGKILL`) laisse
donc une entrée périmée au registre, qu'aucun mécanisme n'évacuera. Il faut
alors intervenir manuellement, ou vider `gds.db`.

## Certificats (optionnel)

Les simulateurs demandent `Basic256Sha256_SignAndEncrypt` en plus de
`NoSecurity`. Sans certificat, asyncua **n'annonce que `NoSecurity`** — la
dégradation est silencieuse, seule une ligne de warning le signale.

```bash
uv run tools/crypto_opcua.py \
  --hostname thermo-plc \
  --application-uri urn:SCIICAD:thermo-plc \
  --output-dir thermo-plc
```

Les fichiers générés sont ignorés par git. Le chemin est relatif au dépôt :
lancer la commande depuis la racine.

---

## Déploiement sur une VM

Ordre de démarrage : le LDS d'abord, puis les simulateurs. Un PLC démarré
avant le LDS n'échoue pas pour autant — il retente d'enregistrement avec un
délai qui double à chaque essai, jusqu'à la période nominale de 60 s.

Sur chaque VM :

```bash
git clone <url> /opt/sciicad-opcua
cd /opt/sciicad-opcua
uv sync
```

**LDS** (`lds` = 193.168.1.20) :

```bash
uv run python -m lds \
  --bind 0.0.0.0 \
  --advertise 193.168.1.20 \
  --database /var/lib/sciicad/lds.db
```

**THERMO-PLC** (`thermo-plc` = 193.168.1.90) :

```bash
uv run thermo-plc/plc_server.py \
  --lds opc.tcp://193.168.1.20:4840 \
  --advertise 193.168.1.90
```

**PROTECT-PLC** (`protect-plc` = 193.168.1.80) :

```bash
uv run protect-plc/plc_server.py \
  --lds opc.tcp://193.168.1.20:4840 \
  --advertise 193.168.1.80
```

`--advertise` est indispensable : sans lui, le serveur annonce le hostname de
la VM, que les autres machines peuvent ne pas résoudre.

### Arrêt et redémarrage

`SIGTERM` est géré par les deux simulateurs : le retrait du LDS est acquitté
en quelques dizaines de millisecondes avant la sortie du processus. Un arrêt
propre laisse le registre cohérent immédiatement.

Un arrêt brutal (arrêt électrique, `SIGKILL`) laisse l'entrée jusqu'à
l'expiration (300 s par défaut). Un redémarrage du LDS vide le registre : les
serveurs se réenregistrent d'eux-mêmes dans la minute.

### Ce qui reste à faire pour un pilotage par service

Les simulateurs gèrent `SIGINT` et `SIGTERM`, mais rien ne les pilote encore.
Pour un déploiement sous `systemd`, un fichier de service suffit :

```ini
[Unit]
Description=Simulateur PLC thermostat SCIICAD
After=network-online.target

[Service]
WorkingDirectory=/opt/sciicad-opcua
ExecStart=/usr/local/bin/uv run thermo-plc/plc_server.py --lds opc.tcp://193.168.1.20:4840 --advertise 193.168.1.90
Restart=on-failure
KillSignal=SIGTERM
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

`TimeoutStopSec` doit laisser le temps au retrait du LDS.

## Auto-tests

Vérifications disponibles, sans toucher au LDS de production :

```bash
uv run tools/selftest_lds.py               # LDS : registre, TTL, pagination
uv run tools/selftest_gds.py               # GDS : services Part 4, portée globale
uv run tools/selftest_thermo_lds.py        # sciicad.discovery : enregistrement/retrait
uv run tools/selftest_thermo_lifecycle.py  # cycle réel des deux PLC, avec SIGTERM
uv run tools/selftest_thermo_lifecycle.py protect-plc   # un seul simulateur
```

Tous sortent avec un code non nul en cas d'échec. Aucun ne touche aux serveurs
de production : ils écoutent sur des ports éphémères et utilisent une base
temporaire.

`selftest_gds.py` interroge chaque service **par son NodeId normatif**. C'est la
vérification qui distingue un vrai serveur de découverte d'une façade : si
`FindServersOnNetwork` renvoie `BadUserAccessDenied`, le service n'est pas
routé, quelle que soit la qualité du journal de démarrage.

## Dépannage

Voir [`depannage.md`](depannage.md) : nœuds introuvables, entrées fantômes au
registre, `Basic256Sha256` indisponible, connexion refusée.
