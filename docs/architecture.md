# Architecture

## Concepts OPC UA utilisés

- **Namespace** : identifie le domaine du serveur. Exemple :
  `urn:SCIICAD:thermo-plc`. Chaque namespace reçoit un **index numérique**
  (`NamespaceIndex`) qui **dépend du serveur** et de son historique de
  démarrage : il ne doit jamais être supposé constant.
- **NodeId** : identifiant d'un nœud = (NamespaceIndex, type, valeur).
  Exemple observé sur le thermo-plc : `NodeId(Identifier=2001,
  NamespaceIndex=1, FourByte)`.
- **Browse name** : nom lisible du nœud (`Thermostat`, `Heating`…). C'est le
  référentiel stable à utiliser dans les clients.
- **Endpoint** : URL de connexion `opc.tcp://<ip>:<port>`. Éventuellement
  pourvue d'un chemin (`/thermo/server/`) — asyncua ignore ce chemin lors de
  la sélection de l'endpoint (match sur schéma + sécurité uniquement).

## Topologie

```
                     ┌─────────────────────────┐
 192.168/193.x  ────▶│  LDS  :4840             │  découverte (FindServers)
 réseau local        │  (Local Discovery Server)│  enregistrement (RegisterServer)
                     └────────────┬────────────┘
                                  │ register_to_discovery (60 s)
                    ┌─────────────┼──────────────────┐
                    │             │                  │
            ┌───────▼─────┐ ┌─────▼──────┐   ┌──────▼────────┐
            │ thermo-plc  │ │ protect-plc│   │  GDS :4840    │
            │ :4840       │ │ :4840      │   │ (Global       │
            │ thermostat  │ │ protection │   │  Discovery)   │
            └─────────────┘ └────────────┘   └───────────────┘
                    │             │                  ▲
                    └──────┬──────┘                  │ demandes de
                           ▼                         │ certificats
                  ┌────────────────┐                 │
                  │  IHM / outils  │─────────────────┘
                  │  se connectent │
                  │  en direct     │
                  └────────────────┘
```

### Rôle du LDS

Le LDS (`lds/`, application `urn:SCIICAD:lds`) implémente les cinq services de
découverte de la Part 4 :

| Service               | Source                              |
|-----------------------|-------------------------------------|
| `FindServers`         | asyncua (natif)                     |
| `GetEndpoints`        | asyncua (natif)                     |
| `RegisterServer`      | asyncua (natif)                     |
| `RegisterServer2`     | asyncua (natif)                     |
| `FindServersOnNetwork`| `lds/services.py` (ajout du dépôt)  |

`UnregisterServer` n'existe pas dans la norme OPC UA. La durée de vie d'une
entrée est au contraire la durée du renouvellement, puis l'éviction par TTL
(`discovery.entry_ttl_seconds`, 300 s par défaut). Le registre est persisté
dans SQLite et survit au redémarrage du LDS. Voir
[`docs/serveurs.md`](serveurs.md#lds--local-discovery-server-lds).

Les PLC appellent `server.register_to_discovery(lds_url, 60)` au démarrage :
cet appel est **périodique** (renouvellement toutes les 60 s). L'appel
`unregister_from_discovery()` présent dans `thermo-plc/plc_server.py` est
inutile — il est de surcroît placé après une boucle infinie, donc
inatteignable.

### Rôle du GDS

Le GDS (`gds/gds_server.py`) implémente le rôle de **Global Discovery Server**
OPC UA (Part 12) :

- gestion des applications (RegisterApplication / QueryApplications /
  UnregisterApplication) ;
- gestion des certificats (CA, GetCertificate, CSR, approbation…) ;
- certificats 3.0 (GetCertificateGroups, GetTrustLists, changements).
- persistance en base (SQLAlchemy, configurable : sqlite par défaut).

## Adressage des nœuds

> **Règle d'or** : résoudre les nœuds par **browse name**, jamais par index
> de namespace ni identifiant numérique codés en dur.

Exemple observé en production sur le simulateur thermo-plc
(`193.168.1.90:4840`) :

| Élément              | Browse name       | NamespaceIndex | Identifiant |
|----------------------|-------------------|----------------|-------------|
| Objet racine         | `Thermostat`      | 1              | 2001        |
| Variables            | `Heating`         | 1              | 2002        |
|                      | `Temperature`     | 1              | 2003        |
|                      | `HighTempAlarm`   | 1              | 2004        |
|                      | `LowTempAlarm`    | 1              | 2005        |
|                      | `MaintenanceMode` | 1              | 2006        |

L'index du namespace `urn:SCIICAD:thermo-plc` est ici **1** (et non 2) ;
les identifiants numériques sont **2001..2006** (et non 1..6). Une ancienne
version de `tools/ihm_action.py` supposait `ns=2` et des identifiants `2..6` :
le thermostat n'était jamais trouvé. Voir [`docs/depannage.md`](depannage.md).

## Flux de données

1. Les PLC simulent l'équipement (boucle asynchrone) et mettent à jour leurs
   variables OPC UA (ex. `Temperature`).
2. Le LDS est tenu informé des serveurs actifs.
3. L'IHM et les outils lisent/écrivent les variables en direct sur l'endpoint
   du PLC :
   - lecture : `Temperature`, alarmes, états ;
   - écriture : `Heating`, `MaintenanceMode` (variables définies inscriptibles).

## Network

Les services tournent sur le réseau local `193.168.1.0/24`. Le plan complet
d'adressage et de nommage figure dans
[`docs/adressage.md`](adressage.md).

| Service        | Adresse (observée) | Port |
|----------------|--------------------|------|
| LDS            | 193.168.1.20       | 4840 |
| thermo-plc     | 193.168.1.90       | 4840 |
| Machine hôte   | 193.168.1.100      | —    |

> Les clients doivent privilégier les **hostnames DNS** (`lds`,
> `thermo-plc`, …) aux adresses IP, conformément aux valeurs par défaut du
> code — cf. [`docs/adressage.md`](adressage.md).