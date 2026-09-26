# Plan d'adressage et de nommage des équipements

## Principes

- Le réseau local des simulateurs est `193.168.1.0/24`
  (passerelle `193.168.1.1`, hôte DNS/service interne). C'est un réseau
  privé non standard : les adresses ne sont **pas** routables publiquement.
- **DNS** : le nommage repose sur le DNS interne, correctement renseigné pour
  les équipements. Les hostnames doivent être utilisés en priorité dans les
  clients et les configurations, jamais les adresses IP codées en dur.
- **Ports** : le port standard OPC UA `4840` est utilisé par tous les
  serveurs. Un seul service peut donc écouter par hôte sur ce port.
- Connexion serveur via l'endpoint : `opc.tcp://<hostname>:4840`.

## Conventions de nommage

| Règle                    | Exemple      | Remarque                            |
|--------------------------|--------------|-------------------------------------|
| `lds`                    | `lds`        | Local Discovery Server              |
| `<role>-plc`             | `thermo-plc`, `protect-plc` | simulateurs PLC            |
| `ihm`                    | `ihm`        | client d'affichage / supervision    |
| hostnames en minuscules  | `thermo-plc` | séparateur `-`                      |
| résolution DNS           | `lds`, `thermo-plc` | utilisée par les Dockerfiles et les défauts du code |

Valeurs par défaut dans le code :

- `thermo-plc/plc_server.py` : `--lds opc.tcp://lds:4840`
- `protect-plc/plc_server.py` : `--lds opc.tcp://lds:4840`
- `ihm/ihm_client.py` : `--host thermo-plc`
- `tools/test_discovery.py` : `--lds-url opc.tcp://127.0.0.1:4840`,
  `--plc-uri urn:SCIICAD:thermo-plc` (le PLC est désigné par son
  `applicationUri`, pas par une URL : c'est le registre du LDS qui fournit
  l'endpoint)
- `tools/ihm_action.py` : `--ip localhost`

## Plan d'adressage IPv4

| Équipement / rôle | Hostname         | IP (observée / à définir) | Port | Remarques                                  |
|-------------------|------------------|---------------------------|------|--------------------------------------------|
| Hôte de travail   | `host`           | 193.168.1.100             | —    | machine de développement (poste actuel)    |
| LDS               | `lds`            | 193.168.1.20              | 4840 | observé ; discovery server                 |
| thermo-plc        | `thermo-plc`     | 193.168.1.90              | 4840 | observé ; thermostat simulé                |
| protect-plc       | `protect-plc`    | 193.168.1.80              | 4840 | observé ; système de protection simulé     |
| IHM               | `ihm`            | *à définir*               | 4840 | client ; consomme `thermo-plc`             |
| GDS (à venir)     | `gds`            | *à définir*               | 4840 | non déployé ; endpoint `…/GlobalDiscoveryServer` |

> Le GDS partage le port 4840 avec les autres serveurs. Il s'en distingue par
> le **chemin** de son endpoint, `/GlobalDiscoveryServer`, imposé par la
> Part 12 : sur une même machine, LDS et GDS ne peuvent pas être déployés tous
> les deux sur 4840 qu'avec des hôtes ou des adresses distinctes. Prévoir un
> second port (4841 par exemple) pour le GDS si les deux doivent tourner côte à
> côte.

## Sous-réseau et réservation

- `193.168.1.1` : passerelle.
- `193.168.1.0/24` : plage locale ; les adresses `.20`, `.90`, `.100` sont
  attribuées (voir tableau).
- Adresses non listées : **libres** — à attribuer par l'administrateur du
  DNS/réseau pour `gds`, `ihm`, etc. Une fois attribuées, les hostnames
  correspondants sont renseignés dans le DNS interne.

## Conséquences pour le code

1. Utiliser les **hostnames** (`lds`, `thermo-plc`, …) dans les URLs, comme
   les valeurs par défaut existantes.
2. Ne jamais committer d'IP en dur dans les scripts clients ; passer les
   adresses en argument (`--ip`, `--host`, `--lds`, `-u`).
3. Pour les nœuds OPC UA, résoudre par **browse name** et non par index de
   namespace (cf. [`docs/architecture.md`](architecture.md)). C'est désormais
   appliqué par `sciicad/nodes.py`, qui fournit `find_node_by_name` et
   `find_by_path`.
4. Sur une VM, renseigner explicitement `--advertise` : l'hôte annoncé doit
   être joignable par les clients, et le hostname de la machine ne l'est pas
   nécessairement.