# Dépannage

## Workflow de diagnostic

1. Vérifier la connectivité réseau : `nc -zv <ip> 4840`.
2. Inspecter le serveur : `uv run tools/analyze.py -u opc.tcp://<ip>:4840`
   (liste les variables, accès et valeurs réelles).
3. Tester le LDS : `uv run tools/test_lds_discovery.py --url opc.tcp://<lds>:4840`.
4. Vérifier les NodeIds normalisés : `tools/check_lds_gds.py` (adapter l'URL).
5. Contrôler le thermostat : `uv run tools/ihm_action.py --ip <ip> --port 4840`.

## Problèmes connus

### 1. « Thermostat non trouvé ! » / nœuds introuvables (namespace codé en dur)

**Symptôme** : le client se connecte mais ne retrouve pas `Thermostat`.

**Cause** : fallait rechercher par `NamespaceIndex == 2` et identifiants
numériques `2..6` codés en dur. Or l'index du namespace `urn:SCIICAD:thermo-plc`
dépend du serveur (observé : **1**) et les NodeIds sont **2001–2006**.

**Correctif** : résoudre par **browse name** (`Thermostat`, `Heating`, …) :

```python
async def find_node_by_name(parent_node, name):
    for child in await parent_node.get_children():
        if (await child.read_browse_name()).Name == name:
            return child
```

C'est la méthode utilisée dans `tools/ihm_action.py`, `ihm/ihm_client.py` et
`tools/test_discovery.py`.

**Note** : l'index de namespace change aussi selon l'ordre
d'enregistrement au démarrage — ne jamais le supposer constant.

### 2. Le LDS annonce un serveur qui ne repond plus

**Symptème** : la decouverte liste un PLC (par exemple `193.168.1.99:4899`)
alors que la connexion echoue en *connection refused*.

**Cause** : la norme OPC UA ne definit **aucun** service de desenregistrement
d'un serveur aupres d'un LDS. Une entree disparait quand le serveur cesse de
se reenregistrer et que le LDS l'evince au bout de
`discovery.entry_ttl_seconds` (300 s par defaut). Un arret brutal laisse donc
l'entree visible jusqu'a l'expiration. C'est aussi ce qui arrive si le PLC
redemarre sur une autre IP : l'ancienne entree et la nouvelle coexistent.

**Correctif** : verifier le renewing cote PLC, puis forcer l'eviction :

```bash
uv run tools/test_lds_discovery.py --url opc.tcp://193.168.1.20:4840
# le registre est en memoire : un redemarrage du LDS le vide, et les
# serveurs se reenregistrent d'eux-memes dans la minute
```

**Note** : un ancien `unregister_from_discovery()` figure dans
`thermo-plc/plc_server.py` et `protect-plc/plc_server.py`. Il n'a aucun effet
et il est de surcroit inatteignable (place apres une boucle infinie).

### 3. Le chemin dans l'URL de connexion ne matche pas

**Symptôme** : `opc.tcp://host:4840/thermo/server/` fonctionne alors que le
serveur annonce `opc.tcp://host:4840`.

**Explication** : asyncua sélectionne l'endpoint par schéma + sécurité,
pas par chemin. Le chemin est toléré mais sans effet. Le plus simple est de
se connecter à `opc.tcp://<ip>:4840`.

### 4. Connexion impossible (timeout / refused)

- Vérifier l'IP : le poste hôte est sur `193.168.1.100` (réseau
  `193.168.1.0/24`) en environnement de test ; les services attendus sont
  LDS `193.168.1.20:4840` et thermo-plc `193.168.1.90:4840`.
- Vérifier que le port 4840 est exposé (hôte ou conteneur Docker).
- Vérifier que le service est bien démarré (log de démarrage).

### 5. Le simulateur reçoit « LDS non disponible »

**Cause fréquente** : URL LDS par défaut `opc.tcp://lds:4840` (résolution du
nom `lds`). En local, passer explicitement l'IP ou le hostname :

```bash
uv run thermo-plc/plc_server.py --lds opc.tcp://193.168.1.20:4840
```

Si le LDS est down, le PLC démarre quand même (simple warning).

### 6. Basic256Sha256 indisponible sur le serveur

**Symptôme** : l'endpoint sécurisé n'apparaît pas dans `analyze.py`.

**Cause** : absence de `server_certificate.pem` / `server_private_key.pem`
dans le dossier du simulateur (chargés au démarrage sinon warning).

**Correctif** : générer les certificats puis redémarrer :

```bash
uv run tools/crypto_opcua.py --hostname thermo-plc --output-dir thermo-plc
```

### 7. Écriture refusée (BadNotWritable / BadUserAccessDenied)

- `Temperature` et les alarmes sont **lecture seule** ; seules `Heating` et
  `MaintenanceMode` sont inscriptibles (et définies `set_writable(True)` côté
  serveur).
- L'accès anonyme est autorisé sur les endpoints `None` ; pour l'endpoint
  `Basic256Sha256`, fournir un certificat client (voir `docs/securite.md`).

### 8. Le GDS ne répond pas aux tests

- Vérifier `opc.tcp://localhost:4840/GlobalDiscoveryServer` dans
  `gds/gds_config.yaml` (chemin significatif pour le GDS).
- Vérifier la base : `gds.db` est créée en sqlite au démarrage ; la
  destruction de ce fichier réinitialise utilisateurs/roles/applications.
- `tools/test_gds.py` attend le GDS sur `opc.tcp://localhost:4840`.

### 9. `ihm/ihm_client.py` ou Docker : dépendances

Les `Dockerfile` référencent un `requirements.txt` non versionné : le créer et
le déposer dans chaque dossier du composant avant `docker build` (contenu des
dépendances de `pyproject.toml`). En natif, utiliser `uv sync` puis
`uv run`.