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

### `check_docs.py` — vérification automatique de la documentation

```bash
uv run tools/check_docs.py
```

Quatre familles d'anomalies, toutes vérifiables sans intervention : **options**
citées qui n'existent plus dans le `--help` du script visé, **liens** relatifs
morts, **ancres** ne correspondant à aucun titre, **chemins** de fichiers cités
en clair et absents. Code de sortie non nul si une seule existe.

> Le calcul des ancres mérite l'attention : GitHub met en minuscules,
> **supprime** ce qui n'est ni lettre, ni chiffre, ni espace, ni tiret, puis
> remplace les espaces par des tirets. Donc `§` disparaît — il ne devient pas un
> tiret — et les accents sont **conservés**, GitHub ne translittère pas.
>
> Un vérificateur qui convertit `§` en `-`, ou qui retire les accents, déclare
> morte une ancre parfaitement valide tout en manquant les vraies. C'est ce qui
> est arrivé ici : trois ancres perdues depuis un certain temps n'ont été
> trouvées qu'après correction de la règle.

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

Le `keyUsage` produit est conforme à la **Table 50** de la Part 6 : pour une clé
RSA, `digitalSignature`, `nonRepudiation`, `keyEncipherment` et
`dataEncipherment`, plus `keyCertSign` puisque le certificat est auto-signé.
C'est ce qui permet au certificat constructeur d'être sa propre ancre.

### `bootstrap_certificates.py` — amorçage de la confiance, Part 12 §7.1

L'étape hors bande sans laquelle aucun déploiement ne démarre : la norme
l'exige explicitement, et ne fournit aucun équivalent en bande.

```bash
uv run tools/bootstrap_certificates.py
```

Génère les **certificats constructeurs des quatre rôles** — `lds`, `gds`,
`thermo-plc`, `protect-plc` — et dépose leurs **copies publiques** dans
`pki/trusted/`. La clé privée reste dans le répertoire du rôle, en `0600` : une
liste de confiance est lisible par tout client autorisé à la lire, et y mettre
une clé l'exposerait.

Les URI d'application sont celles que les serveurs annoncent réellement
(YAML pour le LDS et le GDS, constante de module pour les PLCs). Une URI
divergente produirait un certificat dont le SAN ne correspond pas à ce que le
serveur déclare — et le défaut n'apparaîtrait qu'à la première connexion
sécurisée.

| Option | Effet |
|---|---|
| `--host ROLE=HOST` | Nom d'hôte inscrit au SAN, répétable. Le premier est l'identité principale, les suivants des alias. |
| `--key-size` | bits (défaut 2048) |
| `--validity-days` | durée de validité (défaut 365) |
| `--force` | Écrase l'existant, en prévenant que cela invalide toute confiance établie. |

Sans `--force`, un couple déjà présent est conservé : réécrire une clé par-dessus
une clé en service détruirait le certificat correspondant, qu'aucune liste de
confiance ne pourrait plus valider.

Rappelle enfin la déclaration à porter dans `gds/gds_config.yaml`. Voir
[docs/securite.md](securite.md#amorçage-de-la-confiance-part-12-71) pour la
raison normative.

Avec `--signed`, la même commande produit des certificats **signés par
l'autorité** : la clé privée est générée localement, seule une demande de
signature sort, et l'autorité écrit le certificat à sa place.

```bash
python tools/authority.py init          # l'autorité, d'abord
python tools/bootstrap_certificates.py --signed --force
python tools/authority.py crl           # la CRL de l'autorité
```

L'absence d'autorité est une **erreur franche**, code de sortie 2, jamais un
retour silencieux aux auto-signés : ce repli ferait croire à un déploiement à
autorité là où il n'y a que des certificats constructeur.

### `authority.py` — autorité de certification hors ligne

Quatre sous-commandes. L'outil ne dessert aucun client et n'est jamais exposé
par le GDS : c'est l'extérieur dont la Part 12 admet qu'il existe.

| Commande | Effet |
|---|---|
| `init` | Crée l'autorité. Clé **hors du dépôt**, dans `~/.sciicad/ca/`, en `0600`. |
| `sign CSR` | Signe une demande PKCS #10. `--out`, `--days`. |
| `crl` | Émet une CRL. `--revoke SERIE` (hexadécimal, répétable), `--days`. |
| `retire ROLE...` | Archive les clés constructeurs, rappelle de retirer les ancres. |

```bash
python tools/authority.py init --common-name "SCIICAD CA"
python tools/authority.py sign thermo-plc/server_certificate.csr
python tools/authority.py crl --revoke 1a2b3c4d5e
python tools/authority.py retire lds gds thermo-plc protect-plc
```

**Où vit la clé, et pourquoi c'est le point important.** Elle est dans le
`$HOME` de l'opérateur, jamais dans l'arbre du dépôt : une clé de signature au
besoin du code devient un geste banal, et une clé qu'on signe sans réfléchir est
une clé compromise sans incident. Seul le certificat public est dans `pki/ca/`,
et `pki/` est ignoré par git.

Le profil : `cA=TRUE` avec `pathLength=0`, `keyCertSign` et `cRLSign`, sans EKU
ni SAN. Une autorité ne sert aucun protocole applicatif, et lui en donner un
invite à la présenter comme si elle pouvait ouvrir un canal. `pathLength=0`
interdit une sous-autorité : une hiérarchie à un niveau se audite en entier,
alors qu'une autorité capable d'en créer une autre peut en créer une qu'on ne
verra jamais.

`sign` refuse une demande dont la signature ne tient pas, une demande sans URI
d'application, et une demande portant deux URI — la Table 50 en impose
exactement un, et deux sont deux identités qui se contestent. Il vérifie aussi
que la clé correspond au certificat : sans cela, on émet des certificats dont
la signature ne remonte à rien de vérifiable, ce qui ne se découvre qu'au
premier rejet.

`retire` archive les clés constructeurs dans `pki/retired/` plutôt que de les
supprimer : un déploiement doit pouvoir revenir en arrière, et une clé effacée
est un retour en arrière impossible.

### `selftest_authority.py` — autorité et certificats signés (phases 0 et 1)

```bash
python tools/selftest_authority.py
```

Vérifie que l'autorité existe, qu'elle est conforme (`cA=TRUE`,
`pathLength=0`, `keyCertSign`, `cRLSign`, pas d'EKU), que **sa clé est hors du
dépôt** et en `0600`, que les quatre rôles portent un certificat signé et
conforme au profil, et que le GDS réel charge ancres, autorité et CRL.

**Six contrôles négatifs**, chacun pour un motif distinct, et deux témoins qui
doivent passer : autorité absente, autorité sans CRL, certificat révoqué, **CRL
au bon nom d'émetteur mais signée par une autre clé**, profil non conforme, SAN
à deux URI.

Le quatrième est le plus important, parce qu'il fermait un trou réel : avant,
une CRL était appliquée sur la seule foi de son nom d'émetteur. Quiconque était
autorisé à écrire dans `issuer_crls` — donc tout client habilité à diffuser des CRL,
ce chemin étant le seul normatif — pouvait déposer une CRL portant le nom de
l'autorité et révoquer **tout** le déploiement. Le test le prouve en réactivant
le comportement : la CRL étrangère révoque alors le certificat, et
`selftest_authority.py` échoue.

Une autorité de laboratoire est créée pour ces contrôles. Révoquer le numéro de
série d'un certificat en service pour tester une CRL serait un test qui casse
la production.

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

L'emplacement des groupes de certificats y est vérifié **par le chemin
parcouru**, et non par NodeId : dossier `CertificateGroups` sous
`ServerConfiguration` (§7.8.3.3), aucun `CertificateGroupType` égaré sous
`Server`, propriété `CertificateTypes` renseignée, et `Open` réellement
appelé sur la `TrustList` de chaque groupe. Le dernier point est le
discriminant — un groupe présent mais non câblé répond `BadNothingToDo`, ce que
seul un appel révèle. Ces vérifications ont été ajoutées après coup, en
vérifiant qu'un retour délibéré à la publication sous `Server` les faisait
échouer.

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

Les **cinq propriétés obligatoires** de l'objet `TrustList` y sont lues par le
réseau : `Size` doit renvoyer `BadNotSupported`, `OpenCount` doit suivre les
poignées *dans l'espace d'adressage* et pas seulement dans le modèle Python, et
`LastUpdateTime` doit être postérieur à `DateTime.MinValue` après modification.
Le test laisse volontairement une ouverture non refermée pour observer
`OpenCount` — c'est le cas que la norme veut rendre visible — puis la referme et
vérifie la descente à zéro. Un contrôle vérifie aussi qu'un refus ne publie
rien de faux.

> Ces contrôles ont été ajoutés après avoir constaté que le modèle Python était
> juste tandis que l'espace d'adressage renvoyait `None` : un test qui lit
> `group.open_count()` passe au vert sans qu'aucun client ne voie quoi que ce
> soit. Même angle mort que pour le doublon `ServerConfiguration` — le test
> vérifiait l'objet, pas le chemin d'accès. Validé en désactivant la
> republication : quatre contrôles échouent, dont `nœud=0, modèle=1`.

### `selftest_audit.py` — événements d'audit du GDS (Part 12 §7.8.2.13, §7.10.27)

Souscrit **réellement** aux deux `ObjectType` d'audit et compte ce qui arrive.
Un événement d'audit n'est pas une ligne de journal : il ne parvient qu'aux
clients abonnés, donc un test qui ne s'abonne pas ne prouve rien.

```bash
uv run tools/selftest_audit.py
```

Vérifié notamment : un `AddCertificate` qui modifie la liste émet **un**
`TrustListUpdatedAuditEventType`, portant le `TrustListId` de l'objet et le
`MethodId` de la méthode appelée ; un `AddCertificate` **idempotent** n'émet
rien, alors que la méthode a réussi ; `Open`, `Read` et `Close` n'émettent rien ;
un `UpdateCertificate` **refusé** n'émet rien et un **accepté** en émet un ; les
données volumineuses sont résumées par leur empreinte dans `InputArguments`
plutôt que recopiées ; la sévérité est bien 300, celle d'un audit.

> Deux pièges d'asyncua 1.1.8, tous deux silencieux. Le gestionnaire reçoit **un**
> `Event` par notification, déballe côté client : chercher une liste
> `Events` ne trouve rien et le test compte zéro événement sans jamais échouer
> sur une erreur. Et le `NodeId` d'un nœud **serveur** est un `NumericNodeId`,
> absent de la table des ExtensionObject : le passer en argument casse la
> sérialisation sur `KeyError`. Un client normatif n'a jamais ce second cas.

### `selftest_bootstrap.py` — amorçage de confiance (Part 12 §7.1, Part 6 Table 50)

```bash
uv run tools/selftest_bootstrap.py
```

Vérifie que l'étape hors bande existe, qu'elle produit des certificats
**conformes au profil normatif**, et qu'elle aboutit réellement dans la liste de
confiance du GDS — lue **par le réseau**, depuis un client ordinaire, parce que
ce qui compte est ce qu'un client peut voir, pas ce que le serveur croit avoir
chargé.

Vérifié notamment : le profil Table 50 champ par champ ; exactement un URI dans
le SAN, égal à l'URI d'application ; `cA=FALSE` ; clé privée en `0600` ; aucune
clé privée dans `pki/trusted/` ; les quatre ancres dans
`DefaultApplicationGroup` et **nulle part ailleurs** ; les ancres lisibles sur
le réseau.

**Quatre contrôles négatifs.** Un jeu de tests qui passe ne prouve rien s'il ne
peut pas échouer. Trois certificats sont conformes à tout sauf à un point, et
doivent donc être refusés :

- `keyUsage` incomplet pour une clé RSA — les deux bits que l'ancien code
  exigeait, et lui seul ;
- `serverAuth` absent de l'EKU ;
- `keyCertSign` absent sur un auto-signé.

Un quatrième cas, **conforme**, doit passer : sans lui, un refus dû à un autre
motif passerait pour une preuve.

> Ces contrôles sont construits à partir d'une demande de signature du magasin,
> et non indépendamment de lui. C'est la condition qui les rend valides : sinon
> le magasin refuse d'abord pour *clé absente*, avec le **même**
> `BadCertificateInvalid` que le profil — et le test passerait pour le mauvais
> motif. La révocation est neutralisée dans ce test, qui isole une seule
> variable ; elle a le sien, `selftest_revocation.py`.

Contrôle négatif du contrôle négatif : neutraliser `_check_application_profile`
fait passer les trois refus à `Good`, et l'auto-test échoue. C'est ce qui
prouve qu'il exerce réellement le contrôle.

### `selftest_secure_channel.py` — canal sécurisé du LDS et du GDS (phase 2)

```bash
python tools/selftest_secure_channel.py
```

Établit de **vrais canaux** contre un vrai GDS, et vérifie quatre verdicts
distincts :

- client **déclaré** de confiance → accepté ;
- certificat signé mais **jamais déclaré** → refusé, `BadCertificateUntrusted` ;
- certificat **révoqué** → refusé, `BadCertificateRevoked` ;
- `NoSecurity` toujours accepté, sans certificat.

Vérifie aussi que le LDS annonce `Basic256Sha256_SignAndEncrypt` sans valider de
certificat client — ce n'est pas un oubli, c'est §6.2 qui ne parle d'identité que
pour les services globaux — et que le mode dégradé (sans certificat) n'annonce
que `NoSecurity`.

> **Le contrôle le plus important est le contrôle d'absence.** Retirer le
> validateur doit faire réapparaître l'acceptation du certificat non déclaré.
> Sans lui, ce test ne prouve pas qu'un refus a lieu : il prouve qu'un refus a
> lieu, ce qui est compatible avec un refus dû à n'importe quoi d'autre — et le
> reste du code le démontre, en refusant pour des motifs sans rapport. C'est ce
> contrôle qui attribue le refus à sa cause.

### `selftest_revocation.py` — révocation du GDS (Part 12 §7.8.2.10)

Vérifie le comportement de `DefaultValidationOptions`, qui est **fermé par
défaut** : sans CRL connue d'un émetteur de confiance, un certificat est
refusé, son état de révocation étant inconnu.

```bash
uv run tools/selftest_revocation.py
```

Vérifié notamment : la propriété est publiée au DataType normatif
`TrustListValidationOptions` (i=23564) avec la valeur de §7.8.2.10 ; une CRL
atteint la liste par `Open`/`Write`/`CloseAndUpdate`, le seul chemin que la
norme offre puisqu'aucun `AddCrl` n'existe ; un certificat listé par la CRL est
refusé `Bad_CertificateRevoked` ; une CRL émise par **une autre autorité** est
sans effet, ce qui empêche un déni de service par CRL étrangère ;
`SuppressRevocationStatusUnknown` et `SuppressCertificateExpired` lèvent le
refus correspondant.

> L'agencement du fichier par `Write` a été modifié pour cette conséquence. Une
> écriture bornée à la taille d'origine rendait les listes immuables, donc
> aucune CRL diffusable, donc la propriété sans effet.

### `selftest_certmanager.py` — rôle CertificateManager du GDS (Part 12 §7.10)

Interroge l'objet `ServerConfiguration` **par le réseau**. Le test fabrique sa
propre autorité de certification pour signer, puisque le GDS n'en détient
aucune : le cycle testé est donc le cycle réel de la norme, demande de signature
puis installation du certificat signé.

```bash
uv run tools/selftest_certmanager.py
```

Vérifié notamment : l'objet est bien l'instance normative i=12637 et il n'en
existe qu'une seule ; les trois méthodes ont la signature normative, entrées et
sorties ; la PKCS #10 produite est réellement exploitable et porte l'URI
d'application ; un certificat d'une autorité de confiance est accepté ; un
`Nonce` trop court, un certificat expiré, un certificat portant une autre URI,
un certificat d'une autorité non approuvée et un DER illisible sont chacun refusés
avec leur `StatusCode` normatif.

`GetRejectedList` est vérifié dans les deux sens : elle contient le certificat
**valide mais non approuvé**, et ne contient ni le certificat expiré, ni celui
portant une autre URI, ni le DER illisible. §7.8.3.2 réserve cette liste aux
certificats « that have no unsuppressed validation errors but are not trusted » —
un test qui ne vérifierait que sa présence passerait avec n'importe quelle
sur-population.

> Une vérification vaut mieux qu'un test : le nombre d'objets
> `ServerConfiguration` est vérifié explicitement, parce que le câblage initial
> en créait un second — invisible pour un test qui vise un NodeId, mais fatal
> pour un client qui parcourt l'espace d'adressage et trouverait le premier des
> deux, non câblé.

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
