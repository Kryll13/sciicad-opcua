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

Le certificat est conforme au profil de la **Table 50** de la Part 6 : pour une
clé RSA, `keyUsage` porte `digitalSignature`, `nonRepudiation`, `keyEncipherment`
et `dataEncipherment`, plus `keyCertSign` puisque le certificat est auto-signé ;
`extendedKeyUsage` porte `serverAuth` ; `basicConstraints` porte `cA=FALSE` ; le
SAN contient **exactement un** URI, égal à l'URI d'application.

> Ce profil n'est pas vérifié par la bibliothèque qui écrit le certificat :
> `cryptography` produit ce qu'on lui demande sans le confronter à une norme. Un
> écart ne se découvre qu'auprès d'un validateur — ou jamais. Le GDS, lui, le
> vérifie : `gds/certstore.py` contrôle les quatre bits, `serverAuth`, et
> `keyCertSign` sur un auto-signé, et nomme le bit manquant quand il refuse.

> Si les fichiers sont absents, le serveur démarre quand même (warning) avec
> `NoSecurity` uniquement. Vérifier ces fichiers sur un PLC distant pour
> activer le chiffrement.

## Amorçage de la confiance (Part 12 §7.1)

La Part 12 ne définit **aucun** amorçage en bande. §7.1 exige l'inverse :

> « Clients shall only connect to a CertificateManager which the Client has been
> configured to trust. This may require an out of band configuration step which
> is completed prior to starting the manual onboarding process. »

La confiance se pose donc **hors bande, avant le démarrage**. C'est la seule
lecture possible, et la seule qui marche.

### Pourquoi pas d'autorité de certification interne

Parce que la norme ne lui laisse pas de place, et qu'il faut le voir plutôt que
le contourner :

| Question | Réponse |
|---|---|
| Un rôle « Certificate Authority » existe-t-il ? | **Non.** Recherche sur `CertificateAuthorit` dans le `NodeIds.csv` officiel : **zéro** nœud. |
| Que produit `CreateSigningRequest` ? | Une PKCS #10 — 3.1.3 : *« used to request a new Certificate **from a Certificate Authority** »*. La CA est extérieure par définition. |
| `StartSigning` / `CreateSelfSignedCertificate` ? | Absents du NodeSet courant, comme `CertificateDirectoryType` au §7.9. |

Le GDS est donc un **CertificateManager** (§7.10), pas une CA. Il prépare une
demande, conserve la clé, installe le certificat signé par une autorité
**extérieure**. Le rôle `StartSigning`/`StopSigning` du modèle 1.04 a disparu.

### La séquence

| Étape | Action | Canal |
|---|---|---|
| 1 | Générer les 4 certificats constructeurs + l'ancre publique | hors bande |
| 2 | Déclarer l'ancre dans `gds/gds_config.yaml` | hors bande |
| 3 | Les 4 applications démarrent avec leur certificat constructeur | — |
| 4 | Renouvellement : `CreateSigningRequest` → CA externe → `UpdateCertificate` | chiffré |

L'étape 3 ne demande que le certificat constructeur **du GDS**, produit à
l'étape 1 : il n'y a pas de circularité. La CA n'entre qu'à l'étape 4.

Le certificat constructeur **est** l'ancre. Il n'a pas à être remplacé pour
devenir inutile — il est ce à quoi l'on fait confiance au départ.

Avec une autorité en service, la séquence change de nature : les ancres
auto-signées cèdent la place à des certificats signés. Voir la section
[Autorité de certification](#autorité-de-certification-phases-0-et-1) plus bas.

### Mise en œuvre

```bash
uv run tools/bootstrap_certificates.py
```

Produit, pour `lds`, `gds`, `thermo-plc` et `protect-plc`, un couple
`server_certificate.pem` + `server_private_key.pem` (clé en `0600`), et dépose
les **copies publiques** dans `pki/trusted/`.

La séparation est le point : la clé reste dans le répertoire du rôle, l'ancre
part sans elle. Une liste de confiance est lisible par tout client autorisé à la
lire ; y déposer une clé privée la rendrait lisible aussi.

Puis, dans `gds/gds_config.yaml` :

```yaml
certificates:
  trusted_certificates:
    - pki/trusted
  trusted_certificates_group: DefaultApplicationGroup
```

Ces certificats entrent dans la liste **sans** passer la validation de
`gds/certstore.py`, et c'est délibéré. La validation certifie ce qu'un
certificat présenté par le réseau respecte le profil ; ici, l'administrateur
*décide* que cette clé est de confiance. La faire passer par la validation la
rendrait dépendante d'elle-même — et elle échouerait, car le défaut fermé de
§7.8.2.10 refuse un certificat sans CRL, donc refuse précisément celui qui sert
d'ancre. **Une ancre ne peut pas exiger la preuve de sa propre existence.**

Un ancrage visant un groupe non rattaché est signalé et **ignoré, jamais
redirigé** : placé dans le mauvais groupe, il accepterait des présentations qui
ne doivent pas l'être.

## Autorité de certification (phases 0 et 1)

L'amorçage §7.1 ci-dessus pose des **ancres auto-signées** : c'est le minimum
qui rend un déploiement joignable, et c'est exactement ce qu'un certificat
constructeur est. Dès qu'une autorité existe, les rôles portent des
certificats **signés**, et le mode de validation change de nature : la chaîne
remonte à un `issuer_certificates`, et non plus au certificat lui-même.

| | Amorçage seul | Avec autorité |
|---|---|---|
| Le certificat est validé par | lui-même (ancre) | sa chaîne jusqu'à l'autorité |
| `trusted_certificates` | les 4 ancres | (éventuellement vide) |
| `issuer_certificates` | — | le certificat de l'autorité |
| `issuer_crls` | — | **obligatoire**, sans quoi tout est refusé |

L'autorité est **hors ligne** et sa clé **hors du dépôt** (`tools/authority.py`).
Le GDS ne peut pas être une autorité : la Part 12 ne lui donne pas ce rôle, et la
recherche sur `CertificateAuthorit` dans le `NodeIds.csv` officiel ne rend
**aucun** nœud.

Une CRL n'est pas facultative dès qu'une autorité est déclarée. Sans elle,
l'état de révocation d'un certificat signé est **inconnu**, et le défaut fermé
de §7.8.2.10 le refuse — un refus correct, mais sans issue. C'est pourquoi
`issuer_certificates` et `issuer_crls` se déclarent ensemble, et pourquoi le GDS
avertit explicitement quand il a des émetteurs sans aucune CRL.

Une CRL n'est appliquée que si **sa signature se vérifie avec la clé de
l'émetteur de confiance**, et pas seulement si son nom d'émetteur correspond.
Avant cette correction, une CRL au bon nom mais signée par une autre clé était
appliquée : quiconque pouvait écrire dans `issuer_crls` révoquait tout le
déploiement. Une CRL non signée par son émetteur ne peut plus ni révoquer (elle
ne prouve rien) ni blanchir (les révocations sont une **union**, pas une
intersection).

### Ce que le canal sécurisé ne garantit pas

asyncua ne valide **rien** par lui-même : `security_policies.py` ne fait que
`verify(data, signature)`, qui prouve la possession de la clé privée. Il n'y a
ni `validate_cert`, ni liste de confiance dans la pile.

Mais la pile **expose le point de branchement**, des deux côtés :

| Côté | Point d'entrée | Quand |
|---|---|---|
| Serveur | `Server.set_certificate_validator(...)` | à la création de session, sur le certificat client présenté |
| Client | `Client.certificate_validator` (attribut) | à l'ouverture du canal, sur le certificat serveur reçu |

C'est une correction d'un diagnostic antérieur, qui concluait « asyncua ne le
permet pas ». La réalité est plus petite et plus utile : **personne n'y a
branché de validateur**. Tout le travail consiste à y brancher le nôtre.

Une limite structurelle reste : à l'ouverture du canal, asyncua **tronque la
chaîne** et ne conserve que la feuille. Le client ne voit donc jamais l'autorité
en transit — il doit l'apprendre par la liste de confiance du GDS, ce qui est la
conception voulue.

Conséquence, à connaître avant de dire « le déploiement est sécurisé » :

- `SignAndEncrypt` est **réel** : chiffrement et authentification des messages.
- Tant qu'aucun validateur n'est branché, l'**identité** n'est pas vérifiée. Un
  attaquant détenant son propre couple de clés serait accepté.

Le GDS sait déjà valider côté serveur ; c'est le côté client qui reste à câbler,
et c'est le sens de `set_certificate_validator` / `Client.certificate_validator`.

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
Voir [`serveurs.md`](serveurs.md#rôle-certificatemanager-part-12-710).

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
> [`serveurs.md`](serveurs.md#contrôle-daccès-non-implanté-et-ce-nest-pas-une-référence-manquante).

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
[`serveurs.md`](serveurs.md#gdsgds_serverpy-prototype-non-exposé).

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

**Toute la verbosité des rôles en service — LDS, GDS, les deux simulateurs PLC —
passe par loguru.** Aucun `print()` : la sortie standard est réservée à
l'affichage interactif d'un client, où un humain lit un écran. Un serveur qui
écrit deux flux oblige l'administrateur à deux outils pour lire une seule vie du
processus.

Le niveau se règle par `--log-level`, sur les quatre rôles :

```bash
python -m lds                      --log-level DEBUG
python -m gds                      --log-level DEBUG
python thermo-plc/plc_server.py    --log-level DEBUG
python protect-plc/plc_server.py   --log-level DEBUG
```

Niveaux acceptés : `TRACE`, `DEBUG`, `INFO` (défaut), `WARNING`, `ERROR`,
`CRITICAL`. Un niveau inconnu est **refusé à la ligne de commande** : un
`--log-level VERBOSE` accepté puis jamais atteint laisserait croire à un
réglage qu'il n'a pas.

**Deux formats, pour deux lecteurs.** `sciicad/console.py` expose deux fonctions
et non une avec un paramètre de plus : `setup()` pour les outils de diagnostic,
au message seul, parce qu'un rapport n'a pas besoin d'horodatage ligne à ligne ;
`setup_server()` pour les rôles en service, horodaté et gradé, parce que
l'exploitant lit une trace a posteriori et a besoin du *quand* et de la
gravité. Appliquer le format nu des outils à un serveur ferait perdre
l'information la plus utile en cas d'incident.

Les deux `import logging` qui subsistent — dans `sciicad/console.py` et
`lds/services.py` — ne sont pas des exceptions à la règle mais son **mécanisme** :
ils redirigent les journaux d'asyncua vers loguru. Les retirer ferait revenir la
pile vers le module `logging` de la bibliothèque standard, et le serveur
émettrait alors deux flux de formats différents. Ce ne sont pas des lignes à
« nettoyer ».

> L'invariant est vérifié sur les **sources**, pas sur une exécution : un
> `print` ajouté dans une branche rare ne se voit pas en lançant le serveur.
> `tools/selftest_lds.py` parcourt `lds/`, `gds/`, `thermo-plc/`,
> `protect-plc/` et `sciicad/`, et signale le fichier et la ligne. Un motif naïf
> signalait `thumbprint(` ; le contrôle écarte le cas où `print(` n'est pas en
> début de ligne.

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