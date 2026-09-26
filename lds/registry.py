"""Registre des serveurs du LDS : mémoire, expiration et persistance.

asyncua fournit deja ``RegisterServer`` / ``FindServers`` via un simple
dictionnaire ``InternalServer._known_servers``. Ce module se place au-dessus
sans le remplacer, et lui ajoute ce qui manque pour un LDS exploitable :

* la persistance de chaque enregistrement (deleguee a :mod:`lds.store`) ;
* l'expiration des entrees qui ne se re-enregistrent plus ;
* ``FindServersOnNetwork``, que le serveur asyncua n'expose pas.

Pourquoi l'expiration ? La norme OPC UA ne definit aucun service de
desenregistrement d'un serveur aupres d'un serveur de decouverte. Un serveur
se re-enregistre periodiquement et, s'il cesse de le faire, le LDS doit en
deduire qu'il est arrete. Sans cette etape, le registre ne se vide jamais et
la decouverte continue d'annoncer des equipements eteints.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Optional

from asyncua import ua
from asyncua.server.internal_server import ServerDesc
from loguru import logger


def _to_int(value: Any, default: int = 0) -> int:
    """Convertit un enum, un Variant ou un int en int, sans lever d'exception."""
    if value is None:
        return default
    if isinstance(value, ua.Variant):
        return _to_int(value.Value, default)
    for attr in ("value", "Value"):
        if hasattr(value, attr):
            try:
                return int(getattr(value, attr))
            except (TypeError, ValueError):
                continue
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_str_list(raw: Any) -> list[str]:
    """Deserialise une colonne JSON contenant une liste de chaines."""
    if not raw:
        return []
    if isinstance(raw, (list, tuple)):
        return [str(item) for item in raw]
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return []
    return [str(item) for item in value] if isinstance(value, list) else []


def _capability_list(raw: Any) -> list[str]:
    """Normalise les capacites d'un serveur en liste de chaines.

    Les capacites arrivent soit comme une liste, soit comme une chaine
    (DelimiterSeparated selon le profil de decouverte), soit via un objet
    ExtensionObject. Tout ce qui n'est pas exploitable est ignore plutot que
    transmis tel quel au client.
    """
    if raw is None:
        return []
    if isinstance(raw, ua.Variant):
        return _capability_list(raw.Value)
    if isinstance(raw, (list, tuple, set)):
        return [str(item).strip() for item in raw if str(item).strip()]
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "replace")
    if isinstance(raw, str):
        return [part.strip() for part in raw.split(",") if part.strip()]
    inner = getattr(raw, "ServerCapabilities", None)
    if inner is not None:
        return _capability_list(inner)
    return []


def _normalise_capability_filter(raw: Any) -> list[str]:
    """Normalise le champ ``ServerCapabilityFilter`` en liste de chaines.

    Ce champ est une liste cote asyncua, avec ``[]`` pour defaut. Le convertir
    naivement en chaine donnerait ``'[]'``, c'est-a-dire un filtre non vide
    qui ne correspondrait a aucun serveur et viderait la reponse. Toute valeur
    vide doit donc signif « aucun filtre ».
    """
    if raw is None:
        return []
    if isinstance(raw, ua.Variant):
        return _normalise_capability_filter(raw.Value)
    if isinstance(raw, (list, tuple, set)):
        return [str(item).strip() for item in raw if str(item).strip()]
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "replace")
    text = str(raw).strip()
    return [text] if text and text not in ("[]", "None") else []


class ServerRegistry:
    """Etat du registre, avec ecriture dans le store.

    Le registre sert deux rôles, distingués par ``scope`` :

    * ``local`` (LDS) : une inscription n'est valable que jusqu'à expiration.
      Un serveur se ré-enregistre périodiquement ; faute de renouvellement,
      l'entrée est évacuée.
    * ``global`` (GDS) : une inscription est conservée jusqu'à retrait
      explicite. Rien n'expire, donc aucun renouvellement périodique n'est
      imposé au serveur inscrit. C'est la différence normative entre les deux
      rôles, et la seule : le câblage des services est identique.
    """

    def __init__(
        self,
        iserver: Any,
        store: Optional[Any] = None,
        ttl_seconds: float = 300.0,
        scope: str = "local",
    ) -> None:
        if scope not in ("local", "global"):
            raise ValueError(f"portée de registre inconnue : {scope!r}")
        self.iserver = iserver
        self.store = store
        self.ttl_seconds = float(ttl_seconds)
        self.scope = scope

        # Dernier renouvellement observe en memoire. Ce dict evite une lecture
        # SQLite par entree et par balayage ; le store reste la source de
        # verite durable.
        self._last_seen: dict[str, float] = {}

        # RecordId attribues aux entrees, monotoniquement croissants (clause
        # 5.5.3.1). Un identifiant n'est jamais reutilise apres expiration, et
        # le compteur repart a zero au demarrage du LDS : c'est ce que
        # signale LastCounterResetTime au client.
        self._record_counter = 0
        self._record_ids: dict[str, int] = {}
        self._counter_reset_time = datetime.now(timezone.utc)

        # Le registre doit etre atteignable depuis les patchs d'asyncua, qui
        # n'ont aucune autre reference vers ce serveur LDS.
        iserver._sciicad_registry = self

    # -- acces au dictionnaire d'asyncua -----------------------------------

    @property
    def _known(self) -> dict[str, ServerDesc]:
        return self.iserver._known_servers

    def application_uris(self) -> list[str]:
        return list(self._known.keys())

    def _assign_record_id(self, application_uri: str) -> int:
        """Attribue (ou reutilise) le RecordId d'une entree, de facon monotone."""
        existing = self._record_ids.get(application_uri)
        if existing is not None:
            return existing
        self._record_counter += 1
        self._record_ids[application_uri] = self._record_counter
        return self._record_counter

    def _record_id_of(self, application_uri: str) -> int:
        """RecordId d'une entree, en attribuant a la volee si necessaire."""
        return self._assign_record_id(application_uri)

    def self_uris(self) -> set:
        """URI d'application des endpoints de ce LDS lui-meme.

        Asyncua injecte ses propres endpoints dans ``_known_servers`` au
        demarrage, sans passer par ``register_server``. Sans traitement
        particulier ils seraient immediatement consideres perimes (aucun
        horodatage) puis evacues, et le LDS disparaitrait de sa propre reponse
        de decouverte. Le LDS ne se re-enregistre pas non plus aupres de
        lui-meme : ces entrees doivent donc etre exemptes d'expiration.
        """
        uris = set()
        for edp in getattr(self.iserver, "endpoints", None) or []:
            server = getattr(edp, "Server", None)
            if server is not None and server.ApplicationUri:
                uris.add(server.ApplicationUri)
        return uris

    def describe(self) -> list[dict[str, Any]]:
        """Retourne un apercu du registre, pour le diagnostic."""
        now = time.time()
        own = self.self_uris()
        return [
            {
                "application_uri": uri,
                "discovery_urls": list(getattr(desc.Server, "DiscoveryUrls", []) or []),
                "age_seconds": (
                    None if uri in own else round(now - self._last_seen.get(uri, now), 1)
                ),
                "expired": self.is_expired(uri, now),
                "own_endpoint": uri in own,
            }
            for uri, desc in self._known.items()
        ]

    # -- enregistrement -----------------------------------------------------

    def register(self, registered_server: ua.RegisteredServer, capabilities: Any = None) -> None:
        """Enregistre un serveur (appele par le patch de ``RegisterServer``).

        Cas particulier du passage a hors ligne : la norme OPC UA ne prevoit
        aucun service de desenregistrement. Un serveur qui s'arrete indique
        donc qu'il part hors ligne en appelant RegisterServer une derniere
        fois avec ``IsOnline = False`` (clause 5.5.5.1). L'entree est alors
        retiree immediatement du registre et de la base, sans attendre le TTL.

        Sinon, l'entree est d'abord ecrite par asyncua, puis persistee. Cet
        ordre garantit que la base ne contient jamais un serveur absent du
        registre memoire, meme si l'ecriture echoue.
        """
        application_uri = registered_server.ServerUri
        if not application_uri:
            logger.warning("RegisterServer recu sans ServerUri : ignore")
            return

        if not registered_server.IsOnline:
            self._go_offline(application_uri)
            return

        names = list(registered_server.ServerNames or [])
        name = names[0].Text if names and names[0].Text else None

        self._last_seen[application_uri] = time.time()
        self._assign_record_id(application_uri)

        if self.store is None:
            return

        try:
            self.store.upsert(
                application_uri=application_uri,
                product_uri=registered_server.ProductUri,
                application_name=name,
                application_type=_to_int(registered_server.ServerType),
                gateway_server_uri=registered_server.GatewayServerUri,
                discovery_urls=list(registered_server.DiscoveryUrls or []),
                is_online=True,
            )
            self.store.log_event("register", application_uri, name)
        except Exception as exc:  # la persistance ne doit pas casser le service
            logger.error(f"Persistance de l'enregistrement {application_uri} echouee: {exc}")

    def _go_offline(self, application_uri: str) -> None:
        """Retire une entree signalee hors ligne par le serveur lui-meme."""
        removed = self._known.pop(application_uri, None)
        self._last_seen.pop(application_uri, None)
        self._record_ids.pop(application_uri, None)

        if removed is not None:
            logger.info(f"Desenregistrement recu de {application_uri} (IsOnline=False)")
            if self.store is not None:
                try:
                    self.store.log_event("unregister", application_uri, "IsOnline=False")
                    self.store.delete(application_uri)
                except Exception as exc:
                    logger.error(f"Suppression de {application_uri} echouee: {exc}")
        else:
            logger.debug(f"Desenregistrement de {application_uri} : entree inconnue")

    def unregister(self, application_uri: str) -> bool:
        """Retire une entree du registre. Retourne ``True`` si elle existait.

        Point d'entree du GDS, ou la Part 12 prevoit un retrait explicite sans
        passer par un signal protocolaire. Le LDS passe lui par
        ``RegisterServer`` avec ``IsOnline = False``, traite dans
        :meth:`register` : les deux chemins convergent ici.
        """
        if application_uri not in self._known:
            return False
        self._go_offline(application_uri)
        return True

    # -- expiration ---------------------------------------------------------

    def is_expired(self, application_uri: str, now: Optional[float] = None) -> bool:
        """Indique si une entree n'a pas ete renouee assez recemment.

        En portee ``global`` (GDS) rien n'expire : l'inscription vaut jusqu'au
        retrait explicite du serveur. Les endpoints du serveur de decouverte
        lui-meme ne sont jamais expires non plus : ils sont par definition en
        ligne tant que le processus tourne.
        """
        if self.scope == "global":
            return False
        if application_uri in self.self_uris():
            return False
        last = self._last_seen.get(application_uri)
        if last is None:
            return True
        return (now if now is not None else time.time()) - last > self.ttl_seconds

    async def sweep(self) -> list[str]:
        """Evacue les entrees perimees. Retourne la liste des URI retires.

        Appelee periodiquement en tache de fond, et aussi avant chaque
        FindServersOnNetwork, pour qu'aucun client ne recoive un endpoint mort.
        Sans effet en portee ``global`` : rien n'y expire.
        """
        if self.scope == "global":
            return []
        now = time.time()
        expired = [uri for uri in self.application_uris() if self.is_expired(uri, now)]
        if not expired:
            return []

        for uri in expired:
            self._known.pop(uri, None)
            self._last_seen.pop(uri, None)
            # L'identifiant n'est pas reutilise : un serveur qui revient
            # obtient un RecordId plus eleve, comme l'exige la clause 5.5.3.1.
            self._record_ids.pop(uri, None)

        if self.store is not None:
            try:
                self.store.log_event(
                    "expire",
                    None,
                    f"{len(expired)} entree(s) perimees apres {self.ttl_seconds:.0f}s",
                )
                for uri in expired:
                    self.store.delete(uri)
            except Exception as exc:
                logger.error(f"Journalisation de l'expiration echouee: {exc}")

        logger.info(
            f"Expiration : {len(expired)} entree(s) retiree(s) apres "
            f"{self.ttl_seconds:.0f}s sans renouvellement -> {', '.join(expired)}"
        )
        return expired

    # -- restauration -------------------------------------------------------

    async def restore(self) -> int:
        """Recharge le registre depuis le store au demarrage.

        En portee ``local``, les entrees restaurees sont soumises au TTL : elles
        ne sont pas « ressuscitees », elles sont reconnues jusqu'a ce qu'un
        serveur se re-enregistre ou que le balayage les retire.

        En portee ``global`` (GDS) elles font foi et restent en place : c'est le
        sens d'un registre global, qui survit au redémarrage du service.

        Les endpoints propres du serveur de decouverte ne sont jamais ecrases.
        """
        if self.store is None:
            return 0

        try:
            rows = self.store.load_all()
            self.store.mark_all_stale()
        except Exception as exc:
            logger.error(f"Restauration du registre impossible : {exc}")
            return 0

        now = time.time()
        restored = 0
        for row in rows:
            application_uri = row.get("application_uri")
            if not application_uri or application_uri in self._known:
                continue

            description = ua.ApplicationDescription()
            description.ApplicationUri = application_uri
            description.ProductUri = row.get("product_uri") or ""
            description.ApplicationName = ua.LocalizedText(
                "en", row.get("application_name") or application_uri
            )
            description.ApplicationType_ = _to_int(row.get("application_type"))
            description.GatewayServerUri = row.get("gateway_server_uri")
            description.DiscoveryUrls = _to_str_list(row.get("discovery_urls"))

            # Horodatage d'origine : si l'entree est plus ancienne que le TTL,
            # le prochain balayage l'elimine. Test explicite contre None car
            # `or now` ressusciterait une entree dont l'horodatage vaut 0.
            persisted = row.get("last_registered_at")
            self._last_seen[application_uri] = (
                float(persisted) if persisted is not None else now
            )
            self._known[application_uri] = ServerDesc(description, None)
            restored += 1

        if restored:
            logger.info(
                f"Registre restaure depuis {self.store.path} : {restored} entree(s) "
                "en attente de renouvellement"
            )
        return restored

    # -- FindServersOnNetwork ----------------------------------------------

    def find_servers_on_network(
        self,
        starting_record_id: int = 0,
        max_records: int = 0,
        capability_filter: Optional[Any] = None,
        sockname: Optional[tuple] = None,
    ) -> ua.FindServersOnNetworkResult:
        """Construit la reponse ``FindServersOnNetwork``.

        Chaque element est un ``ServerOnNetwork`` : nom, une seule URL de
        decouverte et les capacites, avec un ``RecordId`` numerique. Le
        registre attribue des RecordId consecutifs a partir de 1, dans un
        ordre stable, et ne les reutilise jamais.

        ``sockname`` est l'adresse source du client : comme pour
        ``FindServers``, les endpoints appartenant au LDS lui-meme sont
        reecrits pour etre joignables depuis le client. Sans cela, cette
        reponse exposerait le hostname brut du LDS alors que ``FindServers``
        renverrait l'IP du client, ce qui est incoherent.
        """
        wanted = _normalise_capability_filter(capability_filter)
        own = self.self_uris()

        entries: list[ua.ServerOnNetwork] = []
        for uri, desc in self._known.items():
            if desc.Server is None or not self._matches_capability(desc, wanted):
                continue
            entry = self._to_server_on_network(desc, self._record_id_of(uri))
            if uri in own:
                entry.DiscoveryUrl = self._mangle_url(entry.DiscoveryUrl, sockname)
            entries.append(entry)

        # Ordre numerique impose par la clause 5.5.3.1 : le client enchaine
        # les pages sur le dernier RecordId recu.
        entries.sort(key=lambda e: e.RecordId or 0)

        # Pagination : StartingRecordId 0 = premier enregistrement, sinon
        # uniquement les identifiants superieurs.
        if starting_record_id:
            entries = [e for e in entries if (e.RecordId or 0) > starting_record_id]
        if max_records and max_records > 0:
            entries = entries[:max_records]

        return ua.FindServersOnNetworkResult(
            LastCounterResetTime=self._counter_reset_time,
            Servers=entries,
        )

    def _mangle_url(self, url: str, sockname: Optional[tuple]) -> str:
        """Reecrit une URL de decouverte pour la rendre joignable.

        Delegue a la meme logique qu'asyncua pour ``FindServers``, afin que
        les deux services de decouverte restent coherents entre eux.
        """
        if not url or sockname is None:
            return url
        mangle = getattr(self.iserver, "_mangle_endpoint_url", None)
        if mangle is None:
            return url
        try:
            return mangle(url, sockname=sockname)
        except Exception as exc:  # la reponse ne doit pas echouer sur une URL
            logger.debug(f"Reecriture d'URL impossible pour {url}: {exc}")
            return url

    @staticmethod
    def _to_server_on_network(desc: ServerDesc, record_id: int) -> ua.ServerOnNetwork:
        """Convertit une entree du registre en ``ServerOnNetwork``."""
        server = desc.Server
        urls = [str(url) for url in (getattr(server, "DiscoveryUrls", None) or [])]
        return ua.ServerOnNetwork(
            RecordId=record_id,
            ServerName=server.ApplicationUri or "",
            DiscoveryUrl=urls[0] if urls else "",
            ServerCapabilities=_capability_list(getattr(desc, "Capabilities", None)),
        )

    @staticmethod
    def _matches_capability(desc: ServerDesc, wanted: list[str]) -> bool:
        """Applique le filtre de capacite demande par le client.

        Un filtre vide ou absent ne retire rien. Sinon un serveur est retenu
        si l'une des chaines demandees apparait dans ses capacites. Les
        entrees sans capacite declaree (le LDS lui-meme, les serveurs
        enregistres via RegisterServer simple) ne survivront pas a un filtre
        non vide, ce qui est conforme : ils n'ont rien a annoncer.
        """
        if not wanted:
            return True
        capabilities = getattr(desc, "Capabilities", None)
        if not capabilities:
            return False
        blob = str(capabilities)
        return any(item in blob for item in wanted)
