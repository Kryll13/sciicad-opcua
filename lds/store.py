"""Persistance SQLite du registre des serveurs du LDS.

La norme OPC UA ne définit aucun service de désenregistrement : un serveur
doit se ré-enregistrer périodiquement (au plus tard toutes les 10 minutes) et
c'est au LDS de décider quand une entrée est périmée. Ce module conserve donc
l'historique de ré-enregistrement pour que cette décision soit possible, et
pour que le registre survive au redémarrage du serveur de découverte.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Optional

from loguru import logger

SCHEMA = """
CREATE TABLE IF NOT EXISTS registered_servers (
    application_uri      TEXT PRIMARY KEY,
    product_uri          TEXT,
    application_name     TEXT,
    application_type     INTEGER NOT NULL DEFAULT 0,
    gateway_server_uri   TEXT,
    discovery_urls       TEXT NOT NULL DEFAULT '[]',
    is_online            INTEGER NOT NULL DEFAULT 1,
    first_registered_at  REAL NOT NULL,
    last_registered_at   REAL NOT NULL,
    register_count       INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_last_registered
    ON registered_servers (last_registered_at);

CREATE TABLE IF NOT EXISTS registry_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp       REAL NOT NULL,
    application_uri TEXT,
    action          TEXT NOT NULL,
    detail          TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_ts
    ON registry_events (timestamp);
"""


class ServerStore:
    """Registre persistant, sûr pour un usage multi-thread.

    SQLite est synchrone et les écritures sont très courtes (de l'ordre de la
    milliseconde pour une dizaine de serveurs). Le store est malgré tout appelé
    via ``asyncio.to_thread`` par le registre, et protégé par un verrou, afin
    de ne jamais bloquer la boucle d'événements.
    """

    def __init__(self, path: str | Path, event_log: bool = True) -> None:
        self.path = Path(path)
        self.event_log_enabled = event_log
        self._lock = threading.Lock()

        if self.path.parent and str(self.path.parent) not in ("", "."):
            self.path.parent.mkdir(parents=True, exist_ok=True)

        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()
        logger.info(f"Registre LDS persisté dans {self.path}")

    # -- écritures ---------------------------------------------------------

    def upsert(
        self,
        *,
        application_uri: str,
        product_uri: Optional[str],
        application_name: Optional[str],
        application_type: int,
        gateway_server_uri: Optional[str],
        discovery_urls: list[str],
        is_online: bool = True,
    ) -> None:
        """Insère ou met à jour une entrée, et réinitialise son horodatage.

        Appelé à chaque ``RegisterServer``. Le compteur et l'horodatage de
        première inscription sont conservés : ils servent de diagnostic.
        """
        now = time.time()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO registered_servers (
                    application_uri, product_uri, application_name,
                    application_type, gateway_server_uri, discovery_urls,
                    is_online, first_registered_at, last_registered_at,
                    register_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(application_uri) DO UPDATE SET
                    product_uri         = excluded.product_uri,
                    application_name    = excluded.application_name,
                    application_type    = excluded.application_type,
                    gateway_server_uri  = excluded.gateway_server_uri,
                    discovery_urls      = excluded.discovery_urls,
                    is_online           = excluded.is_online,
                    last_registered_at  = excluded.last_registered_at,
                    register_count      = registered_servers.register_count + 1
                """,
                (
                    application_uri,
                    product_uri,
                    application_name,
                    int(application_type),
                    gateway_server_uri,
                    json.dumps(discovery_urls),
                    1 if is_online else 0,
                    now,
                    now,
                ),
            )
            self._conn.commit()
        logger.debug(f"Registre: {application_uri} enregistré ({', '.join(discovery_urls)})")

    def delete(self, application_uri: str) -> bool:
        """Supprime une entrée. Retourne True si une ligne a été retirée."""
        with self._lock:
            cursor = self._conn.execute(
                "DELETE FROM registered_servers WHERE application_uri = ?",
                (application_uri,),
            )
            self._conn.commit()
            return cursor.rowcount > 0

    def mark_all_stale(self) -> int:
        """Marque toutes les entrées comme hors ligne.

        Appelé au démarrage : les entrées rechargées depuis le disque n'ont pas
        encore renewé leur enregistrement, elles ne sont donc pas confirmées.
        """
        with self._lock:
            cursor = self._conn.execute("UPDATE registered_servers SET is_online = 0")
            self._conn.commit()
            return cursor.rowcount

    def purge_expired(self, ttl_seconds: float) -> list[str]:
        """Supprime les entrées dont le dernier renouvellement est trop ancien.

        C'est le mécanisme qui remplace l'absent ``UnregisterServer`` : un
        serveur qui ne se ré-enregistre plus est considéré comme arrêté.
        """
        cutoff = time.time() - ttl_seconds
        with self._lock:
            rows = self._conn.execute(
                "SELECT application_uri FROM registered_servers WHERE last_registered_at < ?",
                (cutoff,),
            ).fetchall()
            stale = [row["application_uri"] for row in rows]
            if stale:
                self._conn.executemany(
                    "DELETE FROM registered_servers WHERE application_uri = ?",
                    [(uri,) for uri in stale],
                )
                self._conn.commit()
        return stale

    # -- lectures ----------------------------------------------------------

    def load_all(self) -> list[dict[str, Any]]:
        """Retourne toutes les entrées, de la plus récemment vue à la plus ancienne."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM registered_servers ORDER BY last_registered_at DESC"
            ).fetchall()
        return [dict(row) for row in rows]

    def count(self) -> int:
        with self._lock:
            return int(self._conn.execute("SELECT COUNT(*) FROM registered_servers").fetchone()[0])

    def last_seen(self, application_uri: str) -> Optional[float]:
        """Retourne l'horodatage du dernier renouvellement connu, ou None."""
        with self._lock:
            row = self._conn.execute(
                "SELECT last_registered_at FROM registered_servers WHERE application_uri = ?",
                (application_uri,),
            ).fetchone()
        return float(row["last_registered_at"]) if row else None

    # -- journalisation ----------------------------------------------------

    def log_event(
        self,
        action: str,
        application_uri: Optional[str] = None,
        detail: Optional[str] = None,
    ) -> None:
        """Ajoute une ligne au journal des événements du registre."""
        if not self.event_log_enabled:
            return
        with self._lock:
            self._conn.execute(
                "INSERT INTO registry_events (timestamp, application_uri, action, detail) "
                "VALUES (?, ?, ?, ?)",
                (time.time(), application_uri, action, detail),
            )
            self._conn.commit()

    def recent_events(self, limit: int = 20) -> list[dict[str, Any]]:
        """Retourne les derniers événements du registre (diagnostic)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM registry_events ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(row) for row in rows]

    def close(self) -> None:
        with self._lock:
            self._conn.close()
