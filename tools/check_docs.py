"""Vérification automatique de la documentation.

Quatre familles d'anomalies, toutes vérifiables sans intervention :

* **options** — toute option citée doit exister dans ``--help`` du script cité ;
  c'est ce qui rattrape une option renommée dont la documentation ment encore ;
* **liens** — tout lien relatif doit désigner un fichier présent ;
* **ancres** — toute ancre doit correspondre à un titre réel, selon les règles
  de GitHub ;
* **chemins** — tout chemin de fichier cité en clair doit exister.

Règle d'ancrage
---------------

GitHub compose une ancre en minuscules, **supprime** tout ce qui n'est ni
lettre, ni chiffre, ni espace, ni tiret, puis remplace les espaces par des
tirets. Deux conséquences qui trompent régulièrement :

* ``§`` est **supprimé**, pas converti en tiret ;
* les accents sont **conservés** (GitHub ne translittère pas).

Convertir ``§`` en ``-`` — ce qu'un vérificateur trop zélé ferait — produit un
tiret supplémentaire et déclare morte une ancre parfaitement valide. Ce
vérificateur a été corrigé après avoir signalé à tort deux ancres existantes.

    python tools/check_docs.py
"""

from __future__ import annotations

import pathlib
import re
import subprocess
import sys

DOCS = ["README.md"] + sorted(str(p) for p in pathlib.Path("docs").glob("*.md"))

#: Options citées dans la documentation, par script. Volontairement
#: restrictif : n'y lister que ce que la documentation affirme.
CITED_OPTIONS: dict[str, list[str]] = {
    "python -m lds": [
        "--config", "--port", "--bind", "--advertise", "--ttl",
        "--database", "--no-database", "--log-level",
    ],
    "python -m gds": [
        "--config", "--port", "--bind", "--advertise",
        "--database", "--no-database", "--log-level",
    ],
    "thermo-plc/plc_server.py": ["--lds", "--bind", "--advertise", "--port", "--log-level"],
    "protect-plc/plc_server.py": ["--lds", "--bind", "--advertise", "--port", "--log-level"],
    "ihm/ihm_client.py": ["--host", "--port"],
    "tools/ihm_action.py": ["--ip", "--port", "--heat", "--maintenance"],
    "tools/analyze.py": ["--url", "--depth"],
    "tools/check_lds_gds.py": ["--url", "--expect", "--timeout"],
    "tools/crypto_opcua.py": [
        "--hostname", "--output-dir", "--key-size", "--validity-days",
        "--application-uri",
    ],
    "tools/bootstrap_certificates.py": ["--host", "--key-size", "--validity-days", "--force"],
    "tools/test_discovery.py": [
        "--lds-url", "--plc-uri", "--skip-plc", "--wait", "--timeout", "--depth",
    ],
    "tools/selftest_thermo_lifecycle.py": ["protect-plc", "thermo-plc"],
    "tools/selftest_trustlist.py": [],
    "tools/selftest_certmanager.py": [],
    "tools/selftest_audit.py": [],
    "tools/selftest_revocation.py": [],
    "tools/selftest_bootstrap.py": [],
}

#: Extensions reconnues dans une mention de chemin en clair.
PATH_PATTERN = re.compile(
    r"\b(?:docs|tools|lds|gds|ihm|sciicad|pki)/[a-zA-Z0-9_./-]+\.(?:md|py|yaml|pem)\b"
)


def slug(title: str) -> str:
    """Ancre GitHub d'un titre.

    ``§`` disparaît, il ne devient pas un tiret ; les accents restent. Le
    tiret cadratin disparaît lui aussi, ce qui laisse deux espaces consécutifs
    et donc un **double** tiret — d'où la préférence du dépôt pour les titres
    entre parenthèses, dont l'ancre reste à un seul tiret.
    """
    lowered = title.lower()
    kept = re.sub(r"[^\w\s-]", "", lowered, flags=re.UNICODE)
    return re.sub(r"\s+", "-", kept.strip())


def help_text(target: str) -> str:
    args = (
        ["-m", target.split()[2], "--help"]
        if target.startswith("python -m ")
        else [target, "--help"]
    )
    result = subprocess.run([sys.executable, *args], capture_output=True, text=True)
    return re.sub(r"\s+", " ", result.stdout + result.stderr)


def main() -> int:
    anomalies = 0

    for target, cited in CITED_OPTIONS.items():
        output = help_text(target)
        missing = [option for option in cited if option not in output]
        if missing:
            anomalies += 1
            print(f"  options {target} : {', '.join(missing)}")

    for document in DOCS:
        base = pathlib.Path(document).parent
        body = pathlib.Path(document).read_text(encoding="utf-8")
        for target, anchor in re.findall(
            r"\]\((?!https?:)([^)#]+)(?:#([^)]+))?\)", body
        ):
            if not (base / target).resolve().exists():
                anomalies += 1
                print(f"  lien mort {document} -> {target}")
                continue
            if not anchor:
                continue
            titles = [
                slug(match.group(1))
                for match in re.finditer(r"^#{1,4}\s+(.*)$",
                                         (base / target).read_text(encoding="utf-8"),
                                         re.M)
            ]
            if anchor not in titles:
                anomalies += 1
                print(f"  ancre morte {document} -> {target}#{anchor}")

    seen = set()
    for document in DOCS:
        seen |= set(PATH_PATTERN.findall(
            pathlib.Path(document).read_text(encoding="utf-8")
        ))
    for mention in sorted(seen):
        if not pathlib.Path(mention).exists():
            anomalies += 1
            print(f"  chemin introuvable : {mention}")

    print(f"=== {anomalies} anomalie(s) — options, liens, ancres, chemins ===")
    return 0 if anomalies == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
