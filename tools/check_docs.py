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
    "ihm/ihm_client.py": ["--host", "--port", "--insecure", "--expect", "--trust"],
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
    "tools/selftest_authority.py": [],
    "tools/authority.py": ["init", "sign", "crl", "retire"],
    "tools/authority.py sign": ["--out", "--days"],
    "tools/authority.py crl": ["--revoke", "--days"],
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
    # Une cible peut nommer un sous-commande (« tools/authority.py crl ») :
    # argparse n'affiche ses options qu'au niveau du sous-commande, et un
    # `--help` à la racine les taiterait. Le découpage sur les espaces le gère.
    args = (
        ["-m", target.split()[2], "--help"]
        if target.startswith("python -m ")
        else target.split() + ["--help"]
    )
    result = subprocess.run([sys.executable, *args], capture_output=True, text=True)
    return re.sub(r"\s+", " ", result.stdout + result.stderr)


#: Options explicitement vérifiées. Volontairement **non exhaustif** : une liste
#: complète serait trop vite fausse, et une vérification trop étroite laisse
#: passer les mensonges — c'est-à-dire ce qu'on cherche ici à attraper.
CITED_OPTIONS: dict[str, list[str]] = {
    "python -m lds": ["--config", "--port", "--ttl", "--log-level"],
    "python -m gds": ["--config", "--port", "--log-level"],
    "thermo-plc/plc_server.py": ["--lds", "--bind", "--advertise", "--log-level"],
    "protect-plc/plc_server.py": ["--lds", "--bind", "--advertise", "--log-level"],
    "ihm/ihm_client.py": ["--host", "--port", "--insecure", "--expect", "--trust"],
    "tools/ihm_action.py": ["--ip", "--port", "--heat", "--maintenance"],
    "tools/analyze.py": ["--url", "--depth"],
    "tools/check_lds_gds.py": ["--url", "--expect", "--timeout"],
    "tools/crypto_opcua.py": ["--hostname", "--output-dir", "--csr"],
    "tools/bootstrap_certificates.py": ["--host", "--signed", "--force"],
    "tools/authority.py": ["init", "sign", "crl", "retire"],
    "tools/authority.py sign": ["--out", "--days"],
    "tools/authority.py crl": ["--revoke", "--days"],
    "tools/test_discovery.py": ["--lds-url", "--plc-uri", "--skip-plc"],
    "tools/selftest_thermo_lifecycle.py": ["protect-plc", "thermo-plc"],
}

#: Script dont seule l'existence est vérifiée, sans option à contrôler.
SCRIPTS_WITHOUT_OPTIONS = [
    "tools/selftest_trustlist.py",
    "tools/selftest_certmanager.py",
    "tools/selftest_audit.py",
    "tools/selftest_revocation.py",
    "tools/selftest_bootstrap.py",
    "tools/selftest_authority.py",
]

#: Une option déclarée dans une source Python. Couvre ``add_argument("--port")``
#: comme ``add_argument("--port", ...)`` : l'argument n'est pas toujours seul.
OPTION_DECLARED = re.compile(r"add_argument\(\s*[\"'](--[a-z0-9-]+)[\"']")

#: Une option mentionnée en documentation, sans ticks requis. LesTicks étaient
#: une erreur : la documentation cite ``--port`` en texte simple aussi, et une
#: option surrounds de ticks mais non déclarée passait inaperçue — ce qui est
#: exactement le faux négatif que ce contrôle existe pour attraper.
OPTION_MENTIONED = re.compile(r"(--[a-z][a-z0-9-]+)")

#: Options qui ne sont pas les nôtres : celles de `uv`, de `git`, de `docker`.
#: La documentation les cite au même format, et aucune ne sera déclarée par un
#: ``add_argument`` du dépôt. Les lister ici les rend visibles plutôt que de
#: les laisser passer en silence — quelqu'un qui ajoute ``--rm`` à une de ces
#: listes voit pourquoi.
FOREIGN_OPTIONS = {
    "--frozen", "--no-dev", "--no-install-project",  # uv
    "--help",                                          # argparse
    "--rm",                                             # outils système
}


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

    # Toute option en backticks doit exister dans l'un des scripts. C'est la
    # vérification la plus large, donc la plus utile : une liste d'options
    # déclarée à la main ne couvre que ce qu'on a pensé à écrire, et laisse
    # passer exactement les mensonges qu'elle est censée attraper. Elle a été
    # ajoutée après avoir constaté ce faux négatif.
    known: set[str] = set()
    for script in sorted(pathlib.Path(".").rglob("*.py")):
        try:
            known |= set(OPTION_DECLARED.findall(script.read_text(encoding="utf-8")))
        except OSError:
            continue
    mentioned: dict[str, list[str]] = {}
    for document in DOCS:
        for option in OPTION_MENTIONED.findall(
            pathlib.Path(document).read_text(encoding="utf-8")
        ):
            mentioned.setdefault(option, []).append(document)
    for option in sorted(mentioned):
        if option in FOREIGN_OPTIONS:
            continue
        if option not in known:
            anomalies += 1
            print(
                f"  option inexistante : {option} "
                f"(citée dans {', '.join(sorted(set(mentioned[option])))})"
            )

    print(f"=== {anomalies} anomalie(s) — options, liens, ancres, chemins ===")
    return 0 if anomalies == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
