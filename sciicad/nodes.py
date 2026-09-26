"""Lecture de l'espace d'adressage OPC UA.

Les nœuds sont résolus par **browse name** et non par identifiant numérique :
asyncua attribue ces identifiants selon l'ordre de création, ils ne sont donc
pas stables d'un redémarrage à l'autre. Voir ``docs/depannage.md``.
"""

from __future__ import annotations

from typing import Any, Optional

from asyncua import Client, Node, ua

# Sous-arbres de l'espace d'adressage standard OPC UA : présents sur tout
# serveur, sans rapport avec les données métier, et très verbeux à afficher.
STANDARD_TREES = frozenset(
    {"Server", "Aliases", "Locations", "Types", "Views", "ServerDiagnostics", "Zv"}
)

# Masques de l'attribut AccessLevel (OPC UA Part 3, AccessLevelType).
ACCESS_READ = 0x01
ACCESS_WRITE = 0x02


async def find_node_by_name(parent_node: Node, name: str) -> Optional[Node]:
    """Trouve un nœud enfant par son browse name, ou ``None``."""
    try:
        children = await parent_node.get_children()
    except Exception:
        return None
    for child in children:
        try:
            if (await child.read_browse_name()).Name == name:
                return child
        except Exception:
            continue
    return None


async def find_by_path(root: Node, path: str) -> Optional[Node]:
    """Résout un chemin pointé depuis ``root``, ex ``Thermostat.Heating``.

    Chaque composant est résolu par browse name. Retourne ``None`` si un
    quelconque maillon manque : un chemin affiché par ``list_children`` peut
    devenir invalide si le serveur change entre deux appels.
    """
    node = root
    for part in path.split("."):
        if not part:
            continue
        node = await find_node_by_name(node, part)
        if node is None:
            return None
    return node


async def read_scalar(node: Node, attribute_id: int) -> Any:
    """Lit un attribut et le dépouille jusqu'à une valeur Python.

    ``read_attribute`` renvoie un ``DataValue`` dont ``Value`` est un
    ``Variant`` : il faut déballer les deux. ``get_value`` renvoie déjà la
    valeur dépouillée, ne pas lui ajouter ``.Value``.
    """
    value = (await node.read_attribute(attribute_id)).Value
    while isinstance(value, ua.Variant):
        value = value.Value
    return value


async def read_access(node: Node) -> tuple[bool, bool]:
    """Retourne (lecture, écriture) pour un nœud, d'après son AccessLevel."""
    try:
        level = await read_scalar(node, ua.AttributeIds.AccessLevel)
    except Exception:
        return (False, False)
    return (bool(level & ACCESS_READ), bool(level & ACCESS_WRITE))


async def access_label(node: Node) -> str:
    """Retourne « lecture/écriture » ou « lecture seule » pour l'affichage."""
    can_read, can_write = await read_access(node)
    if can_read and can_write:
        return "lecture/écriture"
    return "lecture seule" if can_read else "accès refusé"


async def read_child_value(parent_node: Node, name: str, default: Any = None) -> Any:
    """Lit la valeur d'un enfant par browse name, avec valeur de repli.

    Ne distingue pas « absent » de « illisible » : les deux retourne le
    défaut. À n'utiliser que pour l'affichage, pas pour une décision.
    """
    node = await find_node_by_name(parent_node, name)
    if node is None:
        return default
    try:
        return await node.get_value()
    except Exception:
        return default


async def read_child_values(parent_node: Node, names: list[str]) -> dict[str, Any]:
    """Lit plusieurs enfants par browse name et retourne un dictionnaire."""
    values: dict[str, Any] = {}
    for name in names:
        values[name] = await read_child_value(parent_node, name)
    return values


async def list_children(
    node: Node,
    depth: int = 1,
    prefix: str = "",
    skip_roots: frozenset = frozenset(),
) -> list[tuple[str, Any]]:
    """Parcourt l'espace d'adressage et retourne ``[(chemin, valeur)]``.

    Résiste aux erreurs de lecture nœud par nœud : un espace d'adressage
    partiellement illisible reste exploitable.
    """
    if depth <= 0:
        return []
    found: list[tuple[str, Any]] = []
    try:
        children = await node.get_children()
    except Exception:
        return found
    for child in children:
        try:
            browse = await child.read_browse_name()
        except Exception:
            continue
        if not prefix and browse.Name in skip_roots:
            continue
        path = f"{prefix}{browse.Name}"
        try:
            value = await child.get_value()
        except Exception:
            value = None
        found.append((path, value))
        found.extend(await list_children(child, depth - 1, f"{path}.", skip_roots))
    return found


async def server_array(client: Client) -> Optional[str]:
    """Retourne l'URI d'application déclarée par le nœud Server.

    Résolue par browse name : les identifiants numériques des propriétés du
    nœud Server varient selon la pile, le nom de browse est stable.
    """
    try:
        node = client.get_node(ua.NodeId(ua.ObjectIds.Server))
        children = await node.get_children()
    except Exception:
        return None
    for child in children:
        try:
            if (await child.read_browse_name()).Name != "ServerArray":
                continue
            value = await child.get_value()
        except Exception:
            continue
        if isinstance(value, (list, tuple)) and value:
            return str(value[0])
        return str(value)
    return None
