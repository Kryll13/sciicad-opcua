"""Identité d'application d'un serveur OPC UA.

``asyncua`` écrit la propriété ``ServerArray`` une seule fois, pendant
``init()``, avec son URI par défaut. Un appel ultérieur à
``set_application_uri()`` ne met à jour que ``NamespaceArray[1]`` : le nœud
``ServerArray`` conserve donc ``urn:freeopcua:python:server``, même après
avoir configuré l'URI du projet.

C'est un problème concret : ``ServerArray`` porte l'URI que le client utilise
pour rapprocher l'identité annoncée du certificat de l'application. Les
fonctions de ce module rétablissent la cohérence.
"""

from __future__ import annotations

from asyncua import Server, ua
from loguru import logger


async def set_application_identity(
    server: Server,
    application_uri: str,
    product_uri: str | None = None,
    server_name: str | None = None,
) -> None:
    """Fixe l'URI d'application et réaligne les propriétés du nœud Server.

    À appeler après ``Server.init()`` et avant ``Server.start()``.
    """
    await server.set_application_uri(application_uri)

    # ServerArray n'est écrit que par init(), avec l'URI par défaut.
    server_array = server.get_node(ua.NodeId(ua.ObjectIds.Server_ServerArray))
    await server_array.write_value(
        ua.DataValue(ua.Variant([application_uri], ua.VariantType.String))
    )

    if product_uri is not None:
        server.product_uri = product_uri
    if server_name is not None:
        server.name = server_name

    written = await _sync_server_property(server, "ProductUri", product_uri)
    written += await _sync_server_property(server, "ServerName", server_name)

    logger.info(f"Identité du serveur : {application_uri} (ServerArray réaligné)")
    if not written:
        # Ce n'est pas une anomalie : asyncua n'expose pas ProductUri ni
        # ServerName comme propriétés du nœud Server. Les valeurs restent
        #	positionnées côté serveur.
        logger.debug(
            "ProductUri/ServerName non exposés comme propriétés du nœud Server "
            "(attendu avec asyncua) ; valeurs conservées côté serveur"
        )


async def _sync_server_property(server: Server, browse_name: str, value: str | None) -> int:
    """Aligne une propriété du nœud Server si elle existe. Retourne 1 ou 0.

    La présence de ces propriétés dépend de l'espace d'adressage standard
    chargé ; leur absence n'est pas une erreur, seulement un alignement de
    moins.
    """
    if value is None:
        return 0
    try:
        server_node = server.get_node(ua.NodeId(ua.ObjectIds.Server))
        for child in await server_node.get_children():
            if (await child.read_browse_name()).Name != browse_name:
                continue
            await child.write_value(ua.DataValue(ua.Variant(value, ua.VariantType.String)))
            return 1
    except Exception as exc:
        logger.debug(f"Propriété {browse_name} non alignée : {exc}")
    return 0
