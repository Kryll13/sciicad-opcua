#!/usr/bin/env python3
"""
Test complet du GDS - Phases 1, 2 et 3
Teste toutes les opérations disponibles sur le serveur GDS.

Ce fichier est autonome et ne dépend pas des autres fichiers du projet.
"""

import asyncio
import json
import sys
from typing import Optional, List, Dict, Any

from asyncua import Client, ua
from loguru import logger


# ============================================================================
# OPCUAClient - Client OPC-UA de base
# ============================================================================

class OPCUAClient:
    """Client OPC-UA de référence"""
    
    def __init__(self, server_url: str, username: Optional[str] = None, password: Optional[str] = None):
        self.server_url = server_url
        self.username = username
        self.password = password
        self.client: Optional[Client] = None
    
    async def connect(self):
        """Se connecte au serveur OPC-UA"""
        try:
            self.client = Client(self.server_url)
            
            if self.username and self.password:
                await self.client.set_user(self.username)
                await self.client.set_password(self.password)
            
            await self.client.connect()
            logger.info(f"Connecté au serveur {self.server_url}")
        except Exception as e:
            logger.error(f"Erreur de connexion: {e}")
            raise
    
    async def disconnect(self):
        """Se déconnecte du serveur"""
        if self.client:
            try:
                await self.client.disconnect()
                logger.info("Déconnecté")
            except Exception as e:
                logger.error(f"Erreur lors de la déconnexion: {e}")
    
    async def read_variable(self, node_id: str):
        """Lit la valeur d'une variable"""
        try:
            node = self.client.get_node(node_id)
            value = await node.read_value()
            return value
        except Exception as e:
            logger.error(f"Erreur de lecture: {e}")
            raise
    
    async def write_variable(self, node_id: str, value: Any):
        """Écrit une valeur dans une variable"""
        try:
            node = self.client.get_node(node_id)
            await node.write_value(value)
            logger.info(f"Valeur écrite: {node_id} = {value}")
        except Exception as e:
            logger.error(f"Erreur d'écriture: {e}")
            raise
    
    async def browse(self, node_id: str = "i=85") -> List[Dict[str, Any]]:
        """Parcourt les nœuds enfants."""
        try:
            node = self.client.get_node(node_id)
            children = []
            
            for child in await node.get_children():
                child_info = {
                    "id": child.nodeid.to_string(),
                    "name": (await child.read_browse_name()).Name,
                    "type": str(await child.read_node_class())
                }
                children.append(child_info)
            
            return children
        except Exception as e:
            logger.error(f"Erreur lors du parcours: {e}")
            raise
    
    async def call_method(self, object_id: str, method_id: str, *args) -> Any:
        """Appelle une méthode du serveur."""
        try:
            obj = self.client.get_node(object_id)
            method = self.client.get_node(method_id)
            
            result = await obj.call_method(method, *args)
            logger.info(f"Méthode appelée: {method_id}")
            
            return result
        except Exception as e:
            logger.error(f"Erreur lors de l'appel: {e}")
            raise
    
    async def subscribe(self, node_id: str, callback):
        """S'abonne aux changements d'une variable."""
        try:
            node = self.client.get_node(node_id)
            
            class SubHandler:
                def __init__(self, cb):
                    self.cb = cb
                
                def datachange_notification(self, node, val, data):
                    self.cb(node, val)
            
            handler = SubHandler(callback)
            sub = await self.client.create_subscription(100, handler)
            await sub.subscribe_data_change(node)
            
            logger.info(f"Abonnement créé pour {node_id}")
        except Exception as e:
            logger.error(f"Erreur lors de l'abonnement: {e}")
            raise


# ============================================================================
# GDSClient - Client spécialisé pour les serveurs Global Discovery Server
# ============================================================================

class GDSClient(OPCUAClient):
    """Client spécialisé pour les serveurs Global Discovery Server"""
    
    async def get_applications(self) -> List[Dict[str, Any]]:
        """Récupère la liste des applications enregistrées"""
        try:
            gds_node = self.client.get_node(ua.NodeId(1002, 0))  # GlobalDiscoveryServer folder
            
            logger.info(f"Found GDS node: {gds_node.nodeid}")
            
            # Try to find QueryApplications method
            logger.info("Browsing GDS folder children...")
            gds_children = []
            for gds_child in await gds_node.get_children():
                gds_browse_name = await gds_child.read_browse_name()
                node_class = str(await gds_child.read_node_class())
                gds_children.append({
                    "nodeid": gds_child.nodeid.to_string(),
                    "browse_name": gds_browse_name.Name,
                    "node_class": node_class
                })
                logger.info(f"  GDS child: {gds_child.nodeid.to_string()} - {gds_browse_name.Name} ({node_class})")
            
            # Look for QueryApplications method
            query_method = None
            for gds_child in gds_children:
                if gds_child["browse_name"] == "QueryApplications":
                    query_method = self.client.get_node(gds_child["nodeid"])
                    logger.info(f"Found QueryApplications method: {gds_child['nodeid']}")
                    break
            
            if query_method:
                # Call QueryApplications with filter (empty string for no filter)
                result = await gds_node.call_method(query_method, 0, 100, "")
                if result:
                    if isinstance(result, list) and len(result) > 0:
                        json_str = result[0]
                        if hasattr(json_str, 'Value'):
                            json_str = json_str.Value
                        apps = json.loads(json_str)
                        logger.info(f"Applications found: {len(apps)}")
                        return apps
                    else:
                        apps = json.loads(result)
                        logger.info(f"Applications found: {len(apps)}")
                        return apps
            return []
            
        except Exception as e:
            logger.error(f"Error retrieving applications: {e}")
            raise
    
    async def register_application(
        self,
        app_name: str,
        app_uri: str,
        app_type: str = "Server",
        endpoints: Optional[List[str]] = None
    ) -> str:
        """Enregistre une nouvelle application au GDS."""
        try:
            gds_node = self.client.get_node(ua.NodeId(1002, 0))
            
            # Find RegisterApplication method
            for child in await gds_node.get_children():
                browse_name = await child.read_browse_name()
                if browse_name.Name == "RegisterApplication":
                    register_method = child
                    break
            else:
                raise Exception("RegisterApplication method not found")
            
            app_record = json.dumps({
                "ApplicationUri": app_uri,
                "ApplicationNames": [{"Locale": "en-US", "Text": app_name}],
                "ApplicationType": app_type
            })
            
            logger.debug(f"Tentative d'enregistrement - Nom: {app_name}, URI: {app_uri}")
            
            if not app_uri.startswith(('opc.tcp://', 'urn:', 'http://', 'https://')):
                logger.warning(f"L'URI fourni ne ressemble pas à une URI valide: {app_uri}")
            
            app_id = await gds_node.call_method(register_method, app_record)
            logger.info(f"Application enregistrée: {app_name} - ID: {app_id}")
            
            return app_id
        except Exception as e:
            logger.error(f"Erreur lors de l'enregistrement: {e}")
            raise
    
    async def unregister_application(self, app_id: str) -> bool:
        """Désenregistre une application."""
        try:
            gds_node = self.client.get_node(ua.NodeId(1002, 0))
            
            # Find UnregisterApplication method
            for child in await gds_node.get_children():
                browse_name = await child.read_browse_name()
                if browse_name.Name == "UnregisterApplication":
                    unregister_method = child
                    logger.debug(f"Méthode UnregisterApplication trouvée: {unregister_method.nodeid}")
                    break
            else:
                raise Exception("UnregisterApplication method not found")
            
            # Vérifier d'abord si l'application existe
            apps = await self.get_applications()
            app_exists = any(str(app.get('application_id', app.get('id', ''))).lower() == app_id.lower() for app in apps)
            logger.info(f"Tentative de désinscription de l'application: {app_id}")
            logger.info(f"Application existe sur le serveur: {app_exists}")
            
            if not app_exists:
                logger.warning(f"L'application {app_id} n'existe pas sur le serveur")
                return False
            
            await gds_node.call_method(unregister_method, app_id)
            logger.info(f"Application désenregistrée: {app_id}")
            
            return True
        except Exception as e:
            logger.error(f"Erreur lors de la désinscription: {e}")
            raise

    # =========================================================================
    # Certificate Management Methods (OPC-UA Part 12)
    # =========================================================================

    async def get_certificate(self, certificate_group_id: str = "", certificate_type_id: str = "") -> Optional[Dict[str, Any]]:
        """Récupère le certificat CA du GDS."""
        try:
            gds_node = self.client.get_node(ua.NodeId(1002, 0))
            
            get_cert_method = None
            for child in await gds_node.get_children():
                browse_name = await child.read_browse_name()
                if browse_name.Name == "GetCertificate":
                    get_cert_method = child
                    logger.info(f"GetCertificate method found: {get_cert_method.nodeid}")
                    break
            
            if not get_cert_method:
                raise Exception("GetCertificate method not found")
            
            result = await gds_node.call_method(get_cert_method, certificate_group_id, certificate_type_id)
            
            if result:
                return {
                    "certificate": result[0] if result else b"",
                    "issuerCertificates": [],
                    "crl": b""
                }
            return None
            
        except Exception as e:
            logger.error(f"Erreur lors de la récupération du certificat: {e}")
            raise

    async def create_certificate_request(
        self,
        application_id: str,
        certificate_group_id: str = "",
        certificate_type_id: str = "",
        subject_name: str = "",
        domain_names: str = "",
        alternative_names: str = "",
        request_type: str = "PKCS#10",
        key_length: int = 2048
    ) -> Dict[str, Any]:
        """Crée une demande de certificat (CSR)."""
        try:
            gds_node = self.client.get_node(ua.NodeId(1002, 0))
            
            create_csr_method = None
            for child in await gds_node.get_children():
                browse_name = await child.read_browse_name()
                if browse_name.Name == "CreateCertificateRequest":
                    create_csr_method = child
                    logger.info(f"CreateCertificateRequest method found: {create_csr_method.nodeid}")
                    break
            
            if not create_csr_method:
                raise Exception("CreateCertificateRequest method not found")
            
            result = await gds_node.call_method(
                create_csr_method,
                application_id,
                certificate_group_id,
                certificate_type_id,
                subject_name,
                domain_names,
                alternative_names,
                request_type
            )
            
            if result:
                request_id_raw = result[0]
                if hasattr(request_id_raw, 'Value'):
                    request_id = str(request_id_raw.Value)
                elif hasattr(request_id_raw, 'value'):
                    request_id = str(request_id_raw.value)
                else:
                    request_id = str(request_id_raw)
                logger.info(f"Certificate request created: {request_id}")
                return {"requestId": request_id, "status": "Pending"}
            return None
            
        except Exception as e:
            logger.error(f"Erreur lors de la création de la demande de certificat: {e}")
            raise

    async def get_certificate_status(self, request_id: str) -> Dict[str, Any]:
        """Récupère le statut d'une demande de certificat."""
        try:
            gds_node = self.client.get_node(ua.NodeId(1002, 0))
            
            get_status_method = None
            for child in await gds_node.get_children():
                browse_name = await child.read_browse_name()
                if browse_name.Name == "GetCertificateStatus":
                    get_status_method = child
                    logger.info(f"GetCertificateStatus method found: {get_status_method.nodeid}")
                    break
            
            if not get_status_method:
                raise Exception("GetCertificateStatus method not found")
            
            result = await gds_node.call_method(get_status_method, request_id)
            
            if result:
                status = str(result[0].Value) if hasattr(result[0], 'Value') else str(result[0])
                logger.info(f"Certificate status retrieved: {status}")
                return {"status": status}
            return None
            
        except Exception as e:
            logger.error(f"Erreur lors de la récupération du statut: {e}")
            raise

    async def approve_certificate_request(
        self,
        request_id: str,
        certificate: Optional[bytes] = None,
        issuer_certificate: Optional[bytes] = None,
        certificate_chain: Optional[bytes] = None
    ) -> Dict[str, Any]:
        """Approuve une demande de certificat."""
        try:
            gds_node = self.client.get_node(ua.NodeId(1002, 0))
            
            approve_method = None
            for child in await gds_node.get_children():
                browse_name = await child.read_browse_name()
                if browse_name.Name == "ApproveCertificateRequest":
                    approve_method = child
                    logger.info(f"ApproveCertificateRequest method found: {approve_method.nodeid}")
                    break
            
            if not approve_method:
                raise Exception("ApproveCertificateRequest method not found")
            
            result = await gds_node.call_method(
                approve_method,
                request_id,
                certificate or "",
                issuer_certificate or "",
                certificate_chain or ""
            )
            
            if result:
                if isinstance(result, bool):
                    success = result
                elif isinstance(result, list) and len(result) > 0:
                    success = result[0].Value if hasattr(result[0], 'Value') else result[0]
                else:
                    success = bool(result)
                logger.info(f"Certificate request approved: {success}")
                return {"approved": success}
            return None
            
        except Exception as e:
            logger.error(f"Erreur lors de l'approbation du certificat: {e}")
            raise
    
    # === Phase 3: Pull Certificate Management Methods ===
    
    async def start_get_certificate_changes(
        self,
        certificate_group_id: Optional[str] = None,
        certificate_type_id: Optional[str] = None
    ) -> Optional[str]:
        """Démarre le suivi des changements de certificats."""
        try:
            gds_node = self.client.get_node(ua.NodeId(1002, 0))
            
            start_changes_method = None
            for child in await gds_node.get_children():
                browse_name = await child.read_browse_name()
                if browse_name.Name == "StartGetCertificateChanges":
                    start_changes_method = child
                    logger.info(f"StartGetCertificateChanges method found: {start_changes_method.nodeid}")
                    break
            
            if not start_changes_method:
                logger.warning("StartGetCertificateChanges method not found")
                return None
            
            result = await gds_node.call_method(
                start_changes_method,
                ua.NodeId(certificate_group_id) if certificate_group_id else ua.NodeId(0),
                ua.NodeId(certificate_type_id) if certificate_type_id else ua.NodeId(0)
            )
            
            if result:
                subscription_id = str(result[0].Value) if hasattr(result[0], 'Value') else str(result[0])
                logger.info(f"Subscription started: {subscription_id}")
                return {"subscription_id": subscription_id}
            return None
            
        except Exception as e:
            logger.error(f"Erreur lors du démarrage du suivi: {e}")
            raise
    
    async def get_certificate_changes(self, subscription_id: str) -> tuple:
        """Récupère les changements de certificats."""
        try:
            gds_node = self.client.get_node(ua.NodeId(1002, 0))
            
            changes_method = None
            for child in await gds_node.get_children():
                browse_name = await child.read_browse_name()
                if browse_name.Name == "GetCertificateChanges":
                    changes_method = child
                    logger.info(f"GetCertificateChanges method found: {changes_method.nodeid}")
                    break
            
            if not changes_method:
                logger.warning("GetCertificateChanges method not found")
                return [], False
            
            result = await gds_node.call_method(changes_method, subscription_id)
            
            if result:
                changes_json = str(result[0].Value) if hasattr(result[0], 'Value') else str(result[0])
                more_changes = result[1].Value if hasattr(result[1], 'Value') else result[1]
                
                try:
                    changes_data = json.loads(changes_json)
                    logger.info(f"Retrieved {len(changes_data.get('changes', []))} changes")
                    return changes_data.get('changes', []), more_changes
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse changes JSON: {changes_json}")
                    return [], more_changes
            return [], False
            
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des changements: {e}")
            raise

    async def get_certificate_groups(self) -> list:
        """Récupère la liste des groupes de certificats."""
        try:
            gds_node = self.client.get_node(ua.NodeId(1002, 0))
            
            groups_method = None
            for child in await gds_node.get_children():
                browse_name = await child.read_browse_name()
                if browse_name.Name == "GetCertificateGroups":
                    groups_method = child
                    logger.info(f"GetCertificateGroups method found: {groups_method.nodeid}")
                    break
            
            if not groups_method:
                logger.warning("GetCertificateGroups method not found")
                return []
            
            result = await gds_node.call_method(groups_method)
            
            if result:
                groups = []
                
                for item in result:
                    if hasattr(item, 'Value'):
                        value = item.Value
                        if isinstance(value, str):
                            try:
                                group_data = json.loads(value)
                                groups.append(group_data)
                            except json.JSONDecodeError as e:
                                logger.warning(f"Failed to parse group JSON: {repr(value[:200])}, error: {e}")
                        elif isinstance(value, list):
                            groups.extend(value)
                    elif isinstance(item, str):
                        try:
                            group_data = json.loads(item)
                            groups.append(group_data)
                        except json.JSONDecodeError as e:
                            logger.warning(f"Failed to parse group JSON: {repr(item[:200])}, error: {e}")
                    elif isinstance(item, dict):
                        groups.append(item)
                
                logger.info(f"Retrieved {len(groups)} certificate groups")
                return groups
            
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des groupes: {e}")
            raise
        return []

    async def get_trust_lists(self, certificate_group_id: Optional[str] = None) -> list:
        """Récupère les listes de confiance pour un groupe."""
        try:
            gds_node = self.client.get_node(ua.NodeId(1002, 0))
            
            trust_method = None
            for child in await gds_node.get_children():
                browse_name = await child.read_browse_name()
                if browse_name.Name == "GetTrustLists":
                    trust_method = child
                    logger.info(f"GetTrustLists method found: {trust_method.nodeid}")
                    break
            
            if not trust_method:
                logger.warning("GetTrustLists method not found")
                return []
            
            node_id = ua.NodeId(certificate_group_id) if certificate_group_id else ua.NodeId(0)
            result = await gds_node.call_method(trust_method, node_id)
            
            if result:
                trust_lists = []
                
                all_strings = []
                for item in result:
                    if hasattr(item, 'Value'):
                        value = item.Value
                        if isinstance(value, str):
                            all_strings.append(value)
                        elif isinstance(value, list):
                            all_strings.extend(value)
                    elif isinstance(item, str):
                        all_strings.append(item)
                
                if len(all_strings) > 1:
                    joined = ''.join(all_strings)
                    try:
                        parsed = json.loads(joined)
                        if isinstance(parsed, list):
                            trust_lists = parsed
                        else:
                            trust_lists = [parsed]
                    except json.JSONDecodeError:
                        for s in all_strings:
                            try:
                                tl_data = json.loads(s)
                                if isinstance(tl_data, list):
                                    trust_lists.extend(tl_data)
                                else:
                                    trust_lists.append(tl_data)
                            except json.JSONDecodeError:
                                pass
                else:
                    for item in result:
                        if hasattr(item, 'Value'):
                            value = item.Value
                            if isinstance(value, str):
                                try:
                                    tl_data = json.loads(value)
                                    trust_lists.append(tl_data)
                                except json.JSONDecodeError as e:
                                    logger.warning(f"Failed to parse trust list JSON: {repr(value[:200])}, error: {e}")
                            elif isinstance(value, list):
                                trust_lists.extend(value)
                        elif isinstance(item, str):
                            try:
                                tl_data = json.loads(item)
                                trust_lists.append(tl_data)
                            except json.JSONDecodeError as e:
                                logger.warning(f"Failed to parse trust list JSON: {repr(item[:200])}, error: {e}")
                        elif isinstance(item, dict):
                            trust_lists.append(item)
                
                logger.info(f"Retrieved {len(trust_lists)} trust lists")
                return trust_lists
            return []
        
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des trust lists: {e}")
            raise
        return []


# ============================================================================
# Tests
# ============================================================================

async def test_phase1_core_methods(client):
    """Test Phase 1: Core GDS Methods"""
    print("\n" + "=" * 60)
    print("PHASE 1: Core GDS Methods")
    print("=" * 60)
    
    # get_applications
    print("\n1. get_applications - Liste des applications:")
    apps = await client.get_applications()
    for app in apps:
        name = app.get('ApplicationNames', [{}])[0].get('Text', 'Unknown')
        uri = app.get('ApplicationUri', 'Unknown')
        print(f"   - {name}: {uri}")
    
    # RegisterApplication (test avec nouvelle app)
    print("\n2. register_application - Nouvelle application de test:")
    test_app_id = await client.register_application(
        app_name="TestApp",
        app_uri="urn:testapp-client",
        app_type="Server",
        endpoints=["opc.tcp://localhost:4850"]
    )
    print(f"   Application TestApp enregistrée: {test_app_id}")
    
    # get_applications pour voir la nouvelle app
    print("\n3. get_applications - Après ajout:")
    apps = await client.get_applications()
    for app in apps:
        name = app.get('ApplicationNames', [{}])[0].get('Text', 'Unknown')
        uri = app.get('ApplicationUri', 'Unknown')
        print(f"   - {name}: {uri}")
    
    # unregister_application
    print(f"\n4. unregister_application - Suppression de TestApp:")
    await client.unregister_application(test_app_id)
    print(f"   Application TestApp supprimée")
    
    print("\n5. get_applications - Après suppression:")
    apps = await client.get_applications()
    for app in apps:
        name = app.get('ApplicationNames', [{}])[0].get('Text', 'Unknown')
        uri = app.get('ApplicationUri', 'Unknown')
        print(f"   - {name}: {uri}")
    
    print("\n[OK] Phase 1 - Core Methods testés avec succès")


async def test_phase2_certificate_management(client):
    """Test Phase 2: Certificate Management Methods"""
    print("\n" + "=" * 60)
    print("PHASE 2: Certificate Management Methods")
    print("=" * 60)
    
    # get_certificate_groups
    print("\n1. get_certificate_groups:")
    groups = await client.get_certificate_groups()
    print(f"   Nombre de groupes: {len(groups)}")
    for group in groups:
        print(f"   - {group.get('display_name', 'Unknown')}")
    
    # get_certificate (CA Certificate)
    print("\n2. get_certificate - Certificat CA du GDS:")
    ca_cert = await client.get_certificate()
    if ca_cert:
        print(f"   Certificat CA récupéré: {len(ca_cert)} bytes")
    else:
        print("   Pas de certificat CA disponible")
    
    # create_certificate_request
    print("\n3. create_certificate_request - Demande de certificat pour Chips:")
    app_id = "d392cbeb-8aab-47e9-afd9-9d072a09f636"  # ID de Chips
    csr_result = await client.create_certificate_request(
        application_id=app_id,
        subject_name="CN=Chips OPC UA Server,O=TestOrg,C=FR",
        domain_names="localhost"
    )
    print(f"   CSR créé: {csr_result.get('request_id', 'N/A')}")
    if csr_result.get('csr'):
        print(f"   CSR (premiers 50 chars): {csr_result['csr'][:50]}...")
    
    # get_certificate_status
    print(f"\n4. get_certificate_status - Statut de la demande:")
    request_id = csr_result.get('request_id')
    if request_id:
        status = await client.get_certificate_status(request_id)
        print(f"   Statut: {status.get('status', 'Unknown')}")
    
    # approve_certificate_request
    print(f"\n5. approve_certificate_request - Approbation du certificat:")
    if request_id:
        approved = await client.approve_certificate_request(request_id)
        print(f"   Certificat approuvé: {approved}")
    
    # get_certificate_status après approbation
    print("\n6. get_certificate_status après approbation:")
    if request_id:
        status = await client.get_certificate_status(request_id)
        print(f"   Statut: {status.get('status', 'Unknown')}")
        if status.get('certificate'):
            print(f"   Certificat émis: {len(status['certificate'])} bytes")
    
    print("\n[OK] Phase 2 - Certificate Management testés avec succès")


async def test_phase3_pull_certificate(client):
    """Test Phase 3: Pull Certificate Management Methods"""
    print("\n" + "=" * 60)
    print("PHASE 3: Pull Certificate Management Methods")
    print("=" * 60)
    
    # get_trust_lists
    print("\n1. get_trust_lists:")
    trust_lists = await client.get_trust_lists()
    print(f"   Trust Lists récupérés:")
    for tl in trust_lists:
        print(f"   - ID: {tl.get('id', 'N/A')}")
        print(f"     Contents: {tl.get('contents', 'N/A')}")
    
    # start_get_certificate_changes
    print("\n2. start_get_certificate_changes:")
    subscription = await client.start_get_certificate_changes()
    print(f"   Subscription ID: {subscription.get('subscription_id', 'N/A')}")
    
    # get_certificate_changes
    print("\n3. get_certificate_changes:")
    if subscription.get('subscription_id'):
        changes = await client.get_certificate_changes(subscription['subscription_id'])
        print(f"   Changes: {changes}")
    
    # get_certificate_groups (déjà testé en Phase 2, mais inclus pour la complétude)
    print("\n4. get_certificate_groups (rappel):")
    groups = await client.get_certificate_groups()
    print(f"   Nombre de groupes: {len(groups)}")
    
    print("\n[OK] Phase 3 - Pull Certificate Management testés avec succès")


async def main():
    """Test complet du client GDS"""
    print("=" * 60)
    print("TEST COMPLET DU CLIENT GDS")
    print("Phases 1, 2 et 3")
    print("=" * 60)
    
    client = GDSClient("opc.tcp://localhost:4840")
    
    try:
        await client.connect()
        print(f"\nConnecté au GDS: opc.tcp://localhost:4840")
        
        # Tester toutes les phases
        await test_phase1_core_methods(client)
        await test_phase2_certificate_management(client)
        await test_phase3_pull_certificate(client)
        
        print("\n" + "=" * 60)
        print("TOUS LES TESTS RÉUSSIS!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n[ERREUR] {e}")
        import traceback
        traceback.print_exc()
    finally:
        await client.disconnect()
        print("\nDéconnecté du GDS")


if __name__ == "__main__":
    asyncio.run(main())
