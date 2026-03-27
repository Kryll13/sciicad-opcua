from asyncua import Client, Node
import asyncio

async def browse_opcua_server(url):
    """Connecte et vérifie l'existence des NodeIds standardisés."""
    client = Client(url)
    try:
        await client.connect()
        print(f"Connecté à : {url}")

        # Liste des NodeIds à vérifier (LDS et GDS)
        nodeids_to_check = [
            ("LDS Root", "ns=0;i=4096"),
            ("GDS Root", "ns=0;i=11524"),
            ("FindServers", "ns=0;i=11530"),
            ("RegisterServer2", "ns=0;i=11533"),
            ("QueryServers", "ns=0;i=11534"),
        ]

        for name, nodeid in nodeids_to_check:
            try:
                node = await client.nodes.root.get_child([f"{nodeid}"])
                print(f"✅ {name} ({nodeid}) : Existe")
            except Exception as e:
                print(f"❌ {name} ({nodeid}) : {str(e)}")

    finally:
        await client.disconnect()

# Exemple d'utilisation
if __name__ == "__main__":
    server_url = "opc.tcp://172.19.112.1:4840"  # Remplacez par l'URL de votre serveur OPC UA
    asyncio.run(browse_opcua_server(server_url))