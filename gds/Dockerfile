FROM python:3.11-slim

WORKDIR /app

# Installation des dépendances système nécessaires pour asyncua
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copie des requirements et installation des packages Python
COPY requirements.txt .
RUN pip install --no-cache-dir uv
RUN pip install --no-cache-dir -r requirements.txt

# Copie du script serveur
COPY gds_server.py .

# Copie du fichier de configuration
COPY gds_config.yaml .

# Exposition du port OPCUA
EXPOSE 4840

# Commande de lancement
CMD ["python", "gds_server.py"]
