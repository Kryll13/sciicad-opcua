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
COPY plc_server.py .

# Exposition des ports OPCUA (4840 par défaut, peut être changé avec -p)
EXPOSE 4840

# Commande de lancement par défaut
CMD ["python", "plc_server.py"]
