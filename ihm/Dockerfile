FROM python:3.11-slim

WORKDIR /app

# Installation des dépendances système
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copie des requirements et installation
COPY requirements.txt .
RUN pip install --no-cache-dir uv
RUN pip install --no-cache-dir -r requirements.txt

# Copie du script IHM
COPY ihm_client.py .

# Commande de lancement par défaut
CMD ["python", "ihm_client.py"]

