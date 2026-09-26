# Image du Local Discovery Server SCIICAD.
#
# Contexte de build : la RACINE du dépôt (nécessaire pour pyproject.toml).
#   docker build -t sciicad-lds -f lds/Dockerfile .
FROM python:3.13-slim

WORKDIR /app

# gcc/g++ sont requis par les dépendances à extension native (cryptography).
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Dépendances depuis le pyproject.toml racine, verrouillées par uv.lock.
COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir uv \
    && uv sync --frozen --no-install-project --no-dev

# Le paquet lds/ et sa configuration.
COPY lds/ /app/lds/

# La base du registre est un volume : la perdre au redémarrage du conteneur
# annulerait la persistance.
VOLUME ["/data"]

EXPOSE 4840

# Le registre écoute sur toutes les interfaces et annonce l'hôte configuré.
CMD ["python", "-m", "lds", "--config", "/app/lds/lds_config.yaml", \
     "--database", "/data/lds.db"]
