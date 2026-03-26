# Usa a sua imagem exata como base
FROM postgis/postgis:15-3.3

# Instala a extensão pgvector e limpa o cache do apt para a imagem não ficar pesada
RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-15-pgvector \
    && rm -rf /var/lib/apt/lists/*