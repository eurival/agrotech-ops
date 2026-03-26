# Constrói a imagem (o ponto no final é obrigatório)
docker build -t eurival/postgis-agrotech:15-3.3-vector .

# Faz o login no Docker Hub (vai pedir sua senha)
docker login

# Envia a imagem para a nuvem
docker push eurival/postgis-agrotech:15-3.3-vector