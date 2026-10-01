#!/bin/bash
# Recria do zero o container da aplicacao principal (Flask/Gunicorn) do Modulo PCP Confeccao:
# verifica se ja existem container/imagem app_principal, remove-os e faz build + deploy novamente.
# Uso: ./criar_container_appPrincipal.sh
set -e

NOME_IMAGEM="app_principal"
NOME_CONTAINER="app_principal"
CAMINHO_DADOS="/home/grupompl/Modulo_PCP/dados"
ARQUIVO_ENV="_ambiente.env"

# Garante que o script rode a partir da raiz do projeto (onde ele esta salvo)
cd "$(dirname "$0")"

echo "==> Verificando container ${NOME_CONTAINER}..."
if [ -n "$(docker ps -aq -f name=^${NOME_CONTAINER}$)" ]; then
  echo "    Container encontrado. Parando e removendo..."
  docker rm -f "${NOME_CONTAINER}"
else
  echo "    Container nao existe."
fi

echo "==> Verificando imagem ${NOME_IMAGEM}..."
if docker image inspect "${NOME_IMAGEM}" > /dev/null 2>&1; then
  echo "    Imagem encontrada. Removendo..."
  docker rmi -f "${NOME_IMAGEM}"
else
  echo "    Imagem nao existe."
fi

echo "==> Limpando imagens orfas..."
docker image prune -f

echo "==> Construindo a imagem ${NOME_IMAGEM}..."
docker build -f Dockerfile.appPrincipal -t "${NOME_IMAGEM}" .

echo "==> Criando o container ${NOME_CONTAINER}..."
docker run -d \
  --name "${NOME_CONTAINER}" \
  --restart always \
  -p 9000:9000 \
  -v "${CAMINHO_DADOS}":/app/dados \
  --env-file "${ARQUIVO_ENV}" \
  "${NOME_IMAGEM}"

echo "==> Container criado com sucesso:"
docker ps --filter name="^${NOME_CONTAINER}$"
