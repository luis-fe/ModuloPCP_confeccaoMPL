#!/bin/bash
# Recria do zero o container de automacao do Modulo PCP Confeccao:
# remove container/imagem antigos, limpa cache de build e sobe tudo novamente.
# Uso: ./criar_container_automacao.sh
set -e

# Garante que o script rode a partir da raiz do projeto (onde ele esta salvo)
cd "$(dirname "$0")"

echo "==> Removendo container antigo (se existir)..."
docker rm -f automacao_pcp 2>/dev/null || true

echo "==> Removendo imagem antiga (se existir)..."
docker rmi -f automacao_mpl_pcp 2>/dev/null || true

echo "==> Limpando cache de build e imagens orfas..."
docker builder prune -f
docker image prune -f

echo "==> Construindo a imagem automacao_mpl_pcp..."
docker build -f Dockerfile.automacao -t automacao_mpl_pcp .

echo "==> Criando o container automacao_pcp..."
docker run -d \
  --name automacao_pcp \
  --restart always \
  -v /home/grupompl/ModuloPCP_confeccaoMPL:/app \
  --env-file _ambiente.env \
  automacao_mpl_pcp

echo "==> Container criado com sucesso:"
docker ps --filter name=automacao_pcp
