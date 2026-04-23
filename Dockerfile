# Use a imagem oficial do Python como base
FROM python:3.11-slim-bullseye

# Instale as dependências do sistema necessárias e o OpenJDK 11
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Defina o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copie o arquivo de requisitos para o diretório de trabalho
COPY requirements.txt requirements.txt

# Instale as dependências do Python
# (Removi o gunicorn aqui, a menos que sua automação também dependa dele de alguma forma)
RUN pip install --no-cache-dir -r requirements.txt

# Copie o código do aplicativo para o diretório de trabalho
COPY . .

# Defina a variável de ambiente para não criar bytecode (.pyc) do Python
ENV PYTHONDONTWRITEBYTECODE=1

# Defina a variável de ambiente para que o output do Python não seja bufferizado
ENV PYTHONUNBUFFERED=1

# Comando direto do Python para rodar a automação
CMD ["python", "run_automacao.py"]