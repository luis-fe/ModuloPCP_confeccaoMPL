# Use a imagem oficial do Python como base
FROM python:3.11-slim

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
RUN pip install --no-cache-dir -r requirements.txt

# Copie o arquivo .jar e o código do aplicativo para o diretório de trabalho
COPY . .

# Defina a variável de ambiente para não criar bytecode (.pyc) do Python
ENV PYTHONDONTWRITEBYTECODE=1

# Defina a variável de ambiente para que o output do Python não seja bufferizado
ENV PYTHONUNBUFFERED=1

# Exponha a porta em que a aplicação Flask estará rodando
EXPOSE 9000

# Comando para rodar a aplicação Flask
CMD ["python", "app_run.py"]
