FROM python:3.9-slim

WORKDIR /app

# Instala dependências do sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Copia os requirements primeiro para aproveitar o cache de camadas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante da aplicação
COPY . .

# Instala o pacote em modo desenvolvimento
RUN pip install --no-cache-dir -e .

# Define o comando padrão (pode ser sobrescrito no compose)
CMD ["python", "run_etl.py"]