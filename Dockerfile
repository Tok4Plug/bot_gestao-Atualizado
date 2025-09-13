# =============================
# BUILD STAGE
# =============================
FROM python:3.11-slim-bullseye AS builder

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=100

# Dependências do sistema para compilar pacotes (cryptography, psycopg2, aiohttp, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    python3-dev \
    curl \
    ca-certificates \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Copia requirements e instala no prefix /install para otimizar camadas
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel
RUN pip install --prefix=/install --no-cache-dir -r requirements.txt

# Copia o restante do código
COPY . .

# =============================
# FINAL STAGE
# =============================
FROM python:3.11-slim-bullseye

WORKDIR /app

# Variáveis de ambiente padrão e compatíveis com funcionalidades avançadas
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=100 \
    SECRET_KEY=${SECRET_KEY} \
    NEW_PIXEL_IDS=${NEW_PIXEL_IDS} \
    REDIS_URL=${REDIS_URL} \
    REDIS_STREAM=${REDIS_STREAM:-buyers_stream} \
    ADMIN_PORT=${ADMIN_PORT:-8000} \
    HEALTHCHECK_URL=${HEALTHCHECK_URL:-http://127.0.0.1:8000/health} \
    REQUEST_TIMEOUT=12 \
    FB_API_VERSION=v19.0 \
    FB_RPS=10 \
    GOOGLE_RPS=10 \
    FB_BATCH_MAX=200 \
    PROCESS=${PROCESS:-bot} \
    BOT_TOKEN=${BOT_TOKEN} \
    VIP_CHANNEL=${VIP_CHANNEL} \
    PIXEL_ID_1=${PIXEL_ID_1} \
    ACCESS_TOKEN_1=${ACCESS_TOKEN_1} \
    PIXEL_ID_2=${PIXEL_ID_2} \
    ACCESS_TOKEN_2=${ACCESS_TOKEN_2} \
    GA_MEASUREMENT_ID=${GA_MEASUREMENT_ID} \
    GA_API_SECRET=${GA_API_SECRET}

# Copia dependências do builder
COPY --from=builder /install /usr/local
COPY --from=builder /app /app

# Instala tini para PID 1 handling (shutdown e sinais corretos)
RUN apt-get update && apt-get install -y --no-install-recommends tini ca-certificates curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && chmod +x /app/entrypoint.sh

# Cria usuário não-root e ajusta permissões
RUN useradd --create-home --home-dir /home/app app \
    && chown -R app:app /app

USER app

# Exposição de portas para bot e admin
EXPOSE 8080 8000

# Healthcheck baseado na URL configurável (default /health)
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -fsS "${HEALTHCHECK_URL}" >/dev/null || exit 1

# Entrypoint dinâmico e CMD padrão
ENTRYPOINT ["/usr/bin/tini", "--", "/app/entrypoint.sh"]
CMD ["python", "bot.py"]