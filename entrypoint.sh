#!/usr/bin/env bash
set -euo pipefail

# =============================
# Entrypoint avançado para container
# Suporte a múltiplos processos via env PROCESS
# Valores possíveis: bot | worker | admin | procfile
# =============================

PROCESS=${PROCESS:-bot}
ADMIN_PORT=${ADMIN_PORT:-8000}

echo "🚀 Starting container. PROCESS=${PROCESS} at $(date -u +"%Y-%m-%dT%H:%M:%SZ")"

# Função de log estruturado
log() {
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] $1"
}

# Executa o processo conforme variável PROCESS
case "${PROCESS}" in
  bot)
    log "-> Running Telegram bot (python bot.py)"
    exec python bot.py
    ;;
  worker)
    log "-> Running worker (python worker.py)"
    exec python worker.py
    ;;
  admin)
    log "-> Running Admin API (uvicorn admin_service:app --host 0.0.0.0 --port ${ADMIN_PORT})"
    exec uvicorn admin_service:app --host 0.0.0.0 --port "${ADMIN_PORT}" --log-level info
    ;;
  procfile)
    log "-> Running admin + worker (procfile mode, experimental)"
    uvicorn admin_service:app --host 0.0.0.0 --port "${ADMIN_PORT}" --log-level info &
    python worker.py &
    wait -n
    ;;
  *)
    log "❌ Unknown PROCESS: ${PROCESS}"
    exit 2
    ;;
esac