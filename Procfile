# =============================
# Procfile atualizado - multi-processos avançado
# =============================

# Bot principal: captura leads e envia eventos para FB/GA + Typebot
bot: python bot.py

# Worker: processa filas de eventos, retro-feed e retries
worker: python worker.py

# Admin: painel HTTP/Prometheus
admin: uvicorn admin_service:app --host 0.0.0.0 --port 8000 --log-level info

# Retro-feed: envia leads antigos para novos pixels
retrofeed: python tools/retrofeed.py

# Warmup: reprocessa leads históricos para melhorar score e priorização
warmup: python tools/warmup.py

# DLQ: processa dead-letter queue, eventos que falharam, com retry e logging detalhado
dlq: python tools/dlq_processor.py

# Scheduler: executa tarefas periódicas, como limpeza de fila, métricas e atualizações automáticas
scheduler: python tools/scheduler.py

# Migração de DB: inicializa ou atualiza banco de dados
migrate: python -c "from db import init_db; init_db()"

# Observação: cada processo pode ser escalado individualmente via orquestrador
# Ex.: 2 instâncias de worker e 1 de bot para alta taxa de eventos