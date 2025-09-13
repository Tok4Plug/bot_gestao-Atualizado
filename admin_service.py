import os, json, hashlib
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.responses import PlainTextResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Counter, Histogram, Gauge
from db import SessionLocal, Buyer, init_db
from typing import Optional
from redis import Redis
from datetime import datetime
from cryptography.fernet import Fernet

# ===============================
# Configurações de ambiente
# ===============================
ADMIN_TOKEN = os.getenv('ADMIN_TOKEN', '')
REDIS_URL = os.getenv('REDIS_URL')
STREAM = os.getenv('REDIS_STREAM', 'buyers_stream')
NEW_PIXEL_IDS = [p for p in os.getenv('NEW_PIXEL_IDS', '').split(',') if p]  # filtra vazios
RETRO_BATCH_SIZE = int(os.getenv('RETRO_BATCH_SIZE', '1000'))
SECRET_KEY = os.getenv("SECRET_KEY", Fernet.generate_key().decode())  # deve ser fixo no .env
fernet = Fernet(SECRET_KEY.encode() if isinstance(SECRET_KEY, str) else SECRET_KEY)

# ===============================
# Métricas Prometheus
# ===============================
LEADS_REQUEUED = Counter('leads_requeued_total', 'Total de leads reprocessados via admin')
PIXEL_RETROFEED = Counter('pixel_retrofeed_total', 'Total de leads retroalimentados para novos pixels')
EVENT_RETRIES = Counter('event_retries_total', 'Total de tentativas de envio de evento')
PROCESS_LATENCY = Histogram('process_latency_seconds', 'Tempo de processamento de retro-feed')
BATCH_SIZE_GAUGE = Gauge('retrofeed_batch_size', 'Tamanho do lote de retro-feed')
PIXELS_SENT = Counter('retrofeed_pixel_sent_total', 'Eventos retroalimentados por pixel', ['pixel_id'])

# ===============================
# Inicialização do app
# ===============================
app = FastAPI(title='Admin')

@app.on_event('startup')
def startup():
    init_db()

# ===============================
# Autenticação básica Bearer
# ===============================
def check_auth(request: Request):
    if ADMIN_TOKEN:
        auth = request.headers.get('Authorization', '')
        if not auth.startswith('Bearer ') or auth.split(' ', 1)[1] != ADMIN_TOKEN:
            raise HTTPException(status_code=401, detail='Unauthorized')

# ===============================
# Endpoints de saúde e métricas
# ===============================
@app.get('/health')
def health():
    return {'status': 'ok'}

@app.get('/metrics')
def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get('/stats')
def stats(auth=Depends(check_auth)):
    """Retorna estatísticas em JSON para monitoramento avançado"""
    return {
        "new_pixel_ids": NEW_PIXEL_IDS,
        "retro_batch_size": RETRO_BATCH_SIZE,
        "leads_requeued_total": LEADS_REQUEUED._value.get(),
        "pixel_retrofeed_total": PIXEL_RETROFEED._value.get(),
        "event_retries_total": EVENT_RETRIES._value.get()
    }

# ===============================
# Função utilitária: criptografia real
# ===============================
def encrypt_data(data: str) -> str:
    """Criptografa dados sensíveis (cookies, emails, etc)"""
    return fernet.encrypt(data.encode()).decode()

def decrypt_data(token: str) -> str:
    """Descriptografa dados sensíveis"""
    return fernet.decrypt(token.encode()).decode()

# ===============================
# Endpoint: Reenvio de lead / retro-feed
# ===============================
@app.post('/resend/{event_key}')
def resend(event_key: str, req: Request, auth=Depends(check_auth)):
    r = Redis.from_url(REDIS_URL, decode_responses=True)
    session = SessionLocal()

    # Pega o lead
    row = session.query(Buyer).filter(Buyer.event_key == event_key).first()
    if not row:
        raise HTTPException(status_code=404, detail='not found')

    # Reset para reprocessamento
    row.sent = False
    row.attempts = 0
    session.add(row)
    session.commit()
    LEADS_REQUEUED.inc()

    # Enriquecimento contínuo de cookies e identificadores
    user_data = row.user_data or {}
    cookies = user_data.get("cookies", {})
    if cookies:
        user_data["cookies"] = {k: encrypt_data(v) for k, v in cookies.items() if v}

    payload = {
        'event_key': row.event_key,
        'sid': row.sid,
        'src_url': row.src_url or '',
        'cid': row.cid or '',
        'gclid': row.gclid or '',
        'value': str(row.value or 0.0),
        'user_data': json.dumps(user_data),
        'custom_data': json.dumps(row.custom_data or {}),
        'ts': str(int(row.created_at.timestamp())),
        'sent_pixels': row.custom_data.get("sent_pixels", []) if row.custom_data else []
    }

    # Evita duplicação para novos pixels
    for pixel_id in NEW_PIXEL_IDS:
        if pixel_id not in payload['sent_pixels']:
            r.xadd(STREAM, {**payload, "pixel_id": pixel_id})
            PIXEL_RETROFEED.inc()
            PIXELS_SENT.labels(pixel_id=pixel_id).inc()

    return {'status': 'requeued', 'event_key': event_key, 'pixels_requeued': NEW_PIXEL_IDS}

# ===============================
# Endpoint: retro-feed em massa com checkpoint
# ===============================
@app.post('/retrofeed_all')
def retrofeed_all(auth=Depends(check_auth)):
    """Envia todos os leads históricos para novos pixels em batches"""
    session = SessionLocal()
    r = Redis.from_url(REDIS_URL, decode_responses=True)
    start = datetime.utcnow()
    processed = 0

    last_id = 0
    while True:
        leads = session.query(Buyer).filter(Buyer.id > last_id).order_by(Buyer.id).limit(RETRO_BATCH_SIZE).all()
        if not leads:
            break

        batch_count = 0
        for row in leads:
            payload = {
                'event_key': row.event_key,
                'sid': row.sid,
                'src_url': row.src_url or '',
                'cid': row.cid or '',
                'gclid': row.gclid or '',
                'value': str(row.value or 0.0),
                'user_data': json.dumps(row.user_data or {}),
                'custom_data': json.dumps(row.custom_data or {}),
                'ts': str(int(row.created_at.timestamp())),
                'sent_pixels': row.custom_data.get("sent_pixels", []) if row.custom_data else []
            }
            for pixel_id in NEW_PIXEL_IDS:
                if pixel_id not in payload['sent_pixels']:
                    r.xadd(STREAM, {**payload, "pixel_id": pixel_id})
                    processed += 1
                    batch_count += 1
                    PIXEL_RETROFEED.inc()
                    PIXELS_SENT.labels(pixel_id=pixel_id).inc()
            last_id = row.id

        BATCH_SIZE_GAUGE.set(batch_count)

    elapsed = (datetime.utcnow() - start).total_seconds()
    return {"status": "done", "leads_processed": processed, "elapsed_seconds": elapsed}