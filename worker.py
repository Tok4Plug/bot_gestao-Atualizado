# worker.py (avançado e otimizado para escala e sincronização)
import os
import time
import json
import traceback
import random
import asyncio
from typing import List, Dict, Any
from redis import Redis
from prometheus_client import Counter, Gauge

# DB helpers
from db import init_db, save_lead, get_unsent_leads_for_pixel, mark_pixels_sent
# Envio
from fb_google import send_event_to_all

# ===========================
# Config / ENV
# ===========================
REDIS_URL = os.getenv("REDIS_URL")
STREAM = os.getenv("REDIS_STREAM", "buyers_stream")
GROUP = os.getenv("REDIS_GROUP", "buyers_group")
DELAYED_ZSET = os.getenv("DELAYED_ZSET", "buyers_delayed")
DLQ_STREAM = os.getenv("DLQ_STREAM", "buyers_dlq")
CONSUMER = f"{os.getenv('REDIS_CONSUMER_PREFIX','worker')}-{random.randint(1000,9999)}"

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
BLOCK_MS = int(os.getenv("BLOCK_MS", "2000"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "6"))
RETRY_BACKOFF_BASE = float(os.getenv("RETRY_BACKOFF_BASE", "2.0"))
RETROFEED_ENABLED = os.getenv("RETROFEED_ENABLED", "false").lower() == "true"

DEFAULT_EVENT_SEQUENCE = os.getenv("DEFAULT_EVENT_SEQUENCE", "Lead,Subscribe,Purchase").split(",")

r = Redis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None

# ===========================
# Métricas
# ===========================
metric_events_sent = Counter("worker_events_sent_total", "Eventos enviados", ["event_type", "pixel"])
metric_events_failed = Counter("worker_events_failed_total", "Eventos falhados", ["event_type", "pixel"])
metric_dlq_count = Counter("worker_dlq_total", "Eventos movidos para DLQ")
metric_queue_size = Gauge("worker_stream_size", "Tamanho aproximado do stream Redis")
metric_retries = Counter("worker_retries_total", "Total de retries agendados")
metric_batches = Counter("worker_batches_total", "Batches processados")

# ===========================
# Helpers de log/redação
# ===========================
def _redact(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: ("REDACTED" if k.lower() in ("email", "phone", "_fbp", "_fbc", "cookies") else _redact(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_redact(v) for v in obj]
    return obj

def _log(msg: str, obj: Any = None):
    try:
        print(msg, json.dumps(_redact(obj), default=str) if obj else "")
    except Exception:
        print(msg)

# ===========================
# Retry / DLQ
# ===========================
def schedule_retry(payload: Dict[str, Any], attempts: int, event_type: str = "Lead"):
    if not r:
        return
    # eventos críticos (Purchase) têm prioridade e backoff menor
    base = RETRY_BACKOFF_BASE * (0.5 if event_type.lower() == "purchase" else 1.0)
    backoff = min(3600, (base ** attempts) + random.uniform(0, 1))
    due = int(time.time() + backoff)
    try:
        r.zadd(DELAYED_ZSET, {json.dumps(payload): due})
        metric_retries.inc()
        _log("[SCHEDULE_RETRY]", {"event_key": payload.get("event_key"), "attempts": attempts, "delay_s": backoff})
    except Exception as e:
        _log("[SCHEDULE_RETRY_ERROR]", {"error": str(e), "payload": payload.get("event_key")})

def reenqueue_due():
    if not r:
        return
    now = int(time.time())
    try:
        items = r.zrangebyscore(DELAYED_ZSET, 0, now, start=0, num=500)
    except Exception:
        return
    for i in items:
        try:
            if not r.zrem(DELAYED_ZSET, i):
                continue
            payload = json.loads(i)
            r.xadd(STREAM, payload, maxlen=2_000_000, approximate=True)
            _log("[REENQUEUED_DUE]", {"event_key": payload.get("event_key")})
        except Exception as e:
            _log("[REENQUEUE_ERROR]", {"error": str(e)})

def move_to_dlq(item: Dict[str, Any], reason: str = "max_attempts"):
    if not r:
        return
    try:
        payload = {
            **{k: (json.dumps(v) if isinstance(v, (dict, list)) else str(v)) for k, v in item.items()},
            "_dlq_reason": reason,
            "_ts": int(time.time())
        }
        r.xadd(DLQ_STREAM, payload)
        metric_dlq_count.inc()
        _log("[MOVED_TO_DLQ]", {"event_key": item.get("event_key"), "reason": reason})
    except Exception as e:
        _log("[DLQ_ERROR]", {"error": str(e)})

# ===========================
# Processamento de evento
# ===========================
async def process_entry(item: Dict[str, Any], mid: str):
    ek = item.get("event_key")
    if not ek:
        r.xack(STREAM, GROUP, mid)
        return

    try:
        await save_lead(item)
    except Exception as e:
        attempts = int(item.get("attempts", 0)) + 1
        item["attempts"] = attempts
        if attempts >= MAX_ATTEMPTS:
            move_to_dlq(item, reason="db_error")
        else:
            schedule_retry(item, attempts)
        r.xack(STREAM, GROUP, mid)
        return

    events = item.get("events") or DEFAULT_EVENT_SEQUENCE
    utm_params = {k: item[k] for k in ("utm_source", "utm_medium", "utm_campaign") if k in item}
    if utm_params:
        item["utm_params"] = utm_params

    success = False
    attempted_pixels = []

    for evt in events:
        try:
            resp = await asyncio.shield(send_event_to_all(item, event_type=evt))
            if resp:
                success = True
                if isinstance(resp, dict):
                    for px, result in resp.items():
                        attempted_pixels.append(px)
                        metric_events_sent.labels(evt, px if px else "unknown").inc() if result.get("ok", True) else metric_events_failed.labels(evt, px).inc()
                await mark_pixels_sent(ek, list(set(attempted_pixels)), event_record={"event": evt, "status": "success", "ts": int(time.time())})
                _log("[EVENT_OK]", {"event_key": ek, "event": evt, "pixels": attempted_pixels})
            else:
                metric_events_failed.labels(evt, "none").inc()
        except Exception as e:
            metric_events_failed.labels(evt, "exception").inc()
            _log("[EVENT_EXCEPTION]", {"event_key": ek, "event": evt, "error": str(e)})
            traceback.print_exc()

    if success:
        r.xack(STREAM, GROUP, mid)
    else:
        attempts = int(item.get("attempts", 0)) + 1
        item["attempts"] = attempts
        if attempts >= MAX_ATTEMPTS:
            move_to_dlq(item, reason="max_attempts")
        else:
            schedule_retry(item, attempts)
        r.xack(STREAM, GROUP, mid)

async def process_batch(entries: List):
    metric_batches.inc()
    _log("[BATCH_START]", {"count": len(entries)})
    tasks = [process_entry(parse_fields(fields), mid) for mid, fields in entries]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    _log("[BATCH_END]", {"count": len(entries)})

# ===========================
# Retrofeed
# ===========================
async def retrofeed_for_pixel(pixel_id: str, batch_size: int = 500):
    try:
        unsent = await get_unsent_leads_for_pixel(pixel_id, limit=batch_size)
        if not unsent:
            return 0
        for lead in unsent:
            lead["_retro_target_pixel"] = pixel_id
            r.xadd(STREAM, {k: json.dumps(v) if isinstance(v, (dict, list)) else str(v) for k, v in lead.items()}, maxlen=2_000_000, approximate=True)
        return len(unsent)
    except Exception as e:
        _log("[RETROFEED_ERROR]", {"pixel": pixel_id, "error": str(e)})
        traceback.print_exc()
        return 0

# ===========================
# Main loop
# ===========================
def parse_fields(fields: Dict[str, str]) -> Dict[str, Any]:
    out = {}
    for k, v in fields.items():
        try:
            out[k] = json.loads(v)
        except Exception:
            out[k] = v
    return out

async def main_loop():
    if not r:
        print("Redis não configurado")
        return
    init_db()
    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except Exception:
        pass

    _log("[WORKER_STARTED]", {"group": GROUP, "consumer": CONSUMER})

    while True:
        try:
            reenqueue_due()
            metric_queue_size.set(r.xlen(STREAM))
            res = r.xreadgroup(GROUP, CONSUMER, {STREAM: ">"}, count=BATCH_SIZE, block=BLOCK_MS)
            if not res:
                if RETROFEED_ENABLED:
                    await retrofeed_for_pixel("PIXEL_DEFAULT")
                await asyncio.sleep(0.05)
                continue
            entries = res[0][1]
            await process_batch(entries)
        except Exception as e:
            _log("[WORKER_LOOP_ERROR]", {"error": str(e)})
            traceback.print_exc()
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main_loop())