# tools/retrofeed.py
"""
Retrofeed avançado — reenvia leads históricos não entregues para novos pixels.

Funcionalidades:
- Async-first (usa coroutines do db.py)
- Batch processing com tamanho configurável
- Descoberta automática de pixels (PIXEL_ID_1, PIXEL_ID_2, NEW_PIXELS_PARSED)
- Modo único (execução once) ou contínuo (AUTO_MODE)
- Logs seguros (redaction)
- Métricas Prometheus
- Push para Redis Stream com maxlen/approximate
"""

import os
import time
import json
import traceback
import asyncio
from typing import Any, Dict, List, Optional

try:
    from redis import Redis
except Exception:
    Redis = None

# DB helpers (assíncronos)
from db import init_db, get_unsent_leads_for_pixel

# utils (espera que NEW_PIXELS_PARSED, PIXEL_ID_1, PIXEL_ID_2 estejam disponíveis)
# se sua utils tiver outro nome/estrutura, ajuste a importação
try:
    from utils import NEW_PIXELS_PARSED, PIXEL_ID_1, PIXEL_ID_2
except Exception:
    # fallback: tenta ler NEW_PIXEL_IDS direto da env
    NEW_PIXELS_PARSED = []
    PIXEL_ID_1 = os.getenv("PIXEL_ID_1")
    PIXEL_ID_2 = os.getenv("PIXEL_ID_2")

# ================
# Config / ENV
# ================
REDIS_URL = os.getenv("REDIS_URL", "")
STREAM = os.getenv("REDIS_STREAM", "buyers_stream")

BATCH_SIZE = int(os.getenv("RETROFEED_BATCH_SIZE", "500"))
SLEEP_INTERVAL = int(os.getenv("RETROFEED_INTERVAL", "60"))  # seconds between continuous cycles
AUTO_MODE = os.getenv("RETROFEED_AUTO", "false").lower() in ("1", "true", "yes")
MAX_PUSH_PER_RUN = int(os.getenv("RETROFEED_MAX_PUSH", "5000"))

# Redis client
r = None
if Redis and REDIS_URL:
    try:
        r = Redis.from_url(REDIS_URL, decode_responses=True)
    except Exception:
        r = None

# ================
# Prometheus metrics (optional; safe if prometheus_client not installed)
# ================
try:
    from prometheus_client import Counter
    metric_retrofeed_pushed = Counter("retrofeed_pushed_total", "Total retrofeed leads pushed", ["pixel"])
    metric_retrofeed_errors = Counter("retrofeed_errors_total", "Total retrofeed errors", ["pixel"])
except Exception:
    metric_retrofeed_pushed = None
    metric_retrofeed_errors = None

# ================
# Helpers
# ================
def _redact(v: Any) -> Any:
    """Redacts likely sensitive fields recursively."""
    if isinstance(v, dict):
        out = {}
        for k, val in v.items():
            kl = k.lower()
            if kl in ("cookies", "_fbp", "_fbc", "email", "phone", "ph", "em"):
                out[k] = "REDACTED"
            else:
                out[k] = _redact(val)
        return out
    if isinstance(v, list):
        return [_redact(i) for i in v]
    return v

def _log(msg: str, obj: Any = None):
    try:
        if obj is not None:
            print(msg, json.dumps(_redact(obj), default=str))
        else:
            print(msg)
    except Exception:
        print(msg)

def _resolve_pixels() -> List[str]:
    """Return set of pixel IDs found from envs and NEW_PIXELS_PARSED"""
    pixels = []
    if PIXEL_ID_1:
        pixels.append(PIXEL_ID_1)
    if PIXEL_ID_2:
        pixels.append(PIXEL_ID_2)
    # NEW_PIXELS_PARSED expected as list of (pid, token) or strings; handle both
    try:
        for entry in NEW_PIXELS_PARSED:
            if isinstance(entry, (list, tuple)) and entry:
                pid = entry[0]
            else:
                pid = str(entry)
            if pid:
                pixels.append(pid)
    except Exception:
        # fallback: read RAW env NEW_PIXEL_IDS comma-separated
        raw = os.getenv("NEW_PIXEL_IDS", "")
        for p in [x.strip() for x in raw.split(",") if x.strip()]:
            if ":" in p:
                pid = p.split(":", 1)[0].strip()
            else:
                pid = p
            pixels.append(pid)
    return list(dict.fromkeys([p for p in pixels if p]))  # preserve order, unique

# ================
# Core retrofeed
# ================
async def retrofeed_for_pixel(pixel_id: str, batch_size: int = BATCH_SIZE, push_limit: Optional[int] = None) -> int:
    """
    Fetch unsent leads for pixel_id and push them into Redis stream for processing.
    Returns number of leads pushed.
    """
    if not r:
        _log("[RETRO_NO_REDIS]")
        return 0

    pushed = 0
    try:
        unsent = await get_unsent_leads_for_pixel(pixel_id, limit=batch_size)
    except Exception as e:
        _log("[RETRO_DB_ERROR]", {"pixel": pixel_id, "error": str(e)})
        if metric_retrofeed_errors:
            metric_retrofeed_errors.labels(pixel_id).inc()
        return 0

    if not unsent:
        _log("[RETRO_EMPTY]", {"pixel": pixel_id})
        return 0

    for lead in unsent:
        if push_limit and pushed >= push_limit:
            break
        try:
            # prepare payload: ensure primitive string values for Redis XADD
            payload = {}
            # Keep important keys as JSON strings (user_data, custom_data)
            for k, v in lead.items():
                # If already basic types, convert to str; for dict/list use json.dumps
                if isinstance(v, (dict, list)):
                    payload[k] = json.dumps(v, default=str)
                else:
                    payload[k] = "" if v is None else str(v)
            # include target pixel so worker can send specifically to that pixel if implemented
            payload["retro_target_pixel"] = pixel_id

            r.xadd(STREAM, payload, maxlen=2_000_000, approximate=True)
            pushed += 1
        except Exception as e:
            _log("[RETRO_PUSH_ERROR]", {"pixel": pixel_id, "error": str(e), "event_key": lead.get("event_key")})
            if metric_retrofeed_errors:
                metric_retrofeed_errors.labels(pixel_id).inc()
            traceback.print_exc()

    _log("[RETRO_PUSH_OK]", {"pixel": pixel_id, "count": pushed})
    if metric_retrofeed_pushed:
        metric_retrofeed_pushed.labels(pixel_id).inc(pushed)
    return pushed

async def retrofeed_all(pixels: Optional[List[str]] = None, once: bool = True, batch_size: int = BATCH_SIZE, max_per_pixel: Optional[int] = None):
    """
    Runs retrofeed for list of pixels (auto-detected if not provided).
    - once=True runs a single pass and exits.
    - once=False runs in continuous loop sleeping SLEEP_INTERVAL seconds between cycles.
    """
    init_db()
    pixels = pixels or _resolve_pixels()
    if not pixels:
        _log("[RETRO_NO_PIXELS]")
        return

    _log("[RETRO_START]", {"pixels": pixels, "auto": not once, "batch_size": batch_size, "max_per_pixel": max_per_pixel})
    while True:
        total = 0
        for pid in pixels:
            pushed = await retrofeed_for_pixel(pid, batch_size=batch_size, push_limit=max_per_pixel)
            total += pushed
            # minor throttle between pixels
            await asyncio.sleep(0.05)
        _log("[RETRO_CYCLE_DONE]", {"total_pushed": total})
        if once:
            break
        await asyncio.sleep(SLEEP_INTERVAL)

# ================
# CLI Entrypoint
# ================
def _parse_args_from_env():
    """Read env variables for flexible invocation"""
    pixels_env = os.getenv("RETRO_PIXELS", "")
    pixels = [p.strip() for p in pixels_env.split(",") if p.strip()] if pixels_env else None
    once_env = os.getenv("RETRO_ONCE", "")
    once = True if once_env.lower() in ("1", "true", "yes") else (not AUTO_MODE)
    batch_size = int(os.getenv("RETRO_BATCH_SIZE", str(BATCH_SIZE)))
    max_per_pixel = os.getenv("RETRO_MAX_PER_PIXEL")
    max_per_pixel_val = int(max_per_pixel) if max_per_pixel and max_per_pixel.isdigit() else None
    return pixels, once, batch_size, max_per_pixel_val

if __name__ == "__main__":
    try:
        pixels, once, batch_size, max_per_pixel_val = _parse_args_from_env()
        # if AUTO_MODE and no explicit overrides, run continuous mode
        if AUTO_MODE and os.getenv("RETRO_ONCE", "") == "":
            once = False
        asyncio.run(retrofeed_all(pixels=pixels, once=once, batch_size=batch_size, max_per_pixel=max_per_pixel_val))
    except KeyboardInterrupt:
        _log("[RETRO_STOPPED]")
    except Exception as exc:
        _log("[RETRO_FATAL]", {"error": str(exc)})
        traceback.print_exc()