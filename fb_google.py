# fb_google.py (reescrito avançado com integração opcional ao db.py)
import os
import time
import random
import hashlib
import traceback
import asyncio
import aiohttp
import base64
from collections import deque
from typing import List, Dict, Any, Optional
from queue import PriorityQueue
from prometheus_client import Counter, Gauge, Histogram

# Optional Fernet encryption (if CRYPTO_KEY provided)
try:
    from cryptography.fernet import Fernet, InvalidToken  # type: ignore
except Exception:
    Fernet = None
    InvalidToken = Exception  # graceful fallback for exception typing

# Try to import DB session / EventLog model if available (db.py may or may not expose EventLog)
try:
    from db import SessionLocal, EventLog  # type: ignore
    _HAS_DB_EVENTLOG = True
except Exception:
    try:
        from db import SessionLocal  # type: ignore
        _HAS_DB_EVENTLOG = False
    except Exception:
        SessionLocal = None  # type: ignore
        _HAS_DB_EVENTLOG = False

# =============================
# Configuration
# =============================
FB_API_VERSION = os.getenv("FB_API_VERSION", "v19.0")
PIXEL_ID_1 = os.getenv("PIXEL_ID_1")
ACCESS_TOKEN_1 = os.getenv("ACCESS_TOKEN_1")
PIXEL_ID_2 = os.getenv("PIXEL_ID_2")
ACCESS_TOKEN_2 = os.getenv("ACCESS_TOKEN_2")
NEW_PIXEL_IDS = os.getenv("NEW_PIXEL_IDS", "")
NEW_PIXEL_IDS = [p.strip() for p in NEW_PIXEL_IDS.split(",") if p.strip()]

GA_MEASUREMENT_ID = os.getenv("GA_MEASUREMENT_ID")
GA_API_SECRET = os.getenv("GA_API_SECRET")

FB_BATCH_MAX = int(os.getenv("FB_BATCH_MAX", "200"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "12"))
CRYPTO_KEY = os.getenv("CRYPTO_KEY")

MAX_QUEUE_SIZE = int(os.getenv("FB_EVENT_QUEUE_MAX", "5000"))
BATCH_PROCESS_INTERVAL = float(os.getenv("FB_BATCH_INTERVAL", "0.05"))
FB_RETRIES = int(os.getenv("FB_RETRIES", "3"))
GA_RETRIES = int(os.getenv("GA_RETRIES", "3"))

# =============================
# Encryption helpers
# =============================
fernet = None
if CRYPTO_KEY and Fernet is not None:
    try:
        fernet = Fernet(CRYPTO_KEY.encode())
    except Exception:
        fernet = None

def encrypt_value(value: str) -> str:
    if not value:
        return ""
    try:
        if fernet:
            return fernet.encrypt(value.encode()).decode()
        return base64.b64encode(value.encode()).decode()
    except Exception:
        return value

def decrypt_value(value: str) -> str:
    if not value:
        return ""
    try:
        if fernet:
            return fernet.decrypt(value.encode()).decode()
        return base64.b64decode(value.encode()).decode()
    except (InvalidToken, Exception):
        return value

# =============================
# Prometheus metrics
# =============================
events_sent = Counter("events_sent_total", "Total de eventos enviados", ["platform", "event_type", "pixel"])
events_failed = Counter("events_failed_total", "Eventos que falharam", ["platform", "event_type", "pixel"])
queue_size_gauge = Gauge("event_queue_size", "Tamanho da fila de eventos")
latency_histogram = Histogram("event_latency_seconds", "Latência do envio de eventos", ["platform"])

# =============================
# Token bucket + circuit breaker
# =============================
class TokenBucket:
    def __init__(self, rate: float, capacity: Optional[float] = None):
        self.rate = rate
        self.capacity = capacity or max(1.0, rate)
        self._tokens = self.capacity
        self._last = time.time()

    def consume(self, tokens: float = 1.0) -> bool:
        now = time.time()
        elapsed = now - self._last
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last = now
        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False

    async def wait_for(self, tokens: float = 1.0):
        while not self.consume(tokens):
            await asyncio.sleep(max(0.001, (tokens - self._tokens) / max(0.1, self.rate)))

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_time: int = 30):
        self.failure_threshold = failure_threshold
        self.recovery_time = recovery_time
        self.failures = 0
        self.last_failure_time = 0.0
        self.opened = False

    def record_success(self):
        self.failures = 0
        self.opened = False

    def record_failure(self):
        self.failures += 1
        self.last_failure_time = time.time()
        if self.failures >= self.failure_threshold:
            self.opened = True

    def allow_request(self) -> bool:
        if self.opened and (time.time() - self.last_failure_time > self.recovery_time):
            self.opened = False
            self.failures = 0
        return not self.opened

_fb_bucket = TokenBucket(rate=float(os.getenv("FB_RPS", "20")), capacity=float(os.getenv("FB_RPS", "20")) * 2)
_google_bucket = TokenBucket(rate=float(os.getenv("GOOGLE_RPS", "20")), capacity=float(os.getenv("GOOGLE_RPS", "20")) * 2)
_fb_breaker = CircuitBreaker()
_google_breaker = CircuitBreaker()

# =============================
# Event queue (priority)
# =============================
event_queue: "PriorityQueue[tuple[int, dict]]" = PriorityQueue()
dead_letter_queue: deque = deque(maxlen=2000)

async def enqueue_event(lead_data: dict, event_type: str = "Lead", score: int = 100):
    """Push event into priority queue (best-effort)"""
    if event_queue.qsize() < MAX_QUEUE_SIZE:
        event_queue.put((score * -1, {"lead_data": lead_data, "event_type": event_type}))
    else:
        # queue full -> drop (but record to DLQ structure)
        dead_letter_queue.append({"lead_data": lead_data, "event_type": event_type, "reason": "queue_full"})

# =============================
# DB event log (optional) - graceful fallback
# =============================
def save_event_db(lead_data: dict, event_type: str, status: str, platform: str, response: Optional[dict] = None):
    """
    Try to persist event log to DB. If EventLog model exists in db.py, use it.
    Otherwise fallback to a lightweight local print (to avoid raising).
    """
    try:
        if SessionLocal is None:
            raise RuntimeError("SessionLocal not available")
        db = SessionLocal()
        if _HAS_DB_EVENTLOG:
            try:
                log = EventLog(
                    telegram_id=lead_data.get("telegram_id"),
                    event_type=event_type,
                    platform=platform,
                    status=status,
                    value=lead_data.get("value"),
                    raw_data=lead_data,
                    response=response or {}
                )
                db.add(log)
                db.commit()
                db.close()
                return
            except Exception:
                # if model not compatible, fallback to raw insert
                db.rollback()
        # Fallback raw insert attempt: try to insert into event_logs if table exists
        try:
            # attempt minimal safe raw insertion if table exists
            insert_stmt = """
            INSERT INTO event_logs (telegram_id, event_type, platform, status, value, raw_data, response, created_at)
            VALUES (:telegram_id, :event_type, :platform, :status, :value, :raw_data::jsonb, :response::jsonb, now())
            """
            params = {
                "telegram_id": lead_data.get("telegram_id"),
                "event_type": event_type,
                "platform": platform,
                "status": status,
                "value": lead_data.get("value"),
                "raw_data": json.dumps(lead_data),
                "response": json.dumps(response or {})
            }
            db.execute(insert_stmt, params)
            db.commit()
        except Exception:
            # final fallback: just print
            db.rollback()
            print("[EVENT LOG FALLBACK]", {"telegram_id": lead_data.get("telegram_id"), "event_type": event_type, "platform": platform, "status": status, "response": response})
        db.close()
    except Exception as e:
        # do not raise; best-effort only
        print("[EVENT LOG ERROR] couldn't persist event:", e)

# =============================
# Helpers: enrich user data and build FB payload
# =============================
def enrich_user_data(lead_data: Dict[str, Any]) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "external_id": str(lead_data.get("telegram_id")),
        "client_user_agent": lead_data.get("user_agent") or "TelegramBot/1.0",
        "client_ip_address": lead_data.get("ip_address") or f"192.168.{random.randint(0,255)}.{random.randint(1,254)}",
    }
    cookies = lead_data.get("cookies") or {}
    if cookies.get("_fbc"):
        data["fbc"] = encrypt_value(cookies["_fbc"])
    if cookies.get("_fbp"):
        data["fbp"] = encrypt_value(cookies["_fbp"])

    # Hash sensitive fields
    sensitive_map = [
        ("nome", "fn"), ("last_name", "ln"), ("email", "em"),
        ("phone", "ph"), ("address", "address"), ("zip", "zp"),
        ("country", "country"), ("dob", "dob"), ("po_box", "po_box"),
        ("gender", "gender"), ("age_range", "age_range")
    ]
    for field, key in sensitive_map:
        v = lead_data.get(field)
        if v:
            try:
                data[key] = hashlib.sha256(str(v).lower().encode()).hexdigest()
            except Exception:
                data[key] = hashlib.sha256(str(v).encode()).hexdigest()

    if lead_data.get("utm_params"):
        data.update(lead_data.get("utm_params"))
    if lead_data.get("referrer"):
        data["referrer"] = lead_data.get("referrer")
    return data

def build_fb_event(lead_data: Dict[str, Any], event_type: str) -> Dict[str, Any]:
    return {
        "event_name": event_type,
        "event_time": int(time.time()),
        "event_source_url": lead_data.get("src_url"),
        "user_data": enrich_user_data(lead_data),
        "custom_data": {
            "currency": lead_data.get("currency", "BRL"),
            "value": lead_data.get("value") or 0,
            "content_name": lead_data.get("content_name") or "",
            "content_ids": lead_data.get("content_ids") or [],
            "num_items": lead_data.get("num_items") or 1
        }
    }

# =============================
# HTTP POST with retry/backoff
# =============================
async def post_with_retry(session: aiohttp.ClientSession, url: str, json_payload: Any, params: Optional[dict] = None,
                          retries: int = 3, platform: str = "fb", event_type: str = "Lead", pixel: str = "", lead_data: dict = None) -> dict:
    backoff = 0.5
    for attempt in range(retries + 1):
        try:
            start = time.time()
            async with session.post(url, json=json_payload, params=params or {}, timeout=REQUEST_TIMEOUT) as resp:
                text = await resp.text()
                latency_histogram.labels(platform).observe(time.time() - start)
                if resp.status in (200, 201):
                    events_sent.labels(platform, event_type, pixel).inc()
                    save_event_db(lead_data or {}, event_type, "success", platform, {"status": resp.status, "body": text})
                    return {"ok": True, "status": resp.status, "body": text}
                else:
                    events_failed.labels(platform, event_type, pixel).inc()
                    # attempt to capture response body for logs
                    try:
                        save_event_db(lead_data or {}, event_type, "failed_http", platform, {"status": resp.status, "body": text})
                    except Exception:
                        pass
        except Exception as e:
            events_failed.labels(platform, event_type, pixel).inc()
            traceback.print_exc()
        # wait and retry
        await asyncio.sleep(backoff)
        backoff *= 2
    # failed after retries -> DLQ & DB log
    dead_letter_queue.append({"url": url, "payload": json_payload, "pixel": pixel, "event": event_type})
    save_event_db(lead_data or {}, event_type, "failed", platform, {"reason": "max_retries"})
    return {"ok": False, "status": None, "reason": "max_retries", "pixel": pixel}

# =============================
# Send to FB chunked (per-pixel)
# =============================
async def send_events_chunked(session: aiohttp.ClientSession, pixel_id: str, access_token: str,
                              events: List[Dict[str, Any]], event_type: str = "Lead", lead_data: dict = None) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    if not pixel_id or not access_token:
        return {pixel_id or "unknown": {"ok": False, "reason": "missing_config"}}
    if not _fb_breaker.allow_request():
        return {pixel_id: {"ok": False, "reason": "circuit_open"}}

    for i in range(0, len(events), FB_BATCH_MAX):
        chunk = events[i:i + FB_BATCH_MAX]
        url = f"https://graph.facebook.com/{FB_API_VERSION}/{pixel_id}/events"
        payload = {"data": chunk}
        params = {"access_token": access_token}
        await _fb_bucket.wait_for(1.0)
        resp = await post_with_retry(session, url, payload, params=params, retries=FB_RETRIES, platform="fb", event_type=event_type, pixel=pixel_id, lead_data=lead_data)
        if resp.get("ok"):
            _fb_breaker.record_success()
        else:
            _fb_breaker.record_failure()
        result[pixel_id] = resp
    return result

async def send_batch_to_fb(events: List[Dict[str, Any]], event_type: str = "Lead", lead_data: dict = None) -> Dict[str, Any]:
    async with aiohttp.ClientSession() as session:
        results: Dict[str, Any] = {}
        tasks = []
        if PIXEL_ID_1 and ACCESS_TOKEN_1:
            tasks.append(send_events_chunked(session, PIXEL_ID_1, ACCESS_TOKEN_1, events, event_type, lead_data))
        if PIXEL_ID_2 and ACCESS_TOKEN_2:
            tasks.append(send_events_chunked(session, PIXEL_ID_2, ACCESS_TOKEN_2, events, event_type, lead_data))
        # NEW_PIXEL_IDS can be "pid:token" pairs or plain pixel ids
        for p in NEW_PIXEL_IDS:
            if ":" in p:
                pid, token = p.split(":", 1)
                tasks.append(send_events_chunked(session, pid, token, events, event_type, lead_data))
            else:
                # try to resolve token from env ACCESS_TOKEN_<PID> if any
                token = os.getenv(f"ACCESS_TOKEN_{p}") or None
                tasks.append(send_events_chunked(session, p, token, events, event_type, lead_data))
        if tasks:
            all_res = await asyncio.gather(*tasks, return_exceptions=True)
            for r in all_res:
                if isinstance(r, dict):
                    results.update(r)
        return results

# =============================
# Send to Google Analytics (GA4)
# =============================
async def send_event_google(event_name: str, cid: Optional[str] = None, gclid: Optional[str] = None,
                            value: Optional[float] = None, currency: str = "BRL", utm_params: Optional[dict] = None,
                            retries: int = GA_RETRIES, lead_data: dict = None) -> Dict[str, Any]:
    if not GA_MEASUREMENT_ID or not GA_API_SECRET:
        save_event_db(lead_data or {}, event_name, "skipped", "ga", {"reason": "missing_config"})
        return {"ga4": {"ok": False, "reason": "missing_config"}}
    if not _google_breaker.allow_request():
        return {"ga4": {"ok": False, "reason": "circuit_open"}}
    await _google_bucket.wait_for(1.0)
    cid = cid or f"anon-{int(time.time())}.{random.randint(1000,9999)}"
    payload = {
        "client_id": cid,
        "events": [{"name": event_name, "params": {"value": value, "currency": currency, "gclid": gclid, **(utm_params or {})}}]
    }
    async with aiohttp.ClientSession() as session:
        resp = await post_with_retry(session,
                                     f"https://www.google-analytics.com/mp/collect?measurement_id={GA_MEASUREMENT_ID}&api_secret={GA_API_SECRET}",
                                     payload,
                                     retries=retries,
                                     platform="ga",
                                     event_type=event_name,
                                     pixel="ga4",
                                     lead_data=lead_data)
        if resp.get("ok"):
            _google_breaker.record_success()
        else:
            _google_breaker.record_failure()
        return {"ga4": resp}

# =============================
# Unified send to FB + GA
# =============================
async def send_event_to_all(lead_data: dict, event_type: str = "Lead") -> Dict[str, Any]:
    """
    Sends event to configured Facebook pixels and GA4, returns a mapping with per-platform/pixel results.
    Also persists an event log (best-effort) for each platform attempt.
    """
    fb_event = [build_fb_event(lead_data, event_type)]

    results: Dict[str, Any] = {}
    try:
        fb_task = send_batch_to_fb(fb_event, event_type, lead_data)
        ga_task = send_event_google(
            event_name=event_type,
            cid=str(lead_data.get("telegram_id")),
            gclid=lead_data.get("gclid"),
            value=lead_data.get("value") or 0,
            currency=lead_data.get("currency", "BRL"),
            utm_params=lead_data.get("utm_params"),
            lead_data=lead_data
        )
        fb_results, ga_results = await asyncio.gather(fb_task, ga_task, return_exceptions=True)

        if isinstance(fb_results, dict):
            results.update(fb_results)
        if isinstance(ga_results, dict):
            results.update(ga_results)
    except Exception as e:
        traceback.print_exc()
        save_event_db(lead_data, event_type, "failed", "system", {"reason": str(e)})
        results["error"] = {"ok": False, "reason": str(e)}
    return results

# =============================
# Background queue processor (consumes event_queue)
# =============================
async def process_event_queue():
    while True:
        try:
            queue_size_gauge.set(event_queue.qsize())
            batch = []
            while not event_queue.empty() and len(batch) < FB_BATCH_MAX:
                _, lead_event = event_queue.get()
                batch.append(lead_event)
            if batch:
                tasks = [send_event_to_all(e["lead_data"], e["event_type"]) for e in batch]
                # run and ignore exceptions per item (they are logged inside)
                await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(BATCH_PROCESS_INTERVAL)
        except Exception as e:
            traceback.print_exc()
            await asyncio.sleep(1.0)

# =============================
# Dry-run helper
# =============================
async def dry_run(lead_data: dict, event_type: str = "Lead"):
    fb_event = [build_fb_event(lead_data, event_type)]
    print("[DRY RUN] FB event:", fb_event)
    if GA_MEASUREMENT_ID and GA_API_SECRET:
        print("[DRY RUN] GA4 payload:", {
            "client_id": str(lead_data.get("telegram_id")),
            "events": [{"name": event_type, "params": {"value": lead_data.get("value") or 0}}]
        })

# Expose small utility for other modules
__all__ = [
    "enqueue_event", "process_event_queue", "send_event_to_all", "dry_run",
    "encrypt_value", "decrypt_value"
]