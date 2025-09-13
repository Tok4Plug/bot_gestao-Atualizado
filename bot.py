import os
import logging
import json
import asyncio
import time
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from db import save_lead, init_db, get_historical_leads, sync_pending_leads
from fb_google import send_event_to_all, enqueue_event, process_event_queue
import redis
import aiohttp
from cryptography.fernet import Fernet

# -----------------------------
# Logs JSON
# -----------------------------
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log = {
            "time": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "name": record.name
        }
        if record.exc_info:
            log["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(log)

logger = logging.getLogger("bot_logger")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(JSONFormatter())
logger.addHandler(ch)

warmup_logger = logging.getLogger("warmup_logger")
warmup_logger.setLevel(logging.INFO)
ch_warmup = logging.StreamHandler()
ch_warmup.setFormatter(JSONFormatter())
warmup_logger.addHandler(ch_warmup)

# -----------------------------
# ENV
# -----------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
VIP_CHANNEL = os.getenv("VIP_CHANNEL")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
TYPEBOT_URL = os.getenv("TYPEBOT_URL")
NEW_PIXEL_IDS = os.getenv("NEW_PIXEL_IDS",'').split(',')
SECRET_KEY = os.getenv("SECRET_KEY", Fernet.generate_key().decode())
fernet = Fernet(SECRET_KEY.encode() if isinstance(SECRET_KEY, str) else SECRET_KEY)

if not BOT_TOKEN or not VIP_CHANNEL:
    logger.error("BOT_TOKEN ou VIP_CHANNEL não configurados")
    raise Exception("Configuração inválida")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# -----------------------------
# Inicialização DB
# -----------------------------
init_db()

# -----------------------------
# Métricas Prometheus
# -----------------------------
from prometheus_client import Counter, Histogram
LEADS_SENT = Counter('bot_leads_sent_total', 'Total de leads enviados')
EVENT_RETRIES = Counter('bot_event_retries_total', 'Total de retries por evento')
PIXEL_RETROFEED = Counter('bot_pixel_retrofeed_total', 'Leads retroalimentados para novos pixels')
PROCESS_LATENCY = Histogram('bot_process_latency_seconds', 'Tempo de processamento de leads')

# -----------------------------
# Segurança avançada
# -----------------------------
def encrypt_data(data: str) -> str:
    if not data: return ''
    return fernet.encrypt(data.encode()).decode()

def decrypt_data(token: str) -> str:
    if not token: return ''
    return fernet.decrypt(token.encode()).decode()

# -----------------------------
# Typebot (real-time)
# -----------------------------
async def forward_to_typebot(lead_data: dict):
    if not TYPEBOT_URL: return
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(TYPEBOT_URL, json=lead_data, timeout=8) as resp:
                if resp.status in (200, 201):
                    logger.info(json.dumps({"event": "TYPEBOT_FORWARD_SUCCESS", "telegram_id": lead_data.get("telegram_id")}))
                else:
                    text = await resp.text()
                    logger.warning(json.dumps({"event": "TYPEBOT_FORWARD_FAIL", "status": resp.status, "body": text}))
    except Exception as e:
        logger.error(json.dumps({"event": "TYPEBOT_FORWARD_ERROR", "error": str(e)}))

# -----------------------------
# VIP Link
# -----------------------------
async def generate_vip_link(event_key: str, member_limit=1, expire_hours=24):
    try:
        invite = await bot.create_chat_invite_link(
            chat_id=int(VIP_CHANNEL),
            member_limit=member_limit,
            name=f"VIP-{event_key}",
            expire_date=int(time.time()) + expire_hours * 3600
        )
        return invite.invite_link
    except Exception as e:
        logger.error(json.dumps({"event": "VIP_LINK_ERROR", "error": str(e), "event_key": event_key}))
        return None

# -----------------------------
# Retry inteligente / backpressure
# -----------------------------
async def send_event_with_retry(event_type, lead_data, retries=5, delay=2, warmup=False):
    attempt = 0
    while attempt < retries:
        try:
            await send_event_to_all({**lead_data, "event_type": event_type})
            LEADS_SENT.inc()
            log_data = {"event": event_type, "telegram_id": lead_data.get("telegram_id"), "status": "success"}
            (warmup_logger if warmup else logger).info(json.dumps(log_data))
            lead_data.setdefault("event_history", []).append({"event": event_type, "status": "success", "ts": int(time.time())})
            return True
        except Exception as e:
            attempt += 1
            EVENT_RETRIES.inc()
            log_data = {
                "event": event_type,
                "telegram_id": lead_data.get("telegram_id"),
                "status": "retry",
                "attempt": attempt,
                "error": str(e)
            }
            (warmup_logger if warmup else logger).warning(json.dumps(log_data))
            await asyncio.sleep(delay ** attempt)
    log_data = {"event": event_type, "telegram_id": lead_data.get("telegram_id"), "status": "failed"}
    (warmup_logger if warmup else logger).error(json.dumps(log_data))
    lead_data.setdefault("event_history", []).append({"event": event_type, "status": "failed", "ts": int(time.time())})
    return False

# -----------------------------
# Processamento de novo lead
# -----------------------------
async def process_new_lead(message: types.Message):
    user_id = message.from_user.id
    user_name = message.from_user.full_name

    cookies = {}
    utm_source, utm_medium, utm_campaign = "", "", ""
    if message.get_args():
        try:
            args = json.loads(message.get_args())
            cookies["_fbc"] = args.get("_fbc")
            cookies["_fbp"] = args.get("_fbp")
            utm_source = args.get("utm_source","")
            utm_medium = args.get("utm_medium","")
            utm_campaign = args.get("utm_campaign","")
        except Exception:
            pass

    lead_data = {
        "telegram_id": user_id,
        "nome": user_name,
        "username": message.from_user.username or "",
        "first_name": message.from_user.first_name or "",
        "last_name": message.from_user.last_name or "",
        "premium": getattr(message.from_user, "is_premium", False) or False,
        "lang": message.from_user.language_code or "",
        "origin": "telegram",
        "user_agent": "TelegramBot/1.0",
        "ip_address": f"192.168.{user_id % 256}.{(user_id // 256) % 256}",
        "event_key": f"tg-{user_id}-{int(time.time())}",
        "cookies": {k: encrypt_data(v) for k,v in cookies.items() if v},
        "utm_source": utm_source,
        "utm_medium": utm_medium,
        "utm_campaign": utm_campaign,
        "sent_pixels": [],
        "event_history": []
    }

    saved = await save_lead(lead_data)
    logger.info(json.dumps({"event": "DB_LEAD", "telegram_id": user_id, "saved": saved}))

    await forward_to_typebot(lead_data)

    for evt in ["Lead","Subscribe","Purchase"]:
        await send_event_with_retry(evt, lead_data)

    vip_link = await generate_vip_link(lead_data["event_key"])
    if vip_link:
        await send_event_with_retry("Subscribe", lead_data)
    else:
        logger.warning(json.dumps({"event": "SUBSCRIBE_SEND_WITHOUT_LINK", "telegram_id": user_id}))
        await send_event_with_retry("Subscribe", lead_data)

    for pixel_id in NEW_PIXEL_IDS:
        if pixel_id not in lead_data["sent_pixels"]:
            lead_data["sent_pixels"].append(pixel_id)
            await send_event_with_retry("Lead", {**lead_data, "pixel_id": pixel_id})

    return vip_link, lead_data

# -----------------------------
# Fila de prioridade / warmup
# -----------------------------
async def build_warmup_batches(batch_size=50):
    historical_leads = await get_historical_leads(limit=batch_size)
    historical_leads = [l for l in historical_leads if l.get("telegram_id")]

    for lead in historical_leads:
        lead["premium"] = lead.get("premium") or False
        lead["purchase_count"] = lead.get("purchase_count") or 0
        score = bool(lead.get("username"))*2 + bool(lead.get("first_name")) + lead["premium"]*2 + lead["purchase_count"]*3
        lead["priority_score"] = score

    historical_leads.sort(key=lambda x: x["priority_score"], reverse=True)
    high_priority = [l for l in historical_leads if l["priority_score"] >= 3]
    low_priority = [l for l in historical_leads if l["priority_score"] < 3]

    warmup_logger.info(json.dumps({
        "event": "WARMUP_BATCHES_BUILT",
        "high_priority_count": len(high_priority),
        "low_priority_count": len(low_priority)
    }))

    return high_priority, low_priority

async def warmup_mixed_loop(new_leads_queue, high_priority, low_priority, high_delay=0.3, low_delay=1.5):
    while True:
        while not new_leads_queue.empty():
            lead = await new_leads_queue.get()
            for evt in ["Lead","Subscribe","Purchase"]:
                await send_event_with_retry(evt, lead, warmup=True)
            await asyncio.sleep(0.1)

        for lead in high_priority:
            for evt in ["Lead","Subscribe","Purchase"]:
                await send_event_with_retry(evt, lead, warmup=True)
            await asyncio.sleep(high_delay)

        for lead in low_priority:
            for evt in ["Lead","Subscribe","Purchase"]:
                await send_event_with_retry(evt, lead, warmup=True)
            await asyncio.sleep(low_delay)

        await asyncio.sleep(60)

# -----------------------------
# /start handler
# -----------------------------
@dp.message_handler(commands=["start"])
async def start_cmd(message: types.Message):
    try:
        await message.answer("👋 Olá! Validando seu acesso ao canal VIP...")
        vip_link, lead_data = await process_new_lead(message)

        if vip_link:
            await message.answer(f"✅ Olá {lead_data['first_name']}! Seu acesso VIP está liberado.\nLink expira em 24h!\n{vip_link}")
        else:
            await message.answer("⚠️ VIP link não gerado, mas seu acesso foi registrado e Subscribe enviado.")

        new_leads_queue.put_nowait(lead_data)
        redis_client.rpush("buffered_leads", json.dumps(lead_data))

    except Exception as e:
        logger.error(json.dumps({"event": "START_ERROR", "telegram_id": message.from_user.id, "error": str(e)}))
        await message.answer("⚠️ Ocorreu um erro. Tente novamente mais tarde.")

# -----------------------------
# Runner
# -----------------------------
if __name__ == "__main__":
    from asyncio import Queue
    new_leads_queue = Queue()

    async def resend_buffered_leads():
        try:
            while True:
                lead_json = redis_client.lpop("buffered_leads")
                if not lead_json:
                    break
                lead = json.loads(lead_json)
                vip_link = await generate_vip_link(lead["event_key"])
                if vip_link:
                    await send_event_with_retry("Subscribe", lead)
                else:
                    logger.warning(json.dumps({"event": "RESEND_SUBSCRIBE_WITHOUT_LINK", "telegram_id": lead.get("telegram_id")}))
                    await send_event_with_retry("Subscribe", lead)
        except Exception as e:
            logger.error(json.dumps({"event": "RESEND_BUFFER_ERROR", "error": str(e)}))

    async def main():
        logger.info(json.dumps({"event": "BOT_START"}))
        high_priority, low_priority = await build_warmup_batches(batch_size=50)
        await resend_buffered_leads()
        asyncio.create_task(warmup_mixed_loop(new_leads_queue, high_priority, low_priority))
        asyncio.create_task(sync_pending_leads())
        asyncio.create_task(process_event_queue())
        await dp.start_polling()

    asyncio.run(main())