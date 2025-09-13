# db.py (versão avançada completa)
import os
import asyncio
import json
import time
import hashlib
import base64
import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from sqlalchemy import (
    create_engine, Column, Integer, String, Boolean,
    DateTime, Float, Text, func
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError, OperationalError

# ======================================
# Logging centralizado
# ======================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("db")

# ======================================
# Criptografia avançada (AES-Fernet com fallback base64)
# ======================================
CRYPTO_KEY = os.getenv("CRYPTO_KEY")
_use_fernet, _fernet = False, None
try:
    if CRYPTO_KEY:
        from cryptography.fernet import Fernet
        derived = base64.urlsafe_b64encode(hashlib.sha256(CRYPTO_KEY.encode()).digest())
        _fernet = Fernet(derived)
        _use_fernet = True
        logger.info("✅ Crypto: Fernet habilitado")
except Exception as e:
    logger.warning(f"⚠️ Fernet indisponível, usando fallback base64: {e}")

def _encrypt_value(s: str) -> str:
    if not s:
        return s
    try:
        return _fernet.encrypt(s.encode()).decode() if _use_fernet else base64.b64encode(s.encode()).decode()
    except Exception:
        return base64.b64encode(s.encode()).decode()

def _decrypt_value(s: str) -> str:
    if not s:
        return s
    try:
        return _fernet.decrypt(s.encode()).decode() if _use_fernet else base64.b64decode(s.encode()).decode()
    except Exception:
        try:
            return base64.b64decode(s.encode()).decode()
        except Exception:
            return s

# ======================================
# Configuração DB
# ======================================
DATABASE_URL = (
    os.getenv("DATABASE_URL")
    or os.getenv("POSTGRES_URL")
    or os.getenv("POSTGRESQL_URL")
)

if not DATABASE_URL:
    logger.error("⚠️ DATABASE_URL não configurado. DB desativado.")

engine = create_engine(
    DATABASE_URL,
    pool_size=int(os.getenv("DB_POOL_SIZE", 50)),
    max_overflow=int(os.getenv("DB_MAX_OVERFLOW", 150)),
    pool_pre_ping=True,
    pool_recycle=1800,
    future=True,
) if DATABASE_URL else None

SessionLocal = sessionmaker(bind=engine, expire_on_commit=False) if engine else None
Base = declarative_base() if engine else None

# ======================================
# Modelo Buyer (rastreamento em tempo real)
# ======================================
class Buyer(Base):
    __tablename__ = "buyers"

    id = Column(Integer, primary_key=True, index=True)
    sid = Column(String(128), index=True, nullable=False)
    event_key = Column(String(128), unique=True, nullable=False, index=True)

    src_url = Column(Text, nullable=True)
    cid = Column(String(128), nullable=True)
    gclid = Column(String(256), nullable=True)
    fbclid = Column(String(256), nullable=True)
    value = Column(Float, nullable=True)

    source = Column(String(32), nullable=True, default="bot")

    # Dados enriquecidos
    user_data = Column(JSONB, nullable=False, default={})
    custom_data = Column(JSONB, nullable=True, default={})

    # Rastreio avançado
    cookies = Column(JSONB, nullable=True)
    postal_code = Column(String(32), nullable=True)
    device_info = Column(JSONB, nullable=True)
    session_metadata = Column(JSONB, nullable=True)

    # Histórico + pixels enviados
    sent_pixels = Column(JSONB, nullable=True, default=list)
    event_history = Column(JSONB, nullable=True, default=list)

    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    sent = Column(Boolean, default=False, index=True)
    attempts = Column(Integer, default=0)
    last_attempt_at = Column(DateTime(timezone=True), nullable=True)
    last_sent_at = Column(DateTime(timezone=True), nullable=True)

# ======================================
# Inicializa DB
# ======================================
def init_db():
    if not engine:
        return
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ DB inicializado e tabelas sincronizadas")
    except SQLAlchemyError as e:
        logger.error(f"Erro init DB: {e}")

# ======================================
# Priority Score (enriquecimento de eventos)
# ======================================
def compute_priority_score(user_data: Dict[str, Any], custom_data: Dict[str, Any]) -> float:
    score = 0.0
    if not user_data: user_data = {}
    if not custom_data: custom_data = {}
    if user_data.get("username"): score += 2
    if user_data.get("first_name"): score += 1
    if user_data.get("premium"): score += 2
    purchase_count = custom_data.get("purchase_count") or 0
    try:
        score += float(purchase_count) * 3
    except:
        pass
    return score

# ======================================
# Save Lead (merge + retries + sincronia total)
# ======================================
async def save_lead(data: dict, event_record: Optional[dict] = None, retries: int = 3) -> bool:
    if not SessionLocal:
        logger.warning("DB desativado - save_lead ignorado")
        return False

    loop = asyncio.get_event_loop()

    def db_sync():
        nonlocal retries
        while retries > 0:
            session = SessionLocal()
            try:
                ek = data.get("event_key")
                telegram_id = (data.get("user_data") or {}).get("telegram_id") or data.get("telegram_id")
                if not ek or not telegram_id:
                    return False

                buyer = session.query(Buyer).filter(Buyer.event_key == ek).first()

                normalized_ud = data.get("user_data") or {"telegram_id": telegram_id}
                for key in [
                    "premium", "lang", "origin", "user_agent",
                    "ip_address", "utm_source", "utm_medium",
                    "utm_campaign", "referrer"
                ]:
                    if data.get(key) is not None:
                        normalized_ud[key] = data.get(key)

                device_info = data.get("device_info") or {}
                session_metadata = data.get("session_metadata") or {"ts": int(time.time()), "source": data.get("source") or "bot"}

                custom = data.get("custom_data") or {}
                custom.setdefault("purchase_count", 0)
                custom["priority_score"] = compute_priority_score(normalized_ud, custom)

                cookies_in = data.get("cookies")
                enc_cookies = None
                if cookies_in:
                    enc_cookies = {k: _encrypt_value(str(v)) for k, v in (cookies_in.items() if isinstance(cookies_in, dict) else [])}

                if buyer:
                    # merge inteligente
                    buyer.user_data = {**(buyer.user_data or {}), **normalized_ud}
                    buyer.custom_data = {**(buyer.custom_data or {}), **custom}
                    buyer.src_url = data.get("src_url") or buyer.src_url
                    buyer.cid = data.get("cid") or buyer.cid
                    buyer.gclid = data.get("gclid") or buyer.gclid
                    buyer.fbclid = data.get("fbclid") or buyer.fbclid
                    buyer.value = data.get("value") or buyer.value
                    buyer.source = data.get("source") or buyer.source

                    if enc_cookies:
                        ec = buyer.cookies or {}
                        ec.update(enc_cookies)
                        buyer.cookies = ec

                    buyer.device_info = device_info or buyer.device_info
                    buyer.session_metadata = {**(buyer.session_metadata or {}), **session_metadata}

                    incoming_sent = data.get("sent_pixels") or []
                    if incoming_sent:
                        buyer.sent_pixels = list(set((buyer.sent_pixels or []) + incoming_sent))

                    if event_record:
                        eh = buyer.event_history or []
                        eh.append(event_record)
                        buyer.event_history = eh

                else:
                    # novo lead
                    buyer = Buyer(
                        sid=data.get("sid") or f"tg-{telegram_id}-{int(datetime.now().timestamp())}",
                        event_key=ek,
                        src_url=data.get("src_url"),
                        cid=data.get("cid"),
                        gclid=data.get("gclid"),
                        fbclid=data.get("fbclid"),
                        value=data.get("value"),
                        source=data.get("source") or "bot",
                        user_data=normalized_ud,
                        custom_data=custom,
                        cookies=enc_cookies,
                        postal_code=data.get("postal_code"),
                        device_info=device_info,
                        session_metadata=session_metadata,
                        sent_pixels=list(data.get("sent_pixels") or []),
                        event_history=[event_record] if event_record else []
                    )
                    session.add(buyer)

                if event_record:
                    if event_record.get("status") == "success":
                        buyer.last_sent_at = datetime.now(timezone.utc)
                        buyer.sent = True
                    elif event_record.get("status") == "failed":
                        buyer.last_attempt_at = datetime.now(timezone.utc)
                        buyer.attempts = (buyer.attempts or 0) + 1

                session.commit()
                return True
            except OperationalError as e:
                session.rollback()
                retries -= 1
                logger.warning(f"Conexão DB falhou, retry... ({retries} left) {e}")
                time.sleep(1)
            except Exception as e:
                session.rollback()
                logger.error(f"Erro save_lead: {e}")
                return False
            finally:
                session.close()
        return False

    return await loop.run_in_executor(None, db_sync)

# ======================================
# Get Historical Leads (retrofeed + análise)
# ======================================
async def get_historical_leads(limit: int = 50, order_by_priority: bool = True):
    if not SessionLocal:
        return []
    loop = asyncio.get_event_loop()

    def db_sync():
        session = SessionLocal()
        try:
            query = session.query(Buyer)
            if order_by_priority:
                query = query.order_by(
                    func.coalesce((Buyer.custom_data['priority_score']).astext.cast(Float), 0.0).desc(),
                    Buyer.created_at.desc()
                )
            else:
                query = query.order_by(Buyer.created_at.desc())

            rows = query.limit(limit).all()
            leads = []
            for r in rows:
                ud, cd = r.user_data or {}, r.custom_data or {}
                dec_cookies = {}
                if r.cookies:
                    try:
                        for k, v in (r.cookies.items() if isinstance(r.cookies, dict) else []):
                            dec_cookies[k] = _decrypt_value(v)
                    except Exception:
                        dec_cookies = r.cookies
                leads.append({
                    "telegram_id": ud.get("telegram_id"),
                    "username": ud.get("username"),
                    "first_name": ud.get("first_name"),
                    "last_name": ud.get("last_name"),
                    "premium": ud.get("premium"),
                    "lang": ud.get("lang"),
                    "cookies": dec_cookies,
                    "event_key": r.event_key,
                    "sid": r.sid,
                    "src_url": r.src_url,
                    "cid": r.cid,
                    "gclid": r.gclid,
                    "fbclid": r.fbclid,
                    "value": r.value,
                    "sent_pixels": r.sent_pixels or [],
                    "event_history": r.event_history or [],
                    "priority_score": cd.get("priority_score") or 0.0
                })
            return leads
        except Exception as e:
            logger.error(f"Erro get_historical_leads: {e}")
            return []
        finally:
            session.close()

    return await loop.run_in_executor(None, db_sync)

# ======================================
# Sync Pending Leads (retry de pixels não enviados)
# ======================================
async def sync_pending_leads(batch_size: int = 20):
    if not SessionLocal:
        return 0
    loop = asyncio.get_event_loop()

    def db_sync():
        session = SessionLocal()
        try:
            pending = session.query(Buyer).filter(Buyer.sent == False).limit(batch_size).all()
            count = 0
            for b in pending:
                # aqui você pode disparar a lógica de reenvio de pixels
                b.attempts = (b.attempts or 0) + 1
                b.last_attempt_at = datetime.now(timezone.utc)
                session.commit()
                count += 1
            return count
        except Exception as e:
            logger.error(f"Erro sync_pending_leads: {e}")
            return 0
        finally:
            session.close()

    return await loop.run_in_executor(None, db_sync)