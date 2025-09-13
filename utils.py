import os
import time
import json
import requests
import traceback
import base64
import hashlib
from typing import Dict, Any, List, Tuple, Optional

# =============================
# Configurações
# =============================
FB_API_VERSION = os.getenv('FB_API_VERSION', 'v19.0')
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '12'))
FB_RETRY_MAX = int(os.getenv('FB_RETRY_MAX', '3'))
FB_RETRY_BACKOFF_BASE = float(os.getenv('FB_RETRY_BACKOFF_BASE', '0.5'))
FB_BATCH_MAX = int(os.getenv('FB_BATCH_MAX', '200'))

PIXEL_ID_1 = os.getenv('PIXEL_ID_1')
ACCESS_TOKEN_1 = os.getenv('ACCESS_TOKEN_1')
PIXEL_ID_2 = os.getenv('PIXEL_ID_2')
ACCESS_TOKEN_2 = os.getenv('ACCESS_TOKEN_2')

NEW_PIXEL_IDS_RAW = os.getenv('NEW_PIXEL_IDS', '')
NEW_PIXEL_IDS = [p.strip() for p in NEW_PIXEL_IDS_RAW.split(',') if p.strip()]

# =============================
# Parsing de novos pixels
# =============================
def _parse_new_pixels() -> List[Tuple[str, Optional[str]]]:
    out = []
    for item in NEW_PIXEL_IDS:
        if ':' in item:
            pid, token = item.split(':', 1)
            out.append((pid.strip(), token.strip()))
        else:
            out.append((item, None))
    return out

NEW_PIXELS_PARSED = _parse_new_pixels()

# =============================
# Helpers - Logging seguro
# =============================
def _redact_sensitive(obj: Any) -> Any:
    """Redacts sensitive fields (cookies, fbp, fbc, email, phone) for safe logging."""
    if isinstance(obj, dict):
        redacted = {}
        for k, v in obj.items():
            kl = k.lower()
            if kl in ('cookies', '_fbp', '_fbc', 'email', 'phone', 'ph', 'em'):
                redacted[k] = 'REDACTED'
            else:
                redacted[k] = _redact_sensitive(v)
        return redacted
    if isinstance(obj, list):
        return [_redact_sensitive(i) for i in obj]
    return obj

def _safe_log(msg: str, obj: Any = None):
    try:
        if obj is not None:
            print(msg, json.dumps(_redact_sensitive(obj), default=str))
        else:
            print(msg)
    except Exception:
        print(msg, "(log error)")

# =============================
# POST síncrono com retry
# =============================
def _post_facebook_sync(pixel_id: str, access_token: str, payload: Dict[str, Any], retries: int = FB_RETRY_MAX) -> Dict[str, Any]:
    """
    POST para Facebook Graph API com retry exponencial e logging seguro.
    Retorna dict com: {ok, status_code, body, attempts}.
    """
    if not pixel_id or not access_token:
        return {'ok': False, 'error': 'pixel_or_token_missing', 'pixel_id': pixel_id}

    url = f'https://graph.facebook.com/{FB_API_VERSION}/{pixel_id}/events'
    backoff = FB_RETRY_BACKOFF_BASE
    attempt = 0

    while attempt <= retries:
        attempt += 1
        try:
            r = requests.post(url, json=payload, params={'access_token': access_token}, timeout=REQUEST_TIMEOUT)
            status = r.status_code
            try:
                body = r.json()
            except Exception:
                body = r.text

            if 200 <= status < 300:
                _safe_log("[FB POST OK]", {"pixel": pixel_id, "attempt": attempt, "status": status})
                return {'ok': True, 'status_code': status, 'body': body, 'attempts': attempt}
            else:
                _safe_log("[FB POST WARN]", {"pixel": pixel_id, "attempt": attempt, "status": status, "body": body})

        except Exception as e:
            _safe_log("[FB POST EXCEPTION]", {"pixel": pixel_id, "attempt": attempt, "error": str(e)})
            traceback.print_exc()

        time.sleep(backoff)
        backoff *= 2  # retry exponencial

    _safe_log("[FB POST FAILED AFTER RETRIES]", {"pixel": pixel_id, "attempts": attempt})
    return {'ok': False, 'status_code': None, 'body': 'failed_after_retries', 'attempts': attempt}

# =============================
# Envio para múltiplos pixels
# =============================
def send_to_pixels(event_payload: Dict[str, Any], pixels: Optional[List[Tuple[str, Optional[str]]]] = None) -> Dict[str, Dict[str, Any]]:
    """
    Envia o mesmo payload para múltiplos pixels.
    Se token não fornecido, resolve a partir do ENV.
    Retorna dict { pixel_id: result_dict }.
    """
    results: Dict[str, Dict[str, Any]] = {}
    send_list: List[Tuple[str, Optional[str]]] = []

    if pixels:
        send_list.extend(pixels)

    # adiciona principais pixels se não presentes
    def _add_if_not_present(pid: Optional[str], token: Optional[str]):
        if not pid:
            return
        for p, _ in send_list:
            if p == pid:
                return
        send_list.append((pid, token))

    _add_if_not_present(PIXEL_ID_1, ACCESS_TOKEN_1)
    _add_if_not_present(PIXEL_ID_2, ACCESS_TOKEN_2)
    for p in NEW_PIXELS_PARSED:
        _add_if_not_present(p[0], p[1])

    # envio sequencial (paralelismo é tratado no worker)
    for pid, token in send_list:
        resolved_token = token
        if not resolved_token:
            if pid == PIXEL_ID_1 and ACCESS_TOKEN_1:
                resolved_token = ACCESS_TOKEN_1
            elif pid == PIXEL_ID_2 and ACCESS_TOKEN_2:
                resolved_token = ACCESS_TOKEN_2
            else:
                resolved_token = os.getenv(f"ACCESS_TOKEN_{pid}")

        _safe_log("[SENDING_EVENT_TO_PIXEL]", {
            "pixel": pid,
            "payload_meta": {
                "event_name": event_payload.get("event_name"),
                "event_time": event_payload.get("event_time")
            }
        })

        try:
            res = _post_facebook_sync(pid, resolved_token, event_payload)
        except Exception as e:
            _safe_log("[SEND_EXCEPTION]", {"pixel": pid, "error": str(e)})
            res = {'ok': False, 'error': str(e)}

        results[pid] = res

    return results

# =============================
# Compatibilidade antiga
# =============================
def send_to_all_pixels(event_payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Wrapper para compatibilidade com versão anterior."""
    return send_to_pixels(event_payload)

# =============================
# Builder de payload FB
# =============================
def build_fb_event(event_name: str, lead_data: Dict[str, Any], custom_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Helper para montar payload FB no formato Graph API.
    Não faz hashing/enrichment profundo — use fb_google.enrich_user_data.
    """
    user_data = lead_data.get("user_data") or {}
    payload = {
        "data": [
            {
                "event_name": event_name,
                "event_time": int(time.time()),
                "event_source_url": lead_data.get("src_url"),
                "user_data": user_data,
                "custom_data": custom_data or {
                    "currency": lead_data.get("currency", "BRL"),
                    "value": lead_data.get("value") or 0
                }
            }
        ]
    }
    return payload