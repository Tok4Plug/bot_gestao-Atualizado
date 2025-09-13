# High-scale Telegram Bot + Worker

Files included:
- bot.py: Telegram bot (producer) - creates invite and enqueues event to Redis Stream
- worker.py: Redis Streams consumer - batching, priority by lead_score, send to FB/GA, retry & DLQ
- db.py: SQLAlchemy models and helper (init_db, save_lead)
- fb_google.py: optimized FB/GA senders with rate limiting and batching
- admin_service.py: simple admin endpoints (resend, metrics, health)
- requirements.txt, Procfile, .env.example

Deploy steps (short):
1. Provision Redis and Postgres on Railway.
2. Set environment variables (see .env.example).
3. Deploy and ensure processes `bot`, `worker`, `admin` are running.

