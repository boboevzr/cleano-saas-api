import os
import re
import base64
import hashlib
import secrets
import logging
import asyncio
import random
import string
from datetime import datetime, timedelta, timezone
from typing import Optional

import json as _json
import aiohttp
from fastapi import FastAPI, HTTPException, Depends, Header, Body, Request, UploadFile, File, Form, WebSocket, WebSocketDisconnect, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, field_validator
from passlib.context import CryptContext
from jose import jwt, JWTError

import database as db
import receipt

logging.basicConfig(level=logging.INFO)

# ── Конфигурация ──
JWT_SECRET        = os.getenv("JWT_SECRET", "dev-secret-change-me")
JWT_ALGORITHM     = "HS256"
JWT_EXPIRE_DAYS   = 30
SMS_CODE_TTL_MIN  = 5
ADMIN_PASS        = os.getenv("ADMIN_PASS", "")
SUPERADMIN_SECRET = os.getenv("SUPERADMIN_SECRET", "")  # SaaS: суперадмин-ключ

async def get_admin_pass() -> str:
    """Читает пароль из БД (если изменён через UI), иначе — env var."""
    return await db.get_config("admin_pass") or ADMIN_PASS
VAPID_PRIVATE  = os.getenv("VAPID_PRIVATE", "")
VAPID_PUBLIC   = os.getenv("VAPID_PUBLIC", "")

BOT_TOKEN          = os.getenv("BOT_TOKEN", "")
GROUP_ID              = os.getenv("GROUP_ID", "")
GROUP_ID_ZARAFSHAN    = os.getenv("GROUP_ID_ZARAFSHAN", "")
LEADS_GROUP_ID        = os.getenv("LEADS_GROUP_ID", "-1004486597965")
GROUP_NEW_CLIENTS_ID  = os.getenv("GROUP_NEW_CLIENTS_ID", "-1003768571929")
GROUP_ID_NAVOI     = os.getenv("GROUP_ID_NAVOI", "")
MEDIA_CHANNEL_ID   = os.getenv("MEDIA_CHANNEL_ID", "-1004453880659")
APP_URL            = os.getenv("APP_URL", "https://web-production-eef2a.up.railway.app")

async def _get_media_channel() -> str:
    ch = await db.get_media_channel_id()
    return ch or MEDIA_CHANNEL_ID
SHEETS_URL = os.getenv("SHEETS_URL", "https://script.google.com/macros/s/AKfycbyU5a3pMuTFme3dBNEgu46qzA1sN1Ekw-Q7p39F1Pg872lnnXZEFhJPjuc4TzZNHlpObQ/exec")

# ── Eskiz SMS ──
ESKIZ_EMAIL    = os.getenv("ESKIZ_EMAIL", "")
ESKIZ_PASSWORD = os.getenv("ESKIZ_PASSWORD", "")
ESKIZ_FROM     = os.getenv("ESKIZ_FROM", "4546")   # имя отправителя — 4546 для тестов
_eskiz_tokens: dict[int, str] = {}  # кэш токена в памяти, per-company (company_id -> token)
                                     # раньше была одна общая строка на процесс — у двух компаний
                                     # с разными Eskiz-аккаунтами токены могли перезаписывать друг
                                     # друга при почти одновременных запросах (см. фикс 2026-08-14)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

app = FastAPI(title="ARTEZ API")

def bi(ru: str, uz: str) -> str:
    """Двуязычное сообщение для ошибок, видимых пользователю."""
    return f"{ru} / {uz}"

# CORS — разрешаем запросы с сайта (уточните домен в проде)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

@app.middleware("http")
async def company_id_middleware(request: Request, call_next):
    """Читает company_id из JWT и записывает в ContextVar перед каждым запросом."""
    company_id = 1
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer "):
        token = auth.removeprefix("Bearer ").strip()
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
            company_id = int(payload.get("company_id") or 1)
        except Exception:
            pass
    db.set_request_company(company_id)
    return await call_next(request)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logging.error(f"422 on {request.method} {request.url.path}: {exc.errors()}")
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors()},
        headers={"Access-Control-Allow-Origin": "*"},
    )

@app.exception_handler(db.SubscriptionExpiredError)
async def subscription_expired_handler(request: Request, exc: "db.SubscriptionExpiredError"):
    """create_lead()/save_site_order() поднимают это, когда у компании нет действующей
    подписки — единая точка перехвата вместо правки каждого из ~7 эндпоинтов, которые
    их вызывают (сайт, staff.html, агент-кабинет, конвертация лида в заказ и т.д.).
    Текст НАМЕРЕННО нейтральный — этот же 402 видит и сотрудник компании (staff.html),
    и обычный посетитель сайта компании (site-common.js показывает detail напрямую
    клиенту, см. submitOrder/extractErrorMessage) — им нельзя знать, что у их
    поставщика услуг закончилась оплата SaaS-подписки. Владелец компании видит
    реальный статус подписки отдельно, на странице тарифа в admin.html."""
    return JSONResponse(
        status_code=402,
        content={"detail": "Извините, сейчас ведутся технические работы. Пожалуйста, попробуйте немного позже."},
        headers={"Access-Control-Allow-Origin": "*"},
    )

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logging.error(f"Unhandled exception on {request.method} {request.url.path}: {type(exc).__name__}: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": f"{type(exc).__name__}: {exc}"},
        headers={"Access-Control-Allow-Origin": "*"},
    )


@app.on_event("startup")
async def startup():
    await db.init_db()
    await db.ensure_plans_table()
    await db.ensure_chat_tables()
    await db.ensure_chat_templates()
    await db.ensure_expense_tables()
    await db.ensure_salary_ledger_table()
    asyncio.create_task(_tg_reminder_worker())
    asyncio.create_task(_salary_accrual_worker())
    asyncio.create_task(_chat_timeout_worker())
    asyncio.create_task(_measure_review_worker())
    asyncio.create_task(_debt_reminder_worker())
    asyncio.create_task(_postpone_reminder_worker())
    asyncio.create_task(_route_rollover_worker())
    await db.ensure_sms_dispatch_table()
    await db.migrate_company1_positions()
    await db.migrate_name_uz()
    await db.ensure_sms_operator_prices()
    await db.ensure_saas_schema()
    asyncio.create_task(_sms_dispatch_worker())
    asyncio.create_task(_subscription_reminder_worker())
    # Webhook не нужен — бот работает в режиме polling (ARTEZ-BOT сервис на Railway)
    # if BOT_TOKEN and APP_URL:
    #     asyncio.create_task(_set_tg_webhook())

async def send_web_push(staff_id: int, title: str, body: str, lead_id: int = None, phone: str = None,
                        order_id: int = None, item_id: int = None, push_type: str = None,
                        driver_staff_id: int = None):
    if not VAPID_PRIVATE or not VAPID_PUBLIC:
        return
    try:
        from pywebpush import webpush, WebPushException
        subs = await db.get_push_subscriptions(staff_id)
        for sub in subs:
            try:
                payload = _json.dumps({"title": title, "body": body, "lead_id": lead_id, "phone": phone,
                                       "order_id": order_id, "item_id": item_id, "type": push_type,
                                       "driver_staff_id": driver_staff_id})
                webpush(
                    subscription_info={"endpoint": sub["endpoint"],
                                       "keys": {"p256dh": sub["p256dh"], "auth": sub["auth"]}},
                    data=payload,
                    vapid_private_key=VAPID_PRIVATE,
                    vapid_claims={"sub": "mailto:admin@artez.uz"},
                )
            except Exception as ex:
                resp = getattr(ex, 'response', None)
                if resp and resp.status_code in (404, 410):
                    await db.delete_push_subscription_by_id(sub["id"])
                else:
                    logging.warning(f"web_push error for sub {sub['id']}: {ex}")
    except ImportError:
        logging.warning("pywebpush not installed, skipping web push")
    except Exception as e:
        logging.warning(f"send_web_push error: {e}")


async def _tg_reminder_worker():
    """Каждую минуту проверяет напоминания и шлёт в Telegram + Web Push."""
    await asyncio.sleep(10)
    while True:
        try:
            if BOT_TOKEN:
                rows = await db.get_pending_tg_reminders()
                for r in rows:
                    lead_code = r["lead_code"] or f"#{r['lead_id']}"
                    client    = r["client_name"] or r["client_phone"]
                    msg       = r["message"] or "Запланированный звонок"
                    tg_id     = r["staff_tg_id"]
                    staff_name = " ".join(filter(None, [r.get("staff_last_name"), r.get("staff_first_name")])) or "Сотрудник"

                    if tg_id:
                        text = (f"⏰ Напоминание о звонке\n\n"
                                f"Лид {lead_code} — {client}\n"
                                f"📞 {r['client_phone']}\n"
                                f"💬 {msg}")
                        await send_tg(tg_id, text)
                    else:
                        text = (f"⏰ Напоминание ({staff_name})\n\n"
                                f"Лид {lead_code} — {client}\n"
                                f"📞 {r['client_phone']}\n"
                                f"💬 {msg}")
                        await send_tg(LEADS_GROUP_ID, text)
                    # Web Push + уведомление — только тому, кто взял лид (target_staff_id)
                    target_id = r.get("target_staff_id") or r.get("staff_id")
                    if target_id:
                        push_body = f"📞 {r['client_phone']}" + (f"\n{msg}" if msg != "Запланированный звонок" else "")
                        asyncio.create_task(send_web_push(
                            target_id,
                            f"🔔 Перезвонить: {client}",
                            push_body,
                            r["lead_id"],
                            r["client_phone"]
                        ))
                        try:
                            await db.create_agent_notification(
                                target_id, r["lead_id"],
                                "callback",
                                f"Пора перезвонить: {client} — {r['client_phone']}"
                                + (f". {msg}" if msg != "Запланированный звонок" else "")
                            )
                        except Exception:
                            pass
                    await db.mark_reminder_sent(r["id"], "tg")
        except Exception as e:
            logging.warning(f"TG reminder worker error: {e}")
        await asyncio.sleep(60)


async def _sms_dispatch_worker():
    """Каждую минуту: отправляет запланированные SMS-рассылки через sms/send."""
    await asyncio.sleep(15)
    while True:
        try:
            pending = await db.get_pending_sms_dispatches()
            for dispatch in pending:
                import json as _j2
                phones = _j2.loads(dispatch["phones"]) if isinstance(dispatch["phones"], str) else dispatch["phones"]
                frm    = dispatch["from_nick"] or "4546"
                msg    = dispatch["message"]
                token  = await _eskiz_get_token()
                sent   = 0
                if token and phones:
                    async with aiohttp.ClientSession() as s:
                        for phone in phones:
                            try:
                                r = await s.post(
                                    "https://notify.eskiz.uz/api/message/sms/send",
                                    headers={"Authorization": f"Bearer {token}"},
                                    data={"mobile_phone": phone, "message": msg,
                                          "from": frm, "callback_url": ""},
                                    timeout=aiohttp.ClientTimeout(total=15),
                                )
                                resp = await _eskiz_parse(r)
                                if resp.get("status") == "waiting":
                                    sent += 1
                            except Exception as e:
                                logging.warning(f"_sms_dispatch_worker send error: {e}")
                await db.mark_sms_dispatch_sent(dispatch["id"], sent)
                logging.info(f"SMS dispatch {dispatch['id']} '{dispatch['name']}': sent {sent}/{len(phones)}")
        except Exception as e:
            logging.warning(f"_sms_dispatch_worker error: {e}")
        await asyncio.sleep(60)


async def _measure_review_worker():
    """Каждые 5 минут: один сводный push на каждого проверяющего, по каждой компании отдельно."""
    await asyncio.sleep(30)
    while True:
        try:
            from datetime import datetime, timezone
            companies = await db.get_all_companies()
            for company in companies:
                db.set_request_company(company["id"])
                reviews   = await db.get_pending_measure_reviews()
                if not reviews:
                    continue
                approvers = await db.get_all_approvers()
                now = datetime.now(timezone.utc)

                # Замеры которые принял кто-то и прошло > 5 мин — напомнить именно ему
                reminded_claimer = set()
                for rev in reviews:
                    claimed_by = rev.get("review_claimed_by")
                    claimed_at = rev.get("review_claimed_at")
                    if claimed_by and claimed_at and claimed_by not in reminded_claimer:
                        elapsed = (now - claimed_at.replace(tzinfo=timezone.utc)).total_seconds()
                        if elapsed > 300:
                            order_num = rev.get("order_num") or f"#{rev['order_id']}"
                            asyncio.create_task(send_web_push(
                                claimed_by, "⏰ Не забудь проверить замеры",
                                f"Принятые замеры ждут утверждения (заказ {order_num})",
                                order_id=rev["order_id"], item_id=rev["item_id"], push_type="measure"
                            ))
                            reminded_claimer.add(claimed_by)

                # Незаклеймленные — один сводный пуш на каждого проверяющего
                unclaimed = [r for r in reviews if not r.get("review_claimed_by")]
                if unclaimed:
                    cnt = len(unclaimed)
                    if cnt == 1:
                        r = unclaimed[0]
                        title = f"📐 Замер на проверку — {r.get('order_num') or '#'+str(r['order_id'])}"
                        body  = f"«{r.get('service') or 'позиция'}» ожидает утверждения"
                        first_order_id = r["order_id"]
                        first_item_id  = r["item_id"]
                    else:
                        orders = list({r["order_id"] for r in unclaimed})
                        title = f"📐 {cnt} замеров ожидают проверки"
                        body  = f"Заказов: {len(orders)} · Нажмите для просмотра"
                        first_order_id = unclaimed[0]["order_id"]
                        first_item_id  = unclaimed[0]["item_id"]
                    for approver in approvers:
                        asyncio.create_task(send_web_push(
                            approver["id"], title, body,
                            order_id=first_order_id, item_id=first_item_id, push_type="measure"
                        ))
        except Exception as e:
            logging.warning(f"measure_review_worker error: {e}")
        await asyncio.sleep(300)


async def send_tg(chat_id, text: str):
    if not BOT_TOKEN or not chat_id:
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        async with aiohttp.ClientSession() as s:
            await s.post(url, json={"chat_id": str(chat_id), "text": text},
                         timeout=aiohttp.ClientTimeout(total=5))
    except Exception as e:
        logging.warning(f"send_tg error: {e}")


async def _send_company_tg(company_id: int, tg_id: int, text: str) -> bool:
    """Отправляет сообщение клиенту через СОБСТВЕННЫЙ бот его компании
    (order_bot_token), а не глобальный BOT_TOKEN/send_tg — тот принадлежит
    другому боту, tg_id клиента с ним может ни разу не пересекаться
    (см. мульти-бот архитектуру, fetch_bot_welcome_video_bytes/bot_register_client).
    Возвращает True, если Telegram подтвердил доставку (ok=true)."""
    token = await db.get_config_for_company("order_bot_token", company_id)
    if not token or not tg_id:
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        async with aiohttp.ClientSession() as s:
            async with s.post(url, json={"chat_id": str(tg_id), "text": text, "parse_mode": "HTML"},
                               timeout=aiohttp.ClientTimeout(total=5)) as r:
                result = await r.json()
                return bool(result.get("ok"))
    except Exception as e:
        logging.warning(f"_send_company_tg error: {e}")
        return False


async def _send_debt_reminders():
    from datetime import date as _date
    today = _date.today()
    debts = await db.get_orders_with_debt()
    if not debts:
        return
    approvers = await db.get_debt_approvers()
    approver_tg_ids = [a['tg_id'] for a in approvers if a.get('tg_id')]
    sent = set()
    async with db.pool.acquire() as conn:
        for d in debts:
            due = d.get('debt_due_date')
            if not due:
                continue
            if hasattr(due, 'isoformat'):
                due_date = due
            else:
                from datetime import date as _d2
                due_date = _d2.fromisoformat(str(due))
            if due_date > today:
                continue
            days_over = (today - due_date).days
            status_line = f"🔴 Просрочен {days_over} дн." if days_over > 0 else "🟡 Срок сегодня"
            msg = (
                f"{status_line}\n"
                f"💸 Долг по заказу {d['order_num']}\n"
                f"👤 {d.get('client_first_name','')} {d.get('client_last_name','')}\n"
                f"📞 {d.get('client_phone') or '—'}\n"
                f"💰 {int(d['debt_amount']):,} сум\n"
                f"📅 Срок: {due_date.strftime('%d.%m.%Y')}"
            ).replace(',', ' ')
            if d.get('responsible_id'):
                row = await conn.fetchrow("SELECT tg_id FROM staff WHERE id=$1", d['responsible_id'])
                if row and row['tg_id']:
                    key = (d['responsible_id'], d['id'])
                    if key not in sent:
                        await send_tg(row['tg_id'], msg)
                        sent.add(key)
            if days_over > 0:
                for tg_id in approver_tg_ids:
                    key = (f"appr_{tg_id}", d['id'])
                    if key not in sent:
                        await send_tg(tg_id, msg)
                        sent.add(key)

async def _debt_reminder_worker():
    from datetime import timezone as _tz, timedelta as _td
    _TZ5 = _tz(_td(hours=5))
    while True:
        from datetime import datetime as _dt
        now = _dt.now(_TZ5)
        target = now.replace(hour=9, minute=0, second=0, microsecond=0)
        if now >= target:
            target = target + _td(days=1)
        await asyncio.sleep((target - now).total_seconds())
        try:
            await _send_debt_reminders()
        except Exception as e:
            logging.warning(f"debt reminder error: {e}")


async def _send_postpone_reminders():
    """За день до истечения отсрочки доставки (см. /orders/{id}/postpone) —
    напомнить admin/менеджерам филиала (своей компании) созвониться с клиентом."""
    orders = await db.get_orders_for_postpone_reminder()
    if not orders:
        return
    for o in orders:
        managers = await db.get_order_managers(o.get("company_id") or 1, o.get("branch"))
        if not managers:
            continue
        client = " ".join(p for p in [o.get("client_first_name"), o.get("client_last_name")] if p).strip() or "—"
        until = o.get("postponed_until")
        until_str = until.strftime("%d.%m.%Y") if hasattr(until, "strftime") else str(until or "")
        msg = (
            f"⏸ Завтра ({until_str}) истекает отсрочка доставки\n"
            f"📋 Заказ {o.get('order_num')}\n"
            f"👤 {client}\n"
            f"📞 {o.get('client_phone') or '—'}\n"
            f"📝 Причина: {o.get('postpone_reason') or '—'}\n"
            f"👷 Отложил: {o.get('postponed_by_name') or '—'}\n\n"
            f"Нужно созвониться с клиентом — согласовать доставку или отложить ещё."
        )
        for m in managers:
            await send_tg(m["tg_id"], msg)


async def _postpone_reminder_worker():
    from datetime import timezone as _tz, timedelta as _td
    _TZ5 = _tz(_td(hours=5))
    while True:
        from datetime import datetime as _dt
        now = _dt.now(_TZ5)
        target = now.replace(hour=9, minute=15, second=0, microsecond=0)
        if now >= target:
            target = target + _td(days=1)
        await asyncio.sleep((target - now).total_seconds())
        try:
            await _send_postpone_reminders()
        except Exception as e:
            logging.warning(f"postpone reminder error: {e}")


async def _salary_accrual_worker():
    """Запускает автоначисление зарплат 1-го числа каждого месяца в 00:05 Ташкент."""
    from datetime import timezone as _tz, timedelta as _td, datetime as _dt, date as _date
    _TZ5 = _tz(_td(hours=5))
    while True:
        now = _dt.now(_TZ5)
        # Следующее 1-е число месяца 00:05
        if now.month == 12:
            nxt = _dt(now.year + 1, 1, 1, 0, 5, tzinfo=_TZ5)
        else:
            nxt = _dt(now.year, now.month + 1, 1, 0, 5, tzinfo=_TZ5)
        await asyncio.sleep((nxt - now).total_seconds())
        try:
            count = await db.auto_accrue_monthly_salaries()
            logging.info(f"salary accrual: {count} entries created")
        except Exception as e:
            logging.warning(f"salary accrual error: {e}")

async def _route_rollover_worker():
    """Каждый день в 00:00:05 Ташкент переносит просроченные planned/active маршруты на сегодня."""
    from datetime import timezone as _tz, timedelta as _td, datetime as _dt
    _TZ5 = _tz(_td(hours=5))
    while True:
        now = _dt.now(_TZ5)
        next_midnight = (now + _td(days=1)).replace(hour=0, minute=0, second=5, microsecond=0)
        await asyncio.sleep((next_midnight - now).total_seconds())
        try:
            n = await db.roll_forward_stale_routes()
            if n:
                logging.info(f"route rollover: перенесено маршрутов на сегодня: {n}")
        except Exception as e:
            logging.warning(f"route rollover error: {e}")

async def _edit_tg_handover_msg(chat_id: int, msg_id: int, text: str):
    """Редактировать TG-сообщение передачи наличных: обновить текст, убрать кнопки."""
    if not BOT_TOKEN or not chat_id or not msg_id:
        return
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
                json={"chat_id": str(chat_id), "message_id": msg_id,
                      "text": text, "parse_mode": "HTML",
                      "reply_markup": {"inline_keyboard": []}},
                timeout=aiohttp.ClientTimeout(total=6),
            )
    except Exception as e:
        logging.warning(f"_edit_tg_handover_msg error: {e}")


async def _send_tg_with_kb(chat_id, text: str, keyboard: dict,
                           parse_mode: str | None = "HTML",
                           silent: bool = False, protect: bool = False) -> int | None:
    """Отправить сообщение с inline-клавиатурой, вернуть message_id."""
    if not BOT_TOKEN or not chat_id:
        return None
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        async with aiohttp.ClientSession() as s:
            payload = {"chat_id": str(chat_id), "text": text,
                       "reply_markup": keyboard, "disable_web_page_preview": True}
            if parse_mode: payload["parse_mode"] = parse_mode
            if silent:  payload["disable_notification"] = True
            if protect: payload["protect_content"]      = True
            r = await s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8))
            d = await r.json()
            if not d.get("ok"):
                # Группа мигрировала в супергруппу — повторяем с новым ID
                new_id = (d.get("parameters") or {}).get("migrate_to_chat_id")
                if new_id:
                    logging.info(f"_send_tg_with_kb: group migrated → {new_id}, retrying")
                    payload["chat_id"] = str(new_id)
                    r2 = await s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8))
                    d2 = await r2.json()
                    if d2.get("ok"):
                        return d2.get("result", {}).get("message_id")
                    logging.warning(f"_send_tg_with_kb retry error: {d2.get('description')}")
                    return None
                logging.warning(f"_send_tg_with_kb TG error: {d.get('description')}")
                return None
            return d.get("result", {}).get("message_id")
    except Exception as e:
        logging.warning(f"_send_tg_with_kb error: {e}")
        return None

async def _edit_tg_with_kb(chat_id, message_id: int, text: str, keyboard: dict):
    if not BOT_TOKEN: return
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
                json={"chat_id": str(chat_id), "message_id": message_id,
                      "text": text, "parse_mode": "HTML",
                      "reply_markup": keyboard, "disable_web_page_preview": True},
                timeout=aiohttp.ClientTimeout(total=5))
    except Exception as e:
        logging.warning(f"_edit_tg_with_kb error: {e}")

async def _tg_answer_callback(callback_query_id: str, text: str, alert: bool = False):
    if not BOT_TOKEN: return
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/answerCallbackQuery",
                json={"callback_query_id": callback_query_id, "text": text, "show_alert": alert},
                timeout=aiohttp.ClientTimeout(total=5))
    except Exception as e:
        logging.warning(f"answerCallbackQuery error: {e}")


async def _tg_edit_message(chat_id, message_id: int, text: str):
    if not BOT_TOKEN: return
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
                json={"chat_id": str(chat_id), "message_id": message_id,
                      "text": text, "parse_mode": "HTML"},
                timeout=aiohttp.ClientTimeout(total=5))
    except Exception as e:
        logging.warning(f"editMessageText error: {e}")


_STATUS_LABELS_RU = {
    "new":       "🆕 Новый",
    "contacted": "📞 Связались с клиентом",
    "no_answer": "📵 Не дозвонились",
    "callback":  "🔔 Перезвонить",
    "converted": "🏆 Стал заказом!",
    "lost":      "❌ Закрыт как потерянный",
}
_STATUS_LABELS_UZ = {
    "new":       "🆕 Yangi",
    "contacted": "📞 Mijoz bilan bog'landi",
    "no_answer": "📵 Qo'ng'iroq qilmadi",
    "callback":  "🔔 Qayta qo'ng'iroq",
    "converted": "🏆 Buyurtmaga aylandi!",
    "lost":      "❌ Yo'qotilgan deb yopildi",
}

async def _notify_agent_status(lead_id: int, status: str, note: str):
    lead = await db.get_lead_by_id(lead_id)
    if not lead or not lead["volunteer_id"]:
        return
    agent = await db.get_staff_by_id(lead["volunteer_id"])
    if not agent:
        return

    code   = lead.get("lead_code") or f"#{lead_id}"
    client = lead.get("client_name") or lead.get("client_phone") or "—"
    phone  = lead.get("client_phone") or "—"
    label_ru = _STATUS_LABELS_RU.get(status, status)
    label_uz = _STATUS_LABELS_UZ.get(status, status)

    msg_ru = (f"🎯 Обновление по вашему лиду {code}\n\n"
              f"👤 {client}\n📞 {phone}\n\n"
              f"Статус: {label_ru}\n"
              + (f"💬 {note}" if note and note not in _STATUS_LABELS_RU.values() else ""))
    msg_uz = (f"🎯 Sizning lidingiz bo'yicha yangilik {code}\n\n"
              f"👤 {client}\n📞 {phone}\n\n"
              f"Holat: {label_uz}\n"
              + (f"💬 {note}" if note and note not in _STATUS_LABELS_RU.values() else ""))

    # В личный кабинет (таблица)
    await db.create_agent_notification(agent["id"], lead_id, f"status_{status}", msg_ru)

    tg_id = agent.get("tg_id")
    if tg_id:
        await send_tg(tg_id, msg_ru + "\n\n" + msg_uz)


async def _notify_new_lead(lead: dict, staff: dict):
    # Web push к callcenter/manager/admin — всегда, независимо от TG настроек
    if db.pool and lead.get("id"):
        try:
            async with db.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT DISTINCT s.id FROM staff s "
                    "JOIN push_subscriptions ps ON ps.staff_id = s.id "
                    "WHERE s.active=TRUE AND s.role IN ('callcenter','manager','admin')"
                )
            lead_code   = lead.get("lead_code") or f"#{lead.get('id')}"
            push_title  = f"🎯 Новый лид — {lead_code}"
            push_body   = f"{lead.get('client_name') or '—'} · {lead.get('client_phone') or '—'}"
            client_phone = lead.get("client_phone", "")
            for row in rows:
                asyncio.create_task(send_web_push(
                    row["id"], push_title, push_body,
                    lead_id=lead.get("id"), phone=client_phone, push_type="new_lead"
                ))
        except Exception as _ex:
            logging.warning(f"_notify_new_lead push error: {_ex}")

    # Telegram группа — только если включено в настройках
    enabled = await _get_cfg("leads_group_enabled")
    if enabled not in ("1", "true"):
        return

    # Роутинг по филиалу: сначала своя группа из карточки филиала (tg_leads_group_id),
    # затем legacy ARTEZ-ключи (для обратной совместимости), затем общий fallback.
    # branch вычисляется ДО ветвления и используется дальше в vars_["branch"] —
    # раньше был объявлен только внутри else и падал с UnboundLocalError, когда
    # филиал резолвился через branch_group_id (сообщение просто не уходило, тихо,
    # т.к. вся функция — fire-and-forget asyncio.create_task).
    branch_slug = (lead.get("branch", "") or "").strip()
    branch = branch_slug.lower().replace("📍", "").strip()
    branch_group_id = await db.get_branch_tg_group_id(branch_slug, "tg_leads_group_id") if branch_slug else None
    if branch_group_id:
        group_id = str(branch_group_id)
    else:
        if branch in ("zarafshan", "зарафшан", "zarafshon"):
            group_id = await _get_cfg("leads_group_zarafshan") or await _get_cfg("leads_group_id")
        elif branch in ("navoi", "навои", "navoiy"):
            group_id = await _get_cfg("leads_group_navoi") or await _get_cfg("leads_group_id")
        else:
            group_id = await _get_cfg("leads_group_id")

    if not group_id:
        return

    template = await _get_cfg("lead_notify_ru")

    role    = staff.get("role", "")
    if role == "agent":   source = "🤝 Агент"
    elif role == "site":  source = "🌐 Сайт"
    elif role == "bot":   source = "✈️ Telegram"
    else:                 source = "👤 Сотрудник"
    creator = " ".join(filter(None, [staff.get("last_name"), staff.get("first_name")])) or staff.get("login", "—")

    # source_full: для агентов/сотрудников добавляем имя, для сайта — домен компании
    # (одна и та же группа может быть общей на несколько компаний — см. вопрос
    # пользователя "может одну группу использовать 2 компании" — без домена в
    # сообщении невозможно понять, с какого именно сайта пришла заявка).
    if role == "agent":
        source_full = f"🤝 {creator}" if creator and creator != "—" else "🤝 Агент"
    elif role == "site":
        site_domain = None
        try:
            lead_company = await db.get_company(lead.get("company_id"))
            slug = (lead_company["slug"] if lead_company else "") or ""
            site_domain = "artez.uz" if slug == "artez" else (f"{slug}.cleano.uz" if slug else None)
        except Exception as e:
            logging.warning(f"_notify_new_lead site_domain error: {e}")
        source_full = f"🌐 {site_domain}" if site_domain else "🌐 Сайт"
    elif role == "bot":
        source_full = "✈️ Telegram"
    else:
        source_full = f"👤 {creator}" if creator and creator != "—" else "👤 Сотрудник"

    loc = (lead.get("location") or "").strip()
    if loc:
        parts = loc.split(",")
        try:
            lat, lon = parts[0].strip(), parts[1].strip()
            map_url = f"https://yandex.uz/maps/?pt={lon},{lat}&z=16"
            location_link = f'<a href="{map_url}">📍 Локация</a>'
        except Exception:
            location_link = ""
    else:
        location_link = ""

    note_full = lead.get("note") or ""
    # note_short: первый сегмент заметки (до " · "), убираем префикс "Тип: "
    note_first = note_full.split(" · ")[0] if note_full else ""
    if note_first.startswith("Тип: "):
        note_first = note_first[5:]
    note_inline = f" · {note_first}" if note_first else ""

    vars_ = {
        "lead_code":     lead.get("lead_code") or f"#{lead.get('id')}",
        "client_name":   lead.get("client_name") or "—",
        "client_phone":  lead.get("client_phone") or "—",
        "branch":        branch_ru(branch) if branch else "—",
        "note":          note_full or "—",
        "note_short":    note_first,
        "note_inline":   note_inline,
        "source":        source,
        "source_full":   source_full,
        "creator":       creator,
        "location_link": location_link,
    }

    text = template
    if not text:
        return

    try:
        msg_text = text.format_map(vars_)
    except Exception:
        msg_text = text

    # Кнопка "Взять лид" — только если лид не занят
    lead_id  = lead.get("id")
    keyboard = None
    if lead_id and not lead.get("assigned_to"):
        keyboard = {"inline_keyboard": [[
            {"text": "✋ Взять лид", "callback_data": f"take_lead_{lead_id}"}
        ]]}

    try:
        lead_company_id = lead.get("company_id")
        token = await db.get_config_for_company("order_bot_token", lead_company_id) if lead_company_id else None
        if not token:
            logging.warning(f"_notify_new_lead: order_bot_token не настроен для company_id={lead_company_id} — пропуск")
            return  # у компании ещё не подключён бот заказов — некому слать уведомление
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": str(group_id),
            "text": msg_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if keyboard:
            payload["reply_markup"] = keyboard
        async with aiohttp.ClientSession() as s:
            async with s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as r:
                result = await r.json()
                if not result.get("ok"):
                    logging.warning(f"_notify_new_lead: Telegram sendMessage failed chat_id={group_id}: {result.get('description')}")
    except Exception as e:
        logging.warning(f"_notify_new_lead error: {e}")


# ══════════════════════════════════════
#  МОДЕЛИ
# ══════════════════════════════════════
PHONE_RE = re.compile(r"^\+998\d{9}$")

def normalize_phone(phone: str) -> str:
    phone = phone.strip().replace(" ", "").replace("-", "")
    if not phone.startswith("+"):
        phone = "+" + phone
    return phone

class RegisterRequest(BaseModel):
    phone: str
    password: str
    first_name: str
    via_tg: bool = False
    lang: str = "ru"
    company_slug: str | None = None

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v):
        v = normalize_phone(v)
        if not PHONE_RE.match(v):
            raise ValueError("Неверный формат номера. Используйте +998XXXXXXXXX")
        return v

    @field_validator("password")
    @classmethod
    def validate_password(cls, v):
        if len(v) < 6:
            raise ValueError("Пароль должен быть не короче 6 символов")
        return v

class VerifyRequest(BaseModel):
    phone: str
    code: str
    company_slug: str | None = None

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v):
        return normalize_phone(v)

class LoginRequest(BaseModel):
    phone: str
    password: str
    company_slug: str | None = None

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v):
        return normalize_phone(v)

class ResendCodeRequest(BaseModel):
    phone: str
    purpose: str = "register"
    via_tg: bool = False
    company_slug: str | None = None

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v):
        return normalize_phone(v)


async def _resolve_client_company_id(company_slug: str | None) -> int:
    """slug -> company_id для клиентских (сайт) эндпоинтов. Без slug — дефолт 1
    (обратная совместимость, пока фронтенд не везде передаёт APP_COMPANY_SLUG)."""
    if not company_slug:
        return 1
    cid = await db.get_company_id_by_slug(company_slug.strip().lower())
    if not cid:
        raise HTTPException(status_code=404, detail="Компания не найдена")
    return cid


class AgentApplyRequest(BaseModel):
    branch: str = ""

class OrderRequest(BaseModel):
    first_name: str
    last_name: str = ""
    phone: str
    branch: str = ""
    city: str = ""
    address: str
    location: str = ""
    location_address: str = ""
    service: str = ""
    service_type: str = ""
    pickup_date: str = ""
    pickup_time: str = ""
    is_quick: bool = False
    total_price: int | None = None
    source: str = "site"  # "site" or "bot"
    client_tg_id: int | None = None
    company_slug: str | None = None

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v):
        v = normalize_phone(v)
        if not PHONE_RE.match(v):
            raise ValueError("Неверный формат номера. Используйте +998XXXXXXXXX")
        return v

    @field_validator("first_name")
    @classmethod
    def validate_name(cls, v):
        if not v.strip():
            raise ValueError("Укажите имя")
        return v.strip()

    @field_validator("address")
    @classmethod
    def validate_address(cls, v):
        return v.strip()  # allow empty for quick/bot orders


class StaffOrderRequest(BaseModel):
    first_name: str
    phone: str
    service: str = ""
    service_type: str = "standard"
    pickup_type: str = "courier"
    delivery_type: str = "courier"
    branch: str = ""
    address: str = ""
    short_address: str = ""
    location: str = ""
    location_address: str = ""
    note: str = ""
    pickup_date: str = ""
    pickup_time: str = ""
    item_count: int = 0
    items: dict[str, int] = {}
    sms_lang: str = "ru"


# ══════════════════════════════════════
#  SMS — Eskiz.uz
# ══════════════════════════════════════
async def _eskiz_get_token() -> str:
    """Получает/обновляет токен Eskiz текущей компании (через _cid(), как email/
    password ниже). Читает email/пароль из БД (приоритет) или env. Кэш токена —
    per-company (_eskiz_tokens), не общая переменная процесса."""
    cid      = db._cid()
    email    = await _get_cfg("eskiz_email")
    password = await _get_cfg("eskiz_password")
    if not email or not password:
        # Fallback: прямой токен сохранённый в config
        return await db.get_config("eskiz_token") or ""

    token = _eskiz_tokens.get(cid)
    if not token:
        token = await db.get_config("eskiz_token") or ""

    async with aiohttp.ClientSession() as session:
        if token:
            resp = await session.patch(
                "https://notify.eskiz.uz/api/auth/refresh",
                headers={"Authorization": f"Bearer {token}"},
            )
            if resp.status == 200:
                data = await resp.json()
                new_token = data.get("data", {}).get("token", token)
                if new_token != token:
                    token = new_token
                    await db.set_config("eskiz_token", token)
                _eskiz_tokens[cid] = token
                return token

        resp = await session.post(
            "https://notify.eskiz.uz/api/auth/login",
            data={"email": email, "password": password},
        )
        if resp.status == 200:
            data = await resp.json()
            token = data.get("data", {}).get("token", "")
            if token:
                await db.set_config("eskiz_token", token)
                _eskiz_tokens[cid] = token
            logging.info("✅ Eskiz: токен получен")
        else:
            body = await resp.text()
            logging.error(f"❌ Eskiz login failed: {resp.status} {body}")
    return token


ESKIZ_BALANCE_CACHE_TTL_SEC = 900  # 15 минут

async def get_eskiz_balance_cached() -> int | None:
    """Баланс Eskiz текущей компании (через _cid(), как _eskiz_get_token) с кэшем
    ~15 минут — используется для sms_registration_available в публичном
    /api/settings/site, нельзя дёргать live Eskiz API на каждой загрузке сайта.
    None — Eskiz не настроен для этой компании или запрос не удался."""
    cached_at  = await db.get_config("eskiz_balance_cached_at")
    cached_val = await db.get_config("eskiz_balance_cached_value")
    if cached_at and cached_val is not None:
        try:
            age = (datetime.utcnow() - datetime.fromisoformat(cached_at)).total_seconds()
            if age < ESKIZ_BALANCE_CACHE_TTL_SEC:
                return int(cached_val)
        except Exception:
            pass

    email    = await _get_cfg("eskiz_email")
    password = await _get_cfg("eskiz_password")
    if not email or not password:
        return None

    token = await _eskiz_get_token()
    if not token:
        return None
    try:
        async with aiohttp.ClientSession() as s:
            r = await s.get(
                "https://notify.eskiz.uz/api/user/get-limit",
                headers={"Authorization": f"Bearer {token}"},
                timeout=aiohttp.ClientTimeout(total=8),
            )
            data = await r.json()
        balance = data.get("data", {}).get("balance")
        if balance is None:
            return None
        balance = int(balance)
    except Exception as e:
        logging.warning(f"get_eskiz_balance_cached error: {e}")
        return None

    await db.set_config("eskiz_balance_cached_at", datetime.utcnow().isoformat())
    await db.set_config("eskiz_balance_cached_value", str(balance))
    return balance


async def send_sms(phone: str, message: str) -> bool:
    """Отправляет SMS через Eskiz.uz. Если ключи не заданы — пишет в лог. Возвращает True/False (доставлен ли запрос в Eskiz)."""
    logging.info(f"📲 [SMS->{phone}] {message}")

    email    = await _get_cfg("eskiz_email")
    password = await _get_cfg("eskiz_password")
    if not email or not password:
        logging.warning("⚠️ eskiz_email/eskiz_password не заданы — SMS не отправлен")
        return False

    token = await _eskiz_get_token()
    if not token:
        logging.error("❌ Eskiz: не удалось получить токен")
        return False

    mobile = phone.lstrip("+")  # Eskiz принимает без «+»

    async with aiohttp.ClientSession() as session:
        resp = await session.post(
            "https://notify.eskiz.uz/api/message/sms/send",
            headers={"Authorization": f"Bearer {token}"},
            data={"mobile_phone": mobile, "message": message, "from": await _get_cfg("eskiz_from")},
        )
        if resp.status == 200:
            data = await resp.json()
            logging.info(f"✅ Eskiz SMS отправлен: {data}")
            return True
        else:
            body = await resp.text()
            logging.error(f"❌ Eskiz SMS error: {resp.status} {body}")
            return False


_platform_eskiz_token = ""

async def _platform_eskiz_get_token() -> str:
    """Токен отдельного, ПЛАТФОРМЕННОГО Eskiz-аккаунта (platform_eskiz_*, настраивается
    в superadmin.html) — используется ТОЛЬКО для напоминаний об оплате подписки, не для
    SMS отдельных компаний (у send_sms/_eskiz_get_token свой, per-company аккаунт через
    _cid()). Слать напоминание об истёкшей подписке через Eskiz САМОЙ этой компании было
    бы нелогично — её аккаунт может быть не настроен, особенно если она как раз не платит."""
    global _platform_eskiz_token
    email    = await db.get_config("platform_eskiz_email")
    password = await db.get_config("platform_eskiz_password")
    if not email or not password:
        return await db.get_config("platform_eskiz_token") or ""
    token = _platform_eskiz_token or await db.get_config("platform_eskiz_token") or ""
    async with aiohttp.ClientSession() as session:
        if token:
            resp = await session.patch(
                "https://notify.eskiz.uz/api/auth/refresh",
                headers={"Authorization": f"Bearer {token}"},
            )
            if resp.status == 200:
                data = await resp.json()
                new_token = data.get("data", {}).get("token", token)
                if new_token != token:
                    token = new_token
                    await db.set_config("platform_eskiz_token", token)
                _platform_eskiz_token = token
                return token
        resp = await session.post(
            "https://notify.eskiz.uz/api/auth/login",
            data={"email": email, "password": password},
        )
        if resp.status == 200:
            data = await resp.json()
            token = data.get("data", {}).get("token", "")
            if token:
                await db.set_config("platform_eskiz_token", token)
                _platform_eskiz_token = token
        else:
            body = await resp.text()
            logging.error(f"❌ Platform Eskiz login failed: {resp.status} {body}")
    return token


async def send_platform_sms(phone: str, message: str) -> bool:
    email    = await db.get_config("platform_eskiz_email")
    password = await db.get_config("platform_eskiz_password")
    if not email or not password:
        logging.warning("⚠️ platform_eskiz_email/password не заданы — SMS не отправлен")
        return False
    token = await _platform_eskiz_get_token()
    if not token:
        logging.error("❌ Platform Eskiz: не удалось получить токен")
        return False
    mobile = phone.lstrip("+")
    from_sender = await db.get_config("platform_eskiz_from") or ""
    async with aiohttp.ClientSession() as session:
        resp = await session.post(
            "https://notify.eskiz.uz/api/message/sms/send",
            headers={"Authorization": f"Bearer {token}"},
            data={"mobile_phone": mobile, "message": message, "from": from_sender},
        )
        if resp.status == 200:
            return True
        body = await resp.text()
        logging.error(f"❌ Platform Eskiz SMS error: {resp.status} {body}")
        return False


async def get_platform_eskiz_balance() -> int | None:
    """Без кэша (в отличие от get_eskiz_balance_cached, читаемого на каждой загрузке
    публичного сайта) — вызывается только по кнопке "Проверить баланс" в superadmin.html."""
    email    = await db.get_config("platform_eskiz_email")
    password = await db.get_config("platform_eskiz_password")
    if not email or not password:
        return None
    token = await _platform_eskiz_get_token()
    if not token:
        return None
    try:
        async with aiohttp.ClientSession() as s:
            r = await s.get(
                "https://notify.eskiz.uz/api/user/get-limit",
                headers={"Authorization": f"Bearer {token}"},
                timeout=aiohttp.ClientTimeout(total=8),
            )
            data = await r.json()
        balance = data.get("data", {}).get("balance")
        return int(balance) if balance is not None else None
    except Exception as e:
        logging.warning(f"get_platform_eskiz_balance error: {e}")
        return None


_SUB_REMINDER_TEXT_RU_DEFAULT = "Cleano: до окончания тарифа компании «{name}» осталось {days} дн. (истекает {end_date}). Просим произвести оплату, чтобы сохранить доступ к сайту и боту."
_SUB_REMINDER_TEXT_UZ_DEFAULT = "Cleano: «{name}» kompaniyasining tarif muddati {days} kun ichida tugaydi (tugash sanasi: {end_date}). Sayt va botga kirishni saqlab qolish uchun to'lovni amalga oshirishingizni so'raymiz."
_SUB_REMINDER_TEXT_EXPIRED_RU_DEFAULT = "Cleano: тариф компании «{name}» истёк {days} дн. назад ({end_date}). Через {days_to_close} дн. доступ к сайту и боту будет полностью закрыт. Пожалуйста, произведите оплату как можно скорее."
_SUB_REMINDER_TEXT_EXPIRED_UZ_DEFAULT = "Cleano: «{name}» kompaniyasining tarifi {days} kun oldin tugadi ({end_date}). {days_to_close} kundan so'ng sayt va botga kirish to'liq yopiladi. Iltimos, imkon qadar tezroq to'lovni amalga oshiring."

async def _send_subscription_reminders() -> list[dict]:
    """Ежедневная рассылка напоминаний об истечении подписки — всем компаниям сразу
    (настройки глобальные, не per-company), от N дней ДО истечения до M дней ПОСЛЕ.
    company_id=1 (ARTEZ) — исключение, как и везде в подписочной логике (см.
    is_subscription_active/get_next_order_num). Возвращает список результатов по
    каждой компании В ОКНЕ рассылки — используется и воркером (просто логирует),
    и ручным "Отправить сейчас" из superadmin.html для диагностики "почему не пришло"
    (нет contact_phone/contact_tg_id, канал выключен, ошибка отправки и т.д.)."""
    results: list[dict] = []
    days_before = int(await db.get_config("subscription_reminder_days_before") or "5")
    days_after  = int(await db.get_config("subscription_reminder_days_after") or "5")
    sms_on = (await db.get_config("subscription_reminder_sms_enabled")) != "false"
    tg_on  = (await db.get_config("subscription_reminder_tg_enabled")) != "false"
    if not sms_on and not tg_on:
        return results
    text_ru = await db.get_config("subscription_reminder_text_ru") or _SUB_REMINDER_TEXT_RU_DEFAULT
    text_uz = await db.get_config("subscription_reminder_text_uz") or _SUB_REMINDER_TEXT_UZ_DEFAULT
    text_expired_ru = await db.get_config("subscription_reminder_text_expired_ru") or _SUB_REMINDER_TEXT_EXPIRED_RU_DEFAULT
    text_expired_uz = await db.get_config("subscription_reminder_text_expired_uz") or _SUB_REMINDER_TEXT_EXPIRED_UZ_DEFAULT
    bot_token = await db.get_config("cleano_bot_token")
    today = datetime.now(timezone.utc).date()

    companies = await db.get_all_companies()
    for c in companies:
        cid = c["id"]
        if cid == 1:
            continue
        sub = await db.get_saas_subscription(cid)
        if not sub or not sub.get("end_date"):
            continue
        days_left = (sub["end_date"] - today).days
        if not (-days_after <= days_left <= days_before):
            continue
        # days_left < 0 — тариф уже истёк: отдельный текст с обратным отсчётом до полного
        # закрытия доступа (days_to_close), не тот же "до истечения осталось N дней".
        if days_left < 0:
            ctx = {"name": c["name"], "end_date": sub["end_date"].strftime("%d.%m.%Y"),
                   "days": abs(days_left), "days_to_close": days_after - abs(days_left)}
            msg = f"{text_expired_ru.format(**ctx)}\n\n{text_expired_uz.format(**ctx)}"
            sms_msg = text_expired_uz.format(**ctx)
        else:
            ctx = {"name": c["name"], "end_date": sub["end_date"].strftime("%d.%m.%Y"), "days": days_left}
            msg = f"{text_ru.format(**ctx)}\n\n{text_uz.format(**ctx)}"
            sms_msg = text_uz.format(**ctx)
        row = {"company_id": cid, "company_name": c["name"], "days_left": days_left,
               "sms": "skipped", "tg": "skipped"}
        if sms_on:
            if not c.get("contact_phone"):
                row["sms"] = "no_phone"
            else:
                try:
                    # Eskiz модерирует SMS ПОСИМВОЛЬНО — склеенный RU+UZ текст никогда не
                    # совпадёт с одобренным шаблоном, поэтому в SMS уходит только UZ
                    # (единственный язык, который сейчас реально прошёл модерацию в Eskiz).
                    # В Telegram (msg ниже) ограничения нет — там остаётся двуязычный текст.
                    row["sms"] = "sent" if await send_platform_sms(c["contact_phone"], sms_msg) else "failed"
                except Exception as e:
                    logging.warning(f"subscription reminder SMS to company {cid} failed: {e}")
                    row["sms"] = "failed"
        if tg_on:
            if not c.get("contact_tg_id"):
                row["tg"] = "no_telegram"
            elif not bot_token:
                row["tg"] = "no_bot_token"
            else:
                try:
                    await _cleano_bot_send(bot_token, c["contact_tg_id"], msg)
                    row["tg"] = "sent"
                except Exception as e:
                    logging.warning(f"subscription reminder TG to company {cid} failed: {e}")
                    row["tg"] = "failed"
        results.append(row)
    return results


async def _subscription_reminder_worker():
    """Время отправки читается из настроек ЗАНОВО на каждом витке (не при старте
    процесса) — чтобы суперадмин мог поменять его в superadmin.html, и это применилось
    без рестарта сервиса на следующий же расчёт цели сна."""
    from datetime import timezone as _tz, timedelta as _td
    _TZ5 = _tz(_td(hours=5))
    while True:
        from datetime import datetime as _dt
        hh, mm = 10, 0
        try:
            time_str = await db.get_config("subscription_reminder_time")
            if time_str and ":" in time_str:
                hh, mm = (int(x) for x in time_str.split(":", 1))
        except Exception:
            pass
        now = _dt.now(_TZ5)
        target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if now >= target:
            target = target + _td(days=1)
        await asyncio.sleep((target - now).total_seconds())
        try:
            await _send_subscription_reminders()
        except Exception as e:
            logging.warning(f"subscription reminder error: {e}")


def _order_num_short(order_num: str) -> str:
    """'ARTEZ-1120' -> '#1120' — компактный номер для SMS клиенту (Eskiz-шаблоны настраиваются
    отправителем под ЭТОТ формат, см. запрос пользователя 2026-07-30)."""
    if not order_num:
        return ""
    tail = str(order_num).rsplit("-", 1)[-1]
    return f"#{tail}"

_SMS_KIND_LABEL = {"pickup": "📲 SMS (забор)", "ready": "📲 SMS (готово)"}

async def _render_sms_notification(kind: str, lang: str, order_num: str, count: int, company_id: int) -> str:
    lang = lang if lang in ("ru", "uz") else "ru"
    tpl = await _get_cfg(f"sms_{kind}_template_{lang}") or await _get_cfg(f"sms_{kind}_template_ru")
    contact_short = await _get_cfg("contact_short")
    contact_main  = await _get_cfg("contact_main")
    phones = ", ".join(p for p in (contact_short, contact_main) if p)
    order_bot_username = await _get_cfg("order_bot_username")
    bot_link = f"t.me/{order_bot_username}" if order_bot_username else ""
    slug = await db.get_company_slug(company_id)
    site_link = f"https://{slug}.cleano.uz/" if slug else "https://cleano.uz"
    # Текст шаблона отправляется КАК ЕСТЬ (без авто-схлопывания переносов строк/пробелов) —
    # пользователь сам контролирует форматирование под то, что одобрено в Eskiz.
    return (tpl.replace("{order_num}", _order_num_short(order_num))
               .replace("{count}", str(count))
               .replace("{phones}", phones)
               .replace("{bot_link}", bot_link)
               .replace("{site_link}", site_link))


async def _send_sms_notification(kind: str, order_id: int, phone: str, order_num: str, count: int, lang: str,
                                  company_id: int, staff_id: int, staff_name: str) -> tuple[bool, str]:
    """kind: 'pickup' (при заборе) | 'ready' (при готовности). Фиксирует результат в
    order_activity (вкладка «Уведомления» карточки заказа читает эти же записи).
    Возвращает (отправлено?, причина_если_нет) — кнопке «📲 SMS» нужна явная ошибка,
    а не молчаливый no-op."""
    lang = lang if lang in ("ru", "uz") else "ru"
    if (await _get_cfg(f"sms_{kind}_enabled")) != "true":
        return False, "Отправка SMS отключена (тогл выключен в Тексты SMS)"
    if not phone:
        return False, "У заказа нет телефона клиента"
    text = await _render_sms_notification(kind, lang, order_num, count, company_id)
    if not text:
        return False, "Шаблон SMS не настроен"
    ok = await send_sms(phone, text)
    action = "sms_sent" if ok else "sms_failed"
    prefix = _SMS_KIND_LABEL.get(kind, "📲 SMS") if ok else "❌ SMS не отправлен (Eskiz не настроен или ошибка)"
    try:
        await db.add_order_activity(order_id, staff_id, staff_name, action,
                                     f"{prefix} ({lang.upper()}) на {phone}: {text[:150]}")
    except Exception as e:
        logging.warning(f"_send_sms_notification activity log failed order={order_id}: {e}")
    return ok, ("" if ok else "Eskiz не настроен или произошла ошибка отправки")


def generate_code() -> str:
    return f"{secrets.randbelow(1000000):06d}"


async def sms_text(code: str, purpose: str = "register") -> str:
    """Формирует текст SMS, читая шаблон из config (если задан)."""
    defaults = {
        "reset":    "Kod vosstanovleniya parolya dlya vhoda na sayt ARTEZ.uz: {code}",
        "login":    "Kod podtverzhdeniya dlya vhoda na sayt ARTEZ.uz: {code}",
        "register": "Kod podtverzhdeniya dlya registracii na sayte ARTEZ.uz: {code}",
    }
    key = f"sms_text_{purpose}"
    tpl = await db.get_config(key) or defaults.get(purpose, defaults["register"])
    return tpl.replace("{code}", code)


# ══════════════════════════════════════
#  JWT
# ══════════════════════════════════════
def create_token(user_id: int, phone: str, company_id: int = 1) -> str:
    payload = {
        "sub": str(user_id),
        "phone": phone,
        "type": "client",
        "company_id": company_id,
        "exp": datetime.now(timezone.utc) + timedelta(days=JWT_EXPIRE_DAYS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def create_staff_token(staff_id: int, login: str, role: str, company_id: int = 1) -> str:
    payload = {
        "sub": str(staff_id),
        "login": login,
        "role": role,
        "type": "staff",
        "company_id": company_id,
        "exp": datetime.now(timezone.utc) + timedelta(days=JWT_EXPIRE_DAYS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def create_superadmin_token() -> str:
    payload = {
        "sub": "superadmin",
        "type": "superadmin",
        "exp": datetime.now(timezone.utc) + timedelta(days=JWT_EXPIRE_DAYS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


# Разрешения по ролям
ROLE_PERMISSIONS: dict[str, list[str]] = {
    "admin":      ["leads", "orders", "clients", "status", "staff", "reports", "settings"],
    "manager":    ["leads", "orders", "clients", "status", "reports"],
    "callcenter": ["leads", "orders", "clients"],
    "driver":     ["leads", "orders", "status_delivery"],
    "logistics":  ["leads", "orders", "status"],
    "washer":     ["orders", "status_wash"],
    "packer":     ["orders", "status"],
    "agent":      ["leads_own"],  # агент видит только свои лиды
}

# Кто может смотреть/отправлять чек клиенту — шире, чем "status" (право менять
# статус заказа): все операционные роли кроме внешних агентов-партнёров.
RECEIPT_ACCESS_ROLES = {"admin", "manager", "logistics", "packer", "washer", "callcenter", "driver"}

# Допустимые переходы статусов для мойщиков
WASHER_STATUS_FLOW = {
    "received": "washing",
    "washing":  "drying",
    "drying":   "packing",
    "packing":  "ready",
}
ALL_ORDER_STATUSES = [
    "new","confirmed","pickup","received","washing","drying","packing","ready","delivery","delivered","cancelled"
]

async def get_current_user(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Не авторизован")
    token = authorization.removeprefix("Bearer ").strip()
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="Недействительный токен")
    if payload.get("type") == "staff":
        raise HTTPException(status_code=401, detail="Используйте клиентский токен")
    user = await db.get_user_by_id(int(payload["sub"]))
    if not user:
        raise HTTPException(status_code=401, detail="Пользователь не найден")
    return user


async def get_optional_user(authorization: str = Header(None)):
    """Как get_current_user, но возвращает None вместо 401 для незалогиненных."""
    if not authorization or not authorization.startswith("Bearer "):
        return None
    token = authorization.removeprefix("Bearer ").strip()
    if token in ("null", "undefined", ""):
        return None
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        return None
    if payload.get("type") == "staff":
        return None
    try:
        return await db.get_user_by_id(int(payload["sub"]))
    except Exception:
        return None


async def get_current_staff(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Не авторизован")
    token = authorization.removeprefix("Bearer ").strip()
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="Недействительный токен")
    if payload.get("type") == "superadmin":
        raise HTTPException(status_code=401, detail="Требуется токен сотрудника")
    # Admin panel token — resolve to real admin staff record (company_id from JWT)
    if payload.get("sub") == "admin":
        company_id = payload.get("company_id", 1)
        admin_staff = await db.get_company_admin_staff(company_id)
        if admin_staff:
            return dict(admin_staff)
        return {"id": None, "login": "admin", "role": "admin", "sub": "admin", "active": True,
                "first_name": "Администратор", "last_name": None, "phone": None,
                "branch": None, "tg_username": None, "position": None, "company_id": company_id}
    if payload.get("type") != "staff":
        raise HTTPException(status_code=401, detail="Требуется токен сотрудника")
    staff = await db.get_staff_by_id(int(payload["sub"]))
    if not staff or not staff["active"]:
        raise HTTPException(status_code=401, detail="Сотрудник не найден или деактивирован")
    s = dict(staff)
    # company_id from DB record (authoritative), fallback to token claim
    s.setdefault("company_id", payload.get("company_id", 1))
    return s


async def get_superadmin(authorization: str = Header(None)):
    """Зависимость для суперадмин-эндпоинтов. Принимает токен типа superadmin."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Не авторизован")
    token = authorization.removeprefix("Bearer ").strip()
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="Недействительный токен")
    if payload.get("type") != "superadmin":
        raise HTTPException(status_code=403, detail="Требуется суперадмин-токен")
    return payload


def require_perm(permission: str):
    async def dep(staff=Depends(get_current_staff)):
        if staff["role"] == "admin":  # admin has all permissions
            return staff
        perms = ROLE_PERMISSIONS.get(staff["role"], [])
        if permission not in perms:
            raise HTTPException(status_code=403, detail="Нет доступа")
        return staff
    return dep


# ══════════════════════════════════════
#  ENDPOINTS
# ══════════════════════════════════════
@app.get("/api/health")
async def health():
    return {"ok": True, "version": "2026-06-20-v1"}


# ══════════════════════════════════════
#  СОТРУДНИКИ — авторизация и профиль
# ══════════════════════════════════════
class StaffLoginRequest(BaseModel):
    login: str
    password: str
    company_id: int = 1
    company_slug: str | None = None

class StaffCreateRequest(BaseModel):
    first_name: str
    last_name: str | None = None
    middle_name: str | None = None
    phone: str | None = None
    login: str
    password: str
    role: str = "callcenter"
    position: str | None = None
    branch: str | None = None
    tg_id: int | None = None
    tg_username: str | None = None
    salary_type: str | None = None
    salary_rate: float | None = None
    hire_date: str | None = None
    note: str | None = None
    gender: str = "M"
    birth_date: str | None = None

def _staff_public(s: dict) -> dict:
    return {
        "id":         s["id"],
        "first_name": s["first_name"],
        "last_name":  s.get("last_name"),
        "login":      s["login"],
        "role":       s["role"],
        "position":   s.get("position"),
        "branch":     s.get("branch"),
        "phone":      s.get("phone"),
        "tg_id":      s.get("tg_id"),
        "tg_username":s.get("tg_username"),
        "active":         s["active"],
        "permissions":    ROLE_PERMISSIONS.get(s["role"], []),
        "can_edit_items":       s.get("can_edit_items", True),
        "can_measure":          s.get("can_measure", False),
        "can_approve_measure":  s.get("can_approve_measure", False),
        "can_override_measure": s.get("can_override_measure", False),
        "can_create_order":    s.get("can_create_order", True),
        "can_confirm_order":   s.get("can_confirm_order", True),
        "can_edit_confirmed":  s.get("can_edit_confirmed", False),
        "can_send_pickup":     s.get("can_send_pickup", False),
        "can_edit_delivery":   s.get("can_edit_delivery", False),
        "can_accept_payment":  s.get("can_accept_payment", False),
        "can_manage_cash":     s.get("can_manage_cash", False),
        "can_approve_debt":    s.get("can_approve_debt", False),
        "can_drive":           s.get("can_drive", False),
        "notify_new_users":    s.get("notify_new_users", False),
        "order_stages":        s.get("order_stages") or None,
        "gender":              s.get("gender", "M"),
        "birth_date":          str(s["birth_date"]) if s.get("birth_date") else None,
        "plain_password":       s.get("plain_password"),
        "fired":                bool(s.get("fired", False)),
        "can_view_timesheet":   bool(s.get("can_view_timesheet", False)),
        "hide_client_phone":    bool(s.get("hide_client_phone", False)),
        "salary_type":          s.get("salary_type"),
    }

@app.post("/api/staff/login")
async def staff_login(req: StaffLoginRequest):
    company_id = req.company_id
    if req.company_slug:
        company_id = await db.get_company_id_by_slug(req.company_slug)
        if not company_id:
            raise HTTPException(status_code=401, detail="Компания не найдена")
    staff = await db.get_staff_by_login(req.login, company_id)
    if not staff:
        raise HTTPException(status_code=401, detail="Неверный логин или пароль")

    pw = req.password[:72]
    valid = pwd_context.verify(pw, staff["password_hash"])

    # Проверяем временный пароль если основной не подошёл
    if not valid and staff.get("temp_password_hash") and staff.get("temp_password_expires"):
        from datetime import datetime, timezone
        if datetime.now(timezone.utc) < staff["temp_password_expires"]:
            valid = pwd_context.verify(pw, staff["temp_password_hash"])

    if not valid:
        raise HTTPException(status_code=401, detail="Неверный логин или пароль")

    token = create_staff_token(staff["id"], staff["login"], staff["role"],
                               company_id=staff.get("company_id") or 1)
    pub = _staff_public(dict(staff))
    pub["must_change_password"] = bool(staff.get("must_change_password"))
    return {"ok": True, "token": token, "staff": pub}

@app.get("/api/staff/me")
async def staff_me(staff=Depends(get_current_staff)):
    return {"ok": True, "staff": _staff_public(staff)}

@app.get("/api/staff/list")
async def staff_list(role: str = None, _=Depends(get_current_staff)):
    rows = await db.get_all_staff()
    staff = [_staff_public(dict(r)) for r in rows]
    if role:
        staff = [s for s in staff if s.get("role") == role]
    return {"ok": True, "staff": staff}

@app.post("/api/staff/create")
async def staff_create(req: StaffCreateRequest, me=Depends(get_current_staff)):
    from datetime import date as date_type
    import traceback
    company_id = me.get("company_id") or 1
    company = await db.get_company(company_id)
    if company:
        max_staff = company.get("max_staff") or 999
        current_count = await db.count_staff_by_company(company_id)
        if current_count >= max_staff:
            raise HTTPException(status_code=400, detail=f"Лимит сотрудников по тарифу: {max_staff}. Перейдите на более высокий план.")
    hashed = pwd_context.hash(req.password[:72])
    hire = None
    if req.hire_date:
        try: hire = date_type.fromisoformat(req.hire_date)
        except ValueError: pass
    try:
        sid = await db.create_staff({
            "first_name": req.first_name, "last_name": req.last_name,
            "middle_name": req.middle_name, "phone": req.phone,
            "login": req.login, "password_hash": hashed, "plain_password": req.password,
            "role": req.role, "position": req.position, "branch": req.branch,
            "tg_id": req.tg_id, "tg_username": req.tg_username,
            "salary_type": req.salary_type, "salary_rate": req.salary_rate,
            "hire_date": hire, "note": req.note,
            "gender": req.gender,
            "birth_date": date_type.fromisoformat(req.birth_date) if req.birth_date else None,
        })
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"DB error: {type(e).__name__}: {e}")
    return {"ok": True, "id": sid}

@app.patch("/api/staff/{staff_id}")
async def staff_update(staff_id: int, body: dict, me=Depends(get_current_staff)):
    is_admin = me.get("role") == "admin"
    is_self  = me.get("id") == staff_id
    if not is_admin and not is_self:
        raise HTTPException(status_code=403, detail="Нет доступа")
    if is_admin:
        allowed = {"first_name","last_name","middle_name","phone","login","role","branch","position","active","is_active","fired","can_view_timesheet","note","hire_date","salary_type","salary_rate","tg_id","tg_username","gender","birth_date"}
    else:
        allowed = {"gender","birth_date","branch"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        raise HTTPException(status_code=400, detail="Нет данных для обновления")
    if "tg_id" in updates and updates["tg_id"] is not None:
        try:
            updates["tg_id"] = int(updates["tg_id"])
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="tg_id должен быть числом")
    if "birth_date" in updates and updates["birth_date"]:
        from datetime import date as _date
        try: updates["birth_date"] = _date.fromisoformat(str(updates["birth_date"]))
        except: updates["birth_date"] = None
    if "hire_date" in updates and updates["hire_date"]:
        from datetime import date as _date
        try: updates["hire_date"] = _date.fromisoformat(str(updates["hire_date"]))
        except: updates["hire_date"] = None
    try:
        await db.update_staff(staff_id, **updates)
    except Exception as e:
        err = str(e)
        if "unique" in err.lower() or "duplicate" in err.lower():
            raise HTTPException(status_code=409, detail="Логин или tg_id уже занят другим сотрудником")
        logging.error(f"update_staff error: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка БД: {err}")
    row = await db.get_staff_by_id(staff_id)
    if not row:
        raise HTTPException(status_code=404, detail="Сотрудник не найден")
    return {"ok": True, "staff": _staff_public(dict(row))}

@app.get("/api/admin/staff/{staff_id}/personal")
async def get_staff_personal_ep(staff_id: int, me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    data = await db.get_staff_personal(staff_id)
    if data and data.get("spouse_birth_date"):
        data["spouse_birth_date"] = str(data["spouse_birth_date"])
    return {"ok": True, "personal": data or {}}

@app.put("/api/admin/staff/{staff_id}/personal")
async def save_staff_personal_ep(staff_id: int, body: dict, me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    from datetime import date as _date
    if body.get("spouse_birth_date"):
        try: body["spouse_birth_date"] = _date.fromisoformat(body["spouse_birth_date"])
        except: body["spouse_birth_date"] = None
    else:
        body["spouse_birth_date"] = None
    if body.get("children_count") is not None:
        try: body["children_count"] = int(body["children_count"])
        except: body["children_count"] = 0
    await db.upsert_staff_personal(staff_id, body)
    return {"ok": True}

@app.get("/api/admin/staff/{staff_id}/salary")
async def get_staff_salary_ep(staff_id: int, me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    data = await db.get_staff_salary(staff_id)
    return {"ok": True, "salary": data}

@app.put("/api/admin/staff/{staff_id}/salary")
async def save_staff_salary_ep(staff_id: int, body: dict, me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    await db.save_staff_salary(staff_id, body)
    return {"ok": True}

@app.get("/api/admin/salary/monthly")
async def get_monthly_salary_ep(year: int, month: int, me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    data = await db.get_monthly_salary_calc(year, month, me.get("company_id") or 1)
    return {"ok": True, "staff": data}

@app.get("/api/admin/monitoring/agents")
async def get_agent_monitoring_ep(me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    data = await db.get_agent_monitoring_stats()
    return {
        "ok": True,
        "agents": data.get("agents", []),
        "order_status_breakdown": data.get("order_status_breakdown", {}),
        "activity_trend_7d": data.get("activity_trend_7d", []),
    }

@app.get("/api/admin/staff/{staff_id}/commissions")
async def get_staff_commissions_ep(staff_id: int, year: int = None, month: int = None,
                                    me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    rows = await db.get_agent_commissions(staff_id, year, month)
    for r in rows:
        if r.get("created_at"): r["created_at"] = str(r["created_at"])
        if r.get("paid_at"):    r["paid_at"]    = str(r["paid_at"])
    return {"ok": True, "commissions": rows}

@app.get("/api/admin/commissions")
async def get_all_commissions_ep(year: int = None, month: int = None,
                                  me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    rows = await db.get_all_commissions(year, month)
    for r in rows:
        if r.get("created_at"): r["created_at"] = str(r["created_at"])
        if r.get("paid_at"):    r["paid_at"]    = str(r["paid_at"])
    return {"ok": True, "commissions": rows}

# ══════════════════════════════════════
#  ТАБЕЛЬ (timesheet)
# ══════════════════════════════════════

class TimesheetEntry(BaseModel):
    staff_id: int
    date:     str
    hours:    float = 8.0
    type:     str   = "work"
    note:     str   = ""

def _can_timesheet(me: dict) -> bool:
    return me.get("role") in ("admin", "manager") or bool(me.get("can_view_timesheet"))

@app.get("/api/admin/timesheet")
async def get_timesheet_ep(year: int, month: int, staff_id: int = None,
                            me=Depends(get_current_staff)):
    # All staff can view their own timesheet; only can_timesheet users can view others
    if not _can_timesheet(me):
        staff_id = me.get("id")
    elif me.get("role") not in ("admin", "manager"):
        staff_id = me.get("id")
    rows = await db.get_timesheet(year, month, staff_id)
    for r in rows:
        r["date"] = str(r["date"]) if r.get("date") else ""
    return {"ok": True, "rows": rows, "records": rows}

@app.post("/api/admin/timesheet")
async def create_timesheet_ep(body: TimesheetEntry, me=Depends(get_current_staff)):
    if not _can_timesheet(me):
        raise HTTPException(status_code=403)
    row = await db.save_timesheet(body.dict())
    return {"ok": True, "entry": row}

@app.put("/api/admin/timesheet/{ts_id}")
async def update_timesheet_ep(ts_id: int, body: TimesheetEntry, me=Depends(get_current_staff)):
    if not _can_timesheet(me):
        raise HTTPException(status_code=403)
    row = await db.update_timesheet(ts_id, body.dict())
    if not row:
        raise HTTPException(status_code=404, detail="Запись не найдена")
    return {"ok": True, "entry": row}

@app.delete("/api/admin/timesheet/{ts_id}")
async def delete_timesheet_ep(ts_id: int, me=Depends(get_current_staff)):
    if not _can_timesheet(me):
        raise HTTPException(status_code=403)
    ok = await db.delete_timesheet(ts_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Запись не найдена")
    return {"ok": True}

@app.post("/api/admin/timesheet/init-month")
async def init_timesheet_month_ep(year: int, month: int, until_today: bool = False, me=Depends(get_current_staff)):
    if not _can_timesheet(me):
        raise HTTPException(status_code=403)
    result = await db.init_timesheet_month(year, month, until_today=until_today)
    return {"ok": True, "created": result["created"]}

@app.post("/api/admin/timesheet/reset-month")
async def reset_timesheet_month_ep(year: int, month: int, body: dict, me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403)
    password = body.get("password", "")
    if not password or password != await get_admin_pass():
        raise HTTPException(status_code=403, detail="Неверный пароль")
    result = await db.reset_timesheet_month(year, month)
    return {"ok": True, "deleted": result["deleted"]}

# ══════════════════════════════════════
#  ОТМЕТКИ ПРИХОДА/УХОДА (staff_attendance)
# ══════════════════════════════════════

@app.post("/api/staff/attendance/checkin")
async def attendance_checkin_ep(me=Depends(get_current_staff)):
    if me.get("salary_type") not in ("fixed", "fixed_percent"):
        raise HTTPException(status_code=403, detail="Доступно только для сотрудников с окладом")
    row = await db.attendance_check_in(me["id"])
    if row.get("error") == "already_in":
        raise HTTPException(status_code=400, detail="Вы уже отметили приход, сначала отметьте уход")
    return {"ok": True, "attendance": row}

@app.post("/api/staff/attendance/checkout")
async def attendance_checkout_ep(me=Depends(get_current_staff)):
    if me.get("salary_type") not in ("fixed", "fixed_percent"):
        raise HTTPException(status_code=403, detail="Доступно только для сотрудников с окладом")
    row = await db.attendance_check_out(me["id"])
    if row.get("error") == "not_checked_in":
        raise HTTPException(status_code=400, detail="Сначала отметьте приход")
    return {"ok": True, "attendance": row}

@app.get("/api/staff/attendance/today")
async def attendance_today_ep(me=Depends(get_current_staff)):
    data = await db.get_attendance_today(me["id"])
    return {"ok": True, **data}

@app.get("/api/admin/attendance")
async def admin_attendance_ep(year: int, month: int, staff_id: int = None,
                               me=Depends(get_current_staff)):
    if not _can_timesheet(me):
        staff_id = me.get("id")
    records = await db.get_admin_attendance(year, month, staff_id)
    return {"ok": True, "records": records}

# ══════════════════════════════════════
#  МАРШРУТЫ (routes)
# ══════════════════════════════════════

@app.get("/api/admin/routes/active-order-ids")
async def active_route_order_ids(me=Depends(get_current_staff)):
    rows = await db.get_active_route_orders()
    result = {}
    for r in rows:
        result[r["order_id"]] = {
            "route_id":   r["route_id"],
            "route_name": r["route_name"] or f"Маршрут #{r['route_id']}",
            "route_date": str(r["route_date"]) if r["route_date"] else "",
            "route_type": r["route_type"] or "",
        }
    return result

@app.get("/api/admin/routes")
async def list_routes(date: str | None = None, driver_id: int | None = None,
                      branch: str | None = None, status: str | None = None,
                      me=Depends(get_current_staff)):
    if me.get("role") not in ("admin","logistics","manager"):
        raise HTTPException(status_code=403)
    await db.roll_forward_stale_routes()
    rows = await db.get_routes(date=date, driver_id=driver_id, branch=branch, status=status)
    for r in rows:
        if r.get("date"): r["date"] = str(r["date"])
        if r.get("created_at"): r["created_at"] = r["created_at"].isoformat()
        if r.get("updated_at"): r["updated_at"] = r["updated_at"].isoformat()
    return {"ok": True, "routes": rows}

@app.post("/api/admin/routes")
async def create_route(body: dict, me=Depends(get_current_staff)):
    if me.get("role") not in ("admin","logistics","manager"):
        raise HTTPException(status_code=403)
    row = await db.create_route(body)
    if row.get("date"): row["date"] = str(row["date"])
    return {"ok": True, "route": row}

@app.get("/api/admin/routes/{route_id}")
async def get_route(route_id: int, me=Depends(get_current_staff)):
    if me.get("role") not in ("admin","logistics","manager","driver"):
        raise HTTPException(status_code=403)
    route = await db.get_route(route_id)
    if not route: raise HTTPException(status_code=404)
    if route.get("date"): route["date"] = str(route["date"])
    for s in route.get("stops", []):
        if s.get("created_at"): s["created_at"] = s["created_at"].isoformat()
    return {"ok": True, "route": route}

@app.patch("/api/admin/routes/{route_id}")
async def update_route(route_id: int, body: dict, me=Depends(get_current_staff)):
    if me.get("role") not in ("admin","logistics","manager"):
        raise HTTPException(status_code=403)
    row = await db.update_route(route_id, body)
    if row.get("date"): row["date"] = str(row["date"])
    return {"ok": True, "route": row}

@app.delete("/api/admin/routes/{route_id}")
async def delete_route(route_id: int, me=Depends(get_current_staff)):
    if me.get("role") not in ("admin","logistics"):
        raise HTTPException(status_code=403)
    await db.delete_route(route_id)
    return {"ok": True}

@app.post("/api/admin/routes/{route_id}/orders")
async def add_route_orders(route_id: int, body: dict, me=Depends(get_current_staff)):
    if me.get("role") not in ("admin","logistics","manager"):
        raise HTTPException(status_code=403)
    order_ids = body.get("order_ids", [])
    # Проверка: заказ уже в другом активном маршруте
    active = await db.get_active_route_orders()
    blocked = {r["order_id"]: r for r in active if r["route_id"] != route_id}
    conflicts = [oid for oid in order_ids if oid in blocked]
    if conflicts:
        names = ", ".join(str(c) for c in conflicts[:5])
        raise HTTPException(400, f"Заказы уже в активном маршруте: {names}")
    count = await db.add_orders_to_route(route_id, order_ids)
    return {"ok": True, "added": count}

@app.delete("/api/admin/routes/{route_id}/orders/{order_id}")
async def remove_route_order(route_id: int, order_id: int, me=Depends(get_current_staff)):
    if me.get("role") not in ("admin","logistics","manager"):
        raise HTTPException(status_code=403)
    await db.remove_order_from_route(route_id, order_id)
    return {"ok": True}

@app.patch("/api/admin/routes/{route_id}/orders/{order_id}")
async def update_route_stop(route_id: int, order_id: int, body: dict, me=Depends(get_current_staff)):
    await db.update_route_stop(route_id, order_id, body)
    return {"ok": True}

@app.post("/api/admin/routes/{route_id}/send-to-driver")
async def send_route_to_driver(route_id: int, me=Depends(get_current_staff)):
    if me.get("role") not in ("admin", "logistics", "manager"):
        raise HTTPException(status_code=403)
    route = await db.get_route(route_id)
    if not route:
        raise HTTPException(status_code=404, detail="Маршрут не найден")
    if not route.get("driver_id"):
        raise HTTPException(status_code=400, detail="Водитель не назначен")

    # Получить tg_id водителя
    if not db.pool:
        raise HTTPException(status_code=503, detail="DB unavailable")
    async with db.pool.acquire() as conn:
        driver = await conn.fetchrow(
            "SELECT first_name, last_name, tg_id FROM staff WHERE id=$1",
            route["driver_id"]
        )
    if not driver or not driver["tg_id"]:
        raise HTTPException(status_code=400, detail="У водителя не указан Telegram ID")

    stops = route.get("stops", [])
    branch_label = "Зарафшан" if route.get("branch") == "zarafshan" else "Навои" if route.get("branch") == "navoi" else ""

    type_map = {"pickup": "Забор", "delivery": "Доставка", "mixed": "Смешанный"}
    type_label = type_map.get(route.get("type", ""), "")

    lines = [
        f"🚗 Маршрут: {route['name']}",
        f"📅 {route.get('date', '')}  {branch_label}  {type_label}".strip(),
        "",
    ]

    import json as _json
    for i, s in enumerate(stops, 1):
        client = f"{s.get('client_first_name', '')} {s.get('client_last_name', '')}".strip()
        addr = s.get("address") or s.get("location_address") or "—"
        line = f"{i}. {s.get('order_num', '')} — {client}\n   📍 {addr}"
        # Google Maps ссылка если есть геометка
        if s.get("location"):
            try:
                loc = _json.loads(s["location"])
                if loc.get("lat") and loc.get("lon"):
                    line += f"\n   🗺 https://maps.google.com/?q={loc['lat']},{loc['lon']}"
            except Exception:
                pass
        if s.get("client_phone"):
            line += f"\n   📞 {s['client_phone']}"
        lines.append(line)

    lines += ["", f"Всего точек: {len(stops)}"]
    if route.get("note"):
        lines += ["", f"📝 {route['note']}"]

    text = "\n".join(lines)
    await send_tg(driver["tg_id"], text)
    return {"ok": True, "sent_to": driver["tg_id"]}


_ORDER_STATUS_RU = {
    "new": "Новый", "confirmed": "Подтверждён", "pickup": "Вывоз",
    "received": "В мастерской", "washing": "Мойка", "drying": "Сушка",
    "packing": "Упаковка", "ready": "Готов", "delivery": "Доставка",
    "delivered": "Доставлен", "cancelled": "Отменён",
}

async def _is_route_closed_for_order(order_id: int) -> bool:
    """True, если маршрут, к которому привязан заказ, уже закрыт (status='done') —
    тогда кнопки действий в канале водителей нужно спрятать, оставив только инфо-ряд."""
    if not db.pool: return False
    async with db.pool.acquire() as conn:
        row = await conn.fetchval("""
            SELECT r.status FROM route_orders ro JOIN routes r ON r.id=ro.route_id
            WHERE ro.order_id=$1 ORDER BY r.id DESC LIMIT 1
        """, order_id)
        return row == "done"

def _route_pickup_kb(order_id: int, status: str, closed: bool = False) -> dict:
    """Inline-клавиатура для сообщения в канале водителей."""
    h = {"text": "📋 История", "callback_data": f"rp:{order_id}:history"}
    r = {"text": "🔄 Обновить", "callback_data": f"rp:{order_id}:refresh"}
    p = {"text": "📦 Позиции", "callback_data": f"rp:{order_id}:items"}
    if closed:
        return {"inline_keyboard": [[p, h, r]]}
    if status == "confirmed":
        return {"inline_keyboard": [
            [{"text": "✅ Забрал", "callback_data": f"rp:{order_id}:take"},
             {"text": "⏭ Пропустить", "callback_data": f"rp:{order_id}:skip"}],
            [p, h, r],
        ]}
    elif status == "pickup":
        return {"inline_keyboard": [
            [{"text": "🏭 Сдал в мастерскую", "callback_data": f"rp:{order_id}:deliver"}],
            [{"text": "↩️ Не забирал", "callback_data": f"rp:{order_id}:undo"}],
            [p, h, r],
        ]}
    elif status == "ready":
        return {"inline_keyboard": [
            [{"text": "🚗 Везу клиенту", "callback_data": f"rp:{order_id}:take_delivery"},
             {"text": "❌ Не забрал", "callback_data": f"rp:{order_id}:ntaken"}],
            [p, h, r],
        ]}
    elif status == "delivery":
        return {"inline_keyboard": [
            [{"text": "✅ Доставил клиенту", "callback_data": f"rp:{order_id}:mark_delivered"}],
            [{"text": "💳 Оплата", "callback_data": f"rp:{order_id}:pay_init"}],
            [{"text": "🔙 Вернул в мастерскую", "callback_data": f"rp:{order_id}:retback"}],
            [p, h, r],
        ]}
    elif status == "delivered":
        return {"inline_keyboard": [[p, h, r]]}
    elif status == "skipped":
        return {"inline_keyboard": [
            [{"text": "↩️ Отменить пропуск", "callback_data": f"rp:{order_id}:unskip"}],
            [p, h, r],
        ]}
    else:
        return {"inline_keyboard": [[p, h, r]]}

def _parse_loc_str(val: str | None):
    if not val: return None
    try:
        import json as _j
        j = _j.loads(val)
        if j.get("lat") and j.get("lon"): return float(j["lat"]), float(j["lon"])
    except Exception: pass
    parts = str(val).split(",")
    if len(parts) == 2:
        try: return float(parts[0]), float(parts[1])
        except Exception: pass
    return None

def _build_stop_text(route: dict, stop: dict, num: int, template: str) -> str:
    branch_label = {"zarafshan": "Зарафшан", "navoi": "Навои"}.get(route.get("branch", ""), "")
    type_label   = {"pickup": "📥 Забор", "delivery": "📤 Доставка", "mixed": "🔄 Смешанный"}.get(route.get("type", ""), "")
    client = f"{stop.get('client_first_name', '')} {stop.get('client_last_name', '')}".strip() or "—"
    addr   = stop.get("short_address") or stop.get("address") or stop.get("location_address") or "—"
    phone  = f"📞 {stop['client_phone']}\n" if stop.get("client_phone") else ""
    loc    = _parse_loc_str(stop.get("location"))
    map_link = f"🗺 https://maps.google.com/?q={loc[0]},{loc[1]}\n" if loc else ""
    status = _ORDER_STATUS_RU.get(stop.get("order_status", ""), "—")
    return template.format(
        route_name=route.get("name", ""),
        route_type=type_label,
        branch=branch_label,
        date=str(route.get("date", "")),
        num=num,
        order_num=stop.get("order_num", ""),
        client=client,
        address=addr,
        phone=phone,
        map_link=map_link,
        status=status,
    )

def _build_stop_text_short(stop: dict, num: int) -> str:
    """Компактный HTML-формат сообщения для канала водителей."""
    import html as _html
    def h(s): return _html.escape(str(s)) if s else ""

    order_num = (stop.get("order_num", "") or "").replace("ARTEZ-", "")
    item_count = stop.get("item_count", 0) or 0
    addr  = stop.get("short_address") or stop.get("address") or stop.get("location_address") or "—"
    first = (stop.get("client_first_name") or "").strip()
    last  = (stop.get("client_last_name")  or "").strip()
    client = f"{first} {last}".strip() or "—"
    phone  = stop.get("client_phone", "") or ""
    loc    = _parse_loc_str(stop.get("location"))

    if loc:
        yandex = f"https://yandex.com/maps/?rtext=~{loc[0]},{loc[1]}&rtt=auto"
        addr_part = f'📍<a href="{yandex}">{h(addr)}</a>'
    else:
        addr_part = f"📍{h(addr)}"

    contact = f"👤 {h(client)}"
    if phone: contact += f" 📞{h(phone)}"

    total = float(stop.get("items_total") or stop.get("total_price") or 0)
    disc  = (float(stop.get("discount_sum") or 0) + float(stop.get("delivery_discount") or 0)
             + float(stop.get("manual_discount") or 0))
    net   = max(0.0, total - disc)
    paid  = float(stop.get("paid_amount") or 0)
    debt  = max(0.0, net - paid)
    def _fmt(n): return f"{int(n):,}".replace(",", " ") + " с" if n > 0 else "—"
    pay_line = f"💰 {_fmt(net)} · Опл: {_fmt(paid)} · Долг: {_fmt(debt)}"

    count_str = f" / {item_count}" if item_count else ""
    return f"📦 #{num}·{h(order_num)}{count_str} {addr_part}\n{contact}\n{pay_line}"

@app.post("/api/admin/routes/{route_id}/send-to-delivery-group")
async def send_route_to_delivery_group(route_id: int, me=Depends(get_current_staff)):
    route = await db.get_route(route_id)
    if not route:
        raise HTTPException(404, "Маршрут не найден")

    branch = route.get("branch", "")
    branch_channel_id = await db.get_branch_tg_group_id(branch, "tg_delivery_channel_id") if branch else None
    if branch_channel_id:
        channel_id = int(branch_channel_id)
    elif branch == "navoi":
        channel_id_str = await _get_cfg("delivery_channel_navoi_id")
        channel_id = int(channel_id_str) if channel_id_str else 0
    else:
        channel_id_str = await _get_cfg("delivery_channel_zarafshan_id")
        channel_id = int(channel_id_str) if channel_id_str else 0
    if not channel_id:
        raise HTTPException(400, "Канал водителей не настроен (Настройки → Telegram → Водители)")

    stops = route.get("stops", [])
    if not stops:
        raise HTTPException(400, "В маршруте нет заказов")

    from datetime import datetime
    from zoneinfo import ZoneInfo
    import json as _jmod
    now_uz     = datetime.now(ZoneInfo("Asia/Tashkent"))
    time_str   = now_uz.strftime("%H:%M:%S")
    type_label = {"pickup": "Забор", "delivery": "Доставка", "mixed": "Смешанный"}.get(route.get("type", ""), "")
    type_emoji = {"pickup": "📥", "delivery": "📤", "mixed": "🔄"}.get(route.get("type", ""), "🚗")
    route_date = str(route.get("date", ""))
    route_name = route.get("name", "")

    # Удаляем предыдущие сообщения из канала (старые записи "__group__" от
    # удалённой фичи группы-уведомления просто не найдутся в канале и молча
    # проигнорируются — see except ниже, это ожидаемая деградация)
    _raw = route.get("tg_delivery_msg_ids")
    if isinstance(_raw, str):
        try: _raw = _jmod.loads(_raw)
        except Exception: _raw = {}
    old_msg_ids: dict = _raw or {}
    if old_msg_ids:
        async with aiohttp.ClientSession() as sess:
            for key, msg_id_str in old_msg_ids.items():
                try:
                    await sess.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage",
                        json={"chat_id": str(channel_id), "message_id": int(msg_id_str)},
                        timeout=aiohttp.ClientTimeout(total=4))
                except Exception:
                    pass

    # ── Канал: заголовок + остановки + подвал ──
    dest = channel_id
    new_msg_ids: dict = {}
    tg_error = None

    header_text = (
        f"{type_emoji} <b>{route_name}</b> · {type_label}\n"
        f"📅 {route_date}  🕐 {time_str}\n"
        f"━━━━━━━━━━"
    )
    hdr_id = await _send_tg_with_kb(dest, header_text, {"inline_keyboard": []}, silent=True, protect=True)
    if hdr_id:
        new_msg_ids["__header__"] = hdr_id

    sent = 0
    for i, s in enumerate(stops, 1):
        text     = _build_stop_text_short(s, i)
        order_id    = s.get("order_id") or s.get("id")
        status      = s.get("order_status", "confirmed")
        stop_status = s.get("stop_status", "pending")
        if stop_status == "skipped":
            kb_status = "skipped"
        elif stop_status == "done":
            kb_status = status  # delivered/received — показать как есть
        elif route.get("type") == "delivery" and status == "delivery" and not s.get("driver_confirmed"):
            kb_status = "ready"  # ещё не подтвердил «Везу клиенту»
        else:
            kb_status = status
        kb       = _route_pickup_kb(order_id, kb_status, closed=route.get("status") == "done")
        msg_id   = await _send_tg_with_kb(dest, text, kb, silent=True, protect=True)
        if msg_id:
            sent += 1
            new_msg_ids[str(order_id)] = msg_id
        elif tg_error is None:
            tg_error = "Ошибка отправки остановки"

    new_msg_ids["__channel__"] = str(dest)  # фактический chat_id куда ушли сообщения
    footer_text = f"━━━━━━━━━━\nКонец списка · {sent} из {len(stops)}\n━━━━━━━━━━"
    ftr_id = await _send_tg_with_kb(dest, footer_text, {"inline_keyboard": []}, silent=True, protect=True)
    if ftr_id:
        new_msg_ids["__footer__"] = ftr_id

    if sent == 0 and tg_error:
        logging.error(f"send-to-delivery-group failed: {tg_error}")
        raise HTTPException(400, f"Telegram: {tg_error}")

    if new_msg_ids and db.pool:
        async with db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE routes SET tg_delivery_msg_ids=$1 WHERE id=$2",
                _jmod.dumps(new_msg_ids), route_id)

    return {"ok": True, "sent": sent}


@app.delete("/api/admin/staff/{staff_id}")
async def delete_staff(staff_id: int, me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    if not db.pool:
        raise HTTPException(status_code=503, detail="DB unavailable")
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id, first_name, last_name FROM staff WHERE id=$1", staff_id)
        if not row:
            raise HTTPException(status_code=404, detail="Сотрудник не найден")
        # NULL out FK references that don't have ON DELETE SET NULL
        await conn.execute("UPDATE leads        SET volunteer_id=NULL WHERE volunteer_id=$1", staff_id)
        await conn.execute("UPDATE leads        SET assigned_to=NULL  WHERE assigned_to=$1",  staff_id)
        await conn.execute("UPDATE leads        SET created_by=NULL   WHERE created_by=$1",   staff_id)
        await conn.execute("UPDATE leads        SET converted_by=NULL WHERE converted_by=$1", staff_id)
        await conn.execute("UPDATE lead_calls   SET operator_id=NULL  WHERE operator_id=$1",  staff_id)
        await conn.execute("UPDATE lead_reminders SET staff_id=NULL   WHERE staff_id=$1",     staff_id)
        try:
            await conn.execute("DELETE FROM staff WHERE id=$1", staff_id)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Ошибка БД: {str(e)}")
    return {"ok": True}

# ── DEPARTMENTS ──────────────────────────────────────────────────────────────
@app.get("/api/departments")
async def get_departments_ep(me=Depends(get_current_staff)):
    cid = me.get("company_id") or 1
    await db.seed_from_template(cid)
    rows = await db.get_departments(cid)
    return {"departments": [dict(r) for r in rows]}

@app.post("/api/departments")
async def create_department_ep(body: dict, me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    cid = me.get("company_id") or 1
    row = await db.create_department(cid, body["name"], body.get("description"), body.get("name_uz", ""), body.get("description_uz", ""))
    return {"department": dict(row)}

@app.patch("/api/departments/{dept_id}")
async def update_department_ep(dept_id: int, body: dict, me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    row = await db.update_department(dept_id, me.get("company_id") or 1, **body)
    if not row: raise HTTPException(404)
    return {"department": dict(row)}

@app.delete("/api/departments/{dept_id}")
async def delete_department_ep(dept_id: int, me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    await db.delete_department(dept_id, me.get("company_id") or 1)
    return {"ok": True}

# ── POSITIONS ────────────────────────────────────────────────────────────────
@app.get("/api/positions")
async def get_positions_ep(me=Depends(get_current_staff)):
    cid = me.get("company_id") or 1
    await db.seed_from_template(cid)
    rows = await db.get_positions(cid)
    return {"positions": [dict(r) for r in rows]}

@app.post("/api/positions")
async def create_position_ep(body: dict, me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    row = await db.create_position(me.get("company_id") or 1, body)
    return {"position": dict(row)}

@app.patch("/api/positions/{pos_id}")
async def update_position_ep(pos_id: int, body: dict, me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    row = await db.update_position(pos_id, me.get("company_id") or 1, **body)
    if not row: raise HTTPException(404)
    return {"position": dict(row)}

@app.delete("/api/positions/{pos_id}")
async def delete_position_ep(pos_id: int, me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    await db.delete_position(pos_id, me.get("company_id") or 1)
    return {"ok": True}


@app.put("/api/staff/{staff_id}/password")
async def staff_change_password(staff_id: int, body: dict, me=Depends(get_current_staff)):
    if me["role"] != "admin" and me["id"] != staff_id:
        raise HTTPException(status_code=403, detail="Нет доступа")
    new_pw = body.get("password", "")
    if len(new_pw) < 6:
        raise HTTPException(status_code=400, detail=bi("Минимум 6 символов","Kamida 6 ta belgi"))
    await db.update_staff_password(staff_id, pwd_context.hash(new_pw[:72]), plain=new_pw)
    return {"ok": True}


# ══════════════════════════════════════
#  ЛИДЫ
# ══════════════════════════════════════
class LeadCreateRequest(BaseModel):
    client_name: str | None = None
    client_phone: str
    service: str | None = None
    branch: str | None = None
    city: str | None = None
    address: str | None = None
    short_address: str | None = None
    note: str | None = None
    assigned_to: int | None = None
    volunteer_id: int | None = None
    location: str | None = None
    location_address: str | None = None
    notify_group: bool = True
    pickup_date: str = ""
    pickup_time: str = ""

@app.get("/api/staff/search")
async def staff_search(q: str = "", limit: int = 8, _=Depends(get_current_staff)):
    """Поиск клиентов из CRM + справочника. Доступен всем авторизованным сотрудникам."""
    q = q.strip()
    if not q or len(q) < 2:
        return {"ok": True, "results": []}
    crm      = await db.get_crm_clients_list(search=q, limit=limit)
    contacts = await db.search_contacts(q, limit=limit)
    seen = set()
    results = []
    for c in crm:
        p = c.get("phone") or ""
        seen.add(p)
        results.append({"phone": p, "phone2": c.get("phone2") or "",
                        "first_name": c.get("first_name") or "", "last_name": c.get("last_name") or "",
                        "middle_name": "", "address": c.get("address") or "",
                        "short_address": c.get("short_address") or "", "_src": "crm"})
    for c in contacts:
        p = c.get("phone") or ""
        if p not in seen:
            results.append({"phone": p, "phone2": c.get("phone2") or "",
                            "first_name": c.get("first_name") or "", "last_name": c.get("last_name") or "",
                            "middle_name": c.get("middle_name") or "", "address": c.get("address") or "",
                            "short_address": c.get("short_address") or "", "_src": "contacts"})
    return {"ok": True, "results": results[:limit]}


@app.post("/api/staff/leads")
async def create_lead(req: LeadCreateRequest, staff=Depends(get_current_staff)):
    role = staff.get("role", "")
    perms = ROLE_PERMISSIONS.get(role, [])
    if "leads" not in perms and "leads_own" not in perms and staff.get("sub") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    creator_id = None if staff.get("sub") == "admin" else staff.get("id")
    # агент автоматически становится agent_id лида
    agent_id = req.volunteer_id
    if role == "agent" and not agent_id:
        agent_id = creator_id
    lead_source = "agent" if role == "agent" else "staff"
    lead = await db.create_lead({
        "client_name": req.client_name,
        "client_phone": req.client_phone, "service": req.service,
        "branch": req.branch, "city": req.city, "address": req.address,
        "short_address": req.short_address, "note": req.note,
        "assigned_to": req.assigned_to, "created_by": creator_id,
        "volunteer_id": agent_id,
        "location": req.location, "location_address": req.location_address,
        "source": lead_source,
        "pickup_date": req.pickup_date or "",
        "pickup_time": req.pickup_time or "",
    })
    if lead:
        # Самый частый способ создания лида (страница «Лиды») раньше не попадал
        # в единую базу контактов вообще — закрываем дыру (см. прод-фикс 04.08).
        await db.upsert_crm_client(phone=normalize_phone(req.client_phone),
                                    first_name=req.client_name or "", source=lead_source)
        await db.refresh_crm_client_stats(normalize_phone(req.client_phone))
        await db.add_lead_call(lead["id"], creator_id, action="created",
                               note=f"Лид создан ({lead.get('lead_code','')})")
        if req.notify_group:
            asyncio.create_task(_notify_new_lead(lead, staff))
        elif creator_id:
            # Взять себе: назначаем на создателя, не отправляем в ТГ
            async with db.pool.acquire() as _conn:
                await _conn.execute(
                    "UPDATE leads SET assigned_to=$1 WHERE id=$2", creator_id, lead["id"])
            await db.add_lead_call(lead["id"], creator_id, action="note",
                                   note="Лид взят создателем")
    return {"ok": True, "lead": lead}

@app.get("/api/staff/leads")
async def get_leads(status: str = None, branch: str = None,
                    staff=Depends(get_current_staff)):
    role = staff.get("role", "")
    perms = ROLE_PERMISSIONS.get(role, [])
    # агент: только свои лиды (где он создатель или агент)
    if "leads_own" in perms and "leads" not in perms:
        rows = await db.get_leads_by_agent(staff["id"], status=status)
    elif "leads" in perms or staff.get("sub") == "admin":
        rows = await db.get_leads(status=status, branch=branch)
    else:
        raise HTTPException(status_code=403, detail="Нет доступа")
    return {"ok": True, "leads": [dict(r) for r in rows]}

@app.patch("/api/staff/leads/{lead_id}")
async def update_lead(lead_id: int, body: dict, staff=Depends(require_perm("leads"))):
    allowed = {"client_name","client_phone","branch","address","short_address","note","volunteer_id","location","location_address","pickup_type","delivery_type","pickup_date","pickup_time"}
    fields = {k: v for k, v in body.items() if k in allowed}
    lead = await db.update_lead(lead_id, **fields)
    operator_id = None if staff.get("sub") == "admin" else staff.get("id")
    await db.add_lead_call(lead_id, operator_id, action="edited", note="Лид отредактирован")
    return {"ok": True, "lead": lead}

@app.patch("/api/staff/leads/{lead_id}/assign")
async def assign_lead(lead_id: int, body: dict = Body({}),
                      staff=Depends(require_perm("leads"))):
    """Взять или освободить лид. assign=true — взять, assign=false — освободить."""
    take = body.get("assign", True)
    staff_id = staff.get("id")
    if not db.pool: raise HTTPException(status_code=503, detail="DB unavailable")
    role = staff.get("role", "")
    is_admin = role in ("admin", "manager")
    async with db.pool.acquire() as conn:
        if take:
            row = await conn.fetchrow("SELECT assigned_to FROM leads WHERE id=$1", lead_id)
            if not row:
                raise HTTPException(status_code=404, detail="Лид не найден")
            # Обычный сотрудник не может взять лид занятый другим; admin/manager могут
            if row["assigned_to"] and row["assigned_to"] != staff_id and not is_admin:
                raise HTTPException(status_code=409, detail="Лид уже взят другим сотрудником")
            await conn.execute("UPDATE leads SET assigned_to=$1 WHERE id=$2", staff_id, lead_id)
            note = f"Лид взят: {staff.get('first_name','')} {staff.get('last_name','')}".strip()
        else:
            row = await conn.fetchrow("SELECT assigned_to FROM leads WHERE id=$1", lead_id)
            if not row:
                raise HTTPException(status_code=404, detail="Лид не найден")
            # Освободить можно свой лид, или admin/manager любой
            if row["assigned_to"] != staff_id and not is_admin:
                raise HTTPException(status_code=403, detail="Можно освободить только свой лид")
            await conn.execute("UPDATE leads SET assigned_to=NULL WHERE id=$1", lead_id)
            note = f"Лид освобождён: {staff.get('first_name','')} {staff.get('last_name','')}".strip()
        await db.add_lead_call(lead_id, staff_id, action="note", note=note)
    lead = await db.get_lead_by_id(lead_id)
    return {"ok": True, "lead": dict(lead) if lead else {}}

@app.patch("/api/staff/leads/{lead_id}/status")
async def update_lead_status(lead_id: int, body: dict,
                             staff=Depends(require_perm("leads"))):
    status = body.get("status")
    if status not in ("new","contacted","callback","converted","lost","no_answer"):
        raise HTTPException(status_code=400, detail="Неверный статус")
    operator_id = None if staff.get("sub") == "admin" else staff.get("id")
    order_num = body.get("order_num")
    if status == "converted" and order_num:
        await db.convert_lead_to_order(lead_id, order_num, operator_id or 0)
    else:
        scheduled_at_pre = body.get("scheduled_at")
        from datetime import datetime as _dt
        sched_pre = _dt.fromisoformat(scheduled_at_pre) if scheduled_at_pre and status == "callback" else None
        await db.update_lead_status(lead_id, status, scheduled_at=sched_pre)
    if status in ("converted", "lost"):
        await db.cancel_pending_lead_reminders(lead_id)
    # лог
    action_labels = {
        "new": "Сменил статус на «Новый»",
        "contacted": "Связался с клиентом",
        "callback": "Клиент попросил перезвонить",
        "no_answer": "Не дозвонился",
        "converted": "Конвертировал в заказ",
        "lost": "Закрыл как потерянный",
    }
    note = body.get("note") or action_labels.get(status, status)
    scheduled_at = body.get("scheduled_at")  # ISO string or None
    from datetime import datetime
    sched = datetime.fromisoformat(scheduled_at) if scheduled_at else None
    await db.add_lead_call(lead_id, operator_id, action=f"status_{status}", note=note, scheduled_at=sched)
    if sched and operator_id:
        await db.add_lead_reminder(lead_id, operator_id, remind_at=sched,
                                   message=f"Перезвонить клиенту — лид {lead_id}")
    # Уведомить агента если лид агентский
    asyncio.create_task(_notify_agent_status(lead_id, status, note))
    return {"ok": True}

@app.get("/api/staff/my-notifications")
async def get_my_notifications(staff=Depends(get_current_staff)):
    rows = await db.get_agent_notifications(staff["id"])
    return {"ok": True, "notifications": [dict(r) for r in rows]}

@app.get("/api/staff/my-notifications/unread-count")
async def get_unread_count(staff=Depends(get_current_staff)):
    count = await db.count_unread_agent_notifications(staff["id"])
    return {"ok": True, "count": count}

@app.post("/api/staff/my-notifications/read")
async def mark_notifications_read(staff=Depends(get_current_staff)):
    await db.mark_agent_notifications_read(staff["id"])
    return {"ok": True}

@app.patch("/api/staff/my-notifications/{notif_id}/read")
async def mark_one_notification_read(notif_id: int, staff=Depends(get_current_staff)):
    await db.mark_agent_notification_read_by_id(notif_id, staff["id"])
    return {"ok": True}

@app.get("/api/staff/leads/{lead_id}/calls")
async def get_lead_calls(lead_id: int, _=Depends(require_perm("leads"))):
    rows = await db.get_lead_calls(lead_id)
    return {"ok": True, "calls": [dict(r) for r in rows]}

@app.post("/api/staff/leads/{lead_id}/calls")
async def add_lead_call(lead_id: int, body: dict, staff=Depends(require_perm("leads"))):
    operator_id = None if staff.get("sub") == "admin" else staff.get("id")
    action = body.get("action", "note")
    note = body.get("note", "")
    scheduled_at = body.get("scheduled_at")
    from datetime import datetime
    sched = datetime.fromisoformat(scheduled_at) if scheduled_at else None
    row = await db.add_lead_call(lead_id, operator_id, action=action, note=note, scheduled_at=sched)
    if sched and operator_id:
        await db.add_lead_reminder(lead_id, operator_id, remind_at=sched,
                                   message=note or "Запланированный звонок")
    return {"ok": True, "call": row}

@app.get("/api/staff/reminders/due")
async def get_due_reminders(staff=Depends(require_perm("leads"))):
    if staff.get("sub") == "admin":
        return {"ok": True, "reminders": []}
    rows = await db.get_due_reminders(staff["id"])
    result = [dict(r) for r in rows]
    return {"ok": True, "reminders": result}

@app.post("/api/staff/reminders/{reminder_id}/ack")
async def ack_reminder(reminder_id: int, staff=Depends(require_perm("leads"))):
    await db.mark_reminder_sent(reminder_id, "browser")
    return {"ok": True}

async def _tg_send_reply_keyboard(chat_id, text: str):
    """Отправляет сообщение с кнопкой 'Поделиться номером'."""
    if not BOT_TOKEN: return
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "reply_markup": {
                    "keyboard": [[{"text": "📱 Поделиться номером", "request_contact": True}]],
                    "resize_keyboard": True,
                    "one_time_keyboard": True,
                }
            }
        )

async def _tg_remove_keyboard(chat_id, text: str):
    """Отправляет сообщение и убирает клавиатуру."""
    if not BOT_TOKEN: return
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "reply_markup": {"remove_keyboard": True}
            }
        )


@app.post("/api/telegram/webhook")
async def telegram_webhook(request: Request):
    """Обрабатывает сообщения и callback_query от Telegram."""
    try:
        data = await request.json()
    except Exception:
        return {"ok": True}

    # ── Обычные сообщения (текст и контакт) ──────────────────────────
    msg = data.get("message") or data.get("edited_message")
    if msg:
        chat_id  = msg.get("chat", {}).get("id")
        tg_user_id = msg.get("from", {}).get("id")
        text     = (msg.get("text") or "").strip()
        contact  = msg.get("contact")

        if text == "/start" and chat_id:
            await _tg_send_reply_keyboard(
                chat_id,
                "👋 <b>Добро пожаловать в ARTEZ!</b>\n\n"
                "Нажмите кнопку ниже, чтобы привязать ваш номер телефона.\n"
                "После этого при регистрации на сайте вы сможете получить код подтверждения через Telegram."
            )
            return {"ok": True}

        if contact and tg_user_id and chat_id:
            phone_raw = contact.get("phone_number", "")
            # Нормализуем: +998901234567 → +998901234567
            phone = phone_raw if phone_raw.startswith("+") else "+" + phone_raw
            owner_tg = contact.get("user_id")
            # Принимаем только собственный контакт
            if owner_tg and int(owner_tg) != int(tg_user_id):
                await _tg_remove_keyboard(chat_id, "❌ Пожалуйста, поделитесь <b>своим</b> номером.")
                return {"ok": True}
            await db.save_tg_phone_link(phone, int(tg_user_id))
            await _tg_remove_keyboard(
                chat_id,
                f"✅ <b>Номер привязан!</b>\n\n"
                f"📱 <code>{phone}</code>\n\n"
                f"Теперь при регистрации на сайте ARTEZ вы можете выбрать "
                f"«Получить код через Telegram»."
            )
            return {"ok": True}

    # ── Callback query (кнопка 'Взять лид') ──────────────────────────
    cq = data.get("callback_query")
    if not cq:
        return {"ok": True}

    cq_id      = cq["id"]
    cq_data    = cq.get("data", "")
    tg_user_id = cq["from"]["id"]
    message    = cq.get("message", {})
    chat_id    = message.get("chat", {}).get("id")
    message_id = message.get("message_id")
    orig_text  = message.get("text", "")

    if not cq_data.startswith("take_lead_"):
        return {"ok": True}

    try:
        lead_id = int(cq_data.split("_")[2])
    except (IndexError, ValueError):
        await _tg_answer_callback(cq_id, "❌ Ошибка: неверный формат данных")
        return {"ok": True}

    # Проверяем — сотрудник ли нажавший (не агент)
    staff = await db.get_staff_by_tg_id(tg_user_id)
    if not staff:
        await _tg_answer_callback(cq_id,
            "❌ Ваш Telegram не привязан к аккаунту сотрудника ARTEZ.\n"
            "Обратитесь к администратору.", alert=True)
        return {"ok": True}
    if staff.get("role") == "agent":
        await _tg_answer_callback(cq_id,
            "❌ Агенты не могут брать лиды через Telegram.\n"
            "Лиды берут только сотрудники.", alert=True)
        return {"ok": True}

    staff_id   = staff["id"]
    staff_name = f"{staff.get('first_name','')} {staff.get('last_name','')}".strip() or staff.get("login","")
    took_verb  = "Взяла" if staff.get("gender") == "F" else "Взял"

    if not db.pool:
        await _tg_answer_callback(cq_id, "❌ Ошибка базы данных", alert=True)
        return {"ok": True}

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT assigned_to, lead_code FROM leads WHERE id=$1", lead_id)
        if not row:
            await _tg_answer_callback(cq_id, "❌ Лид не найден", alert=True)
            return {"ok": True}

        if row["assigned_to"] and row["assigned_to"] != staff_id:
            taker = await db.get_staff_by_id(row["assigned_to"])
            taker_name = ""
            taker_verb = "Взяла" if taker and taker.get("gender") == "F" else "Взял"
            if taker:
                taker_name = f"{taker.get('first_name','')} {taker.get('last_name','')}".strip()
            await _tg_answer_callback(cq_id,
                f"❌ Лид уже взят: {taker_name or 'другой сотрудник'}", alert=True)
            # Убираем кнопку из сообщения — лид уже не свободен
            new_text = orig_text.rstrip("━━━━━━━━━━").rstrip() + f"\n━━━━━━━━━━\n✅ {taker_verb}: {taker_name or 'другой сотрудник'}"
            await _tg_edit_message(chat_id, message_id, new_text)
            return {"ok": True}

        if row["assigned_to"] == staff_id:
            await _tg_answer_callback(cq_id, "✅ Этот лид уже ваш!")
            return {"ok": True}

        await conn.execute(
            "UPDATE leads SET assigned_to=$1 WHERE id=$2", staff_id, lead_id)

    await db.add_lead_call(lead_id, staff_id, action="note",
                           note=f"Лид взят через Telegram: {staff_name}")

    await _tg_answer_callback(cq_id, f"✅ Лид взят! Откройте приложение.")

    # Редактируем сообщение — убираем кнопку, добавляем кто взял
    new_text = orig_text.rstrip("━━━━━━━━━━").rstrip() + f"\n━━━━━━━━━━\n✅ {took_verb}: {staff_name}"
    await _tg_edit_message(chat_id, message_id, new_text)

    return {"ok": True}


@app.get("/api/push/vapid-key")
async def get_vapid_key():
    return {"public_key": VAPID_PUBLIC}

@app.post("/api/staff/push-subscription")
async def save_push_subscription(body: dict, staff=Depends(get_current_staff)):
    endpoint = body.get("endpoint")
    keys     = body.get("keys") or {}
    p256dh   = keys.get("p256dh")
    auth     = keys.get("auth")
    if not endpoint or not p256dh or not auth:
        raise HTTPException(400, "Неверные данные подписки")
    await db.upsert_push_subscription(staff["id"], endpoint, p256dh, auth)
    return {"ok": True}

@app.delete("/api/staff/push-subscription")
async def remove_push_subscription(body: dict, staff=Depends(get_current_staff)):
    endpoint = body.get("endpoint")
    if endpoint:
        await db.delete_push_subscription(endpoint, staff["id"])
    return {"ok": True}

@app.delete("/api/staff/leads/{lead_id}")
async def delete_lead_staff(lead_id: int, body: dict, _=Depends(require_perm("leads"))):
    if not body.get("admin_password") or body["admin_password"] != await get_admin_pass():
        raise HTTPException(status_code=403, detail="Неверный пароль администратора")
    ok = await db.delete_lead(lead_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Лид не найден")
    return {"ok": True}

@app.post("/api/staff/leads/bulk-delete")
async def bulk_delete_leads(body: dict, _=Depends(require_perm("leads"))):
    if not body.get("admin_password") or body["admin_password"] != await get_admin_pass():
        raise HTTPException(status_code=403, detail="Неверный пароль администратора")
    ids = body.get("ids", [])
    if not ids:
        raise HTTPException(status_code=400, detail="Нет ID лидов")
    deleted = 0
    for lead_id in ids:
        ok = await db.delete_lead(int(lead_id))
        if ok:
            deleted += 1
    return {"ok": True, "deleted": deleted}

@app.post("/api/staff/leads/bulk-status")
async def bulk_status_leads(body: dict, staff=Depends(require_perm("leads"))):
    status = body.get("status")
    if status not in ("new","contacted","callback","converted","lost","no_answer"):
        raise HTTPException(status_code=400, detail="Неверный статус")
    ids = body.get("ids", [])
    if not ids:
        raise HTTPException(status_code=400, detail="Нет ID лидов")
    operator_id = None if staff.get("sub") == "admin" else staff.get("id")
    for lead_id in ids:
        await db.update_lead_status(int(lead_id), status)
        await db.add_lead_call(int(lead_id), operator_id, action=f"status_{status}",
                               note=f"Массовая смена статуса")
    return {"ok": True, "updated": len(ids)}

@app.post("/api/staff/orders/bulk-status")
async def bulk_status_orders(body: dict, staff=Depends(require_perm("orders"))):
    status = body.get("status")
    valid = {"new","confirmed","pickup","received","washing","drying","packing","ready","delivery","delivered","cancelled"}
    if status not in valid:
        raise HTTPException(status_code=400, detail="Неверный статус")
    ids = body.get("ids", [])
    if not ids:
        raise HTTPException(status_code=400, detail="Нет ID заказов")
    for order_id in ids:
        await db.update_order_status(int(order_id), status, note="Массовая смена статуса")
    return {"ok": True, "updated": len(ids)}

@app.post("/api/staff/orders/bulk-delete")
async def bulk_delete_orders(body: dict, _=Depends(require_perm("orders"))):
    if not body.get("admin_password") or body["admin_password"] != await get_admin_pass():
        raise HTTPException(status_code=403, detail="Неверный пароль администратора")
    ids = body.get("ids", [])
    if not ids:
        raise HTTPException(status_code=400, detail="Нет ID заказов")
    deleted = 0
    skipped = []
    for order_id in ids:
        try:
            ok = await db.delete_order(int(order_id))
            if ok: deleted += 1
        except ValueError as e:
            if "has_payments" in str(e):
                skipped.append(int(order_id))
    result = {"ok": True, "deleted": deleted}
    if skipped:
        result["skipped"] = skipped
        result["skipped_reason"] = "Заказы с платежами не удалены — сначала удалите платежи в карточке заказа"
    return result


# ══════════════════════════════════════
#  ЗАЯВКИ — для сотрудников
# ══════════════════════════════════════

# Какие статусы видит сотрудник в зависимости от этапа
_STAGE_STATUSES = {
    "pickup":  {"new", "confirmed", "pickup", "cancelled"},
    "wash":    {"received", "washing", "cancelled"},
    "dry":     {"washing", "drying", "cancelled"},
    "pack":    {"drying", "packing", "ready", "cancelled"},
    "deliver": {"ready", "delivery", "delivered", "cancelled"},
}

@app.get("/api/staff/orders")
async def staff_orders(status: str = None, branch: str = None, limit: int = 50, offset: int = 0,
                       staff=Depends(require_perm("orders"))):
    # Фильтр по этапам: если order_stages заданы — показывать только нужные статусы
    stages_raw = staff.get("order_stages") or ""
    stages = [s.strip() for s in stages_raw.split(",") if s.strip()]
    visible = set()
    for stage in stages:
        visible |= _STAGE_STATUSES.get(stage, set())
    result, total = await db.get_admin_orders(
        status=status, statuses=list(visible) if stages else None,
        branch=branch, limit=limit, offset=offset)
    if staff.get("hide_client_phone"):
        for o in result:
            o["client_phone"] = ""
    return {"ok": True, "orders": result, "total": total, "limit": limit, "offset": offset}

@app.get("/api/staff/orders/own")
async def staff_own_orders(status: str = None, limit: int = 50, offset: int = 0,
                            staff=Depends(get_current_staff)):
    result, total = await db.get_admin_orders(
        status=status, branch=staff.get("branch"), limit=limit, offset=offset)
    if staff.get("hide_client_phone"):
        for o in result:
            o["client_phone"] = ""
    return {"ok": True, "orders": result, "total": total, "limit": limit, "offset": offset}

@app.post("/api/staff/orders/create")
async def staff_create_order(req: StaffOrderRequest, staff=Depends(require_perm("orders"))):
    if not staff.get("can_create_order", True):
        raise HTTPException(status_code=403, detail="Нет права создавать заказы")
    req.phone = normalize_phone(req.phone)
    if not PHONE_RE.match(req.phone):
        raise HTTPException(status_code=400, detail="Неверный формат номера. Используйте +998XXXXXXXXX")
    try:
        order_num = await db.get_next_order_num()
        first_name = staff.get("first_name") or ""
        last_name  = staff.get("last_name") or ""
        login      = staff.get("login") or ""
        staff_label = " ".join(filter(None, [first_name, last_name])) or login or "сотрудник"
        if login and login != staff_label:
            staff_label = f"{staff_label} (@{login})"
        branch = req.branch or staff.get("branch") or ""
        location = req.location or ""
        location_address = req.location_address or ""
        note_full = f"📱 Заявка от сотрудника: {staff_label}" + (f"\n{req.note}" if req.note else "")
        await db.save_site_order({
            "order_num":   order_num,
            "first_name":  req.first_name,
            "last_name":   "",
            "phone":       req.phone,
            "branch":      branch,
            "city":        "",
            "address":       req.address or "",
            "short_address": req.short_address or "",
            "location":      location,
            "service":      req.service,
            "service_type": req.service_type or "standard",
            "pickup_type":  req.pickup_type or "courier",
            "delivery_type": req.delivery_type or "courier",
            "pickup_date": req.pickup_date or "",
            "pickup_time": req.pickup_time or "",
            "note":        note_full,
            "total_price": None,
        }, source="staff")
        # Уведомление в Telegram — строим текст вручную, без Pydantic
        if BOT_TOKEN:
            staff_chat_id = await _group_id_for_branch(branch)
            if staff_chat_id:
                full_name = req.first_name
                staff_name = staff_label
                if location:
                    try:
                        lat, lon = location.split(",", 1)
                        yandex_url = f"https://yandex.uz/maps/?pt={lon.strip()},{lat.strip()}&z=16"
                        link_text = location_address if location_address else f"{lat.strip()}, {lon.strip()}"
                        loc_line = f"\n🗺 <a href=\"{yandex_url}\">{link_text}</a>"
                    except Exception:
                        loc_line = f"\n🗺 {location_address or location}"
                else:
                    loc_line = ""
                SERVICE_RU = {
                    "carpet":      "Ковры",
                    "carpet_home": "Ковры на дому",
                    "sofa":        "Диваны",
                    "mattress":    "Матрасы",
                    "curtains":    "Шторы",
                }
                service_ru = SERVICE_RU.get(req.service, req.service or "—")
                text = (
                    f"📱 Заявка от сотрудника {order_num}\n"
                    f"━━━━━━━━━━\n"
                    f"👤 {full_name}\n"
                    f"📞 {req.phone}\n"
                    f"🏢 {branch_ru(branch)}\n"
                    f"🧺 {service_ru}\n"
                    f"🏠 {req.short_address or req.address or '—'}{(' | ' + req.address) if req.short_address and req.address and req.short_address != req.address else ''}{loc_line}\n"
                    f"👷 {staff_name}\n"
                    f"━━━━━━━━━━"
                )
                # Без кнопок Принять/Отклонить — заявка уже подтверждена тем сотрудником,
                # который её создал в admin.html, группа тут только для просмотра
                # (кнопки нужны только для неподтверждённых лидов с сайта).
                async with aiohttp.ClientSession() as session:
                    await session.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                        json={"chat_id": staff_chat_id, "text": text,
                              "parse_mode": "HTML", "disable_web_page_preview": True},
                        timeout=aiohttp.ClientTimeout(total=8),
                    )
        # Авто-регистрация клиента в CRM
        await db.upsert_crm_client(
            phone=req.phone,
            first_name=req.first_name,
            source="staff",
        )
        await db.refresh_crm_client_stats(req.phone)
        # SMS больше не шлём молча — фронт после успешного создания сам спросит
        # (модалка "Отправить SMS клиенту?" с выбором языка), если item_count > 0,
        # и вызовет /send-sms явно (портировано из прода, запрос 2026-08-05).
        order_id = None
        async with db.pool.acquire() as conn:
            _row = await conn.fetchrow("SELECT id FROM orders WHERE order_num=$1 AND company_id=$2",
                                        order_num, staff["company_id"])
        if _row:
            order_id = _row["id"]
            if req.items or req.item_count:
                if req.items:
                    await db.create_empty_items_by_service(order_id, req.items)
                else:
                    await db.create_empty_items(order_id, max(0, req.item_count))
        return {"ok": True, "order_num": order_num, "id": order_id, "item_count": req.item_count or len(req.items or [])}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Ошибка: {type(e).__name__}: {e}")


# ══════════════════════════════════════
#  CRM КЛИЕНТЫ
# ══════════════════════════════════════
class ClientCreateRequest(BaseModel):
    phone: str
    phone2: str = ""
    first_name: str = ""
    last_name: str = ""
    source: str = "staff"
    status: str = "new"
    note: str = ""
    address: str = ""
    short_address: str = ""

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v):
        v = normalize_phone(v)
        if not PHONE_RE.match(v):
            raise ValueError("Неверный формат номера")
        return v

# ── Admin auth helpers (defined early so they can be used anywhere below) ──────

async def _get_admin(authorization: str = Header(None)):
    """Проверяет admin JWT (sub='admin') или staff JWT с role='admin'."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Не авторизован")
    token = authorization.removeprefix("Bearer ").strip()
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        if payload.get("sub") != "admin" and payload.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Нет доступа")
    except JWTError:
        raise HTTPException(status_code=401, detail="Недействительный токен")
    return True

async def _get_admin_cid(authorization: str = Header(None)) -> int:
    """Как _get_admin, но возвращает company_id вызывающего — для запросов,
    которым нужно ограничить действие своей компанией (см. #IDOR staff/permissions)."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Не авторизован")
    token = authorization.removeprefix("Bearer ").strip()
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        if payload.get("sub") != "admin" and payload.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Нет доступа")
    except JWTError:
        raise HTTPException(status_code=401, detail="Недействительный токен")
    return int(payload.get("company_id") or 1)

async def _get_admin_or_staff_clients(authorization: str = Header(None)):
    """Принимает admin JWT или staff JWT с пермиссией clients."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Не авторизован")
    token = authorization.removeprefix("Bearer ").strip()
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="Недействительный токен")
    if payload.get("sub") == "admin":
        return True
    if payload.get("type") != "staff":
        raise HTTPException(status_code=403, detail="Нет доступа")
    staff = await db.get_staff_by_id(int(payload["sub"]))
    if not staff or not staff["active"]:
        raise HTTPException(status_code=403, detail="Нет доступа")
    role = staff.get("role") or ""
    perms = ROLE_PERMISSIONS.get(role, [])
    if "clients" in perms or role == "admin":
        return True
    raise HTTPException(status_code=403, detail="Нет доступа")

# ──────────────────────────────────────────────────────────────────────────────

class ClientUpdateRequest(BaseModel):
    phone2: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    status: str | None = None
    note: str | None = None
    address: str | None = None
    short_address: str | None = None


@app.get("/api/clients")
async def clients_list(search: str = "", limit: int = 50, offset: int = 0,
                       _=Depends(_get_admin_or_staff_clients)):
    rows = await db.get_crm_clients_list(search=search, limit=limit, offset=offset)
    counts = await db.get_crm_clients_count()
    return {"ok": True, "clients": rows, "counts": counts}


@app.get("/api/clients/by-phone/{phone}")
async def client_by_phone(phone: str, _=Depends(_get_admin_or_staff_clients)):
    phone = normalize_phone(phone)
    row = await db.get_crm_client_by_phone(phone)
    return {"ok": True, "client": row}


@app.get("/api/clients/{client_id}")
async def client_detail(client_id: int, _=Depends(_get_admin_or_staff_clients)):
    row = await db.get_crm_client_by_id(client_id)
    if not row:
        raise HTTPException(status_code=404, detail="Клиент не найден")
    orders = await db.get_crm_client_orders(row["phone"])
    return {"ok": True, "client": row, "orders": orders}


@app.post("/api/clients")
async def client_create(req: ClientCreateRequest, _=Depends(_get_admin_or_staff_clients)):
    existing = await db.get_crm_client_by_phone(req.phone)
    if existing:
        raise HTTPException(status_code=409, detail={
            "msg": "Клиент с таким номером уже существует",
            "client": existing
        })
    row = await db.upsert_crm_client(
        phone=req.phone, first_name=req.first_name, last_name=req.last_name,
        source=req.source, address=req.address, short_address=req.short_address,
    )
    if req.phone2 or req.note or req.status != "new":
        row = await db.update_crm_client(
            row["id"], phone2=req.phone2 or None,
            note=req.note or None, status=req.status
        ) or row
    return {"ok": True, "client": row}


@app.get("/api/admin/active-contacts")
async def active_contacts_list(search: str = "", limit: int = 50, offset: int = 0,
                                _=Depends(_get_admin_or_staff_clients)):
    rows = await db.get_active_contacts_list(search=search, limit=limit, offset=offset)
    count = await db.get_active_contacts_count(search=search)
    return {"ok": True, "contacts": rows, "count": count}


@app.post("/api/admin/active-contacts/backfill")
async def active_contacts_backfill(_=Depends(_get_admin)):
    """Разовый импорт исторических контактов (crm_clients + лиды + подтверждённые
    пользователи сайта) своей компании в единую базу актуальных контактов."""
    stats = await db.backfill_active_contacts()
    return {"ok": True, "stats": stats}


@app.get("/api/admin/blacklist")
async def blacklist_list(search: str = "", limit: int = 50, offset: int = 0,
                          _=Depends(_get_admin_or_staff_clients)):
    rows = await db.get_blacklist_list(search=search, limit=limit, offset=offset)
    count = await db.get_blacklist_count(search=search)
    return {"ok": True, "entries": rows, "count": count}


@app.post("/api/admin/blacklist/toggle")
async def blacklist_toggle(body: dict = Body(...), staff=Depends(get_current_staff)):
    role = staff.get("role", "")
    perms = ROLE_PERMISSIONS.get(role, [])
    if "clients" not in perms and role != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    phone = normalize_phone(body.get("phone", ""))
    if not phone:
        raise HTTPException(status_code=400, detail="Не указан телефон")
    if body.get("blacklisted", True):
        added_by = staff.get("login") or staff.get("first_name") or ""
        entry = await db.upsert_blacklist_entry(phone, note=body.get("note", ""), added_by=added_by,
                                                 name=body.get("name", ""))
        return {"ok": True, "entry": entry}
    else:
        await db.remove_from_blacklist(phone)
        return {"ok": True}


@app.post("/api/admin/blacklist/sync")
async def blacklist_sync(_=Depends(_get_admin)):
    """Кнопка «Обновить» — досвежа проставляет флаг blacklisted во всех
    таблицах своей компании (справочник/актуальные/пользователи сайта/TG-клиенты)."""
    stats = await db.sync_blacklist_flags()
    return {"ok": True, "stats": stats}


@app.put("/api/clients/{client_id}")
async def client_update(client_id: int, req: ClientUpdateRequest,
                        _=Depends(_get_admin_or_staff_clients)):
    updates = {k: v for k, v in req.dict().items() if v is not None}
    row = await db.update_crm_client(client_id, **updates)
    if not row:
        raise HTTPException(status_code=404, detail="Клиент не найден")
    return {"ok": True, "client": row}


class ClientDeleteRequest(BaseModel):
    password: str

@app.post("/api/clients/{client_id}/delete")
async def client_delete(client_id: int, req: ClientDeleteRequest):
    if not (apass := await get_admin_pass()) or req.password != apass:
        raise HTTPException(status_code=403, detail="Неверный пароль")
    ok = await db.delete_crm_client(client_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Клиент не найден")
    return {"ok": True}

@app.get("/api/clients/{client_id}/orders")
async def client_orders(client_id: int, _=Depends(_get_admin_or_staff_clients)):
    row = await db.get_crm_client_by_id(client_id)
    if not row:
        raise HTTPException(status_code=404, detail="Клиент не найден")
    orders = await db.get_crm_client_orders(row["phone"])
    return {"ok": True, "orders": orders}


# ── Постоянная категория скидки (пенсионер / инвалид) ──────────────────────────
DISCOUNT_CATEGORIES = ("pensioner", "disabled")

def _require_clients_perm(staff: dict):
    role = staff.get("role") or ""
    perms = ROLE_PERMISSIONS.get(role, [])
    if role != "admin" and "clients" not in perms:
        raise HTTPException(status_code=403, detail="Нет доступа")

class ClientDiscountCategoryRequest(BaseModel):
    discount_category: str | None = None       # 'pensioner' | 'disabled' | None — снять категорию
    discount_category_pct: float | None = None  # обязателен при установке категории

@app.put("/api/clients/{client_id}/discount-category")
async def client_set_discount_category(client_id: int, req: ClientDiscountCategoryRequest,
                                       staff=Depends(get_current_staff)):
    _require_clients_perm(staff)
    existing = await db.get_crm_client_by_id(client_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Клиент не найден")
    category = (req.discount_category or "").strip() or None
    if category and category not in DISCOUNT_CATEGORIES:
        raise HTTPException(status_code=400, detail="Некорректная категория скидки")
    pct = req.discount_category_pct if category else None
    if category and (pct is None or not (0 < pct <= 100)):
        raise HTTPException(status_code=400, detail="Укажите корректный процент скидки (0–100)")
    row = await db.set_crm_client_discount_category(
        client_id, category, pct, staff.get("id") if category else None
    )
    if not row:
        raise HTTPException(status_code=404, detail="Клиент не найден")
    return {"ok": True, "client": row}

@app.post("/api/clients/{client_id}/discount-category/photo")
async def client_upload_discount_photo(client_id: int, file: UploadFile = File(...),
                                       staff=Depends(get_current_staff)):
    _require_clients_perm(staff)
    existing = await db.get_crm_client_by_id(client_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Клиент не найден")
    media_ch = await _get_media_channel()
    if not BOT_TOKEN or not media_ch:
        raise HTTPException(status_code=503, detail="Медиа-хранилище не настроено")
    content_type = file.content_type or "image/jpeg"
    tg_method = "sendDocument" if not content_type.startswith("image/") else "sendPhoto"
    tg_field  = "document" if tg_method == "sendDocument" else "photo"
    tg_type   = "document" if tg_method == "sendDocument" else "photo"
    staff_name  = " ".join(filter(None, [staff.get("last_name"), staff.get("first_name")])) or staff.get("login", "")
    client_name = " ".join(filter(None, [existing.get("first_name"), existing.get("last_name")])) or existing.get("phone", "")
    file_bytes = await file.read()
    form = aiohttp.FormData()
    form.add_field("chat_id", str(media_ch))
    form.add_field(tg_field, file_bytes, filename=file.filename or "document.jpg", content_type=content_type)
    form.add_field("caption", f"🪪 Документ льготы\n👤 Клиент: {client_name}\n📞 {existing.get('phone','')}\n✅ Проверил: {staff_name}")
    async with aiohttp.ClientSession() as s:
        async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{tg_method}", data=form) as r:
            result = await r.json()
    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=f"Telegram: {result.get('description','upload failed')}")
    msg = result["result"]
    file_id = msg["photo"][-1]["file_id"] if tg_type == "photo" else msg[tg_type]["file_id"]
    row = await db.save_crm_client_discount_photo(client_id, file_id)
    return {"ok": True, "client": row}

@app.get("/api/clients/{client_id}/discount-category/photo")
async def client_get_discount_photo(client_id: int, staff=Depends(get_current_staff)):
    _require_clients_perm(staff)
    row = await db.get_crm_client_by_id(client_id)
    if not row or not row.get("discount_category_photo_file_id"):
        raise HTTPException(status_code=404, detail="Фото не найдено")
    if not BOT_TOKEN:
        raise HTTPException(status_code=503, detail="Бот не настроен")
    try:
        from fastapi.responses import StreamingResponse
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile",
                             params={"file_id": row["discount_category_photo_file_id"]},
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
            if not data.get("ok"):
                raise HTTPException(status_code=404, detail="Файл не найден в TG")
            file_path = data["result"]["file_path"]
            file_url  = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
            ext = file_path.rsplit(".", 1)[-1].lower() if "." in file_path else ""
            ctype = ("image/jpeg" if ext in ("jpg", "jpeg") else
                     "image/png"  if ext == "png" else
                     "application/octet-stream")
            async with s.get(file_url, timeout=aiohttp.ClientTimeout(total=30)) as fr:
                content = await fr.read()
        return StreamingResponse(iter([content]), media_type=ctype,
                                 headers={"Content-Disposition": "inline"})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════════════════
# CONTACTS — справочник контактов
# ══════════════════════════════════════════════════════════════════════════════


class ContactCreateRequest(BaseModel):
    phone:         str
    first_name:    str = ""
    last_name:     str = ""
    middle_name:   str = ""
    phone2:        str = ""
    address:       str = ""
    short_address: str = ""
    source:        str = "ARTEZ"

class ContactUpdateRequest(BaseModel):
    phone:         str | None = None
    first_name:    str | None = None
    last_name:     str | None = None
    middle_name:   str | None = None
    phone2:        str | None = None
    address:       str | None = None
    short_address: str | None = None
    source:        str | None = None

class ContactsBulkRequest(BaseModel):
    rows: list[dict]

@app.get("/api/contacts/search")
async def contacts_search(q: str = "", limit: int = 10, _=Depends(get_current_staff)):
    results = await db.search_contacts(q.strip(), limit=min(limit, 20))
    return {"ok": True, "contacts": results}

@app.get("/api/contacts")
async def contacts_list(search: str = "", limit: int = 50, offset: int = 0,
                        _=Depends(_get_admin)):
    contacts = await db.get_contacts_list(search, limit=min(limit, 200), offset=offset)
    total    = await db.get_contacts_total(search)
    counts   = await db.get_contacts_source_counts()
    return {"ok": True, "contacts": contacts, "total": total, "counts": counts}

@app.post("/api/contacts")
async def contact_create(req: ContactCreateRequest, _=Depends(_get_admin)):
    contact = await db.upsert_contact(
        phone=req.phone, first_name=req.first_name, last_name=req.last_name,
        middle_name=req.middle_name, phone2=req.phone2,
        address=req.address, short_address=req.short_address, source=req.source)
    return {"ok": True, "contact": contact}

@app.post("/api/contacts/bulk")
async def contacts_bulk(req: ContactsBulkRequest, _=Depends(_get_admin)):
    result = await db.bulk_insert_contacts(req.rows)
    return {"ok": True, **result}

@app.get("/api/contacts/export")
async def contacts_export(
    search: str = "", source: str = "",
    has_phone2: bool = False, has_address: bool = False, valid_phone: bool = False,
    cid: int = Depends(_get_admin_cid)
):
    """Экспорт контактов своей компании без лимита с фильтрами."""
    async with db.pool.acquire() as conn:
        conditions = ["NOT COALESCE(blacklisted, FALSE)"]
        params: list = []
        i = 1
        if search:
            conditions.append(
                f"(phone ILIKE ${i} OR phone2 ILIKE ${i} OR first_name ILIKE ${i} "
                f"OR last_name ILIKE ${i} OR middle_name ILIKE ${i} OR address ILIKE ${i} OR short_address ILIKE ${i})"
            )
            params.append(f"%{search}%"); i += 1
        if source:
            conditions.append(f"source = ${i}"); params.append(source); i += 1
        if has_phone2:
            conditions.append("phone2 IS NOT NULL AND phone2 != ''")
        if has_address:
            conditions.append("(address IS NOT NULL AND address != '' OR short_address IS NOT NULL AND short_address != '')")
        if valid_phone:
            conditions.append("length(regexp_replace(regexp_replace(phone,'[^0-9]','','g'),'^998','')) >= 9")
        conditions.append(f"company_id = ${i}"); params.append(cid); i += 1
        sql = f"SELECT * FROM contacts WHERE {' AND '.join(conditions)} ORDER BY id DESC"
        rows = await conn.fetch(sql, *params)
    return {"ok": True, "contacts": [dict(r) for r in rows], "total": len(rows)}


@app.get("/api/contacts/duplicates")
async def contacts_duplicates(cid: int = Depends(_get_admin_cid)):
    """Анализ дублирующих телефонных номеров в справочнике своей компании."""
    async with db.pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM contacts WHERE company_id=$1", cid)

        # Одинаковые номера (разные форматы записи)
        format_dups = await conn.fetch("""
            WITH n AS (
                SELECT id, phone, first_name, last_name,
                       regexp_replace(regexp_replace(phone, '[^0-9]', '', 'g'), '^998', '') AS norm
                FROM contacts WHERE phone IS NOT NULL AND phone != '' AND company_id=$1
            )
            SELECT norm, COUNT(*) AS cnt,
                   array_agg(id    ORDER BY id) AS ids,
                   array_agg(phone ORDER BY id) AS phones,
                   array_agg(trim(coalesce(first_name,'')||' '||coalesce(last_name,'')) ORDER BY id) AS names
            FROM n WHERE length(norm) >= 7
            GROUP BY norm HAVING COUNT(*) > 1
            ORDER BY cnt DESC, norm
        """, cid)

        # Основной номер одного контакта = доп. номер другого
        phone2_cross = await conn.fetch("""
            SELECT c1.id AS id1, c1.phone AS phone1,
                   trim(coalesce(c1.first_name,'')||' '||coalesce(c1.last_name,'')) AS name1,
                   c2.id AS id2, c2.phone2 AS phone2_raw,
                   trim(coalesce(c2.first_name,'')||' '||coalesce(c2.last_name,'')) AS name2
            FROM contacts c1
            JOIN contacts c2
              ON regexp_replace(regexp_replace(c1.phone,'[^0-9]','','g'),'^998','')
               = regexp_replace(regexp_replace(c2.phone2,'[^0-9]','','g'),'^998','')
            WHERE c1.id != c2.id AND c2.phone2 IS NOT NULL AND c2.phone2 != ''
              AND c1.company_id=$1 AND c2.company_id=$1
            ORDER BY c1.phone, c1.id
        """, cid)

        # Короткие / некорректные номера (< 7 цифр после нормализации)
        short_phones = await conn.fetch("""
            SELECT id, phone, trim(coalesce(first_name,'')||' '||coalesce(last_name,'')) AS name,
                   length(regexp_replace(regexp_replace(phone,'[^0-9]','','g'),'^998','')) AS norm_len
            FROM contacts
            WHERE phone IS NOT NULL AND phone != '' AND company_id=$1
              AND length(regexp_replace(regexp_replace(phone,'[^0-9]','','g'),'^998','')) < 7
            ORDER BY norm_len, phone
        """, cid)

    return {
        "ok": True,
        "total": total,
        "format_duplicates": [
            {"norm": r["norm"], "cnt": r["cnt"],
             "ids": list(r["ids"]), "phones": list(r["phones"]), "names": list(r["names"])}
            for r in format_dups
        ],
        "phone2_matches": [
            {"id1": r["id1"], "phone1": r["phone1"], "name1": r["name1"],
             "id2": r["id2"], "phone2_raw": r["phone2_raw"], "name2": r["name2"]}
            for r in phone2_cross
        ],
        "short_phones": [
            {"id": r["id"], "phone": r["phone"], "name": r["name"], "norm_len": r["norm_len"]}
            for r in short_phones
        ],
    }


@app.get("/api/contacts/{contact_id}")
async def contact_get(contact_id: int, cid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM contacts WHERE id=$1 AND company_id=$2", contact_id, cid)
    if not row:
        raise HTTPException(status_code=404, detail="Контакт не найден")
    return {"ok": True, "contact": dict(row)}

@app.put("/api/contacts/{contact_id}")
async def contact_update(contact_id: int, req: ContactUpdateRequest,
                         _=Depends(_get_admin)):
    data = {k: v for k, v in req.dict().items() if v is not None}
    contact = await db.update_contact(contact_id, **data)
    if not contact:
        raise HTTPException(status_code=404, detail="Контакт не найден")
    return {"ok": True, "contact": contact}

class ContactDeleteRequest(BaseModel):
    password: str

@app.post("/api/contacts/{contact_id}/delete")
async def contact_delete(contact_id: int, req: ContactDeleteRequest):
    if not (apass := await get_admin_pass()) or req.password != apass:
        raise HTTPException(status_code=403, detail="Неверный пароль")
    ok = await db.delete_contact(contact_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Контакт не найден")
    return {"ok": True}


class ContactsPurgeRequest(BaseModel):
    password: str

@app.post("/api/contacts/purge")
async def contacts_purge(req: ContactsPurgeRequest):
    """Удалить все контакты — только по паролю администратора."""
    if not (apass := await get_admin_pass()) or req.password != apass:
        raise HTTPException(status_code=403, detail="Неверный пароль")
    deleted = await db.delete_all_contacts()
    return {"ok": True, "deleted": deleted}


@app.get("/api/prices")
async def get_prices(company_slug: str = None):
    """Возвращает актуальные цены из БД для калькулятора и прайс-листа на сайте"""
    cid = await _resolve_client_company_id(company_slug)
    db.set_request_company(cid)
    prices = await db.get_all_prices()
    if not prices:
        # Дефолты на случай пустой таблицы
        prices = {
            "carpet":      {"standard": {"price": 13000, "unit_key": "m2", "min_order": 10}, "express": {"price": 18000, "unit_key": "m2", "min_order": 10}},
            "carpet_home": {"standard": {"price": 15000, "unit_key": "m2", "min_order": 10}, "express": {"price": 20000, "unit_key": "m2", "min_order": 10}},
            "sofa":        {"standard": {"price": 100000, "unit_key": "m2", "min_order": None}, "express": {"price": 150000, "unit_key": "m2", "min_order": None}},
            "mattress":    {"standard": {"price": 30000, "unit_key": "m2", "min_order": None}, "express": {"price": 40000, "unit_key": "m2", "min_order": None}},
            "curtains":    {"standard": {"price": 5000,  "unit_key": "m2", "min_order": None}, "express": {"price": 8000,  "unit_key": "m2", "min_order": None}},
        }
    units = await db.get_all_units()
    units_dict = {u["key"]: dict(u) for u in units}
    return {"ok": True, "prices": prices, "units": units_dict}


@app.get("/api/check-tg-link")
async def check_tg_link(phone: str):
    """Проверяет, привязан ли телефон к Telegram боту."""
    normalized = normalize_phone(phone)
    tg_id = await db.get_tg_id_by_phone(normalized)
    return {"has_tg": tg_id is not None}


@app.post("/api/tg-phone-link")
async def tg_phone_link(body: dict):
    """Бот вызывает этот endpoint когда клиент делится номером для привязки к сайту."""
    phone  = str(body.get("phone", "")).strip()
    tg_id  = body.get("tg_id")
    if not phone or not tg_id:
        raise HTTPException(400, "phone and tg_id required")
    phone = normalize_phone(phone)
    await db.save_tg_phone_link(phone, int(tg_id))
    # Легаси-эндпоинт, вызывается только старым прод-ботом ARTEZ (company_id=1
    # захардкожен там же) — не трогаем поведение, SaaS-бот его не использует.
    user = await db.get_user_by_phone(phone, 1)
    return {"ok": True, "registered": user is not None and user.get("is_verified", False)}


@app.post("/api/register")
async def register(req: RegisterRequest):
    uz = req.lang == "uz"
    cid = await _resolve_client_company_id(req.company_slug)
    if not req.via_tg and await db.get_config_for_company("sms_registration_enabled", cid) == "false":
        raise HTTPException(status_code=403, detail=(
            "Ro'yxatdan o'tish faqat Telegram orqali mavjud" if uz
            else "Регистрация доступна только через Telegram"))
    existing = await db.get_user_by_phone(req.phone, cid)
    if existing and existing["is_verified"]:
        raise HTTPException(status_code=400, detail=(
            "Bu raqam allaqachon ro'yxatdan o'tgan" if uz
            else "Этот номер уже зарегистрирован"))

    ok, err = await db.check_sms_rate_limit(req.phone, "register")
    if not ok:
        raise HTTPException(status_code=429, detail=err)

    password_hash = pwd_context.hash(req.password[:72])
    await db.create_user(req.phone, password_hash, req.first_name, cid)

    code = generate_code()
    expires_at = datetime.utcnow() + timedelta(minutes=SMS_CODE_TTL_MIN)
    await db.save_sms_code(req.phone, code, "register", expires_at)

    if req.via_tg:
        tg_id = await db.get_tg_id_by_phone(req.phone)
        if not tg_id:
            raise HTTPException(status_code=400, detail=(
                "Telegram не привязан. Сначала напишите боту /start и поделитесь номером."
                if not uz else
                "Telegram bog'lanmagan. Botga /start yozing va raqamingizni ulashing."))
        code_text = (
            f"🔐 <b>ARTEZ</b> — код подтверждения регистрации:\n\n<code>{code}</code>\n\n⏱ Действителен 5 минут."
            if not uz else
            f"🔐 <b>ARTEZ</b> — ro'yxatdan o'tish tasdiqlash kodi:\n\n<code>{code}</code>\n\n⏱ 5 daqiqa davomida amal qiladi."
        )
        await send_tg(tg_id, code_text)
        return {"ok": True, "via_tg": True, "message": "Код отправлен в Telegram", "phone": req.phone}

    await send_sms(req.phone, await sms_text(code, "register"))
    return {"ok": True, "via_tg": False, "message": "Код подтверждения отправлен", "phone": req.phone}


@app.post("/api/verify")
async def verify(req: VerifyRequest):
    cid = await _resolve_client_company_id(req.company_slug)
    db.set_request_company(cid)
    ok = await db.check_sms_code(req.phone, req.code, "register")
    if not ok:
        raise HTTPException(status_code=400, detail=bi("Неверный или просроченный код","Noto'g'ri yoki muddati o'tgan kod"))

    await db.verify_user(req.phone, cid)
    user = await db.get_user_by_phone(req.phone, cid)
    asyncio.create_task(db.update_user_last_login(user["id"]))
    # Регистрация на сайте раньше не попадала в единую базу контактов — закрываем дыру.
    asyncio.create_task(db.upsert_crm_client(phone=user["phone"], first_name=user.get("first_name") or "",
                                              source="site_register"))
    asyncio.create_task(_notify_new_site_user(user.get("first_name") or "", user["phone"], "sms"))
    token = create_token(user["id"], user["phone"], cid)

    return {
        "ok": True,
        "token": token,
        "user": {
            "id": user["id"],
            "phone": user["phone"],
            "first_name": user["first_name"],
            "address": user["address"],
            "must_change_password": user.get("must_change_password", False),
        }
    }


@app.post("/api/resend-code")
async def resend_code(req: ResendCodeRequest):
    cid = await _resolve_client_company_id(req.company_slug)
    user = await db.get_user_by_phone(req.phone, cid)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    ok, err = await db.check_sms_rate_limit(req.phone, req.purpose)
    if not ok:
        raise HTTPException(status_code=429, detail=err)

    code = generate_code()
    expires_at = datetime.utcnow() + timedelta(minutes=SMS_CODE_TTL_MIN)
    await db.save_sms_code(req.phone, code, req.purpose, expires_at)

    if req.via_tg:
        tg_id = await db.get_tg_id_by_phone(req.phone)
        if tg_id:
            await send_tg(tg_id,
                f"🔐 <b>ARTEZ</b> — код подтверждения:\n\n<code>{code}</code>\n\n⏱ Действителен 5 минут.")
            return {"ok": True, "message": "Код отправлен в Telegram"}
    await send_sms(req.phone, await sms_text(code, req.purpose))
    return {"ok": True, "message": "Код отправлен повторно"}


@app.post("/api/login")
async def login(req: LoginRequest):
    cid = await _resolve_client_company_id(req.company_slug)
    user = await db.get_user_by_phone(req.phone, cid)
    if not user or not pwd_context.verify(req.password[:72], user["password_hash"]):
        raise HTTPException(status_code=401, detail=bi("Неверный номер или пароль","Noto'g'ri telefon yoki parol"))

    if not user["is_verified"]:
        raise HTTPException(status_code=403, detail=bi("Номер не подтверждён. Запросите код заново","Raqam tasdiqlanmagan. Kodni qayta so'rang"))

    asyncio.create_task(db.update_user_last_login(user["id"]))
    token = create_token(user["id"], user["phone"], cid)
    return {
        "ok": True,
        "token": token,
        "user": {
            "id": user["id"],
            "phone": user["phone"],
            "first_name": user["first_name"],
            "address": user["address"],
            "must_change_password": user.get("must_change_password", False),
        }
    }


@app.post("/api/forgot-password/request")
async def forgot_password_request(body: dict):
    """Отправляет код сброса пароля — в Telegram (через СВОЙ бот компании), если
    номер к нему привязан, иначе по SMS (тот же принцип, что и register-via-tg)."""
    phone = normalize_phone((body.get("phone") or "").strip())
    uz = (body.get("lang") or "ru") == "uz"
    cid = await _resolve_client_company_id(body.get("company_slug"))
    if not phone:
        raise HTTPException(400, "Telefon raqami kerak" if uz else "Укажите номер телефона")

    user = await db.get_user_by_phone(phone, cid)
    if not user or not user.get("is_verified"):
        # По явному запросу пользователя (2026-08-18) сообщаем, что номер не
        # найден — раньше отвечали {"ok":true} на любой номер (anti-enumeration),
        # но UX-приоритет перевесил: пользователь предпочёл явную ошибку.
        raise HTTPException(400, "Bunday raqam bilan ro'yxatdan o'tgan foydalanuvchi topilmadi" if uz else "Пользователь с таким номером не найден")

    ok, err = await db.check_sms_rate_limit(phone, "reset")
    if not ok:
        raise HTTPException(status_code=429, detail=err)

    code = generate_code()
    expires_at = datetime.utcnow() + timedelta(minutes=SMS_CODE_TTL_MIN)
    await db.save_sms_code(phone, code, "reset", expires_at)

    tg_id = user.get("tg_id")
    if tg_id:
        from zoneinfo import ZoneInfo
        expires_str = (datetime.now(ZoneInfo("Asia/Tashkent")) + timedelta(minutes=SMS_CODE_TTL_MIN)).strftime("%d.%m.%Y %H:%M")
        text = (
            f"🔐 <b>Код для сброса пароля:</b>\n\n<code>{code}</code>\n\n⏱ Действителен {SMS_CODE_TTL_MIN} минут (до {expires_str})."
            if not uz else
            f"🔐 <b>Parolni tiklash kodi:</b>\n\n<code>{code}</code>\n\n⏱ {SMS_CODE_TTL_MIN} daqiqa amal qiladi ({expires_str} gacha)."
        )
        if await _send_company_tg(cid, tg_id, text):
            return {"ok": True}

    await send_sms(phone, await sms_text(code, "reset"))
    return {"ok": True}


@app.post("/api/forgot-password/confirm")
async def forgot_password_confirm(body: dict):
    phone = normalize_phone((body.get("phone") or "").strip())
    code = (body.get("code") or "").strip()
    new_password = (body.get("password") or "").strip()
    uz = (body.get("lang") or "ru") == "uz"
    cid = await _resolve_client_company_id(body.get("company_slug"))

    if not phone or not code or not new_password:
        raise HTTPException(400, "Yetishmayotgan maydonlar" if uz else "Заполните все поля")
    if len(new_password) < 6:
        raise HTTPException(400, "Parol kamida 6 ta belgi" if uz else "Пароль минимум 6 символов")

    # Ограничиваем число попыток подбора кода (не только частоту его запроса).
    ok, err = await db.check_sms_rate_limit(phone, "reset_attempt")
    if not ok:
        raise HTTPException(status_code=429, detail=err)
    await db.save_sms_code(phone, "attempt", "reset_attempt", datetime.utcnow() + timedelta(minutes=1))

    if not await db.check_sms_code(phone, code, "reset"):
        raise HTTPException(400, "Kod noto'g'ri yoki eskirgan" if uz else "Неверный или просроченный код")

    user = await db.get_user_by_phone(phone, cid)
    if not user:
        raise HTTPException(400, "Foydalanuvchi topilmadi" if uz else "Пользователь не найден")

    password_hash = pwd_context.hash(new_password[:72])
    await db.update_user_password(user["id"], password_hash)
    asyncio.create_task(db.update_user_last_login(user["id"]))
    token = create_token(user["id"], user["phone"], cid)
    return {
        "ok": True,
        "token": token,
        "user": {
            "id": user["id"],
            "phone": user["phone"],
            "first_name": user["first_name"],
            "address": user["address"],
            "must_change_password": user.get("must_change_password", False),
        }
    }


@app.get("/api/me")
async def me(user = Depends(get_current_user)):
    return {
        "id": user["id"],
        "phone": user["phone"],
        "first_name": user["first_name"],
        "is_verified": user["is_verified"],
        "address": user["address"],
        "tg_id": user.get("tg_id"),
        "must_change_password": user.get("must_change_password", False),
    }


@app.get("/api/promo/status")
async def promo_status(channel: str = "site", user = Depends(get_current_user)):
    """Статус текущей промо-акции для клиента: показывать ли модалку и с звуком ли.
    mode: 'full' (первый показ, со звуком), 'silent' (тихое напоминание), 'none' (не показывать)."""
    try:
        result = await db.check_promo_eligibility(user["id"], user["phone"], channel)
    except Exception as e:
        logging.error(f"promo_status failed: {e}")
        result = None
    return result or {"mode": "none"}


@app.get("/api/promo/public")
async def promo_public(company_slug: str = None):
    """Общая информация об активной акции для НЕзарегистрированных посетителей
    (реклама без персонального трекинга/окна). mode всегда 'public' или 'none'."""
    try:
        cid = await _resolve_client_company_id(company_slug)
        db.set_request_company(cid)
        result = await db.get_active_promotion_public()
    except Exception as e:
        logging.error(f"promo_public failed: {e}")
        result = None
    return result or {"mode": "none"}


_TASHKENT_TZ = timezone(timedelta(hours=5))

def _parse_promo_dt(s: str | None):
    """ISO-строка → datetime UTC. Без таймзоны в строке — считаем время Ташкентским (UTC+5)."""
    if not s:
        return None
    s2 = str(s).strip().replace("T", " ")
    if len(s2) == 16:
        s2 += ":00"
    dt = datetime.fromisoformat(s2)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_TASHKENT_TZ)
    return dt.astimezone(timezone.utc)


class PromotionCreateRequest(BaseModel):
    code:            str
    title_ru:        str
    title_uz:        str
    text_ru:         str
    text_uz:         str
    discount_pct:    float
    starts_at:       str | None = None
    ends_at:         str
    window_hours:    int = 48
    sound_enabled:   bool = True
    target_new_only: bool = False
    is_active:       bool = True


class PromotionUpdateRequest(BaseModel):
    code:            str | None = None
    title_ru:        str | None = None
    title_uz:        str | None = None
    text_ru:         str | None = None
    text_uz:         str | None = None
    discount_pct:    float | None = None
    starts_at:       str | None = None
    ends_at:         str | None = None
    window_hours:    int | None = None
    sound_enabled:   bool | None = None
    target_new_only: bool | None = None
    is_active:       bool | None = None


@app.get("/api/admin/promotions")
async def admin_list_promotions(_=Depends(_get_admin)):
    """Список всех промо-кампаний (конструктор акций) для админки."""
    rows = await db.list_promotions()
    return {"ok": True, "promotions": rows}


@app.post("/api/admin/promotions")
async def admin_create_promotion(body: PromotionCreateRequest, _=Depends(_get_admin)):
    """Создаёт новую промо-кампанию. При is_active=true остальные кампании деактивируются
    (правило "не более одной активной одновременно")."""
    if not (0 < body.discount_pct <= 100):
        raise HTTPException(status_code=400, detail="Скидка должна быть в диапазоне от 0 до 100%")
    starts_at = _parse_promo_dt(body.starts_at)
    ends_at = _parse_promo_dt(body.ends_at)
    if starts_at and ends_at and ends_at <= starts_at:
        raise HTTPException(status_code=400, detail="Дата окончания должна быть позже даты начала")
    try:
        row = await db.create_promotion(
            code=body.code.strip(), title_ru=body.title_ru, title_uz=body.title_uz,
            text_ru=body.text_ru, text_uz=body.text_uz, discount_pct=body.discount_pct,
            ends_at=ends_at, starts_at=starts_at, window_hours=body.window_hours,
            sound_enabled=body.sound_enabled, target_new_only=body.target_new_only,
            is_active=body.is_active,
        )
    except Exception as e:
        if "unique" in str(e).lower() or "duplicate" in str(e).lower():
            raise HTTPException(status_code=400, detail="Акция с таким кодом уже существует")
        logging.error(f"create_promotion error: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка БД: {e}")
    return {"ok": True, "promotion": row}


@app.patch("/api/admin/promotions/{promo_id}")
async def admin_update_promotion(promo_id: int, body: PromotionUpdateRequest, _=Depends(_get_admin)):
    """Частичное обновление промо-кампании. При is_active=true остальные кампании
    деактивируются (правило "не более одной активной одновременно")."""
    data = body.dict(exclude_unset=True)
    if "discount_pct" in data and data["discount_pct"] is not None:
        if not (0 < data["discount_pct"] <= 100):
            raise HTTPException(status_code=400, detail="Скидка должна быть в диапазоне от 0 до 100%")
    if "starts_at" in data:
        data["starts_at"] = _parse_promo_dt(data["starts_at"])
    if "ends_at" in data:
        data["ends_at"] = _parse_promo_dt(data["ends_at"])
    if data.get("starts_at") and data.get("ends_at") and data["ends_at"] <= data["starts_at"]:
        raise HTTPException(status_code=400, detail="Дата окончания должна быть позже даты начала")
    if "code" in data and data["code"] is not None:
        data["code"] = data["code"].strip()
    if not data:
        raise HTTPException(status_code=400, detail="Нет данных для обновления")
    try:
        row = await db.update_promotion(promo_id, **data)
    except Exception as e:
        if "unique" in str(e).lower() or "duplicate" in str(e).lower():
            raise HTTPException(status_code=400, detail="Акция с таким кодом уже существует")
        logging.error(f"update_promotion error: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка БД: {e}")
    if not row:
        raise HTTPException(status_code=404, detail="Акция не найдена")
    return {"ok": True, "promotion": row}


class UpdateProfileRequest(BaseModel):
    first_name: str
    address: str | None = None

    @field_validator("first_name")
    @classmethod
    def validate_name(cls, v):
        if not v.strip():
            raise ValueError("Имя не может быть пустым")
        return v.strip()


class UpdatePasswordRequest(BaseModel):
    old_password: str | None = None
    new_password: str

    @field_validator("new_password")
    @classmethod
    def validate_new_password(cls, v):
        if len(v) < 6:
            raise ValueError("Пароль должен быть не короче 6 символов")
        return v


@app.patch("/api/me")
async def update_profile(req: UpdateProfileRequest, user = Depends(get_current_user)):
    await db.update_user_profile(user["id"], req.first_name, req.address)
    return {"ok": True, "first_name": req.first_name}


@app.patch("/api/me/password")
async def update_password(req: UpdatePasswordRequest, user = Depends(get_current_user)):
    # must_change_password (пароль выдан автоматически ботом) — пропускаем проверку
    # старого пароля, клиент его никогда не вводил сам. В остальных случаях (обычная
    # смена пароля в настройках) старый пароль обязателен как раньше.
    if not user.get("must_change_password"):
        if not req.old_password or not pwd_context.verify(req.old_password[:72], user["password_hash"]):
            raise HTTPException(status_code=400, detail="Неверный текущий пароль")
    new_hash = pwd_context.hash(req.new_password[:72])
    await db.update_user_password(user["id"], new_hash)
    return {"ok": True}


class LinkTgRequest(BaseModel):
    user_id: int
    tg_id: int
    tg_username: str | None = None

@app.post("/api/user/link-tg")
async def link_tg(req: LinkTgRequest):
    """Бот вызывает этот endpoint чтобы привязать tg_id к аккаунту сайта."""
    user = await db.get_user_by_id(req.user_id)
    if not user:
        raise HTTPException(404, "Пользователь не найден")
    await db.link_user_tg_id(user["phone"], req.tg_id, user["company_id"])
    return {"ok": True, "phone": user["phone"], "name": user.get("first_name") or ""}

@app.get("/api/orders")
async def my_orders(user = Depends(get_current_user)):
    orders = await db.get_orders_by_phone(user["phone"], user.get("company_id") or 1)
    return {"orders": [dict(o) for o in orders]}


@app.post("/api/orders/{order_num}/cancel")
async def cancel_order(order_num: str, user = Depends(get_current_user)):
    order = await db.cancel_order_by_phone(order_num, user["phone"], user.get("company_id") or 1)
    if not order:
        raise HTTPException(status_code=400, detail="Заказ не найден или уже нельзя отменить")
    asyncio.create_task(notify_group_client_cancel(order))
    return {"ok": True}


@app.get("/api/orders/{order_num}/items")
async def my_order_items(order_num: str, user = Depends(get_current_user)):
    cid = user.get("company_id") or 1
    order = await db.get_order_by_num_and_phone(order_num, user["phone"], cid)
    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")
    items = await db.get_order_items(order["id"])
    safe = [{
        "id": it["id"], "service": it["service"],
        "service_ru": it.get("service_ru"), "service_uz": it.get("service_uz"),
        "width_cm": it.get("width_cm"), "length_cm": it.get("length_cm"),
        "sqm": it.get("sqm"), "price_per_sqm": it.get("price_per_sqm"),
        "total_sum": it.get("total_sum"), "measure_status": it.get("measure_status"),
    } for it in items]
    return {"ok": True, "items": safe}


@app.get("/api/orders/{order_num}/photos")
async def my_order_photos(order_num: str, user = Depends(get_current_user)):
    cid = user.get("company_id") or 1
    order = await db.get_order_by_num_and_phone(order_num, user["phone"], cid)
    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")
    photos = await db.get_order_photos(order["id"])
    safe = [{
        "id": p["id"], "photo_type": p.get("photo_type"), "tg_file_type": p.get("tg_file_type"),
        "note": p.get("note"), "created_at": p.get("created_at"),
    } for p in photos]
    return {"ok": True, "photos": safe}


@app.get("/api/orders/{order_num}/items/{item_id}/media")
async def my_order_item_media(order_num: str, item_id: int, user = Depends(get_current_user)):
    cid = user.get("company_id") or 1
    order = await db.get_order_by_num_and_phone(order_num, user["phone"], cid)
    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")
    items = await db.get_order_items(order["id"])
    if not any(it["id"] == item_id for it in items):
        raise HTTPException(status_code=404, detail="Позиция не найдена")
    media = await db.get_item_media(item_id)
    safe = [{
        "id": m["id"], "tg_file_type": m.get("tg_file_type"),
        "created_at": m.get("created_at"), "created_by": m.get("created_by"),
    } for m in media]
    return {"ok": True, "media": safe}


async def notify_group_client_cancel(order: dict):
    if not BOT_TOKEN or not GROUP_ID:
        return
    text = (
        f"🚫 Заявка {order['order_num']} отменена клиентом\n"
        f"━━━━━━━━━━\n"
        f"👤 {order.get('client_name') or '—'}\n"
        f"📞 {order.get('client_phone') or '—'}\n"
        f"🧺 {order.get('service') or '—'}\n"
        f"🏢 {branch_ru(order.get('branch') or '')}\n"
        f"━━━━━━━━━━"
    )
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(url, json={"chat_id": GROUP_ID, "text": text})
    except Exception as e:
        logging.warning(f"Cancel notify error: {e}")


# ══════════════════════════════════════
#  УВЕДОМЛЕНИЕ TELEGRAM-ГРУППЫ О НОВОЙ ЗАЯВКЕ С САЙТА
# ══════════════════════════════════════
def md_escape(text):
    if not text:
        return ""
    text = str(text)
    for ch in ['_', '*', '[', ']', '`']:
        text = text.replace(ch, f"\\{ch}")
    return text


BRANCH_RU = {
    "zarafshan": "Зарафшан", "зарафшан": "Зарафшан", "zarafshon": "Зарафшан",
    "navoi":     "Навои",    "навои":    "Навои",    "navoiy":    "Навои",
}

def branch_ru(branch: str) -> str:
    if not branch: return "—"
    key = branch.lower().replace("📍", "").strip()
    return BRANCH_RU.get(key, branch.strip("📍 ").strip())

async def _group_id_for_branch(branch: str) -> str:
    """Возвращает chat_id группы заказов для указанного филиала: сначала из карточки
    филиала (tg_orders_channel_id), затем legacy ARTEZ-ключи, затем общий fallback."""
    branch_group_id = await db.get_branch_tg_group_id(branch, "tg_orders_channel_id") if branch else None
    if branch_group_id:
        return str(branch_group_id)
    if branch in ("zarafshan", "Зарафшан"):
        gid = await _get_cfg("tg_group_zarafshan")
        return gid or GROUP_ID
    if branch in ("navoi", "Навои"):
        gid = await _get_cfg("tg_group_navoi")
        return gid or GROUP_ID
    return GROUP_ID

async def notify_group_new_order(order_num: str, data: "OrderRequest"):
    if not BOT_TOKEN:
        logging.warning("BOT_TOKEN not set — skipping group notification")
        return
    chat_id = await _group_id_for_branch(getattr(data, "branch", "") or "")
    if not chat_id:
        logging.warning("No GROUP_ID configured — skipping group notification")
        return

    full_name = f"{data.first_name} {data.last_name}".strip()

    # Строим ссылку на Яндекс Карты, если есть координаты
    location_url = None
    loc_display = "—"
    if data.location:
        try:
            lat_s, lon_s = data.location.split(",", 1)
            location_url = f"https://yandex.uz/maps/?pt={lon_s.strip()},{lat_s.strip()}&z=16"
        except Exception:
            pass
        loc_display = data.location_address if data.location_address else data.location

    def he(s):
        return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;") if s else "—"

    loc_line = f'🗺 <a href="{location_url}">{he(loc_display)}</a>' if location_url else f"🗺 {he(loc_display)}"

    if data.is_quick:
        text = (
            f"⚡ Быстрая заявка {order_num} (сайт)\n"
            f"━━━━━━━━━━\n"
            f"👤 {he(full_name)}\n"
            f"📞 {he(data.phone)}\n"
            f"━━━━━━━━━━"
        )
    else:
        text = (
            f"🌐 Новая заявка {order_num} (сайт)\n"
            f"━━━━━━━━━━\n"
            f"👤 {he(full_name)}\n"
            f"📞 {he(data.phone)}\n"
            f"🏢 {he(branch_ru(data.branch))}\n"
            f"📍 {he(data.city)}\n"
            f"🏠 {he(data.address)}\n"
            f"{loc_line}\n"
            f"🧺 {he(data.service)}\n"
            f"⚙️ {he(data.service_type)}\n"
            f"📅 {he(data.pickup_date)}\n"
            f"🕐 {he(data.pickup_time)}\n"
            f"━━━━━━━━━━"
        )

    kb_rows = []
    kb_rows.extend([
        [{"text": "✅ Принять заказ", "callback_data": f"accept_{order_num}_0"}],
        [
            {"text": "🚗 Назначить водителя", "callback_data": f"driver_{order_num}_0"},
            {"text": "❌ Отклонить", "callback_data": f"reject_{order_num}_0"},
        ],
    ])
    keyboard = {"inline_keyboard": kb_rows}

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "reply_markup": keyboard, "parse_mode": "HTML", "disable_web_page_preview": True}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logging.warning(f"Telegram notify failed: {resp.status} {body}")
    except Exception as e:
        logging.warning(f"Telegram notify error: {e}")


# ══════════════════════════════════════
#  GOOGLE-ТАБЛИЦА — ТА ЖЕ, КУДА ПИШЕТ БОТ
# ══════════════════════════════════════
async def send_to_sheets(data: dict):
    url = await _get_cfg("sheets_url")
    if not url:
        logging.warning("sheets_url not set — skipping sheets export")
        return
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(url, json=data, timeout=aiohttp.ClientTimeout(total=10))
    except Exception as e:
        logging.warning(f"Sheets error: {e}")


async def notify_sheets_new_order(order_num: str, data: "OrderRequest"):
    """Формирует строку для Google-таблицы в том же формате, что использует бот"""
    full_name = f"{data.first_name} {data.last_name}".strip()
    await send_to_sheets({
        "name":         full_name,
        "tg_id":        "",
        "tg_username":  "",
        "tg_name":      "",
        "phone":        data.phone,
        "branch":       data.branch,
        "city":         data.city,
        "address":      data.address,
        "location":     data.location or "",
        "service":      data.service,
        "service_type": data.service_type,
        "date":         data.pickup_date,
        "time":         data.pickup_time,
        "note":         f"Сайт ARTEZ {order_num}",
        "status":       "Новый",
    })


# ══════════════════════════════════════
#  ADMIN
# ══════════════════════════════════════
class AdminLoginRequest(BaseModel):
    login: str
    password: str
    company_id: int = 1
    company_slug: str | None = None

class SetPriceRequest(BaseModel):
    service_key: str
    type_key: str
    price: int
    unit_key: str = None
    min_order: float = None
    min_order_total: float = None  # мин. по заказу — порог по услуге суммарно по всем позициям

class ServiceRequest(BaseModel):
    key: str
    name_ru: str
    name_uz: str
    emoji: str = ''
    order_idx: int = 0

class UnitRequest(BaseModel):
    key: str
    name_ru: str
    name_uz: str
    symbol_ru: str
    symbol_uz: str

ADMIN_TOKEN_PREFIX = "admin:"

def create_admin_token(company_id: int = 1) -> str:
    payload = {
        "sub": "admin",
        "company_id": company_id,
        "exp": datetime.now(timezone.utc) + timedelta(days=30),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

async def get_admin(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Не авторизован")
    token = authorization.removeprefix("Bearer ").strip()
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        if payload.get("sub") != "admin" and payload.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Нет доступа")
    except JWTError:
        raise HTTPException(status_code=401, detail="Недействительный токен")
    return True

@app.post("/api/admin/login")
async def admin_login(req: AdminLoginRequest):
    company_id = req.company_id
    if req.company_slug:
        company_id = await db.get_company_id_by_slug(req.company_slug)
        if not company_id:
            raise HTTPException(status_code=401, detail="Компания не найдена")
    company = await db.get_company(company_id)
    if not company or not company["contact_tg_id"]:
        raise HTTPException(status_code=403, detail="Компания не привязана к Telegram-боту Cleano. "
                             "Откройте бота по ссылке от нашей поддержки и поделитесь контактом.")
    staff = await db.get_staff_by_login(req.login, company_id)
    _ADMIN_ROLES = ("admin", "manager", "callcenter")
    if not staff or staff["role"] not in _ADMIN_ROLES:
        raise HTTPException(status_code=401, detail="Неверный логин или пароль")
    if not pwd_context.verify(req.password[:72], staff["password_hash"]):
        raise HTTPException(status_code=401, detail="Неверный логин или пароль")
    if staff["role"] == "admin":
        token = create_admin_token(company_id)
    else:
        token = create_staff_token(staff["id"], staff["login"], staff["role"], company_id)
    return {"ok": True, "token": token}

@app.post("/api/admin/change-master-password")
async def change_master_password(body: dict, _=Depends(_get_admin)):
    current = body.get("current_password", "")
    new_pass = body.get("new_password", "")
    if not current or current != await get_admin_pass():
        raise HTTPException(status_code=403, detail="Неверный текущий пароль")
    if not new_pass or len(new_pass) < 4:
        raise HTTPException(status_code=400, detail="Новый пароль минимум 4 символа")
    await db.set_config("admin_pass", new_pass)
    return {"ok": True}

# ══════════════════════════════════════════════════════════════════════════════
# ADMIN LEADS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/admin/leads")
async def admin_get_leads(status: str = None, branch: str = None,
                          search: str = "", _=Depends(_get_admin)):
    rows = await db.get_leads(status=status, branch=branch, limit=500)
    leads = [dict(r) for r in rows]
    if search:
        q = search.lower()
        leads = [l for l in leads if
                 q in (l.get("client_name") or "").lower() or
                 q in (l.get("client_phone") or "").lower() or
                 q in (l.get("address") or "").lower() or
                 q in (l.get("short_address") or "").lower()]
    return {"ok": True, "leads": leads}

class LeadUpdateRequest(BaseModel):
    client_name:  str | None = None
    client_phone: str | None = None
    service:      str | None = None
    branch:       str | None = None
    city:         str | None = None
    address:      str | None = None
    short_address: str | None = None
    note:         str | None = None
    status:       str | None = None

@app.put("/api/admin/leads/{lead_id}")
async def admin_update_lead(lead_id: int, req: LeadUpdateRequest, _=Depends(_get_admin)):
    updates = {k: v for k, v in req.dict().items() if v is not None}
    row = await db.update_lead(lead_id, **updates)
    if not row:
        raise HTTPException(status_code=404, detail="Лид не найден")
    return {"ok": True, "lead": row}

@app.patch("/api/admin/leads/{lead_id}/status")
async def admin_update_lead_status(lead_id: int, body: dict, _=Depends(_get_admin)):
    status = body.get("status")
    if status not in ("new","contacted","callback","converted","lost"):
        raise HTTPException(status_code=400, detail="Неверный статус")
    await db.update_lead_status(lead_id, status)
    return {"ok": True}

class LeadDeleteRequest(BaseModel):
    password: str

@app.post("/api/admin/leads/{lead_id}/delete")
async def admin_delete_lead(lead_id: int, req: LeadDeleteRequest):
    if not (apass := await get_admin_pass()) or req.password != apass:
        raise HTTPException(status_code=403, detail="Неверный пароль")
    ok = await db.delete_lead(lead_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Лид не найден")
    return {"ok": True}

@app.post("/api/admin/leads")
async def admin_create_lead(req: LeadCreateRequest, _=Depends(_get_admin)):
    lead = await db.create_lead({
        "client_name": req.client_name,
        "client_phone": req.client_phone, "service": req.service,
        "branch": req.branch, "city": req.city, "address": req.address,
        "short_address": req.short_address, "note": req.note,
        "assigned_to": req.assigned_to, "created_by": None,
    })
    if lead:
        admin_staff = {"role": "admin", "first_name": "Админ", "last_name": "", "login": "admin"}
        asyncio.create_task(_notify_new_lead(lead, admin_staff))
    return {"ok": True, "lead": lead}

# ══════════════════════════════════════════════════════════════════════════════
# АГЕНТЫ — регистрация и сброс пароля
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/agent/status")
async def agent_status(user=Depends(get_current_user)):
    """Возвращает статус агента для текущего пользователя сайта."""
    # 1. По site_user_id
    staff = await db.get_staff_by_site_user(user["id"])
    if staff:
        return {"ok": True, "is_agent": True, "must_change_password": bool(staff.get("must_change_password"))}
    # 2. По tg_id
    if user.get("tg_id"):
        staff = await db.get_staff_by_tg_id(user["tg_id"])
        if staff and staff["role"] == "agent":
            return {"ok": True, "is_agent": True, "must_change_password": bool(staff.get("must_change_password"))}
    # 3. По номеру телефона (логину)
    staff = await db.get_staff_by_login(user["phone"], user.get("company_id") or 1)
    if staff and staff["role"] == "agent":
        # Заодно прописываем site_user_id чтобы следующий раз найти быстрее
        await db.link_staff_to_site_user(staff["id"], user["id"])
        return {"ok": True, "is_agent": True, "must_change_password": bool(staff.get("must_change_password"))}
    return {"ok": True, "is_agent": False}

@app.post("/api/agent/apply")
async def agent_apply(req: AgentApplyRequest, user=Depends(get_current_user)):
    """Пользователь сайта регистрируется как агент.
    Ищет клиента бота по clients.tg_phone = users.phone.
    Если не найден — возвращает needs_bot=True (нужно написать боту).
    """
    if not user.get("is_verified"):
        raise HTTPException(400, "Сначала подтвердите номер телефона")

    # Уже агент?
    existing = await db.get_staff_by_site_user(user["id"])
    if existing:
        return {"ok": True, "already": True, "message": "Вы уже зарегистрированы как агент"}
    existing2 = await db.get_staff_by_login(user["phone"], user.get("company_id") or 1)
    if existing2 and existing2["role"] == "agent":
        await db.link_staff_to_site_user(existing2["id"], user["id"])
        return {"ok": True, "already": True, "message": "Вы уже зарегистрированы как агент"}

    # Ищем клиента бота по tg_phone = phone сайта
    client = await db.get_client_by_tg_phone(user["phone"], user.get("company_id") or 1)
    if not client:
        # Telegram-контакт не верифицирован — нужно зайти в бот и поделиться номером
        return {"ok": False, "needs_bot": True}

    # Привязываем tg_id к аккаунту сайта (если ещё не привязан)
    tg_id = client.get("tg_id")
    if tg_id and not user.get("tg_id"):
        await db.link_user_tg_id(user["phone"], int(tg_id), user.get("company_id") or 1)

    site_user = await db.get_user_by_id(user["id"])
    password_hash = site_user["password_hash"] if site_user else None
    if not password_hash:
        raise HTTPException(400, "Пароль не установлен.")

    # Передаём актуальный tg_id в create_agent_from_user
    user_data = dict(user)
    if tg_id:
        user_data["tg_id"] = int(tg_id)

    staff_id = await db.create_agent_from_user(user_data, password_hash, req.branch)
    if not staff_id:
        return {"ok": True, "already": True, "message": "Аккаунт агента уже существует"}

    return {"ok": True, "already": False, "message": "Вы зарегистрированы как агент! Войдите через artez.uz/staff.html"}

class ApplyByTgRequest(BaseModel):
    tg_id: int
    phone: str | None = None  # телефон из базы бота как запасной вариант
    company_slug: str | None = None  # SaaS-бот передаёт свою компанию явно

async def _find_site_user_for_bot(tg_id: int, phone: str | None, company_id: int = 1):
    """Ищет пользователя сайта: сначала по tg_id, потом по телефону из бота.

    company_id обязательно скопирован в КАЖДЫЙ lookup ниже — иначе поиск мог
    зацепить аккаунт ДРУГОЙ компании с тем же tg_id/телефоном (см. фикс
    get_user_by_tg_id/get_user_by_phone/link_user_tg_id, 2026-07-29). Дефолт
    =1 сохранён для обратной совместимости со старым прод-ботом ARTEZ
    (`ARTEZ-BOT`), который вызывает /api/agent/* без company_id вообще —
    трогать тот бот нельзя, поведение для него не меняется.
    """
    try:
        user = await db.get_user_by_tg_id(tg_id, company_id)
        if user:
            return user
    except Exception:
        pass
    if phone:
        try:
            norm = normalize_phone(phone)
            user = await db.get_user_by_phone(norm, company_id)
            if not user and norm.startswith("+"):
                user = await db.get_user_by_phone(norm[1:], company_id)
        except Exception:
            user = None
        if user:
            try:
                await db.link_user_tg_id(user["phone"], tg_id, company_id)
            except Exception:
                pass
            return user
    return None

async def _agent_status_for_bot(tg_id: int, phone: str | None, company_id: int) -> dict:
    """Общая логика для /api/agent/status-by-tg — вынесена отдельно, чтобы
    order_bot_handlers.py (SaaS-бот, тот же процесс) могла вызывать её напрямую
    с уже известным company_id, без HTTP-петли на саму себя и без дублирования
    company-scoping логики."""
    staff = await db.get_staff_by_tg_id_and_company(tg_id, company_id)
    if staff and staff["role"] == "agent":
        return {"ok": True, "is_agent": True, "has_site_account": True}
    site_user = await _find_site_user_for_bot(tg_id, phone, company_id)
    return {"ok": True, "is_agent": False, "has_site_account": bool(site_user)}

@app.get("/api/agent/status-by-tg/{tg_id}")
async def agent_status_by_tg_endpoint(tg_id: int, phone: str | None = None, company_slug: str | None = None):
    """Для бота: проверить статус агента по tg_id без авторизации.

    company_slug опционален — старый прод-бот ARTEZ его не передаёт (дефолт
    company_id=1, как и раньше); SaaS-бот заказов передаёт свой slug явно.
    """
    cid = await _resolve_client_company_id(company_slug) if company_slug else 1
    return await _agent_status_for_bot(tg_id, phone, cid)

async def _agent_apply_for_bot(tg_id: int, phone: str | None, company_id: int) -> dict:
    """Общая логика для /api/agent/apply-by-tg — см. _agent_status_for_bot."""
    site_user = await _find_site_user_for_bot(tg_id, phone, company_id)
    if not site_user:
        return {"ok": False, "reason": "no_site_account"}
    if not site_user.get("is_verified"):
        return {"ok": False, "reason": "not_verified"}
    existing = await db.get_staff_by_login(site_user["phone"], site_user.get("company_id") or 1)
    if existing and existing["role"] == "agent":
        return {"ok": True, "already": True, "phone": site_user["phone"]}
    password_hash = site_user.get("password_hash")
    if not password_hash:
        return {"ok": False, "reason": "no_password"}
    staff_id = await db.create_agent_from_user(dict(site_user), password_hash)
    if not staff_id:
        return {"ok": True, "already": True, "phone": site_user["phone"]}
    return {"ok": True, "already": False, "phone": site_user["phone"], "name": site_user.get("first_name") or ""}

@app.post("/api/agent/apply-by-tg")
async def agent_apply_by_tg(req: ApplyByTgRequest):
    """Бот регистрирует агента по tg_id — ищет аккаунт сайта по tg_id или телефону."""
    cid = await _resolve_client_company_id(req.company_slug) if req.company_slug else 1
    return await _agent_apply_for_bot(req.tg_id, req.phone, cid)

@app.post("/api/agent/reset-password")
async def agent_reset_password(body: dict):
    """Сброс пароля агента — отправляет временный пароль через Telegram."""
    phone = normalize_phone(body.get("phone", ""))
    staff = await db.get_staff_by_login(phone)
    if not staff or staff["role"] != "agent":
        # Не раскрываем что аккаунта нет
        return {"ok": True, "message": "Если аккаунт агента найден — пароль отправлен в Telegram"}

    if not staff.get("tg_id"):
        raise HTTPException(400, "Telegram не привязан. Обратитесь к администратору.")

    import random, string
    from datetime import datetime, timezone, timedelta
    temp_pw = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    expires = datetime.now(timezone.utc) + timedelta(minutes=10)
    hashed  = pwd_context.hash(temp_pw)
    await db.set_staff_temp_password(staff["id"], hashed, expires)

    text = (f"🔑 Временный пароль для входа в систему ARTEZ:\n\n"
            f"<b>{temp_pw}</b>\n\n"
            f"⏰ Действует 10 минут.\n"
            f"После входа сразу смените пароль.")
    tg_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    async with aiohttp.ClientSession() as s:
        await s.post(tg_url, json={"chat_id": staff["tg_id"], "text": text, "parse_mode": "HTML"},
                     timeout=aiohttp.ClientTimeout(total=8))

    return {"ok": True, "message": "Временный пароль отправлен в Telegram"}

@app.post("/api/agent/change-password")
async def agent_change_password(body: dict, staff=Depends(get_current_staff)):
    """Смена пароля после входа по временному."""
    if staff.get("role") != "agent":
        raise HTTPException(403, "Только для агентов")
    new_pw = (body.get("password") or "").strip()
    if len(new_pw) < 6:
        raise HTTPException(400, "Пароль минимум 6 символов")
    hashed = pwd_context.hash(new_pw[:72])
    await db.update_staff_password(staff["id"], hashed, plain=new_pw)
    await db.clear_staff_temp_password(staff["id"])
    return {"ok": True}


class ResetByTgRequest(BaseModel):
    tg_id: int

@app.post("/api/agent/reset-password-by-tg")
async def agent_reset_password_by_tg(req: ResetByTgRequest):
    """Для бота: сброс пароля агента по tg_id."""
    staff = await db.get_staff_by_tg_id(str(req.tg_id))
    if not staff or staff["role"] != "agent":
        return {"ok": True}
    import random, string
    from datetime import datetime, timezone, timedelta
    temp_pw = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    expires = datetime.now(timezone.utc) + timedelta(minutes=10)
    hashed  = pwd_context.hash(temp_pw)
    await db.set_staff_temp_password(staff["id"], hashed, expires)
    text = (f"🔑 Временный пароль для входа в систему ARTEZ:\n\n"
            f"<b>{temp_pw}</b>\n\n"
            f"⏰ Действует 10 минут.\n"
            f"Войдите на artez.uz/staff.html и сразу смените пароль.")
    tg_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    async with aiohttp.ClientSession() as s:
        await s.post(tg_url, json={"chat_id": req.tg_id, "text": text, "parse_mode": "HTML"},
                     timeout=aiohttp.ClientTimeout(total=8))
    return {"ok": True}

# ══════════════════════════════════════════════════════════════════════════════
# ADMIN: ПОЛЬЗОВАТЕЛИ САЙТА
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/admin/site-users")
async def admin_get_site_users(search: str = "", _=Depends(_get_admin)):
    rows = await db.get_all_site_users(search=search.strip())
    def _row(r):
        d = dict(r)
        for k in ("created_at", "updated_at", "last_login"):
            if d.get(k) and hasattr(d[k], "isoformat"):
                d[k] = d[k].isoformat()
        return d
    return {"ok": True, "users": [_row(r) for r in rows]}

@app.patch("/api/admin/site-users/{user_id}")
async def admin_update_site_user(user_id: int, body: dict, cid: int = Depends(_get_admin_cid)):
    if not await db.get_site_user_in_company(user_id, cid):
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    first_name   = (body.get("first_name")   or "").strip() or None
    address      = (body.get("address")      or "").strip() or None
    await db.update_user_profile(user_id, first_name, address)
    return {"ok": True}

@app.post("/api/admin/site-users/{user_id}/reset-password")
async def admin_reset_site_user_password(user_id: int, body: dict, cid: int = Depends(_get_admin_cid)):
    if not await db.get_site_user_in_company(user_id, cid):
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    new_password = (body.get("new_password") or "").strip()
    if len(new_password) < 4:
        raise HTTPException(status_code=400, detail="Пароль минимум 4 символа")
    send_tg = bool(body.get("send_tg", False))
    hashed = pwd_context.hash(new_password[:72])
    await db.update_user_password(user_id, hashed)
    if send_tg and BOT_TOKEN:
        user = await db.get_user_by_id(user_id)
        tg_id = user.get("tg_id") if user else None
        if tg_id:
            text = (
                f"🔑 <b>ARTEZ</b> — ваш пароль изменён администратором.\n\n"
                f"📱 Логин: <code>{user['phone']}</code>\n"
                f"🔑 Новый пароль: <code>{new_password}</code>\n\n"
                f"Не передавайте пароль третьим лицам."
            )
            try:
                async with aiohttp.ClientSession() as _s:
                    await _s.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                        json={"chat_id": str(tg_id), "text": text, "parse_mode": "HTML"},
                        timeout=aiohttp.ClientTimeout(total=5)
                    )
            except Exception:
                pass
    return {"ok": True, "tg_sent": send_tg and bool(body.get("send_tg"))}


@app.post("/api/register-via-tg")
async def register_via_tg(body: dict):
    phone      = (body.get("phone") or "").strip()
    first_name = (body.get("first_name") or "").strip()
    password   = (body.get("password") or "").strip()
    uz = (body.get("lang") or "ru") == "uz"
    cid = await _resolve_client_company_id(body.get("company_slug"))

    if not phone or not first_name or not password:
        raise HTTPException(400, "Yetishmayotgan maydonlar" if uz else "Заполните все поля")
    if len(password) < 6:
        raise HTTPException(400, "Parol kamida 6 ta belgi" if uz else "Пароль минимум 6 символов")

    tg_id = await db.get_tg_id_by_phone(phone)
    if not tg_id:
        raise HTTPException(400,
            "Bu raqam botda topilmadi. Avval bot bilan telefon raqamingizni ulashing."
            if uz else
            "Телефон не найден в боте. Сначала поделитесь номером через бота.")

    existing = await db.get_user_by_phone(phone, cid)
    if existing and existing["is_verified"]:
        raise HTTPException(400,
            "Bu raqam allaqachon ro'yxatdan o'tgan" if uz
            else "Этот номер уже зарегистрирован")

    password_hash = pwd_context.hash(password[:72])
    await db.create_user(phone, password_hash, first_name, cid)
    await db.verify_user(phone, cid)
    await db.set_user_tg_id(phone, tg_id)

    user = await db.get_user_by_phone(phone, cid)
    asyncio.create_task(db.update_user_last_login(user["id"]))
    token = create_token(user["id"], user["phone"], cid)

    # Отправляем данные аккаунта в Telegram
    if BOT_TOKEN:
        text = (
            f"🎉 <b>ARTEZ</b> — регистрация завершена!\n\n"
            f"👤 Имя: <b>{first_name}</b>\n"
            f"📱 Номер / Логин: <code>{phone}</code>\n"
            f"🔑 Пароль: <code>{password}</code>\n\n"
            f"Используйте эти данные для входа на сайте artez.uz"
        ) if not uz else (
            f"🎉 <b>ARTEZ</b> — ro'yxatdan o'tdingiz!\n\n"
            f"👤 Ism: <b>{first_name}</b>\n"
            f"📱 Raqam / Login: <code>{phone}</code>\n"
            f"🔑 Parol: <code>{password}</code>\n\n"
            f"artez.uz saytiga kirish uchun ushbu ma'lumotlardan foydalaning."
        )
        asyncio.create_task(_send_tg_safe(tg_id, text))

    asyncio.create_task(_notify_new_site_user(first_name, phone, "tg"))

    return {
        "ok": True,
        "token": token,
        "user": {
            "id": user["id"],
            "phone": user["phone"],
            "first_name": user["first_name"],
            "address": user.get("address"),
            "must_change_password": user.get("must_change_password", False),
        }
    }


async def _send_tg_safe(tg_id: int, text: str):
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": str(tg_id), "text": text, "parse_mode": "HTML"},
                timeout=aiohttp.ClientTimeout(total=5))
    except Exception:
        pass


async def _notify_new_site_user(first_name: str, phone: str, method: str):
    """Уведомляет группу и персональных сотрудников о новой регистрации."""
    if not BOT_TOKEN:
        return
    from datetime import datetime
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    method_icon = "✈️ Telegram" if method == "tg" else "📱 SMS"
    text = (
        f"👤 {first_name}, 📞 <code>{phone}</code>, 🔐 {method_icon}, 🌐\n"
        f"📅 {now}"
    )
    # Только группа — раньше слало ещё персонально каждому сотруднику с тумблером
    # "Уведомления о новых клиентах", включая владельца бота (утечка в личку).
    targets = []
    group_id = await _get_cfg("new_clients_group_id")
    if group_id:
        targets.append(group_id)
    async with aiohttp.ClientSession() as s:
        for chat_id in targets:
            try:
                await s.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json={"chat_id": str(chat_id), "text": text, "parse_mode": "HTML"},
                    timeout=aiohttp.ClientTimeout(total=5))
            except Exception:
                pass


@app.delete("/api/admin/site-users/{user_id}")
async def admin_delete_site_user(user_id: int, body: dict, _=Depends(_get_admin)):
    if not (apass := await get_admin_pass()) or body.get("admin_password") != apass:
        raise HTTPException(status_code=403, detail="Неверный пароль администратора")
    ok = await db.delete_site_user(user_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    return {"ok": True}

# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/admin/prices")
async def admin_get_prices(_=Depends(get_admin)):
    prices = await db.get_all_prices()
    return {"ok": True, "prices": prices}

@app.put("/api/admin/prices")
async def admin_set_price(req: SetPriceRequest, _=Depends(get_admin)):
    if not req.service_key.strip():
        raise HTTPException(status_code=400, detail="Укажите ключ услуги")
    if not req.type_key.strip():
        raise HTTPException(status_code=400, detail="Укажите тип")
    if req.price <= 0:
        raise HTTPException(status_code=400, detail="Цена должна быть > 0")
    if req.min_order is not None and req.min_order <= 0:
        raise HTTPException(status_code=400, detail="Минимальный заказ должен быть > 0")
    if req.min_order_total is not None and req.min_order_total <= 0:
        raise HTTPException(status_code=400, detail="Минимальный заказ по услуге должен быть > 0")
    await db.set_price(req.service_key, req.type_key, req.price, unit_key=req.unit_key,
                        min_order=req.min_order, min_order_total=req.min_order_total)
    return {"ok": True}

@app.get("/api/services")
async def get_services_public(company_slug: str = None):
    """Публичный эндпоинт — список услуг с именами RU/UZ"""
    cid = await _resolve_client_company_id(company_slug)
    db.set_request_company(cid)
    svcs = await db.get_services()
    return {"ok": True, "services": svcs}

@app.get("/api/admin/services")
async def admin_get_services(_=Depends(get_admin)):
    svcs = await db.get_services()
    return {"ok": True, "services": svcs}

@app.put("/api/admin/services")
async def admin_upsert_service(req: ServiceRequest, _=Depends(get_admin)):
    if not req.key.strip():
        raise HTTPException(status_code=400, detail="Укажите ключ услуги")
    if not req.name_ru.strip():
        raise HTTPException(status_code=400, detail="Укажите название на RU")
    if not req.name_uz.strip():
        raise HTTPException(status_code=400, detail="Укажите название на UZ")
    await db.upsert_service(req.key.strip(), req.name_ru.strip(), req.name_uz.strip(),
                            req.emoji.strip(), req.order_idx)
    return {"ok": True}

@app.delete("/api/admin/services/{key}")
async def admin_delete_service(key: str, _=Depends(get_admin)):
    ok = await db.delete_service(key)
    if not ok:
        raise HTTPException(status_code=404, detail="Услуга не найдена")
    return {"ok": True}

@app.get("/api/units")
async def get_units_public():
    """Публичный эндпоинт — список единиц измерения для сайта"""
    units = await db.get_all_units()
    return {"ok": True, "units": [dict(u) for u in units]}

@app.get("/api/admin/units")
async def admin_get_units(_=Depends(get_admin)):
    units = await db.get_all_units()
    return {"ok": True, "units": [dict(u) for u in units]}

@app.put("/api/admin/units")
async def admin_set_unit(req: UnitRequest, _=Depends(get_admin)):
    if not req.key.strip():
        raise HTTPException(status_code=400, detail="Укажите ключ единицы измерения")
    await db.add_unit(req.key.strip(), req.name_ru.strip(), req.name_uz.strip(),
                       req.symbol_ru.strip(), req.symbol_uz.strip())
    return {"ok": True}

@app.delete("/api/admin/units/{key}")
async def admin_delete_unit(key: str, _=Depends(get_admin)):
    ok = await db.delete_unit(key)
    if not ok:
        raise HTTPException(status_code=404, detail="Единица измерения не найдена")
    return {"ok": True}

@app.get("/api/admin/orders")
async def admin_get_orders(_=Depends(get_admin), status: str = None, limit: int = 50, offset: int = 0):
    orders, total = await db.get_admin_orders(status=status, limit=limit, offset=offset)
    return {"ok": True, "orders": orders, "total": total, "limit": limit, "offset": offset}

@app.get("/api/admin/orders/debts")
async def get_debt_orders(_=Depends(_get_admin)):
    rows = await db.get_orders_with_debt()
    return {"ok": True, "debts": rows}

@app.get("/api/admin/orders/{order_id}")
async def admin_get_order(order_id: int, staff=Depends(get_current_staff)):
    order = await db.get_order_by_id(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")
    if staff.get("hide_client_phone"):
        order = dict(order)
        order["client_phone"] = ""
    return {"ok": True, "order": order}

_ORDER_EDITABLE_STATUSES = {"new","confirmed","pickup","received","washing","drying","packing","ready"}

@app.patch("/api/admin/orders/{order_id}")
async def update_order_data(order_id: int, body: dict = Body(...), staff=Depends(get_current_staff)):
    role = staff.get("role", "")
    perms = ROLE_PERMISSIONS.get(role, [])
    if "orders" not in perms and role != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    order = await db.get_order_by_id(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")
    can_edit_delivery = staff.get("can_edit_delivery", False)
    # staff.get("sub") никогда не совпадает с "admin" для staff-логина (это поле JWT,
    # не колонка БД) — сотрудник с role="admin" ошибочно попадал под общее ограничение
    # по статусу и не мог редактировать заказ в "Доставлено" (портировано из прода).
    if (role != "admin"
            and order.get("status") not in _ORDER_EDITABLE_STATUSES
            and not (order.get("status") == "delivery" and can_edit_delivery)):
        raise HTTPException(status_code=400, detail="Нельзя редактировать заказ в этом статусе")
    allowed = {"client_first_name","client_last_name","client_phone",
               "branch","address","short_address","location","location_address","note","deadline","service_type",
               "pickup_type","self_pickup_discount","discount_sum","manual_discount",
               "delivery_type","delivery_discount","delivery_discount_pct"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        raise HTTPException(status_code=400, detail="Нет данных для обновления")
    if "client_phone" in updates and updates["client_phone"]:
        normalized_phone = normalize_phone(str(updates["client_phone"]))
        if not PHONE_RE.match(normalized_phone):
            raise HTTPException(status_code=400, detail="Неверный формат номера. Используйте +998XXXXXXXXX")
        updates["client_phone"] = normalized_phone
    # asyncpg требует объект date, а не строку
    if "deadline" in updates and isinstance(updates["deadline"], str):
        from datetime import date
        try:
            updates["deadline"] = date.fromisoformat(updates["deadline"])
        except ValueError:
            updates["deadline"] = None
    try:
        updated = await db.update_order(order_id, **updates)
        return {"ok": True, "order": {k: str(v) if hasattr(v, 'isoformat') else v
                                      for k, v in updated.items()}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка обновления: {str(e)}")

@app.get("/api/staff/orders/{order_id}/history")
async def get_order_history(order_id: int, _=Depends(get_current_staff)):
    order = await db.get_order_by_id(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")
    rows = await db.get_order_status_history(order.get("order_num", ""))
    return {"ok": True, "history": [
        {k: str(v) if hasattr(v, 'isoformat') else v for k, v in r.items()}
        for r in rows
    ]}

@app.get("/api/staff/check-phone")
async def check_phone(phone: str, _=Depends(get_current_staff)):
    result = await db.check_phone_duplicate(phone)
    return {"ok": True, **result}

@app.post("/api/staff/leads/{lead_id}/convert")
async def convert_lead_to_order(lead_id: int, body: dict = Body({}),
                                 staff=Depends(require_perm("orders"))):
    lead = await db.get_lead_by_id(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Лид не найден")
    name_parts = (lead.get("name") or "").split(maxsplit=1)
    first = name_parts[0] if name_parts else ""
    last  = name_parts[1] if len(name_parts) > 1 else ""
    order_num = await db.get_next_order_num()
    lead_note = lead.get("note") or ""
    note_text = f"Конвертирован из лида #{lead_id}" + (f". {lead_note}" if lead_note else "")
    await db.save_site_order({
        "order_num":     order_num,
        "first_name":    first,
        "last_name":     last,
        "phone":         lead.get("phone", ""),
        "branch":        lead.get("branch") or body.get("branch", ""),
        "city":          "",
        "address":       lead.get("address", ""),
        "short_address": lead.get("short_address", ""),
        "location":      lead.get("location", ""),
        "service":       "",
        "pickup_type":   lead.get("pickup_type", "courier"),
        "delivery_type": lead.get("delivery_type", "courier"),
        "pickup_date":   lead.get("pickup_date", ""),
        "pickup_time":   lead.get("pickup_time", ""),
        "note":          note_text,
        "total_price":   None,
    }, source="staff")
    await db.update_lead_status(lead_id, "converted")
    # Промо-акция: заказ считается "использованием" одноразового окна только если
    # лид пришёл с сайта/бота и привязан к зарегистрированному пользователю.
    if lead.get("source") in ("site", "bot") and lead.get("client_phone"):
        try:
            promo_user = await db.get_user_by_phone(lead["client_phone"], lead["company_id"])
            if promo_user:
                await db.apply_promo_to_order(order_num, promo_user["id"])
        except Exception as e:
            logging.error(f"convert_lead_to_order: promo apply failed: {e}")
    return {"ok": True, "order_num": order_num}

@app.get("/api/admin/staff/packers")
async def get_packers(_=Depends(get_current_staff)):
    """Активные сотрудники с ролью упаковщика. Fallback — все активные не-агенты."""
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, login, first_name, last_name, branch, role
            FROM staff
            WHERE (active IS NULL OR active = TRUE)
            ORDER BY last_name, first_name
        """)
    packers = [dict(r) for r in rows if r['role'] in ('packer', 'operator_packing')]
    if not packers:
        packers = [dict(r) for r in rows if r['role'] not in ('agent',)]
    return {"ok": True, "packers": packers}


@app.patch("/api/admin/orders/{order_id}/packer")
async def set_order_packer(order_id: int,
                            packer_login: str = Body(..., embed=True),
                            staff=Depends(get_current_staff)):
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE orders SET packer_login=$1 WHERE id=$2 RETURNING id", packer_login, order_id)
    if not row:
        raise HTTPException(status_code=404, detail="Заказ не найден")
    return {"ok": True}


@app.patch("/api/admin/orders/{order_id}/status")
async def admin_change_order_status(order_id: int, staff=Depends(get_current_staff),
                                     status: str = Body(..., embed=True),
                                     note: str = Body("", embed=True),
                                     packer_login: str = Body(None, embed=True)):
    role = staff.get("role", "")
    if role == "washer":
        order = await db.get_order_by_id(order_id)
        if not order:
            raise HTTPException(status_code=404, detail="Заказ не найден")
        allowed = WASHER_STATUS_FLOW.get(order.get("status", ""))
        if status != allowed:
            raise HTTPException(status_code=403, detail=f"Мойщик может изменить статус только на: {allowed}")
    elif "status" not in ROLE_PERMISSIONS.get(role, []) and role != "admin":
        # Любой с orders может подтвердить заказ (new → confirmed), если есть can_confirm_order
        perms = ROLE_PERMISSIONS.get(role, [])
        if "orders" in perms and status == "confirmed":
            if not staff.get("can_confirm_order", True):
                raise HTTPException(status_code=403, detail="Нет права подтверждать заказы")
            order = await db.get_order_by_id(order_id)
            if not order or order.get("status") != "new":
                raise HTTPException(status_code=403, detail="Можно подтвердить только новый заказ")
        elif status == "pickup" and staff.get("can_send_pickup") is True:
            order = await db.get_order_by_id(order_id)
            if not order or order.get("status") != "confirmed":
                raise HTTPException(status_code=403, detail="Можно передать на вывоз только подтверждённый заказ")
        elif status == "received" and staff.get("can_send_pickup") is True:
            order = await db.get_order_by_id(order_id)
            if not order or order.get("status") != "pickup":
                raise HTTPException(status_code=403, detail="Можно передать в мастерскую только заказ со статусом «Вывоз»")
        else:
            raise HTTPException(status_code=403, detail="Нет прав для смены статуса")
    if status not in ALL_ORDER_STATUSES:
        raise HTTPException(status_code=400, detail="Неизвестный статус")

    # Замер больше НЕ блокирует переход в «Мойка» — незамеренные позиции домеряются
    # уже мойщиком в статусе «Мойка». Но если за позицией уже кто-то закреплён, это
    # должен быть мойщик (а не менеджер/админ, снявший замер в исключительном
    # порядке) — иначе зарплата за мойку уйдёт не тому.
    if status == "washing":
        items = await db.get_order_items(order_id)
        logins = {i.get("washer_login") for i in items if i.get("washer_login")}
        if logins:
            roles = await db.get_staff_roles_by_logins(list(logins))
            bad = [i for i in items if i.get("washer_login") and roles.get(i["washer_login"]) != "washer"]
            if bad:
                raise HTTPException(status_code=400,
                    detail=f"У {len(bad)} позиций замер провёл не мойщик — назначьте мойщика "
                           f"или откройте позицию, чтобы её мог взять мойщик")

    staff_name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
    order = await db.update_order_status(order_id, status,
                                          note=note or bi(f"Статус изменён сотрудником {staff_name}",
                                                           f"{staff_name} xodim tomonidan holat oʻzgartirildi"))
    if status == 'packing' and packer_login:
        async with db.pool.acquire() as _pc:
            await _pc.execute("UPDATE orders SET packer_login=$1 WHERE id=$2", packer_login, order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")

    # ── Telegram уведомление клиенту ──────────────────────────────────────
    tg_id = order.get("client_tg_id")
    if tg_id and BOT_TOKEN:
        try:
            tmpl = await db.get_tg_status_message(status)
            if tmpl and tmpl.get("enabled"):
                # Определяем язык клиента — пробуем найти в таблице clients
                lang = "ru"
                try:
                    async with db.pool.acquire() as _c:
                        row = await _c.fetchrow(
                            "SELECT language FROM clients WHERE tg_id=$1", int(tg_id))
                        if row and row["language"] in ("uz", "ru"):
                            lang = row["language"]
                except Exception:
                    pass

                raw = tmpl.get(f"message_{lang}") or tmpl.get("message_ru") or ""
                if raw:
                    STATUS_EMOJI = {
                        "new":"🆕","confirmed":"✅","pickup":"🚗","received":"📦",
                        "washing":"🧼","drying":"💨","packing":"📦","ready":"✅",
                        "delivery":"🚚","delivered":"✅","cancelled":"❌",
                    }
                    STATUS_NAME_RU = {
                        "new":"Новый","confirmed":"Подтверждён","pickup":"Вывоз",
                        "received":"В мастерской","washing":"Мойка","drying":"Сушка",
                        "packing":"Упаковка","ready":"Готов","delivery":"Доставка",
                        "delivered":"Доставлен","cancelled":"Отменён",
                    }
                    text = raw.format(
                        order_num  = order.get("order_num", ""),
                        status     = STATUS_NAME_RU.get(status, status),
                        status_emoji = STATUS_EMOJI.get(status, ""),
                        client_name  = order.get("client_first_name", ""),
                        service      = order.get("service", ""),
                        branch       = order.get("branch", ""),
                        pickup_date  = str(order.get("pickup_date", "") or ""),
                        phone        = order.get("client_phone", ""),
                    )
                    async with aiohttp.ClientSession() as session:
                        await session.post(
                            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                            json={"chat_id": tg_id, "text": text, "parse_mode": "HTML"},
                            timeout=aiohttp.ClientTimeout(total=8),
                        )
                    try:
                        await db.add_order_activity(order_id, staff.get("id"), staff.get("login", ""),
                                                      "tg_sent", f"🔔 TG-уведомление ({lang.upper()}): {text[:150]}")
                    except Exception as _ae:
                        logging.warning(f"add_order_activity (tg) failed: {_ae}")
        except Exception as e:
            logging.warning(f"TG notify failed for order {order_id}: {e}")

    # SMS при переходе в «Готов» больше не шлём молча — фронт после успешной смены
    # статуса сам спросит (модалка "Отправить SMS клиенту?" с выбором языка), тот же
    # эндпоинт что и ручная отправка. Портировано из прода (запрос 2026-08-07).

    # ── Синхронизировать stop_status в маршруте ──────────────────────────────
    # ── Комиссия агента при доставке ─────────────────────────────────────────
    if status == "delivered":
        try:
            total = order.get("total_price") or 0
            onum  = order.get("order_num", "")
            if total and onum:
                await db.trigger_order_agent_commission(order_id, onum, float(total))
        except Exception as _ce:
            logging.warning(f"agent commission failed order={order_id}: {_ce}")

    try:
        async with db.pool.acquire() as _c:
            if status == "delivered":
                await _c.execute(
                    "UPDATE route_orders SET stop_status='done' WHERE order_id=$1 AND stop_status!='done'",
                    order_id)
            elif status == "cancelled":
                await _c.execute(
                    "UPDATE route_orders SET stop_status='skipped' WHERE order_id=$1 AND stop_status='pending'",
                    order_id)
            else:
                # Любой активный статус (delivery, ready, washing и т.д.) — сбросить 'done' на 'pending'
                await _c.execute(
                    "UPDATE route_orders SET stop_status='pending' WHERE order_id=$1 AND stop_status='done'",
                    order_id)
    except Exception as _e:
        logging.warning(f"route_orders sync failed order={order_id}: {_e}")

    # ── Обновить кнопки в канале водителей ───────────────────────────────────
    try:
        branch, ch_msg_id = await db.get_channel_msg_for_order(order_id)
        logging.info(f"[channel_kb] order={order_id} status={status} branch={branch!r} msg_id={ch_msg_id}")
        if ch_msg_id and BOT_TOKEN:
            ch_key = "delivery_channel_navoi_id" if branch == "navoi" else "delivery_channel_zarafshan_id"
            ch_id_str = await _get_cfg(ch_key)
            logging.info(f"[channel_kb] ch_key={ch_key} ch_id={ch_id_str!r}")
            if ch_id_str:
                new_kb = _route_pickup_kb(order_id, status, closed=await _is_route_closed_for_order(order_id))
                async with aiohttp.ClientSession() as _sess:
                    resp = await _sess.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageReplyMarkup",
                        json={"chat_id": ch_id_str, "message_id": int(ch_msg_id),
                              "reply_markup": new_kb},
                        timeout=aiohttp.ClientTimeout(total=5))
                    resp_json = await resp.json()
                    if not resp_json.get("ok"):
                        logging.warning(f"[channel_kb] TG error: {resp_json}")
    except Exception as e:
        logging.warning(f"[channel_kb] failed order={order_id}: {e}", exc_info=True)

    try:
        _status_labels_bc = {
            "new":"Новый","confirmed":"Подтверждён","pickup":"Вывоз",
            "received":"В мастерской","washing":"Мойка","drying":"Сушка",
            "packing":"Упаковка","ready":"Готов","delivery":"Доставка",
            "delivered":"Доставлен","cancelled":"Отменён",
        }
        actor_name = " ".join(p for p in [staff.get("first_name"), staff.get("last_name")] if p).strip() or staff.get("login", "")
        await _chat.broadcast_staff({
            "type": "order_status_changed",
            "order_id": order_id,
            "status": status,
            "status_label": _status_labels_bc.get(status, status),
            "changed_by_name": actor_name,
            "changed_by_role": staff.get("role", ""),
        }, exclude=staff.get("id"), company_id=staff.get("company_id") or 1)
    except Exception as e:
        logging.warning(f"order_status_changed broadcast error: {e}")

    return {"ok": True, "order": order}


@app.post("/api/admin/orders/{order_id}/postpone")
async def postpone_order_delivery(order_id: int,
                                   until_date: str = Body(..., embed=True),
                                   reason: str = Body("", embed=True),
                                   staff=Depends(require_perm("orders"))):
    """Отложить доставку заказа в статусе "Готов" — клиент попросил подождать
    (ремонт, переезд, отпуск). Заказ остаётся в статусе ready, дата+причина
    показываются бейджем вместо простого "Готов". За день до даты — напоминание
    администраторам/менеджерам созвониться с клиентом (см. _send_postpone_reminders).
    Портировано из прода."""
    from datetime import date as _date
    try:
        until = _date.fromisoformat(until_date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Неверный формат даты")
    if until <= _date.today():
        raise HTTPException(status_code=400, detail="Дата должна быть в будущем")
    order = await db.set_order_postpone(order_id, until, reason, staff.get("id"))
    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден или не в статусе «Готов»")
    name = " ".join(p for p in [staff.get("first_name"), staff.get("last_name")] if p).strip() or staff.get("login", "")
    await db.add_order_activity(order_id, staff.get("id"), name, "postponed",
                                 f"⏸ Доставка отложена до {until.strftime('%d.%m.%Y')}" + (f": {reason}" if reason else ""))
    return {"ok": True, "order": order}


def _substitute_receipt_tokens(text: str, order: dict, grand_total: float) -> str:
    if not text:
        return text
    client_name = " ".join(p for p in [order.get("client_first_name"), order.get("client_last_name")] if p).strip()
    branch_labels = {"zarafshan": "Зарафшан", "navoi": "Навои"}
    created_at = order.get("created_at")
    date_str = created_at.strftime("%d.%m.%Y") if hasattr(created_at, "strftime") else str(created_at or "")
    replacements = {
        "{{order_id}}":    str(order.get("order_num") or order.get("id") or ""),
        "{{client_name}}": client_name,
        "{{phone}}":       order.get("client_phone") or "",
        "{{date}}":        date_str,
        "{{total}}":       f"{int(grand_total):,}".replace(",", " ") + " сум",
        "{{branch}}":      branch_labels.get(order.get("branch"), order.get("branch") or ""),
    }
    for token, value in replacements.items():
        text = text.replace(token, value)
    return text


async def _render_order_receipt(order_id: int) -> tuple[bytes, dict]:
    order = await db.get_order_by_id(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")
    items = await db.get_order_items(order_id)
    grand_total = (await db.get_orders_items_totals([order_id])).get(order_id, 0.0)

    branch = order.get("branch")
    branch_1_key = "contact_navoi_1" if branch == "navoi" else "contact_zarafshan_1"
    contact_main = await _get_cfg("contact_main")
    contact_branch1 = await _get_cfg(branch_1_key)
    branch_contacts = [c for c in (contact_main, contact_branch1) if c]

    header_text = _substitute_receipt_tokens(await _get_cfg("receipt_header_text"), order, grand_total)
    slogan      = _substitute_receipt_tokens(await _get_cfg("receipt_slogan"), order, grand_total)
    footer_note = _substitute_receipt_tokens(await _get_cfg("receipt_footer_note"), order, grand_total)

    order_bot_username = await _get_cfg("order_bot_username")
    bot_link = f"t.me/{order_bot_username}" if order_bot_username else ""

    jpeg_bytes = receipt.generate_receipt_jpeg(order, items, branch_contacts,
                                                header_text, slogan, footer_note,
                                                bot_link, grand_total=grand_total)
    return jpeg_bytes, order


@app.post("/api/admin/orders/{order_id}/send-receipt")
async def admin_send_order_receipt(order_id: int, staff=Depends(get_current_staff),
                                    note: str = Body("", embed=True),
                                    silent: bool = Body(False, embed=True)):
    """Отправляет клиенту JPG-чек заказа в Telegram (для статуса «Готов»)."""
    role = staff.get("role", "")
    if role not in RECEIPT_ACCESS_ROLES:
        raise HTTPException(status_code=403, detail="Нет прав")

    order = await db.get_order_by_id(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")

    tg_id = await db.get_receipt_tg_id(order)
    if not tg_id:
        raise HTTPException(status_code=400, detail="У клиента нет Telegram")

    if not note:
        prior = await db.get_last_receipt_send(order_id)
        if prior:
            sent_at = prior.get("created_at")
            sent_at_str = sent_at.strftime("%d.%m.%Y %H:%M") if hasattr(sent_at, "strftime") else str(sent_at or "")
            raise HTTPException(status_code=409,
                detail=f"Чек уже отправлялся {sent_at_str} ({prior.get('staff_name') or '—'}). "
                       f"Укажите причину повторной отправки.")

    try:
        jpeg_bytes, order = await _render_order_receipt(order_id)
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"send-receipt render failed order={order_id}: {e}")
        raise HTTPException(status_code=500, detail="Не удалось сформировать чек")

    if not BOT_TOKEN:
        raise HTTPException(status_code=502, detail="BOT_TOKEN не настроен")

    try:
        form = aiohttp.FormData()
        form.add_field("chat_id", str(tg_id))
        form.add_field("photo", jpeg_bytes, filename="receipt.jpg", content_type="image/jpeg")
        form.add_field("disable_notification", "true" if silent else "false")
        async with aiohttp.ClientSession() as session:
            resp = await session.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
                data=form, timeout=aiohttp.ClientTimeout(total=10))
            resp_json = await resp.json()
        if resp.status != 200 or not resp_json.get("ok"):
            logging.error(f"send-receipt TG error order={order_id}: {resp_json}")
            raise HTTPException(status_code=502, detail=resp_json.get("description") or "Ошибка отправки в Telegram")
        new_message_id = resp_json.get("result", {}).get("message_id")
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"send-receipt TG request failed order={order_id}: {e}")
        raise HTTPException(status_code=502, detail="Ошибка отправки в Telegram")

    try:
        prior_msgs = await db.get_prior_receipt_messages(order_id)
    except Exception as e:
        logging.warning(f"receipt prior lookup failed order={order_id}: {e}")
        prior_msgs = []

    try:
        actor_name = " ".join(p for p in [staff.get("first_name"), staff.get("last_name")] if p).strip() or staff.get("login", "")
        await db.log_receipt_send(order_id, staff.get("id"), actor_name, note, tg_id, new_message_id)
    except Exception as e:
        logging.warning(f"receipt send log failed order={order_id}: {e}")

    if prior_msgs:
        try:
            async with aiohttp.ClientSession() as session:
                for m in prior_msgs:
                    try:
                        await session.post(
                            f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage",
                            json={"chat_id": m["tg_chat_id"], "message_id": m["tg_message_id"]},
                            timeout=aiohttp.ClientTimeout(total=8))
                    except Exception as e:
                        logging.warning(f"receipt delete failed order={order_id} msg={m}: {e}")
        except Exception as e:
            logging.warning(f"receipt cleanup failed order={order_id}: {e}")

    return {"ok": True}


@app.get("/api/admin/orders/{order_id}/receipt-image")
async def get_order_receipt_image(order_id: int, staff=Depends(get_current_staff)):
    role = staff.get("role", "")
    if role not in RECEIPT_ACCESS_ROLES:
        raise HTTPException(status_code=403, detail="Нет прав")
    jpeg_bytes, _order = await _render_order_receipt(order_id)
    return Response(content=jpeg_bytes, media_type="image/jpeg")


@app.post("/api/admin/receipt/preview")
async def preview_receipt(body: dict, _=Depends(get_admin)):
    """Рендерит превью JPEG-чека с переданными текстами шапки/слогана/примечания (для настроек сайта)."""
    header_text = body.get("header_text") or "ARTEZ"
    slogan      = body.get("slogan") or ""
    footer_note = body.get("footer_note") or ""
    mock_order = {
        "id": 0, "order_num": "0000", "created_at": datetime.now(),
        "client_first_name": "Иванов", "client_last_name": "Пётр",
        "client_phone": "+998901234567", "branch": "zarafshan",
    }
    mock_items = [
        {"service": "Ковёр шерстяной", "width_cm": 200, "length_cm": 300, "sqm": 6.0, "price_per_sqm": 25000, "total_sum": 150000},
        {"service": "Диван 3-местный", "width_cm": None, "length_cm": None, "sqm": None, "price_per_sqm": None, "total_sum": 300000},
    ]
    grand_total = sum(float(it.get("total_sum") or 0) for it in mock_items)
    header_text = _substitute_receipt_tokens(header_text, mock_order, grand_total)
    slogan      = _substitute_receipt_tokens(slogan, mock_order, grand_total)
    footer_note = _substitute_receipt_tokens(footer_note, mock_order, grand_total)
    contacts = [c for c in (await _get_cfg("contact_main"), await _get_cfg("contact_zarafshan_1")) if c]
    order_bot_username = await _get_cfg("order_bot_username")
    bot_link = f"t.me/{order_bot_username}" if order_bot_username else ""
    jpeg_bytes = receipt.generate_receipt_jpeg(mock_order, mock_items, contacts, header_text, slogan, footer_note,
                                                bot_link)
    return Response(content=jpeg_bytes, media_type="image/jpeg")


@app.get("/api/admin/orders/{order_id}/items")
async def admin_get_order_items(order_id: int, _=Depends(get_current_staff)):
    items = await db.get_order_items(order_id)
    return {"ok": True, "items": items}

class OrderItemRequest(BaseModel):
    service: str
    service_ru: str | None = None
    service_uz: str | None = None
    sqm: float | None = None
    width_cm: float | None = None
    length_cm: float | None = None
    price_per_sqm: float = 0

@app.post("/api/admin/orders/{order_id}/items")
async def admin_create_order_item(order_id: int, req: OrderItemRequest, _=Depends(get_current_staff)):
    sqm = req.sqm
    if not sqm and req.width_cm and req.length_cm:
        sqm = round(req.width_cm * req.length_cm / 10000, 3)
    item = await db.create_order_item(
        order_id=order_id, service=req.service, sqm=sqm or 0,
        price_per_sqm=req.price_per_sqm,
        width_cm=req.width_cm, length_cm=req.length_cm)
    return {"ok": True, "item": item}

@app.post("/api/admin/orders/{order_id}/items/bulk")
async def admin_bulk_create_items(order_id: int, count: int = Body(..., embed=True),
                                   _=Depends(get_current_staff)):
    if count < 1 or count > 50:
        raise HTTPException(status_code=400, detail="Количество от 1 до 50")
    items = await db.create_empty_items(order_id, count)
    return {"ok": True, "items": items, "count": len(items)}

@app.put("/api/admin/orders/{order_id}/items/{item_id}")
async def admin_update_order_item(order_id: int, item_id: int,
                                   req: OrderItemRequest, staff=Depends(get_current_staff)):
    sqm = req.sqm
    if not sqm and req.width_cm and req.length_cm:
        sqm = round(req.width_cm * req.length_cm / 10000, 3)
    # Fetch old values + position number for diff logging
    old = {}
    item_pos = None
    if db.pool:
        async with db.pool.acquire() as _c:
            _row = await _c.fetchrow("SELECT * FROM order_items WHERE id=$1", item_id)
            if _row: old = dict(_row)
            item_pos = await _c.fetchval(
                "SELECT COUNT(*) FROM order_items WHERE order_id=$1 AND id <= $2",
                order_id, item_id
            )
    updates = {"service": req.service, "price_per_sqm": req.price_per_sqm}
    if sqm: updates["sqm"] = sqm
    if req.width_cm: updates["width_cm"] = req.width_cm
    if req.length_cm: updates["length_cm"] = req.length_cm
    # service_ru/service_uz — заполняются, только когда позиция выбрана из справочника
    # (staff.html знает оба варианта сразу); для "своей услуги" остаются NULL —
    # рендер везде фолбэкается на service.
    if req.service_ru is not None: updates["service_ru"] = req.service_ru
    if req.service_uz is not None: updates["service_uz"] = req.service_uz
    item = await db.update_order_item(item_id, **updates)
    if not item:
        raise HTTPException(status_code=404, detail="Позиция не найдена")
    sname = f"{staff.get('first_name','')} {staff.get('last_name','')}".strip() or staff.get('login','?')
    parts = []
    def _fmt_dim(w, l): return f"{int(w)}×{int(l)} см" if w and l else "—"
    if old:
        if (old.get('service') or '') != (req.service or ''):
            parts.append(f"Услуга: {old.get('service') or '—'} → {req.service or '—'}")
        old_sqm = float(old.get('sqm') or 0)
        new_sqm = float(sqm or 0)
        if abs(old_sqm - new_sqm) > 0.001:
            parts.append(f"Площадь: {old_sqm:.2f} → {new_sqm:.2f} м²")
        old_p = float(old.get('price_per_sqm') or 0)
        new_p = float(req.price_per_sqm or 0)
        if abs(old_p - new_p) > 0.5:
            parts.append(f"Цена: {int(old_p):,} → {int(new_p):,} сум/м²")
        old_w, old_l = old.get('width_cm'), old.get('length_cm')
        if (old_w, old_l) != (req.width_cm, req.length_cm):
            parts.append(f"Размер: {_fmt_dim(old_w, old_l)} → {_fmt_dim(req.width_cm, req.length_cm)}")
    if not parts:
        parts = [req.service or '—']
        if sqm: parts.append(f"{sqm:.2f} м²")
    prefix = f"#{item_pos} {old.get('service') or req.service or '—'}: " if item_pos else ""
    await db.add_order_activity(order_id, staff.get("id"), sname, "item_edited", prefix + "; ".join(parts))
    return {"ok": True, "item": item}

@app.delete("/api/admin/orders/{order_id}/items/{item_id}")
async def admin_delete_order_item(order_id: int, item_id: int, _=Depends(get_current_staff)):
    ok = await db.delete_order_item(item_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Позиция не найдена")
    return {"ok": True}

@app.get("/api/admin/orders/{order_id}/photos")
async def get_order_photos(order_id: int, _=Depends(get_current_staff)):
    photos = await db.get_order_photos(order_id)
    return {"ok": True, "photos": photos}

@app.post("/api/admin/orders/{order_id}/photos")
async def upload_order_photo(
    order_id: int,
    file: UploadFile = File(...),
    photo_type: str = Form("before"),
    note: str = Form(""),
    staff=Depends(get_current_staff),
):
    media_ch = await _get_media_channel()
    if not BOT_TOKEN or not media_ch:
        raise HTTPException(status_code=503, detail="Медиа-хранилище не настроено")
    content_type = file.content_type or ""
    if content_type.startswith("video/"):
        tg_method, tg_field, tg_type = "sendVideo",    "video",    "video"
    elif content_type.startswith("image/"):
        tg_method, tg_field, tg_type = "sendPhoto",    "photo",    "photo"
    else:
        tg_method, tg_field, tg_type = "sendDocument", "document", "document"

    # Получаем номер заказа для подписи
    order_row = await db.get_order_by_id(order_id)
    order_num = order_row.get("order_num", f"#{order_id}") if order_row else f"#{order_id}"
    type_labels = {"before": "До", "after": "После", "damage": "Повреждение"}
    type_label  = type_labels.get(photo_type, photo_type)
    staff_name  = " ".join(filter(None, [staff.get("last_name"), staff.get("first_name")])) or staff.get("login","")
    caption = f"📷 {type_label}\n🧾 Заказ: {order_num}\n👤 {staff_name}"

    file_bytes = await file.read()
    form = aiohttp.FormData()
    form.add_field("chat_id", str(media_ch))
    form.add_field(tg_field, file_bytes, filename=file.filename, content_type=content_type)
    form.add_field("caption", caption)

    async with aiohttp.ClientSession() as s:
        async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{tg_method}", data=form) as r:
            result = await r.json()

    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=f"Telegram: {result.get('description','upload failed')}")

    msg = result["result"]
    if tg_type == "photo":
        file_id = msg["photo"][-1]["file_id"]
    else:
        file_id = msg[tg_type]["file_id"]

    name = " ".join(filter(None, [staff.get("last_name"), staff.get("first_name")])) or staff.get("login","")
    photo = await db.save_order_photo(order_id, file_id, tg_type, photo_type, note, name)
    return {"ok": True, "photo": photo}

@app.delete("/api/admin/orders/{order_id}/photos/{photo_id}")
async def delete_order_photo(order_id: int, photo_id: int, staff=Depends(get_current_staff)):
    ok = await db.delete_order_photo(photo_id, staff.get("company_id") or 1)
    if not ok:
        raise HTTPException(status_code=404)
    return {"ok": True}

# ── Платежи заказа ────────────────────────────────────────────────────────────

@app.get("/api/admin/orders/{order_id}/payments")
async def get_order_payments(order_id: int, _=Depends(get_current_staff)):
    rows = await db.get_order_payments(order_id)
    return {"ok": True, "payments": rows}

@app.post("/api/admin/orders/{order_id}/payments")
async def add_order_payment(
    order_id: int,
    amount:   float = Body(..., embed=False),
    method:   str   = Body(..., embed=False),
    purpose:  str   = Body("payment", embed=False),
    note:     str   = Body("", embed=False),
    staff=Depends(get_current_staff),
):
    name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
    row = await db.add_order_payment(order_id, amount, method, purpose, note, name, None, staff.get("id"))
    mLabel = {"cash":"💵 Нал","card":"💳 Карта","transfer":"📲 Перевод"}
    pLabel = {"prepayment":"Предоплата","partial":"Частичная оплата","final":"Окончательный расчёт"}
    details = f"{pLabel.get(purpose,purpose)}: {int(amount):,} сум ({mLabel.get(method,method)})"
    await db.add_order_activity(order_id, staff.get("id"), name, "payment_added", details)
    # Порядковый номер платежа в заказе
    pay_num = row.get("id", payment_id if 'payment_id' in dir() else "?")
    try:
        async with db.pool.acquire() as conn:
            pay_num = await conn.fetchval(
                "SELECT COUNT(*) FROM order_payments WHERE order_id=$1 AND id<=$2",
                order_id, row["id"]) or 1
    except Exception:
        pass
    # Уведомление в канал кассы
    ch = await db.get_cash_tg_channel()
    if ch:
        phone = staff.get("phone") or ""
        text = (f"💰 <b>Новый платёж</b> · Заказ #{order_id} · №{pay_num}\n"
                f"{pLabel.get(purpose, purpose)} · {mLabel.get(method, method)}\n"
                f"<b>{int(amount):,} сум</b>\n"
                f"👤 {name}")
        asyncio.create_task(_send_tg_cash(ch, text, phone=phone,
                                          btn_label="🟢 Проверить", btn_cb=f"chk:g:{order_id}"))
    # Пуш-уведомление всем ответственным за кассу (только для карты/перевода)
    if method in ("card", "transfer"):
        cashiers = await db.get_all_cashiers_for_push()
        push_title = "💳 Оплата на проверку"
        push_body  = f"Заказ #{order_id} · {pLabel.get(purpose,purpose)} · {int(amount):,} сум · {name}"
        for c in cashiers:
            if c["id"] != staff.get("id"):
                asyncio.create_task(send_web_push(c["id"], push_title, push_body,
                                                  order_id=order_id, push_type="payment_review"))
    # Наличные сразу считаются в paid_amount — обновляем канал
    if method == "cash":
        asyncio.create_task(_update_api_channel_stop(order_id))
    return {"ok": True, "payment": row}


@app.patch("/api/admin/orders/{order_id}/payments/{payment_id}")
async def edit_order_payment(
    order_id:   int,
    payment_id: int,
    amount:  float = Body(..., embed=True),
    method:  str   = Body(..., embed=True),
    purpose: str   = Body(..., embed=True),
    staff=Depends(get_current_staff),
):
    if not db.pool: raise HTTPException(status_code=503, detail="DB unavailable")
    async with db.pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT * FROM order_payments WHERE id=$1 AND order_id=$2", payment_id, order_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Платёж не найден")
    can_edit = (staff.get("sub") == "admin"
                or staff.get("can_manage_cash")
                or existing.get("created_by_staff_id") == staff.get("id"))
    if not can_edit:
        raise HTTPException(status_code=403, detail="Нет доступа")
    row = await db.edit_order_payment(payment_id, amount, method, purpose)
    name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
    mLabel = {"cash":"💵 Нал","card":"💳 Карта","transfer":"📲 Перевод"}
    pLabel = {"prepayment":"Предоплата","partial":"Частичная оплата","final":"Окончательный расчёт"}
    diff_parts = []
    old_amt = float(existing.get("amount") or 0)
    if abs(old_amt - amount) > 0.5:
        diff_parts.append(f"Сумма: {int(old_amt):,} → {int(amount):,} сум")
    old_method = existing.get("method") or ""
    if old_method != method:
        diff_parts.append(f"Способ: {mLabel.get(old_method, old_method)} → {mLabel.get(method, method)}")
    old_purpose = existing.get("purpose") or ""
    if old_purpose != purpose:
        diff_parts.append(f"Вид: {pLabel.get(old_purpose, old_purpose)} → {pLabel.get(purpose, purpose)}")
    details = "; ".join(diff_parts) if diff_parts else f"Платёж: {int(amount):,} сум ({mLabel.get(method,method)}, {pLabel.get(purpose,purpose)})"
    await db.add_order_activity(order_id, staff.get("id"), name, "payment_edited", details)
    ch = await db.get_cash_tg_channel()
    if ch:
        phone = staff.get("phone") or ""
        text = (f"✏️ <b>Платёж изменён</b> · Заказ #{order_id}\n"
                f"{pLabel.get(purpose, purpose)} · {mLabel.get(method, method)}\n"
                f"<b>{int(amount):,} сум</b>\n"
                f"👤 {name}")
        asyncio.create_task(_send_tg_cash(ch, text, phone=phone,
                                          btn_label="🟢 Проверить", btn_cb=f"chk:g:{order_id}"))
    return {"ok": True, "payment": row}


@app.get("/api/admin/staff/cashiers")
async def get_cashiers(_=Depends(get_current_staff)):
    rows = await db.get_cashiers()
    return {"ok": True, "cashiers": rows}


@app.get("/api/admin/cash/balance")
async def get_cash_balance(_=Depends(_get_admin)):
    rows = await db.get_cash_balance()
    return {"ok": True, "balances": rows}


@app.get("/api/admin/cash/handovers")
async def list_cash_handovers(_=Depends(_get_admin)):
    rows = await db.get_cash_handovers()
    return {"ok": True, "handovers": rows}


@app.post("/api/admin/cash/handover")
async def create_cash_handover(
    from_staff_id: int   = Body(..., embed=True),
    to_staff_id:   int   = Body(..., embed=True),
    amount:        float = Body(..., embed=True),
    note:          str   = Body("", embed=True),
    _=Depends(_get_admin),
):
    row = await db.add_cash_handover(from_staff_id, to_staff_id, amount, note)
    handover_id = row.get("id")
    # Эта запись создаётся вручную самим админом ("✅ Подтвердить приём") — в
    # отличие от staff-инициированной передачи (тому нужно отдельное подтверждение
    # получателя ради подотчётности), здесь админ уже сам удостоверяет факт передачи,
    # поэтому сразу помечаем confirmed — иначе баланс "На руках" не менялся, пока
    # получатель отдельно не подтвердит в Telegram, о чём никто не знал (см. фикс).
    if handover_id:
        row = await db.confirm_cash_handover(handover_id, to_staff_id)

    from_staff = await db.get_staff_by_id(from_staff_id)
    to_staff   = await db.get_staff_by_id(to_staff_id)
    from_name  = " ".join(filter(None, [from_staff.get("last_name",""), from_staff.get("first_name","")])).strip() if from_staff else f"#{from_staff_id}"

    dm_text = (f"💵 <b>Вам сдали наличные</b>\n"
               f"От: {from_name}\n"
               f"Сумма: <b>{int(amount):,} сум</b>"
               + (f"\nПримечание: {note}" if note else "")
               + "\n\n✅ Уже зачтено на баланс.")
    if to_staff and to_staff.get("tg_id") and handover_id:
        tg_chat = int(to_staff["tg_id"])
        msg_id = await _send_tg_with_kb(tg_chat, dm_text, keyboard=None)
        if msg_id:
            asyncio.create_task(db.update_handover_tg_msg(handover_id, tg_chat, msg_id))

    return {"ok": True, "handover": row}


async def _set_tg_webhook():
    """Установить webhook Telegram при старте."""
    await asyncio.sleep(3)
    url = f"{APP_URL.rstrip('/')}/api/tg/webhook"
    try:
        async with aiohttp.ClientSession() as s:
            r = await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
                             json={"url": url}, timeout=aiohttp.ClientTimeout(total=10))
            body = await r.json()
            logging.info(f"setWebhook → {body.get('description','')}")
    except Exception as e:
        logging.warning(f"setWebhook error: {e}")


async def _send_tg_cash(chat_id, text: str, photo_bytes: bytes = None, filename: str = None,
                        phone: str = None, btn_label: str = None, btn_cb: str = None):
    """Отправить сообщение (или фото) в ТГ-канал кассы."""
    if not BOT_TOKEN or not chat_id:
        logging.warning(f"_send_tg_cash skip: BOT_TOKEN={bool(BOT_TOKEN)} chat_id={repr(chat_id)}")
        return
    phone_clean = (phone or "").strip()
    if phone_clean:
        text += f"\n📞 {phone_clean}"
    reply_markup = None
    if btn_label and btn_cb:
        reply_markup = {"inline_keyboard": [[{"text": btn_label, "callback_data": btn_cb}]]}
    try:
        async with aiohttp.ClientSession() as s:
            if photo_bytes:
                form = aiohttp.FormData()
                form.add_field("chat_id", str(chat_id))
                form.add_field("photo", photo_bytes, filename=filename or "receipt.jpg", content_type="image/jpeg")
                form.add_field("caption", text)
                if reply_markup:
                    form.add_field("reply_markup", _json.dumps(reply_markup))
                r = await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto", data=form,
                                 timeout=aiohttp.ClientTimeout(total=10))
                logging.info(f"_send_tg_cash photo → {r.status}")
            else:
                payload = {"chat_id": str(chat_id), "text": text, "parse_mode": "HTML"}
                if reply_markup:
                    payload["reply_markup"] = reply_markup
                r = await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                                 json=payload, timeout=aiohttp.ClientTimeout(total=5))
                body = await r.json()
                logging.info(f"_send_tg_cash msg → {r.status} {body.get('description','')}")
    except Exception as e:
        logging.warning(f"_send_tg_cash error: {e}")


@app.get("/api/tg/chat-info")
async def tg_chat_info(chat_id: int, company_id: int | None = None, authorization: str = Header(None)):
    """Проверка ID группы/канала (кнопка 🔍 на формах Telegram-настроек, admin.html
    и superadmin.html). Группы лидов/заказов/доставки принимают сообщения от
    СОБСТВЕННОГО бота компании (order_bot_token, см. _notify_new_lead) — не от
    глобального Cleano-бота (BOT_TOKEN, тот только для медиа-хранилища).
    Проверять нужно тем же ботом, которым реально будут слаться сообщения,
    иначе getChat падает с 'chat not found', даже если ID верный.

    Вызывается и обычным admin (своя компания, из токена), и superadmin
    (любая компания — редактирует чужие, company_id передаётся явно)."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Не авторизован")
    token_str = authorization.removeprefix("Bearer ").strip()
    try:
        payload = jwt.decode(token_str, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except Exception:
        raise HTTPException(status_code=401, detail="Токен недействителен")
    if payload.get("type") == "superadmin":
        if not company_id:
            raise HTTPException(status_code=400, detail="company_id обязателен для суперадмина")
        cid = company_id
    elif payload.get("sub") == "admin" or payload.get("role") == "admin":
        cid = int(payload.get("company_id") or 1)
    else:
        raise HTTPException(status_code=403, detail="Нет доступа")
    token = await db.get_config_for_company("order_bot_token", cid)
    if not token:
        raise HTTPException(400, detail="Бот заказов ещё не подключён для этой компании")
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f"https://api.telegram.org/bot{token}/getChat",
            params={"chat_id": chat_id},
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            data = await r.json()
    if not data.get("ok"):
        raise HTTPException(400, detail=data.get("description", "ID неверный или бот не является участником группы/канала"))
    chat = data["result"]
    return {
        "title": chat.get("title") or chat.get("first_name") or "",
        "type": chat.get("type", ""),
        "username": chat.get("username"),
    }


@app.post("/api/tg/webhook")
async def tg_webhook(request: Request):
    """Единый обработчик всех callback-кнопок Telegram (take_lead, chk:, accept_, reject_)."""
    try:
        data = await request.json()
    except Exception:
        return {"ok": True}

    # ── Обычные сообщения (text / contact) ──────────────────────────
    msg = data.get("message") or data.get("edited_message")
    if msg:
        chat_id_msg  = msg.get("chat", {}).get("id")
        tg_uid_msg   = msg.get("from", {}).get("id")
        text_msg     = (msg.get("text") or "").strip()
        contact      = msg.get("contact")
        if text_msg == "/start" and chat_id_msg:
            await _tg_send_reply_keyboard(
                chat_id_msg,
                "👋 <b>Добро пожаловать в ARTEZ!</b>\n\n"
                "Нажмите кнопку ниже, чтобы привязать ваш номер телефона.\n"
                "После этого при регистрации на сайте вы сможете получить код подтверждения через Telegram."
            )
            return {"ok": True}
        if contact and tg_uid_msg and chat_id_msg:
            phone_raw = contact.get("phone_number", "")
            phone = phone_raw if phone_raw.startswith("+") else "+" + phone_raw
            owner_tg = contact.get("user_id")
            if owner_tg and int(owner_tg) != int(tg_uid_msg):
                await _tg_remove_keyboard(chat_id_msg, "❌ Пожалуйста, поделитесь <b>своим</b> номером.")
                return {"ok": True}
            await db.save_tg_phone_link(phone, int(tg_uid_msg))
            await _tg_remove_keyboard(
                chat_id_msg,
                f"✅ <b>Номер привязан!</b>\n\n"
                f"📱 <code>{phone}</code>\n\n"
                f"Теперь при регистрации на сайте ARTEZ вы можете выбрать "
                f"«Получить код через Telegram»."
            )
            return {"ok": True}

    cq = data.get("callback_query")
    if not cq:
        return {"ok": True}
    cq_id      = cq["id"]
    cb_data    = cq.get("data", "")
    msg        = cq.get("message", {})
    chat_id    = msg.get("chat", {}).get("id")
    msg_id     = msg.get("message_id")
    orig_text  = msg.get("text", "")
    tg_user_id = cq["from"]["id"]
    uname      = cq["from"].get("username")
    fname      = cq["from"].get("first_name", "")
    lname      = cq["from"].get("last_name", "")
    display    = f"@{uname}" if uname else " ".join(filter(None, [fname, lname])) or "кто-то"

    # ── Взять лид ─────────────────────────────────────────────────
    if cb_data.startswith("take_lead_"):
        try:
            lead_id = int(cb_data.split("_")[2])

            if not db.pool:
                await _tg_answer_callback(cq_id, "❌ Ошибка базы данных", alert=True)
                return {"ok": True}

            staff = await db.get_staff_by_tg_id(tg_user_id)
            if not staff:
                await _tg_answer_callback(cq_id,
                    "❌ Ваш Telegram не привязан к аккаунту сотрудника ARTEZ.\n"
                    "Обратитесь к администратору.", alert=True)
                return {"ok": True}
            if staff.get("role") == "agent":
                await _tg_answer_callback(cq_id,
                    "❌ Агенты не могут брать лиды через Telegram.", alert=True)
                return {"ok": True}

            staff_id   = staff["id"]
            staff_name = f"{staff.get('first_name','')} {staff.get('last_name','')}".strip() or staff.get("login","")
            took_verb  = "Взяла" if staff.get("gender") == "F" else "Взял"

            async with db.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT assigned_to, lead_code FROM leads WHERE id=$1", lead_id)
                if not row:
                    await _tg_answer_callback(cq_id, "❌ Лид не найден", alert=True)
                    return {"ok": True}

                if row["assigned_to"] and row["assigned_to"] != staff_id:
                    taker = await db.get_staff_by_id(row["assigned_to"])
                    taker_name = ""
                    taker_verb = "Взяла" if taker and taker.get("gender") == "F" else "Взял"
                    if taker:
                        taker_name = f"{taker.get('first_name','')} {taker.get('last_name','')}".strip()
                    await _tg_answer_callback(cq_id,
                        f"❌ Лид уже взят: {taker_name or 'другой сотрудник'}", alert=True)
                    new_text = orig_text.rstrip("━━━━━━━━━━").rstrip() + f"\n━━━━━━━━━━\n✅ {taker_verb}: {taker_name or 'другой сотрудник'}"
                    await _tg_edit_message(chat_id, msg_id, new_text)
                    return {"ok": True}

                if row["assigned_to"] == staff_id:
                    await _tg_answer_callback(cq_id, "✅ Этот лид уже ваш!")
                    return {"ok": True}

                await conn.execute(
                    "UPDATE leads SET assigned_to=$1 WHERE id=$2", staff_id, lead_id)

            await db.add_lead_call(lead_id, staff_id, action="note",
                                   note=f"Лид взят через Telegram: {staff_name}")
            await _tg_answer_callback(cq_id, "✅ Лид взят! Откройте приложение.")
            new_text = orig_text.rstrip("━━━━━━━━━━").rstrip() + f"\n━━━━━━━━━━\n✅ {took_verb}: {staff_name}"
            await _tg_edit_message(chat_id, msg_id, new_text)

        except Exception as e:
            logging.warning(f"take_lead handler error: {e}")
            try:
                await _tg_answer_callback(cq_id, "❌ Ошибка сервера. Попробуйте ещё раз.", alert=True)
            except Exception:
                pass
        return {"ok": True}

    # ── Маршрут: забор/сдача (rp:) ────────────────────────────────
    if cb_data.startswith("rp:"):
        try:
            parts   = cb_data.split(":")
            order_id = int(parts[1])
            action   = parts[2]  # take | undo | deliver

            order_row = await db.get_order_by_id(order_id)
            if not order_row:
                await _tg_answer_callback(cq_id, "❌ Заказ не найден", alert=True)
                return {"ok": True}

            cur_status = order_row.get("status", "")

            if action == "take":
                if cur_status != "confirmed":
                    await _tg_answer_callback(cq_id,
                        f"ℹ️ Статус уже: {_ORDER_STATUS_RU.get(cur_status, cur_status)}", alert=False)
                    return {"ok": True}
                new_status = "pickup"
                toast = "✅ Забрал — статус: Вывоз"

            elif action == "undo":
                if cur_status != "pickup":
                    await _tg_answer_callback(cq_id,
                        f"ℹ️ Статус уже: {_ORDER_STATUS_RU.get(cur_status, cur_status)}", alert=False)
                    return {"ok": True}
                new_status = "confirmed"
                toast = "↩️ Отменено — статус: Подтверждён"

            elif action == "deliver":
                if cur_status != "pickup":
                    await _tg_answer_callback(cq_id,
                        f"ℹ️ Статус уже: {_ORDER_STATUS_RU.get(cur_status, cur_status)}", alert=False)
                    return {"ok": True}
                new_status = "received"
                toast = "🏭 Сдан в мастерскую"

            else:
                return {"ok": True}

            await db.update_order_status(order_id, new_status)

            # Обновить клавиатуру сообщения
            new_kb = _route_pickup_kb(order_id, new_status, closed=await _is_route_closed_for_order(order_id))
            # Обновить текст: поменять строку статуса
            new_text = orig_text
            for old_s, new_s in _ORDER_STATUS_RU.items():
                new_text = new_text.replace(f"📌 Статус: {_ORDER_STATUS_RU[old_s]}", f"📌 Статус: {_ORDER_STATUS_RU.get(new_status, new_status)}")
            # Простая замена последней строки статуса
            lines_t = orig_text.rsplit("📌 Статус:", 1)
            if len(lines_t) == 2:
                new_text = lines_t[0] + "📌 Статус: " + _ORDER_STATUS_RU.get(new_status, new_status)

            await _edit_tg_with_kb(chat_id, msg_id, new_text, new_kb)
            await _tg_answer_callback(cq_id, toast)

        except Exception as e:
            logging.warning(f"rp: callback error: {e}")
            await _tg_answer_callback(cq_id, "❌ Ошибка сервера", alert=True)
        return {"ok": True}

    # ── Проверка оплаты (chk:) ─────────────────────────────────────
    if cb_data.startswith("chk:"):
        parts  = cb_data.split(":")
        color  = parts[1] if len(parts) > 1 else "g"
        icon   = "🟢" if color == "g" else "🔴"
        new_kb = {"inline_keyboard": [[{"text": f"{icon} ✅ Проверено · {display}", "callback_data": "done"}]]}
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageReplyMarkup",
                             json={"chat_id": chat_id, "message_id": msg_id, "reply_markup": new_kb},
                             timeout=aiohttp.ClientTimeout(total=5))
                await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/answerCallbackQuery",
                             json={"callback_query_id": cq_id, "text": "✅ Отмечено"},
                             timeout=aiohttp.ClientTimeout(total=5))
        except Exception as e:
            logging.warning(f"webhook callback error: {e}")

    return {"ok": True}


# ── Передача наличных (staff → ответственный) ─────────────────────────────────

@app.post("/api/admin/cash/staff-handover")
async def staff_cash_handover(
    to_staff_id: int   = Body(..., embed=True),
    amount:      float = Body(..., embed=True),
    note:        str   = Body("", embed=True),
    staff=Depends(get_current_staff),
):
    from_name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
    row = await db.add_cash_handover(staff["id"], to_staff_id, amount, note)
    handover_id = row["id"]

    # Пуш получателю
    asyncio.create_task(send_web_push(
        to_staff_id,
        title="💵 Вам сдают наличные",
        body=f"{from_name} · {int(amount):,} сум",
        push_type="cash_handover",
    ))

    # ТГ личка + канал кассы
    to_staff = await db.get_staff_by_id(to_staff_id)
    to_name = " ".join(filter(None,[to_staff.get("last_name",""),to_staff.get("first_name","")])).strip() if to_staff else f"#{to_staff_id}"
    dm_text = (f"💵 <b>Вам сдают наличные</b>\n"
               f"От: {from_name}\n"
               f"Сумма: <b>{int(amount):,} сум</b>"
               + (f"\nПримечание: {note}" if note else ""))
    if to_staff and to_staff.get("tg_id"):
        tg_chat = int(to_staff["tg_id"])
        msg_id = await _send_tg_with_kb(
            tg_chat, dm_text,
            keyboard={"inline_keyboard": [[
                {"text": "✅ Подтвердить", "callback_data": f"cash_confirm:{handover_id}"},
                {"text": "❌ Отклонить",   "callback_data": f"cash_reject:{handover_id}"},
            ]]},
        )
        if msg_id:
            asyncio.create_task(db.update_handover_tg_msg(handover_id, tg_chat, msg_id))
    ch = await db.get_cash_tg_channel()
    ch_text = (f"💵 <b>Передача наличных</b>\n"
               f"От: {from_name}\n"
               f"Кому: {to_name}\n"
               f"Сумма: <b>{int(amount):,} сум</b>"
               + (f"\nПримечание: {note}" if note else ""))
    asyncio.create_task(_send_tg_cash(ch, ch_text))

    return {"ok": True, "handover": row}


@app.post("/api/admin/cash/staff-handover/{handover_id}/confirm")
async def confirm_staff_handover(handover_id: int, staff=Depends(get_current_staff)):
    row = await db.confirm_cash_handover(handover_id, staff["id"])
    if not row:
        raise HTTPException(status_code=404, detail="Не найдено")
    confirmer_name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
    amount = int(float(row.get("amount", 0)))

    # Пуш отправителю
    asyncio.create_task(send_web_push(
        row["from_staff_id"],
        title="✅ Наличные получены",
        body=f"{confirmer_name} подтвердил получение {amount:,} сум",
        push_type="cash_confirmed",
    ))

    # ТГ личка отправителю + канал кассы
    confirmed_text = f"✅ <b>Наличные получены</b>\nПолучил: {confirmer_name}\nСумма: <b>{amount:,} сум</b>"
    from_staff = await db.get_staff_by_id(row["from_staff_id"])
    if from_staff and from_staff.get("tg_id"):
        asyncio.create_task(_send_tg_cash(int(from_staff["tg_id"]), confirmed_text))
    ch = await db.get_cash_tg_channel()
    asyncio.create_task(_send_tg_cash(ch, confirmed_text))

    # Обновить TG-сообщение у получателя (убрать кнопки)
    if row.get("tg_chat_id") and row.get("tg_msg_id"):
        from_name_for_tg = " ".join(filter(None,[from_staff.get("last_name",""),from_staff.get("first_name","")])).strip() if from_staff else "—"
        edited_text = (f"💵 <b>Вам сдали наличные</b>\n"
                       f"От: {from_name_for_tg}\n"
                       f"Сумма: <b>{amount:,} сум</b>\n\n"
                       f"✅ Подтверждено: <b>{confirmer_name}</b>")
        asyncio.create_task(_edit_tg_handover_msg(row["tg_chat_id"], row["tg_msg_id"], edited_text))

    return {"ok": True, "handover": row}


@app.post("/api/admin/cash/staff-handover/{handover_id}/reject")
async def reject_staff_handover(handover_id: int, staff=Depends(get_current_staff)):
    row = await db.reject_cash_handover(handover_id, staff["id"])
    if not row:
        raise HTTPException(status_code=404, detail="Не найдено или уже обработано")
    rejector_name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
    amount = int(float(row.get("amount", 0)))
    asyncio.create_task(send_web_push(
        row["from_staff_id"],
        title="❌ Передача наличных отклонена",
        body=f"{rejector_name} отклонил {amount:,} сум",
        push_type="cash_rejected",
    ))
    from_staff = await db.get_staff_by_id(row["from_staff_id"])
    if from_staff and from_staff.get("tg_id"):
        asyncio.create_task(send_tg(
            int(from_staff["tg_id"]),
            f"❌ <b>Передача наличных отклонена</b>\n{rejector_name} отклонил получение <b>{amount:,} сум</b>",
        ))

    # Обновить TG-сообщение у получателя (убрать кнопки)
    if row.get("tg_chat_id") and row.get("tg_msg_id"):
        from_name_for_tg = " ".join(filter(None,[from_staff.get("last_name",""),from_staff.get("first_name","")])).strip() if from_staff else "—"
        edited_text = (f"💵 <b>Вам сдали наличные</b>\n"
                       f"От: {from_name_for_tg}\n"
                       f"Сумма: <b>{amount:,} сум</b>\n\n"
                       f"❌ Отклонено: <b>{rejector_name}</b>")
        asyncio.create_task(_edit_tg_handover_msg(row["tg_chat_id"], row["tg_msg_id"], edited_text))

    return {"ok": True}


@app.get("/api/admin/cash/pending-handovers")
async def get_pending_handovers(staff=Depends(get_current_staff)):
    rows = await db.get_pending_handovers_for(staff["id"])
    return {"ok": True, "handovers": rows}

@app.post("/api/admin/cash/bank-deposit")
async def bank_deposit(
    from_staff_id: int   = Body(None,   embed=True),
    to_type:       str   = Body("bank", embed=True),
    amount:        float = Body(...,    embed=True),
    note:          str   = Body("",    embed=True),
    staff=Depends(get_current_staff),
):
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только для admin")
    if to_type not in ("bank", "safe"):
        raise HTTPException(status_code=400, detail="to_type must be bank or safe")
    depositor_id = from_staff_id or staff.get("id")
    row = await db.create_bank_deposit(depositor_id, amount, to_type, note)
    if not row:
        raise HTTPException(status_code=500, detail="Ошибка создания")
    return {"ok": True, "deposit": row}

@app.get("/api/admin/cash/bank-deposits")
async def bank_deposits_list(staff=Depends(get_current_staff)):
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только для admin")
    rows = await db.get_bank_deposits()
    return {"ok": True, "deposits": rows}

@app.post("/api/admin/cash/safe-deposit")
async def admin_safe_deposit(
    amount: float = Body(..., embed=True),
    note:   str   = Body("",  embed=True),
    staff=Depends(get_current_staff),
):
    """Создать pending-запрос сдачи в сейф (для администратора из staff.html)."""
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только для admin")
    if amount <= 0:
        raise HTTPException(status_code=400, detail="amount required")
    row = await db.add_safe_deposit(staff["id"], amount, note)
    if not row:
        raise HTTPException(status_code=500, detail="Ошибка создания")
    handover_id = row["id"]
    tg_id = staff.get("tg_id")
    if tg_id:
        from_name = f"{staff.get('last_name','')} {staff.get('first_name','')}".strip()
        fmt_amt = f"{round(amount):,}".replace(",", " ")
        text = (f"🔒 <b>Сдача в сейф</b>\n"
                f"От: {from_name}\n"
                f"Сумма: {fmt_amt} сум"
                + (f"\n📝 {note}" if note else ""))
        kb = {"inline_keyboard": [[
            {"text": "✅ Подтвердить", "callback_data": f"safe_confirm_{handover_id}"},
            {"text": "❌ Отклонить",   "callback_data": f"safe_reject_{handover_id}"},
        ]]}
        msg_id = await _send_tg_with_kb(str(tg_id), text, kb)
        if msg_id:
            await db.update_handover_tg_msg(handover_id, int(tg_id), msg_id)
    return {"ok": True, "id": handover_id}

@app.get("/api/admin/cash/pending-safe-deposits")
async def pending_safe_deposits(staff=Depends(get_current_staff)):
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только для admin")
    rows = await db.get_pending_safe_deposits()
    return {"ok": True, "deposits": rows}

@app.post("/api/admin/cash/safe-deposit/{deposit_id}/confirm")
async def confirm_safe_deposit(deposit_id: int, staff=Depends(get_current_staff)):
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только для admin")
    row = await db.confirm_cash_handover(deposit_id, staff["id"])
    if not row:
        raise HTTPException(status_code=404, detail="Не найдено")
    if row.get("tg_chat_id") and row.get("tg_msg_id"):
        fmt_amt = f"{round(row.get('amount',0)):,}".replace(",", " ")
        await _edit_tg_handover_msg(row["tg_chat_id"], row["tg_msg_id"],
                                    f"✅ Сдача в сейф подтверждена\nСумма: {fmt_amt} сум")
    return {"ok": True}

@app.post("/api/admin/cash/safe-deposit/{deposit_id}/reject")
async def reject_safe_deposit(deposit_id: int, staff=Depends(get_current_staff)):
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только для admin")
    row = await db.reject_cash_handover(deposit_id, staff["id"])
    if not row:
        raise HTTPException(status_code=404, detail="Не найдено")
    if row.get("tg_chat_id") and row.get("tg_msg_id"):
        fmt_amt = f"{round(row.get('amount',0)):,}".replace(",", " ")
        await _edit_tg_handover_msg(row["tg_chat_id"], row["tg_msg_id"],
                                    f"❌ Сдача в сейф отклонена\nСумма: {fmt_amt} сум")
    return {"ok": True}


@app.delete("/api/admin/cash/handovers/{handover_id}")
async def admin_cancel_handover(handover_id: int, _cid: int = Depends(_get_admin_cid)):
    row = await db.cancel_cash_handover(handover_id, None, is_admin=True)
    if not row:
        raise HTTPException(status_code=404, detail="Не найдено")
    return {"ok": True}


@app.delete("/api/admin/cash/my-handovers/{handover_id}")
async def staff_cancel_own_handover(handover_id: int, staff=Depends(get_current_staff)):
    is_admin = staff.get("role") == "admin"
    row = await db.cancel_cash_handover(handover_id, staff["id"], is_admin=is_admin)
    if not row:
        raise HTTPException(status_code=404, detail="Не найдено или нет прав")
    return {"ok": True}


# ── Расходы ───────────────────────────────────────────────────────────────────

@app.get("/api/admin/expenses/categories")
async def expense_categories_list(staff=Depends(get_current_staff)):
    cats = await db.get_expense_categories()
    return {"ok": True, "categories": cats}

@app.get("/api/admin/expenses/categories/tree")
async def expense_categories_tree(staff=Depends(get_current_staff)):
    tree = await db.get_expense_categories_tree()
    return {"ok": True, "categories": tree}

@app.post("/api/admin/expenses/categories")
async def create_expense_category(
    name_ru:          str   = Body(..., embed=True),
    name_uz:          str   = Body(..., embed=True),
    icon:             str   = Body("",      embed=True),
    parent_id:        int   = Body(None,    embed=True),
    approve_level:    str   = Body("manager", embed=True),
    receipt_required: bool  = Body(False,   embed=True),
    amount_threshold: float = Body(None,    embed=True),
    sort_order:       int   = Body(0,       embed=True),
    for_staff:        bool  = Body(False,   embed=True),
    staff=Depends(get_current_staff),
):
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только для admin")
    cat = await db.create_expense_category(
        name_ru, name_uz, icon, parent_id, approve_level,
        receipt_required, amount_threshold, sort_order, for_staff)
    return {"ok": True, "category": cat}


@app.put("/api/admin/expenses/categories/{cat_id}")
async def update_expense_category(
    cat_id:           int,
    name_ru:          str   = Body(..., embed=True),
    name_uz:          str   = Body(..., embed=True),
    icon:             str   = Body("",      embed=True),
    parent_id:        int   = Body(None,    embed=True),
    approve_level:    str   = Body("manager", embed=True),
    receipt_required: bool  = Body(False,   embed=True),
    amount_threshold: float = Body(None,    embed=True),
    sort_order:       int   = Body(0,       embed=True),
    active:           bool  = Body(True,    embed=True),
    for_staff:        bool  = Body(False,   embed=True),
    staff=Depends(get_current_staff),
):
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только для admin")
    cat = await db.update_expense_category(
        cat_id, name_ru, name_uz, icon, parent_id, approve_level,
        receipt_required, amount_threshold, sort_order, active, for_staff)
    if not cat:
        raise HTTPException(status_code=404, detail="Категория не найдена")
    return {"ok": True, "category": cat}

@app.delete("/api/admin/expenses/categories/{cat_id}")
async def delete_expense_category(cat_id: int, staff=Depends(get_current_staff)):
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только для admin")
    result = await db.delete_expense_category(cat_id)
    if not result.get("ok"):
        err = result.get("error", "unknown")
        if err == "has_expenses":
            raise HTTPException(status_code=400, detail="Есть расходы по этой категории")
        if err == "has_children":
            raise HTTPException(status_code=400, detail="Сначала удалите подкатегории")
        raise HTTPException(status_code=400, detail=err)
    return {"ok": True}

@app.post("/api/admin/expenses")
async def create_expense(
    category_id:  int   = Body(..., embed=True),
    amount:       float = Body(..., embed=True),
    description:  str   = Body("",   embed=True),
    for_staff_id: int   = Body(None, embed=True),
    staff=Depends(get_current_staff),
):
    branch = staff.get("branch") or ""
    row = await db.create_expense(category_id, amount, description, staff["id"], branch, for_staff_id)
    if not row:
        raise HTTPException(status_code=500, detail="Ошибка создания расхода")
    # Пуш менеджерам/admin о новом расходе
    cat_rows = await db.get_expense_categories()
    cat = next((c for c in cat_rows if c["id"] == category_id), {})
    creator = " ".join(filter(None, [staff.get("last_name"), staff.get("first_name")])) or staff.get("login","")
    title = f"{cat.get('icon','🧾')} Новый расход: {int(amount):,} сум"
    body  = f"{creator} · {cat.get('name_ru','')}"
    # Уведомить всех менеджеров кассы и admin у которых этот branch
    managers = await db.get_cashiers()
    for m in managers:
        asyncio.create_task(send_web_push(m["id"], title=title, body=body, push_type="new_expense"))
    return {"ok": True, "expense": row}

@app.get("/api/admin/expenses/my")
async def my_expenses(staff=Depends(get_current_staff)):
    rows = await db.get_my_expenses(staff["id"])
    return {"ok": True, "expenses": rows}

@app.get("/api/admin/expenses")
async def list_expenses(
    branch:      str = None,
    status:      str = None,
    category_id: int = None,
    staff=Depends(get_current_staff),
):
    role = staff.get("sub") or staff.get("role","")
    can_manage = staff.get("can_manage_cash") or role == "admin"
    if not can_manage:
        raise HTTPException(status_code=403, detail="Нет доступа")
    rows = await db.get_expenses(branch=branch, status=status, category_id=category_id)
    return {"ok": True, "expenses": rows}

@app.get("/api/admin/expenses/pending-manager")
async def pending_for_manager(staff=Depends(get_current_staff)):
    can_manage = staff.get("can_manage_cash") or staff.get("role") == "admin"
    if not can_manage:
        raise HTTPException(status_code=403, detail="Нет доступа")
    branch = staff.get("branch") if staff.get("role") != "admin" else None
    rows = await db.get_pending_expenses_for_manager(branch)
    return {"ok": True, "expenses": rows}

@app.get("/api/admin/expenses/pending-admin")
async def pending_for_admin(staff=Depends(get_current_staff)):
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    rows = await db.get_pending_expenses_for_admin()
    return {"ok": True, "expenses": rows}

@app.patch("/api/admin/expenses/{expense_id}/approve")
async def approve_expense(expense_id: int, staff=Depends(get_current_staff)):
    is_admin = staff.get("role") == "admin"
    can_manage = staff.get("can_manage_cash") or is_admin
    if not can_manage:
        raise HTTPException(status_code=403, detail="Нет доступа")
    if is_admin:
        row = await db.approve_expense_admin(expense_id, staff["id"])
    else:
        row = await db.approve_expense_manager(expense_id, staff["id"])
    if not row:
        raise HTTPException(status_code=404, detail="Расход не найден или уже обработан")
    approver = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
    new_status = row.get("status","")
    # Пуш создателю расхода
    if row.get("created_by_staff_id"):
        title = "✅ Расход утверждён" if new_status == "paid" else "📋 Расход ждёт Admin"
        body  = f"{approver} · {int(float(row.get('amount',0))):,} сум"
        asyncio.create_task(send_web_push(row["created_by_staff_id"], title=title, body=body, push_type="expense_approved"))
    # Если mgr_approved — пуш admin
    if new_status == "mgr_approved":
        admin_rows = await db.get_expenses(status=None)  # получим admin staff через другой путь
        async with db.pool.acquire() as conn:
            admins = await conn.fetch("SELECT id FROM staff WHERE role='admin' AND active=TRUE")
        for a in admins:
            asyncio.create_task(send_web_push(a["id"], title="📋 Расход ждёт подтверждения", body=body, push_type="expense_mgr_approved"))
    return {"ok": True, "expense": row}

@app.patch("/api/admin/expenses/{expense_id}/reject")
async def reject_expense(
    expense_id: int,
    reason: str = Body("", embed=True),
    staff=Depends(get_current_staff),
):
    can_manage = staff.get("can_manage_cash") or staff.get("role") == "admin"
    if not can_manage:
        raise HTTPException(status_code=403, detail="Нет доступа")
    row = await db.reject_expense(expense_id, staff["id"], reason)
    if not row:
        raise HTTPException(status_code=404, detail="Расход не найден или уже обработан")
    if row.get("created_by_staff_id"):
        rejecter = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
        asyncio.create_task(send_web_push(
            row["created_by_staff_id"],
            title="❌ Расход отклонён",
            body=f"{rejecter}" + (f": {reason}" if reason else ""),
            push_type="expense_rejected",
        ))
    return {"ok": True, "expense": row}

@app.patch("/api/admin/expenses/{expense_id}/pay")
async def pay_expense(
    expense_id: int,
    paid_from: str = Body("cash", embed=True),
    staff=Depends(get_current_staff),
):
    if staff.get("role") != "admin" and not staff.get("can_manage_cash"):
        raise HTTPException(status_code=403, detail="Нет доступа")
    if paid_from not in ("cash", "safe", "bank"):
        raise HTTPException(status_code=400, detail="Некорректный источник выплаты")
    row = await db.mark_expense_paid(expense_id, staff["id"], paid_from)
    if not row:
        raise HTTPException(status_code=404, detail="Расход не найден или не в статусе 'утверждён'")
    if row.get("created_by_staff_id"):
        payer = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
        asyncio.create_task(send_web_push(
            row["created_by_staff_id"],
            title="💸 Расход выплачен",
            body=f"{payer} · {int(float(row.get('amount',0))):,} сум",
            push_type="expense_paid",
        ))
    return {"ok": True, "expense": row}


@app.post("/api/admin/expenses/{expense_id}/receipt")
async def upload_expense_receipt(
    expense_id: int,
    file: UploadFile = File(...),
    staff=Depends(get_current_staff),
):
    async with db.pool.acquire() as conn:
        exp = await conn.fetchrow("SELECT * FROM expenses WHERE id=$1", expense_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Расход не найден")
    if exp["created_by_staff_id"] != staff["id"] and staff.get("sub") != "admin" and not staff.get("can_manage_cash"):
        raise HTTPException(status_code=403, detail="Нет доступа")
    media_ch = await _get_media_channel()
    if not BOT_TOKEN or not media_ch:
        raise HTTPException(status_code=503, detail="Медиа-хранилище не настроено")
    content_type = file.content_type or "image/jpeg"
    tg_method = "sendVideo" if content_type.startswith("video/") else "sendPhoto" if content_type.startswith("image/") else "sendDocument"
    tg_field  = "video"    if content_type.startswith("video/") else "photo"    if content_type.startswith("image/") else "document"
    tg_type   = "video"    if content_type.startswith("video/") else "photo"    if content_type.startswith("image/") else "document"
    staff_name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
    file_bytes = await file.read()
    form = aiohttp.FormData()
    form.add_field("chat_id", str(media_ch))
    form.add_field(tg_field, file_bytes, filename=file.filename or "receipt.jpg", content_type=content_type)
    form.add_field("caption", f"🧾 Чек расхода #{expense_id}\n👤 {staff_name}")
    async with aiohttp.ClientSession() as s:
        async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{tg_method}", data=form) as r:
            result = await r.json()
    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=f"Telegram: {result.get('description','upload failed')}")
    msg = result["result"]
    file_id = msg["photo"][-1]["file_id"] if tg_type == "photo" else msg[tg_type]["file_id"]
    row = await db.save_expense_receipt(expense_id, file_id)
    return {"ok": True, "receipt_url": file_id, "expense": row}


async def _update_api_channel_stop(order_id: int):
    """Обновляет текст+кнопки сообщения заказа в канале после изменения оплаты."""
    try:
        info = await db.get_channel_stop_full(order_id)
        if not info or not info.get("msg_id") or not info.get("channel_id"):
            logging.warning(f"_update_api_channel_stop order={order_id}: no channel info (msg_id={info.get('msg_id') if info else None}, ch={info.get('channel_id') if info else None})")
            return
        ch_id = info["channel_id"]
        num = (info.get("sort_order") or 1)
        new_text = _build_stop_text_short(info, num)
        status = info.get("status", "delivery")
        new_kb = _route_pickup_kb(order_id, status, closed=await _is_route_closed_for_order(order_id))
        async with aiohttp.ClientSession() as _sess:
            resp = await _sess.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
                json={"chat_id": ch_id, "message_id": int(info["msg_id"]),
                      "text": new_text, "reply_markup": new_kb,
                      "parse_mode": "HTML", "disable_web_page_preview": True},
                timeout=aiohttp.ClientTimeout(total=5))
            res_json = await resp.json()
            if not res_json.get("ok"):
                logging.warning(f"_update_api_channel_stop TG error order={order_id} ch={ch_id} msg={info['msg_id']}: {res_json}")
            else:
                logging.info(f"_update_api_channel_stop ok order={order_id} ch={ch_id} msg={info['msg_id']}")
    except Exception as e:
        logging.warning(f"_update_api_channel_stop order={order_id}: {e}")


async def _get_order_channel_url(order_id: int) -> str | None:
    """Строит ссылку на сообщение заказа в канале водителей."""
    branch, msg_id = await db.get_channel_msg_for_order(order_id)
    ch_key = "delivery_channel_navoi_id" if branch == "navoi" else "delivery_channel_zarafshan_id"
    lnk_key = "delivery_channel_navoi_link" if branch == "navoi" else "delivery_channel_zarafshan_link"
    ch_id_str = await _get_cfg(ch_key)
    if ch_id_str and msg_id:
        try:
            ch_abs = str(abs(int(ch_id_str)))
            if ch_abs.startswith("100"): ch_abs = ch_abs[3:]
            return f"https://t.me/c/{ch_abs}/{msg_id}"
        except Exception:
            pass
    return await _get_cfg(lnk_key)


async def _notify_driver_payment(row: dict, order_id: int, text: str):
    """Отправляет уведомление водителю в личку."""
    if not BOT_TOKEN:
        return
    driver_tg_id = row.get("driver_tg_id")
    # Fallback: берём tg_id из staff по created_by_staff_id
    if not driver_tg_id:
        staff_id = row.get("created_by_staff_id")
        if staff_id and db.pool:
            try:
                async with db.pool.acquire() as _c:
                    r = await _c.fetchrow("SELECT tg_id FROM staff WHERE id=$1", int(staff_id))
                    if r and r["tg_id"]:
                        driver_tg_id = r["tg_id"]
            except Exception:
                pass
    if not driver_tg_id:
        return
    try:
        await _send_tg_with_kb(driver_tg_id, text, {"inline_keyboard": []})
    except Exception as e:
        logging.warning(f"_notify_driver_payment error: {e}")


# ── Подтверждение оплат картой/переводом ──────────────────────────────────────

@app.get("/api/admin/cash/unconfirmed-payments")
async def get_unconfirmed_payments(_=Depends(get_current_staff)):
    rows = await db.get_unconfirmed_payments()
    return {"ok": True, "payments": rows}


@app.post("/api/admin/orders/{order_id}/payments/{payment_id}/confirm")
async def confirm_payment(order_id: int, payment_id: int, staff=Depends(get_current_staff)):
    if staff.get("sub") != "admin" and not staff.get("can_manage_cash"):
        raise HTTPException(status_code=403, detail="Нет доступа")
    row = await db.confirm_payment(payment_id, staff["id"])
    if not row:
        raise HTTPException(status_code=404, detail="Платёж не найден")
    name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
    mLabel = {"cash":"💵 Нал","card":"💳 Карта","transfer":"📲 Перевод"}
    details = f"Подтверждён платёж: {int(float(row['amount'])):,} сум ({mLabel.get(row['method'],'')})"
    await db.add_order_activity(order_id, staff["id"], name, "payment_confirmed", details)
    async with db.pool.acquire() as _c:
        _o2 = await _c.fetchrow("SELECT order_num FROM orders WHERE id=$1", order_id)
    order_label2 = _o2["order_num"] if _o2 and _o2["order_num"] else f"#{order_id}"
    driver2 = row.get("created_by") or ""
    ch = await db.get_cash_tg_channel()
    cash_text = (f"✅ <b>Платёж подтверждён</b> · {order_label2}\n"
                 f"{mLabel.get(row['method'],'')} · <b>{int(float(row['amount'])):,} сум</b>\n"
                 + (f"💼 Принял: {driver2}\n" if driver2 else "")
                 + f"Подтвердил: {name}")
    asyncio.create_task(_send_tg_cash(ch, cash_text))
    asyncio.create_task(_notify_driver_payment(dict(row), order_id,
        f"✅ <b>Платёж подтверждён</b> · {order_label2}\n"
        f"{mLabel.get(row['method'],'')} · <b>{int(float(row['amount'])):,} сум</b>\n"
        f"Подтвердил: {name}"))
    asyncio.create_task(_update_api_channel_stop(order_id))
    # Push водителю чтобы staff.html обновил вкладку доставки
    drv_staff_id = await _get_driver_staff_id(order_id, dict(row))
    if drv_staff_id:
        asyncio.create_task(send_web_push(drv_staff_id, "✅ Оплата подтверждена",
                                          f"Заказ #{order_id} · {int(float(row['amount'])):,} сум",
                                          order_id=order_id, push_type="delivery_reload"))
    # Auto-deliver: если заказ в статусе "delivery" и долг погашен после подтверждения
    try:
        async with db.pool.acquire() as _c:
            o = await _c.fetchrow("""
                SELECT o.status,
                       COALESCE((SELECT SUM(COALESCE(sqm*price_per_sqm,0)) FROM order_items WHERE order_id=o.id),0) AS items_total,
                       COALESCE(o.discount_sum,0)+COALESCE(o.delivery_discount,0)+COALESCE(o.manual_discount,0) AS disc,
                       COALESCE((SELECT SUM(amount) FROM order_payments
                                 WHERE order_id=o.id AND confirmed=TRUE),0) AS paid_conf
                FROM orders o WHERE o.id=$1
            """, order_id)
        if o and o["status"] == "delivery":
            debt_left = float(o["items_total"]) - float(o["disc"]) - float(o["paid_conf"])
            if debt_left <= 0:
                async with db.pool.acquire() as _c:
                    await _c.execute("UPDATE orders SET status='delivered',delivered_at=NOW() WHERE id=$1", order_id)
                    await _c.execute("UPDATE route_orders SET stop_status='done' WHERE order_id=$1", order_id)
    except Exception as _ae:
        logging.warning(f"confirm_payment auto-deliver: {_ae}")
    return {"ok": True, "payment": row}


@app.post("/api/admin/orders/{order_id}/payments/{payment_id}/reject")
async def reject_payment(order_id: int, payment_id: int,
                         note: str = Body("", embed=True),
                         staff=Depends(get_current_staff)):
    if staff.get("sub") != "admin" and not staff.get("can_manage_cash"):
        raise HTTPException(status_code=403, detail="Нет доступа")
    row = await db.reject_payment(payment_id, staff["id"], note)
    if not row:
        raise HTTPException(status_code=404, detail="Платёж не найден")
    name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
    mLabel = {"cash":"💵 Нал","card":"💳 Карта","transfer":"📲 Перевод"}
    details = f"Платёж отклонён: {int(float(row['amount'])):,} сум ({mLabel.get(row['method'],'')})"
    await db.add_order_activity(order_id, staff["id"], name, "payment_rejected", details)
    ch = await db.get_cash_tg_channel()
    async with db.pool.acquire() as _c:
        _o = await _c.fetchrow("SELECT order_num FROM orders WHERE id=$1", order_id)
    order_label = _o["order_num"] if _o and _o["order_num"] else f"#{order_id}"
    driver = row.get("created_by") or ""
    text = (f"❌ <b>Платёж отклонён</b> · {order_label}\n"
            f"{mLabel.get(row['method'],'')} · <b>{int(float(row['amount'])):,} сум</b>\n"
            + (f"💼 Принял: {driver}\n" if driver else "")
            + f"Отклонил: {name}"
            + (f"\n📝 {note}" if note else ""))
    asyncio.create_task(_send_tg_cash(ch, text))
    asyncio.create_task(_notify_driver_payment(dict(row), order_id, text))
    asyncio.create_task(_update_api_channel_stop(order_id))
    drv_staff_id = await _get_driver_staff_id(order_id, dict(row))
    if drv_staff_id:
        push_body = f"Заказ #{order_id} · {int(float(row['amount'])):,} сум" + (f" · {note}" if note else "")
        asyncio.create_task(send_web_push(drv_staff_id, "❌ Оплата отклонена", push_body,
                                          order_id=order_id, push_type="payment_rejected"))
    return {"ok": True, "payment": row}


@app.get("/api/admin/orders/{order_id}/payments/{payment_id}/receipt-file")
async def get_receipt_file(order_id: int, payment_id: int, staff=Depends(get_current_staff)):
    """Возвращает URL для просмотра чека через TG."""
    if not db.pool:
        raise HTTPException(status_code=503)
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT receipt_url, receipt_file_id FROM order_payments WHERE id=$1 AND order_id=$2", payment_id, order_id)
    if not row or (not row["receipt_url"] and not row["receipt_file_id"]):
        raise HTTPException(status_code=404, detail="Чек не найден")
    file_id = row["receipt_url"] or row["receipt_file_id"]
    if not BOT_TOKEN:
        raise HTTPException(status_code=503, detail="Бот не настроен")
    try:
        from fastapi.responses import StreamingResponse
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile",
                             params={"file_id": file_id},
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
            if not data.get("ok"):
                raise HTTPException(status_code=404, detail="Файл не найден в TG")
            file_path = data["result"]["file_path"]
            file_url  = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
            # Проксируем содержимое — браузер не может напрямую читать TG-файлы (CORS)
            ext = file_path.rsplit(".", 1)[-1].lower() if "." in file_path else ""
            ctype = ("video/mp4" if ext in ("mp4","mov","avi") else
                     "image/jpeg" if ext in ("jpg","jpeg") else
                     "image/png"  if ext == "png" else
                     "application/octet-stream")
            async with s.get(file_url, timeout=aiohttp.ClientTimeout(total=30)) as fr:
                content = await fr.read()
        return StreamingResponse(iter([content]), media_type=ctype,
                                 headers={"Content-Disposition": "inline"})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/staff/payments/{payment_id}/receipt")
async def get_driver_payment_receipt(payment_id: int, _=Depends(get_current_staff)):
    """Отдаёт фото квитанции, сохранённое водителем через Telegram (receipt_file_id)."""
    if not db.pool:
        raise HTTPException(status_code=503)
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT receipt_file_id FROM order_payments WHERE id=$1", payment_id)
    if not row or not row["receipt_file_id"]:
        raise HTTPException(status_code=404, detail="Квитанция не найдена")
    if not BOT_TOKEN:
        raise HTTPException(status_code=503, detail="Бот не настроен")
    try:
        from fastapi.responses import StreamingResponse
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile",
                             params={"file_id": row["receipt_file_id"]},
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
            if not data.get("ok"):
                raise HTTPException(status_code=404, detail="Файл не найден в TG")
            file_path = data["result"]["file_path"]
            file_url  = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
            ext = file_path.rsplit(".", 1)[-1].lower() if "." in file_path else ""
            ctype = ("image/jpeg" if ext in ("jpg","jpeg") else
                     "image/png"  if ext == "png" else "image/jpeg")
            async with s.get(file_url, timeout=aiohttp.ClientTimeout(total=30)) as fr:
                content = await fr.read()
        return StreamingResponse(iter([content]), media_type=ctype,
                                 headers={"Content-Disposition": "inline"})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/orders/{order_id}/payments/{payment_id}/receipt")
async def upload_payment_receipt(
    order_id:   int,
    payment_id: int,
    file: UploadFile = File(...),
    staff=Depends(get_current_staff),
):
    content = await file.read()
    ct = file.content_type or "image/jpeg"
    tg_method = "sendDocument" if not ct.startswith("image/") else "sendPhoto"
    field     = "document" if tg_method == "sendDocument" else "photo"
    name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")

    # Порядковый номер платежа внутри заказа
    pay_num = 1
    if db.pool:
        try:
            async with db.pool.acquire() as conn:
                pay_num = await conn.fetchval(
                    "SELECT COUNT(*) FROM order_payments WHERE order_id=$1 AND id<=$2",
                    order_id, payment_id) or 1
        except Exception:
            pass

    mLabel = {"cash":"💵 Нал","card":"💳 Карта","transfer":"📲 Перевод"}
    pLabel = {"prepayment":"Предоплата","partial":"Частичная оплата","final":"Окончательный расчёт",
              "delivery":"Оплата при доставке"}
    # Получаем данные платежа + order_num для caption
    pay_row = None
    order_num_str = f"#{order_id}"
    if db.pool:
        try:
            async with db.pool.acquire() as conn:
                pay_row = await conn.fetchrow(
                    "SELECT op.amount, op.method, op.purpose, o.order_num "
                    "FROM order_payments op JOIN orders o ON o.id=op.order_id "
                    "WHERE op.id=$1", payment_id)
                if pay_row and pay_row["order_num"]:
                    order_num_str = f"#{order_id} · {pay_row['order_num']}"
        except Exception:
            pass

    amount_str  = f"{int(float(pay_row['amount'])):,} сум" if pay_row else ""
    method_str  = mLabel.get(pay_row['method'], pay_row['method'] if pay_row else '') if pay_row else ""
    purpose_str = pLabel.get(pay_row['purpose'], '') if pay_row else ""
    info_line   = " · ".join(filter(None, [purpose_str, method_str]))
    caption = (f"🧾 Чек · Заказ {order_num_str} · Платёж №{pay_num}\n"
               f"{info_line}\n"
               f"💰 {amount_str}\n"
               f"👤 {name}")

    reply_markup = _json.dumps({
        "inline_keyboard": [[{"text": "🟢 Проверить", "callback_data": f"chk:g:{order_id}"}]]
    })

    receipt_url = None
    cash_ch = await db.get_cash_tg_channel()
    upload_ch = cash_ch or await _get_media_channel()
    if BOT_TOKEN and upload_ch:
        try:
            async with aiohttp.ClientSession() as s:
                form = aiohttp.FormData()
                form.add_field("chat_id", str(upload_ch))
                form.add_field(field, content, filename=file.filename or "receipt.jpg", content_type=ct)
                form.add_field("caption", caption)
                form.add_field("reply_markup", reply_markup)
                async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{tg_method}", data=form,
                                  timeout=aiohttp.ClientTimeout(total=15)) as r:
                    res = await r.json()
                if res.get("ok"):
                    msg = res["result"]
                    receipt_url = msg["photo"][-1]["file_id"] if tg_method == "sendPhoto" else msg["document"]["file_id"]
        except Exception as e:
            logging.warning(f"receipt upload error: {e}")

    row = await db.save_payment_receipt(payment_id, receipt_url or file.filename)
    # Push к менеджерам только после того как чек сохранён (избегаем race condition)
    if pay_row and pay_row.get("purpose") == "delivery":
        try:
            cashiers = await db.get_all_cashiers_for_push()
            drv_id = staff.get("id")
            push_body = f"Заказ {order_num_str} · {amount_str}"
            for c in cashiers:
                if c["id"] != drv_id:
                    asyncio.create_task(send_web_push(
                        c["id"], "💳 Оплата на проверку (Доставка)", push_body,
                        order_id=order_id, push_type="payment_review"))
        except Exception as _pe:
            logging.warning(f"receipt push error: {_pe}")
    return {"ok": True, "receipt_url": receipt_url, "uploaded": receipt_url is not None}


# ── Plans (roadmap) ─────────────────────────────────────────────────────────

class PlanBody(BaseModel):
    title: str
    description: str = ""
    priority: str = "normal"

class PlanUpdateBody(BaseModel):
    title: str | None = None
    description: str | None = None
    priority: str | None = None
    status: str | None = None

@app.get("/api/admin/plans")
async def list_plans(_=Depends(_get_admin)):
    return {"plans": await db.get_plans()}

@app.post("/api/admin/plans")
async def create_plan(body: PlanBody, _=Depends(_get_admin)):
    plan = await db.create_plan(body.title, body.description, body.priority)
    return {"ok": True, "plan": plan}

@app.put("/api/admin/plans/{plan_id}")
async def update_plan(plan_id: int, body: PlanUpdateBody, _=Depends(_get_admin)):
    from datetime import datetime, timezone
    kwargs = {k: v for k, v in body.model_dump().items() if v is not None}
    if kwargs.get("status") == "done" and "done_at" not in kwargs:
        kwargs["done_at"] = datetime.now(timezone.utc)
    if kwargs.get("status") == "pending":
        kwargs["done_at"] = None
    plan = await db.update_plan(plan_id, **kwargs)
    return {"ok": True, "plan": plan}

@app.delete("/api/admin/plans/{plan_id}")
async def delete_plan(plan_id: int, _=Depends(_get_admin)):
    await db.delete_plan(plan_id)
    return {"ok": True}

# ── TEMP: чистка БД от мусора (удалить группу после чистки) ─────────────────
_TEMP_TABLES = {
    'order_payments':   'Оплаты',
    'order_items':      'Позиции заказов',
    'order_activity':   'Активность / статусы',
    'order_photos':     'Фото заказов',
    'order_item_media': 'Медиа позиций',
}

class _TempDeleteBody(BaseModel):
    ids: list[int]

@app.get("/api/admin/temp/orphan-counts")
async def temp_orphan_counts(cid: int = Depends(_get_admin_cid)):
    if not db.pool: raise HTTPException(503)
    result = {}
    async with db.pool.acquire() as conn:
        for tbl in _TEMP_TABLES:
            result[tbl] = int(await conn.fetchval(
                f"SELECT COUNT(*) FROM {tbl} t LEFT JOIN orders o ON o.id=t.order_id "
                f"WHERE o.id IS NULL AND t.company_id=$1", cid
            ))
    return result

@app.get("/api/admin/temp/records/{table}")
async def temp_get_records(table: str, orphans_only: bool = True, cid: int = Depends(_get_admin_cid)):
    if table not in _TEMP_TABLES: raise HTTPException(404, "Unknown table")
    if not db.pool: raise HTTPException(503)
    where = "WHERE o.id IS NULL AND t.company_id=$1" if orphans_only else "WHERE t.company_id=$1"
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT t.*, (o.id IS NULL) AS is_orphan
            FROM {table} t
            LEFT JOIN orders o ON o.id = t.order_id
            {where}
            ORDER BY t.id DESC LIMIT 1000
        """, cid)
    return {"rows": [dict(r) for r in rows], "table": table, "label": _TEMP_TABLES[table]}

@app.delete("/api/admin/temp/records/{table}")
async def temp_delete_records(table: str, body: _TempDeleteBody, cid: int = Depends(_get_admin_cid)):
    if table not in _TEMP_TABLES: raise HTTPException(404, "Unknown table")
    if not body.ids: return {"ok": True, "deleted": 0}
    if not db.pool: raise HTTPException(503)
    async with db.pool.acquire() as conn:
        result = await conn.execute(
            f"DELETE FROM {table} WHERE id = ANY($1::int[]) AND company_id=$2", body.ids, cid)
    deleted = int(result.split()[-1]) if result else 0
    return {"ok": True, "deleted": deleted}

# ── История чатов (admin) ─────────────────────────────────────────────────────

class _ChatHistoryDeleteBody(BaseModel):
    date_from: str   # YYYY-MM-DD
    date_to:   str   # YYYY-MM-DD

@app.get("/api/admin/chat/history/stats")
async def chat_history_stats(date_from: str, date_to: str, cid: int = Depends(_get_admin_cid)):
    if not db.pool: raise HTTPException(503)
    try:
        df = datetime.strptime(date_from, "%Y-%m-%d").date()
        dt = datetime.strptime(date_to,   "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, "date_from / date_to must be YYYY-MM-DD")
    async with db.pool.acquire() as conn:
        sessions = int(await conn.fetchval(
            "SELECT COUNT(*) FROM chat_sessions WHERE status='closed' AND created_at::date BETWEEN $1 AND $2 AND company_id=$3",
            df, dt, cid
        ))
        messages = int(await conn.fetchval(
            """SELECT COUNT(*) FROM chat_messages cm
               JOIN chat_sessions cs ON cs.id = cm.session_id
               WHERE cs.status='closed' AND cs.created_at::date BETWEEN $1 AND $2 AND cs.company_id=$3""",
            df, dt, cid
        ))
    return {"ok": True, "sessions": sessions, "messages": messages, "date_from": date_from, "date_to": date_to}

@app.delete("/api/admin/chat/history")
async def chat_history_delete(body: _ChatHistoryDeleteBody, cid: int = Depends(_get_admin_cid)):
    if not db.pool: raise HTTPException(503)
    try:
        df = datetime.strptime(body.date_from, "%Y-%m-%d").date()
        dt = datetime.strptime(body.date_to,   "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, "date_from / date_to must be YYYY-MM-DD")
    async with db.pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM chat_sessions WHERE status='closed' AND created_at::date BETWEEN $1 AND $2 AND company_id=$3",
            df, dt, cid
        )
    deleted = int(result.split()[-1]) if result else 0
    return {"ok": True, "deleted_sessions": deleted}

# ── Настройки кассы (admin) ───────────────────────────────────────────────────

@app.get("/api/admin/settings/cash-channel")
async def get_cash_channel(_=Depends(_get_admin)):
    ch = await db.get_cash_tg_channel()
    return {"ok": True, "cash_tg_channel_id": ch}

@app.put("/api/admin/settings/cash-channel")
async def set_cash_channel(cash_tg_channel_id: str = Body(..., embed=True), cid: int = Depends(_get_admin_cid)):
    if not db.pool: raise HTTPException(503)
    await _upsert_setting("cash_tg_channel_id", cash_tg_channel_id, cid)
    return {"ok": True}

@app.get("/api/admin/settings/media-channel")
async def get_media_channel(_=Depends(_get_admin)):
    ch = await db.get_media_channel_id()
    return {"ok": True, "media_channel_id": ch or MEDIA_CHANNEL_ID}

@app.put("/api/admin/settings/media-channel")
async def set_media_channel(media_channel_id: str = Body(..., embed=True), cid: int = Depends(_get_admin_cid)):
    if not db.pool: raise HTTPException(503)
    await _upsert_setting("media_channel_id", media_channel_id, cid)
    return {"ok": True}

async def _upsert_setting(col: str, val: str, company_id: int):
    """Обновить настройку своей компании — гарантирует наличие строки в settings.
    Раньше при отсутствующем company_id в WHERE это обновляло колонку у ВСЕХ компаний разом."""
    async with db.pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM settings WHERE company_id=$1", company_id)
        if count == 0:
            await conn.execute(f"INSERT INTO settings({col}, company_id) VALUES($1,$2)", val, company_id)
        else:
            await conn.execute(f"UPDATE settings SET {col}=$1 WHERE company_id=$2", val, company_id)
    logging.info(f"_upsert_setting {col}={repr(val)} company_id={company_id}")


@app.get("/api/admin/cash/my-balance")
async def get_my_cash_balance(staff=Depends(get_current_staff)):
    """Баланс наличных текущего сотрудника."""
    bal = await db.get_my_cash_balance(staff["id"])
    return {"ok": True, **bal}

@app.get("/api/admin/cash/debug")
async def cash_debug(staff=Depends(get_current_staff)):
    """Диагностика: что в БД по наличным для текущего сотрудника."""
    if not db.pool: return {"ok": False}
    sid = staff["id"]
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, order_id, amount, method, purpose, created_by, created_by_staff_id, handed_to_staff_id, created_at FROM order_payments WHERE created_by_staff_id=$1 OR handed_to_staff_id=$1 ORDER BY created_at DESC LIMIT 20",
            sid)
        name_rows = await conn.fetch(
            "SELECT id, order_id, amount, method, purpose, created_by, created_by_staff_id, handed_to_staff_id FROM order_payments WHERE method='cash' AND created_by_staff_id IS NULL ORDER BY created_at DESC LIMIT 10")
        staff_row = await conn.fetchrow("SELECT id, first_name, last_name, login FROM staff WHERE id=$1", sid)
    return {
        "staff_id": sid,
        "staff_name": f"{staff_row['last_name'] or ''} {staff_row['first_name'] or ''}".strip() if staff_row else None,
        "payments_by_id": [dict(r) for r in rows],
        "recent_null_staff_cash": [dict(r) for r in name_rows],
    }

@app.get("/api/admin/cash/my-payments")
async def get_my_cash_payments(staff=Depends(get_current_staff)):
    """Наличные платежи где текущий сотрудник создал платёж или указан получателем."""
    if not db.pool: return {"ok": True, "payments": []}
    my_id = staff["id"]
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT p.*,
                   o.order_num,
                   TRIM(COALESCE(o.client_first_name,'') || ' ' || COALESCE(o.client_last_name,'')) AS client_name,
                   o.client_phone,
                   COALESCE(o.short_address, o.address) AS client_address
            FROM order_payments p
            LEFT JOIN orders o ON o.id = p.order_id
            WHERE p.method='cash'
              AND (p.created_by_staff_id=$1 OR p.handed_to_staff_id=$1)
            ORDER BY p.created_at DESC LIMIT 100
        """, my_id)
        return {"ok": True, "payments": [dict(r) for r in rows]}


@app.get("/api/admin/cash/my-handovers")
async def get_my_sent_handovers_ep(staff=Depends(get_current_staff)):
    """Исходящие передачи наличных текущего сотрудника."""
    handovers = await db.get_my_sent_handovers(staff["id"])
    return {"ok": True, "handovers": handovers}


@app.get("/api/admin/cash/my-received-handovers")
async def get_my_received_handovers_ep(staff=Depends(get_current_staff)):
    """Входящие подтверждённые передачи наличных текущего сотрудника."""
    handovers = await db.get_my_received_handovers(staff["id"])
    return {"ok": True, "handovers": handovers}


@app.delete("/api/admin/orders/{order_id}/payments/{payment_id}")
async def delete_order_payment(
    order_id:   int,
    payment_id: int,
    reason: str = Body("", embed=True),
    staff=Depends(get_current_staff),
):
    if not db.pool: raise HTTPException(status_code=503, detail="DB unavailable")
    async with db.pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT * FROM order_payments WHERE id=$1 AND order_id=$2", payment_id, order_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Платёж не найден")
    can_delete = (staff.get("sub") == "admin"
                  or staff.get("can_manage_cash")
                  or existing.get("created_by_staff_id") == staff.get("id"))
    if not can_delete:
        raise HTTPException(status_code=403, detail="Нет доступа")
    deleted = await db.delete_order_payment(payment_id)
    name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
    mLabel = {"cash":"💵 Нал","card":"💳 Карта","transfer":"📲 Перевод"}
    amt = int(float(deleted.get('amount', 0)))
    mth = deleted.get('method', '')
    details = f"Удалён платёж: {amt:,} сум ({mLabel.get(mth,'')}) — Причина: {reason or '—'}"
    await db.add_order_activity(order_id, staff.get("id"), name, "payment_deleted", details)
    ch = await db.get_cash_tg_channel()
    if ch:
        phone = staff.get("phone") or ""
        text = (f"🗑 <b>Платёж удалён</b> · Заказ #{order_id}\n"
                f"{mLabel.get(mth, mth)} · <b>{amt:,} сум</b>\n"
                f"Причина: {reason or '—'}\n"
                f"👤 {name}")
        asyncio.create_task(_send_tg_cash(ch, text, phone=phone,
                                          btn_label="🔴 Проверить", btn_cb=f"chk:r:{order_id}"))
    return {"ok": True}


@app.get("/api/admin/orders/{order_id}/activity")
async def get_order_activity(order_id: int, _=Depends(get_current_staff)):
    rows = await db.get_order_activity(order_id)
    return {"ok": True, "activity": rows}

# ── Касса ─────────────────────────────────────────────────────────────────────

@app.get("/api/admin/cash/summary")
async def cash_summary(
    date_from: str = None,
    date_to:   str = None,
    _=Depends(get_current_staff),
):
    from datetime import date
    today = date.today().isoformat()
    data = await db.get_cash_summary(date_from or today, date_to or today)
    return {"ok": True, **data}

@app.get("/api/admin/cash/payments-log")
async def payments_log(
    date_from: str = None,
    date_to:   str = None,
    _=Depends(get_current_staff),
):
    from datetime import date
    today = date.today().isoformat()
    rows = await db.get_payments_log(date_from or today, date_to or today)
    return {"ok": True, "payments": rows}

@app.post("/api/admin/cash/close-shift")
async def close_shift(
    shift_date: str  = Body(None, embed=False),
    note:       str  = Body("",  embed=False),
    staff=Depends(get_current_staff),
):
    from datetime import date
    name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
    d = date.fromisoformat(shift_date) if shift_date else date.today()
    row = await db.close_cash_shift(d, name, note)
    return {"ok": True, "shift": row}

@app.post("/api/admin/cash/open-shift")
async def open_shift(staff=Depends(get_current_staff)):
    if staff.get("role") != "admin" and not staff.get("can_manage_cash"):
        raise HTTPException(status_code=403, detail="Нет доступа")
    row = await db.open_cash_shift(staff.get("id"))
    if row is None:
        raise HTTPException(status_code=409, detail="Уже есть открытая смена — сначала закройте её")
    return {"ok": True, "shift": row}

@app.delete("/api/admin/cash/shifts/{shift_id}")
async def delete_shift(shift_id: int, me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    await db.delete_cash_shift(shift_id)
    return {"ok": True}

@app.get("/api/admin/cash/current-shift")
async def current_shift(_=Depends(get_current_staff)):
    row = await db.get_current_shift()
    return {"ok": True, "shift": row if row else None}

@app.get("/api/admin/cash/dashboard")
async def cash_dashboard(_=Depends(get_current_staff)):
    data = await db.get_cash_dashboard()
    return {"ok": True, **data}

@app.get("/api/admin/cash/shifts")
async def get_shifts(_=Depends(get_current_staff)):
    rows = await db.get_cash_shifts()
    return {"ok": True, "shifts": rows}

@app.get("/api/admin/cash/daily-total")
async def cash_daily_total(date: str = None, me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403)
    from datetime import date as _date
    d = date or str(_date.today())
    data = await db.get_cash_daily_total(d)
    return {"ok": True, "date": d, **data}

@app.get("/api/admin/cash/history")
async def cash_payment_history(year: int = None, month: int = None, day: int = 0, branch: str = '', me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403)
    from datetime import date as _date
    today = _date.today()
    rows = await db.get_cash_payment_history(year or today.year, month or today.month, branch, day)
    return {"ok": True, "rows": rows}

@app.get("/api/media/{photo_id}")
async def serve_order_photo(
    photo_id: int,
    t: str = None,
    authorization: str = Header(None),
):
    token = t or (authorization[7:] if authorization and authorization.startswith("Bearer ") else None)
    if not token:
        raise HTTPException(status_code=401)
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except Exception:
        raise HTTPException(status_code=401)

    row = await db.get_photo_by_id(photo_id, payload.get("company_id") or 1)
    if not row:
        raise HTTPException(status_code=404)

    if payload.get("type") != "staff":
        owner_phone = await db.get_order_client_phone(row["order_id"], payload.get("company_id") or 1)
        if not owner_phone or owner_phone != payload.get("phone"):
            raise HTTPException(status_code=403)

    async with aiohttp.ClientSession() as s:
        async with s.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile?file_id={row['tg_file_id']}") as r:
            data = await r.json()
    if not data.get("ok"):
        raise HTTPException(status_code=502, detail="Не удалось получить файл")

    from fastapi.responses import RedirectResponse
    file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{data['result']['file_path']}"
    return RedirectResponse(url=file_url)

@app.get("/api/item-media/{media_id}")
async def serve_item_media(
    media_id: int,
    t: str = None,
    authorization: str = Header(None),
):
    token = t or (authorization[7:] if authorization and authorization.startswith("Bearer ") else None)
    if not token:
        raise HTTPException(status_code=401)
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except Exception:
        raise HTTPException(status_code=401)

    row = await db.get_item_media_by_id(media_id, payload.get("company_id") or 1)
    if not row:
        raise HTTPException(status_code=404)

    if payload.get("type") != "staff":
        owner_phone = await db.get_order_client_phone(row["order_id"], payload.get("company_id") or 1)
        if not owner_phone or owner_phone != payload.get("phone"):
            raise HTTPException(status_code=403)

    async with aiohttp.ClientSession() as s:
        async with s.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile?file_id={row['tg_file_id']}") as r:
            data = await r.json()
    if not data.get("ok"):
        raise HTTPException(status_code=502, detail="Не удалось получить файл")

    from fastapi.responses import RedirectResponse
    file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{data['result']['file_path']}"
    return RedirectResponse(url=file_url)

@app.patch("/api/admin/orders/{order_id}/discount")
async def admin_set_order_discount(order_id: int, staff=Depends(get_current_staff),
                                    discount_sum: float = Body(0, embed=True)):
    role = staff.get("role", "")
    if role not in ("admin", "manager") and "status" not in ROLE_PERMISSIONS.get(role, []):
        raise HTTPException(status_code=403, detail="Нет прав")
    order = await db.update_order_discount(order_id, discount_sum)
    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")
    return {"ok": True, "order": order}

@app.patch("/api/admin/orders/{order_id}/items/{item_id}/measure")
async def admin_measure_item(order_id: int, item_id: int, staff=Depends(get_current_staff),
                              action: str = Body(..., embed=True),
                              actual_width_cm: float = Body(None, embed=True),
                              actual_length_cm: float = Body(None, embed=True),
                              quantity: float = Body(None, embed=True),
                              note: str = Body("", embed=True)):
    is_admin = staff.get("role") == "admin"
    if action == "submit" and not (staff.get("can_measure") or is_admin):
        raise HTTPException(status_code=403, detail="Нет прав для проведения замера")
    if action in ("approve", "reject") and not (staff.get("can_approve_measure") or is_admin):
        raise HTTPException(status_code=403, detail="Нет прав для проверки замеров")
    if action == "direct_approve" and not (staff.get("can_override_measure") or is_admin):
        raise HTTPException(status_code=403, detail="Нет прав для переопределения замера")
    if action == "submit":
        if quantity:
            await db.save_measure_qty(item_id, quantity)
        elif actual_width_cm and actual_length_cm:
            await db.save_measure_dims(item_id, actual_width_cm, actual_length_cm)
        else:
            raise HTTPException(status_code=400, detail="Укажите ширину и длину или количество")
        media = await db.get_item_media(item_id)
        if not media:
            raise HTTPException(status_code=400, detail="Добавьте фото или видео замера")
        item = await db.submit_item_measure(item_id)
        if not item:
            raise HTTPException(status_code=400, detail="Ошибка при отправке на проверку")
        sname = f"{staff.get('last_name','')} {staff.get('first_name','')}".strip() or staff.get('login','')
        await db.add_order_activity(order_id, staff.get("id"), sname, "measure_submitted", item.get("service",""))
        # Push всем кто может проверять замеры
        try:
            approvers = await db.get_all_approvers()
            order_row = await db.get_order_by_id(order_id)
            order_num = (order_row or {}).get("order_num") or f"#{order_id}"
            svc       = item.get("service") or "позиция"
            for ap in approvers:
                asyncio.create_task(send_web_push(
                    ap["id"],
                    f"📐 Новый замер — {order_num}",
                    f"Замер «{svc}» ожидает вашего утверждения",
                    order_id=order_id, item_id=item_id, push_type="measure"
                ))
        except Exception as _pe:
            logging.warning(f"measure push error: {_pe}")
    elif action == "approve":
        # Проверяющий мог поправить замер прямо в модалке перед утверждением —
        # но редактировать размеры вправе только тот же круг, что и для
        # direct_approve (can_override_measure/admin), иначе любой с одним
        # только can_approve_measure смог бы прислать произвольные размеры
        # через approve, в обход проверки прав на direct_approve.
        can_override = bool(staff.get("can_override_measure")) or is_admin
        if can_override and quantity:
            item = await db.direct_approve_measure_qty(item_id, quantity)
        elif can_override and actual_width_cm and actual_length_cm:
            item = await db.direct_approve_measure(item_id, actual_width_cm, actual_length_cm)
        else:
            item = await db.approve_item_measure(item_id)
        try:
            washer_login = item.get("washer_login")
            if washer_login:
                washer = await db.get_staff_by_login(washer_login)
                if washer:
                    order_row = await db.get_order_by_id(order_id)
                    order_num = (order_row or {}).get("order_num") or f"#{order_id}"
                    svc       = item.get("service") or "позиция"
                    push_body = f"«{svc}» — замер принят. Отличная работа!"
                    asyncio.create_task(send_web_push(
                        washer["id"],
                        f"✅ Замер утверждён — {order_num}",
                        push_body,
                        order_id=order_id, item_id=item_id, push_type="measure_approved"
                    ))
                    await db.create_washer_notification(
                        washer["id"], order_id, order_num, push_body,
                        item_id=item_id, notification_type="measure_approved"
                    )
        except Exception as _pe:
            logging.warning(f"measure approved push error: {_pe}")
    elif action == "direct_approve":
        if quantity:
            item = await db.direct_approve_measure_qty(item_id, quantity)
        elif actual_width_cm and actual_length_cm:
            item = await db.direct_approve_measure(item_id, actual_width_cm, actual_length_cm)
        else:
            raise HTTPException(status_code=400, detail="Укажите ширину и длину или количество")
    elif action == "reject":
        if not note:
            raise HTTPException(status_code=400, detail="Укажите причину отклонения")
        item = await db.reject_item_measure(item_id, note)
        try:
            washer_login = item.get("washer_login")
            if washer_login:
                washer = await db.get_staff_by_login(washer_login)
                if washer:
                    order_row = await db.get_order_by_id(order_id)
                    order_num = (order_row or {}).get("order_num") or f"#{order_id}"
                    svc = item.get("service") or "позиция"
                    push_body = f"«{svc}» — {note}"
                    asyncio.create_task(send_web_push(
                        washer["id"],
                        f"❌ Замер отклонён — {order_num}",
                        push_body,
                        order_id=order_id, item_id=item_id, push_type="measure_rejected"
                    ))
                    await db.create_washer_notification(
                        washer["id"], order_id, order_num, push_body,
                        item_id=item_id, notification_type="measure_rejected"
                    )
        except Exception as _pe:
            logging.warning(f"measure reject push error: {_pe}")
    else:
        raise HTTPException(status_code=400, detail="Неверное действие")
    if not item:
        raise HTTPException(status_code=404, detail="Позиция не найдена")
    try:
        await _chat.broadcast_staff({"type": "item_updated", "order_id": order_id, "item_id": item_id},
                                     exclude=staff.get("id"), company_id=staff.get("company_id") or 1)
    except Exception as e:
        logging.warning(f"item_updated broadcast error: {e}")
    return {"ok": True, "item": item}

@app.post("/api/admin/orders/{order_id}/items/{item_id}/measure/claim")
async def claim_measure_review(order_id: int, item_id: int, staff=Depends(get_current_staff)):
    if not staff.get("can_approve_measure"):
        raise HTTPException(status_code=403, detail="Нет прав для проверки замеров")
    item = await db.claim_measure_review(item_id, staff["id"])
    if not item:
        raise HTTPException(status_code=404, detail="Замер не найден или уже утверждён")
    return {"ok": True, "item": item}

@app.get("/api/staff/pending-payment-reviews")
async def get_pending_payment_reviews(staff=Depends(get_current_staff)):
    if not staff.get("can_manage_cash") and staff.get("sub") != "admin":
        return {"ok": True, "payments": []}
    payments = await db.get_unconfirmed_payments()
    return {"ok": True, "payments": payments}

@app.get("/api/staff/pending-position-requests")
async def get_pending_position_requests(staff=Depends(get_current_staff)):
    """Список заказов с активным (не принятым) запросом позиции — для поллинга."""
    if not db.pool:
        return {"ok": True, "order_ids": []}
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id FROM orders WHERE pos_request_pending=TRUE"
        )
    return {"ok": True, "order_ids": [r["id"] for r in rows]}

# ── Филиалы (публичный GET) — для сайта: шапка/футер/меню/калькулятор/карта ──
@app.get("/api/site-branches")
async def get_site_branches(company_slug: str = None):
    """Публичный список филиалов компании. Отдаёт только то, что нужно сайту:
    без внутренних Telegram group/channel id (роутинг заявок) и без workshop-координат."""
    if not db.pool:
        return {"ok": True, "branches": []}
    import json
    cid = await _resolve_client_company_id(company_slug)
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT slug, name_ru, name_uz, lat, lon, phones, telegram_link, whatsapp, instagram, admin_tg_link
            FROM branches WHERE company_id=$1 AND active=TRUE ORDER BY id
        """, cid)
    result = []
    for r in rows:
        d = dict(r)
        try:
            phones = json.loads(d.get("phones") or "[]")
        except (TypeError, ValueError):
            phones = d.get("phones") or []
        d["phones"] = [p.get("n") for p in phones if isinstance(p, dict) and p.get("site") and p.get("n")]
        result.append(d)
    return {"ok": True, "branches": result}

@app.get("/api/staff/pending-reviews")
async def get_pending_reviews(staff=Depends(get_current_staff)):
    if not staff.get("can_approve_measure"):
        return {"ok": True, "reviews": []}
    reviews = await db.get_pending_measure_reviews()
    # Вернуть только те, которые не приняты другим сотрудником (или приняты мной)
    my_id = staff["id"]
    visible = [r for r in reviews if not r["review_claimed_by"] or r["review_claimed_by"] == my_id]
    return {"ok": True, "reviews": visible}

@app.post("/api/staff/orders/{order_id}/request-position")
async def request_position(
    order_id: int,
    note:  str = Body(..., embed=True),
    count: int = Body(1,  embed=True),
    staff=Depends(get_current_staff)
):
    """Мойщик просит добавить позицию — пуш всем с can_approve_measure."""
    order = await db.get_order_by_id(order_id)
    if not order:
        raise HTTPException(404, "Заказ не найден")
    washer_name  = " ".join(filter(None, [staff.get("first_name"), staff.get("last_name")])) or staff.get("login", "Мойщик")
    order_num    = order.get("order_number") or f"#{order_id}"
    items        = await db.get_order_items(order_id)
    current_cnt  = len(items)
    add_str      = f"+{count}" if count > 1 else "+1"
    title = f"📋 {order_num} — сейчас {current_cnt} поз., нужно {add_str}"
    body  = f"{washer_name}: {note}" if note else washer_name
    approvers = await db.get_all_approvers()
    for a in approvers:
        asyncio.create_task(send_web_push(
            a["id"], title, body,
            order_id=order_id, push_type="position_request"
        ))
    # Ставим флаг pending в БД
    if db.pool:
        async with db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE orders SET pos_request_pending=TRUE, pos_request_at=NOW() WHERE id=$1",
                order_id
            )
    return {"ok": True}

@app.post("/api/staff/orders/{order_id}/claim-position-request")
async def claim_position_request(order_id: int, staff=Depends(get_current_staff)):
    """Менеджер принимает запрос — уведомляем остальных чтобы не дублировали."""
    order = await db.get_order_by_id(order_id)
    if not order:
        raise HTTPException(404, "Заказ не найден")
    my_id       = staff["id"]
    order_num   = order.get("order_number") or f"#{order_id}"
    my_name     = " ".join(filter(None, [staff.get("first_name"), staff.get("last_name")])) or staff.get("login", "")
    title = f"✅ {order_num} — принято"
    body  = f"{my_name} принял запрос на добавление позиции"
    # Сбрасываем флаг pending
    if db.pool:
        async with db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE orders SET pos_request_pending=FALSE WHERE id=$1", order_id
            )
    approvers = await db.get_all_approvers()
    for a in approvers:
        if a["id"] == my_id:
            continue
        asyncio.create_task(send_web_push(
            a["id"], title, body,
            order_id=order_id, push_type="position_claimed"
        ))
    return {"ok": True}

@app.post("/api/staff/orders/{order_id}/notify-washer")
async def notify_washer_new_item(
    order_id: int,
    washer_id: int = Body(None, embed=True),   # None = всем мойщикам
    item_id: int   = Body(None, embed=True),
    staff=Depends(get_current_staff),
):
    order = await db.get_order_by_id(order_id)
    if not order:
        raise HTTPException(404, "Заказ не найден")
    order_num   = order.get("order_num") or f"#{order_id}"
    items       = await db.get_order_items(order_id)
    item_count  = len(items)
    sender      = " ".join(filter(None, [staff.get("first_name"), staff.get("last_name")])) or staff.get("login", "Менеджер")
    title = f"📋 Новая позиция — {order_num}"
    body  = f"Сейчас {item_count} поз. в заказе {order_num}. {sender} добавил позицию."

    if washer_id:
        target_ids = [washer_id]
    else:
        all_staff  = await db.get_all_staff()
        target_ids = [s["id"] for s in all_staff if s.get("role") == "washer" and s.get("active")]

    sent = 0
    no_sub = 0
    for wid in target_ids:
        await db.create_washer_notification(wid, order_id, order_num, body)
        subs = await db.get_push_subscriptions(wid)
        if subs:
            asyncio.create_task(send_web_push(wid, title, body, order_id=order_id, push_type="new_item"))
            sent += 1
        else:
            no_sub += 1
            logging.warning(f"notify_washer: no push sub for staff_id={wid}")

    return {"ok": True, "sent": sent, "no_subscription": no_sub}


@app.get("/api/staff/my-order-notifications")
async def get_my_order_notifications(staff=Depends(get_current_staff)):
    rows = await db.get_washer_notifications(staff["id"])
    return {"ok": True, "notifications": rows}

@app.get("/api/staff/my-order-notifications/unread-count")
async def get_order_notif_unread(staff=Depends(get_current_staff)):
    count = await db.count_unread_washer_notifications(staff["id"])
    return {"ok": True, "count": count}

@app.post("/api/staff/my-order-notifications/read")
async def mark_order_notifs_read(staff=Depends(get_current_staff)):
    await db.mark_washer_notifications_read(staff["id"])
    return {"ok": True}

@app.patch("/api/staff/my-order-notifications/{notif_id}/read")
async def mark_order_notif_read(notif_id: int, staff=Depends(get_current_staff)):
    await db.mark_washer_notification_read(notif_id, staff["id"])
    return {"ok": True}

@app.get("/api/admin/orders/{order_id}/items/{item_id}/media")
async def get_item_media(order_id: int, item_id: int, _=Depends(get_current_staff)):
    media = await db.get_item_media(item_id)
    return {"ok": True, "media": media}

@app.post("/api/admin/orders/{order_id}/items/{item_id}/media")
async def upload_item_media(
    order_id: int, item_id: int,
    file: UploadFile = File(...),
    staff=Depends(get_current_staff),
):
    media_ch = await _get_media_channel()
    if not BOT_TOKEN or not media_ch:
        raise HTTPException(status_code=503, detail="Медиа-хранилище не настроено")
    content_type = file.content_type or ""
    if content_type.startswith("video/"):
        tg_method, tg_field, tg_type = "sendVideo", "video", "video"
    else:
        tg_method, tg_field, tg_type = "sendPhoto", "photo", "photo"

    order_row = await db.get_order_by_id(order_id)
    order_num = order_row.get("order_num", f"#{order_id}") if order_row else f"#{order_id}"
    staff_name = " ".join(filter(None, [staff.get("last_name"), staff.get("first_name")])) or staff.get("login", "")
    # Порядковый номер позиции внутри заказа (1-based)
    order_items = await db.get_order_items(order_id)
    item_ids = [i["id"] for i in order_items]
    item_pos = item_ids.index(item_id) + 1 if item_id in item_ids else item_id
    caption = f"📐 Замер\n🧾 Заказ: {order_num} | Позиция #{item_pos}\n👤 {staff_name}"

    file_bytes = await file.read()
    form = aiohttp.FormData()
    form.add_field("chat_id", str(media_ch))
    form.add_field(tg_field, file_bytes, filename=file.filename, content_type=content_type)
    form.add_field("caption", caption)

    async with aiohttp.ClientSession() as s:
        async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{tg_method}", data=form) as r:
            result = await r.json()

    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=f"Telegram: {result.get('description','upload failed')}")

    msg = result["result"]
    file_id = msg["photo"][-1]["file_id"] if tg_type == "photo" else msg[tg_type]["file_id"]
    row = await db.add_item_media(item_id, order_id, file_id, tg_type, staff_name)
    # Log to order activity
    sname = f"{staff.get('first_name','')} {staff.get('last_name','')}".strip() or staff.get('login','?')
    item_service = ''; item_pos2 = None
    if db.pool:
        async with db.pool.acquire() as _c:
            _ir = await _c.fetchrow("SELECT service FROM order_items WHERE id=$1", item_id)
            if _ir: item_service = _ir['service'] or ''
            item_pos2 = await _c.fetchval(
                "SELECT COUNT(*) FROM order_items WHERE order_id=$1 AND id <= $2", order_id, item_id)
    media_label = "🎥 Видео добавлено" if tg_type == "video" else "📸 Фото добавлено"
    pos_prefix = f"#{item_pos2} " if item_pos2 else ""
    act_detail = f"{media_label} — {pos_prefix}{item_service}" if item_service else media_label
    await db.add_order_activity(order_id, staff.get("id"), sname, "item_media_added", act_detail)
    return {"ok": True, "media": row}

@app.delete("/api/admin/orders/{order_id}/items/{item_id}/media/{media_id}")
async def delete_item_media(order_id: int, item_id: int, media_id: int, staff=Depends(get_current_staff)):
    cid = staff.get("company_id") or 1
    # Fetch media type and item service before deleting
    media_row = await db.get_item_media_by_id(media_id, cid)
    if not media_row:
        raise HTTPException(status_code=404)
    await db.delete_item_media(media_id, cid)
    sname = f"{staff.get('first_name','')} {staff.get('last_name','')}".strip() or staff.get('login','?')
    item_service = ''; item_pos3 = None
    if db.pool:
        async with db.pool.acquire() as _c:
            _ir = await _c.fetchrow("SELECT service FROM order_items WHERE id=$1", item_id)
            if _ir: item_service = _ir['service'] or ''
            item_pos3 = await _c.fetchval(
                "SELECT COUNT(*) FROM order_items WHERE order_id=$1 AND id <= $2", order_id, item_id)
    tg_type = (media_row.get('tg_file_type') or 'photo') if media_row else 'photo'
    media_label = "🎥 Видео удалено" if tg_type == "video" else "🗑 Фото удалено"
    pos_prefix = f"#{item_pos3} " if item_pos3 else ""
    act_detail = f"{media_label} — {pos_prefix}{item_service}" if item_service else media_label
    await db.add_order_activity(order_id, staff.get("id"), sname, "item_media_deleted", act_detail)
    return {"ok": True}

@app.patch("/api/admin/orders/{order_id}/items/{item_id}/washer")
async def admin_set_item_washer(order_id: int, item_id: int, staff=Depends(get_current_staff),
                                 washer_login: str = Body("", embed=True)):
    item = await db.update_item_washer(item_id, washer_login or None)
    if not item:
        raise HTTPException(status_code=404, detail="Позиция не найдена")
    sname = f"{staff.get('last_name','')} {staff.get('first_name','')}".strip() or staff.get('login','')
    action_type = "washer_taken" if washer_login else "washer_released"
    await db.add_order_activity(order_id, staff.get("id"), sname, action_type, item.get("service",""))
    try:
        await _chat.broadcast_staff({"type": "item_updated", "order_id": order_id, "item_id": item_id},
                                     exclude=staff.get("id"), company_id=staff.get("company_id") or 1)
    except Exception as e:
        logging.warning(f"item_updated broadcast error: {e}")
    return {"ok": True, "item": item}

@app.patch("/api/admin/staff/{staff_id}/can-edit-items")
async def admin_set_can_edit_items(staff_id: int, cid: int = Depends(_get_admin_cid),
                                    can_edit_items: bool = Body(..., embed=True)):
    if not db.pool: raise HTTPException(status_code=503, detail="DB unavailable")
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE staff SET can_edit_items=$3 WHERE id=$1 AND company_id=$2 RETURNING id, can_edit_items",
            staff_id, cid, can_edit_items)
    if not row:
        raise HTTPException(status_code=404, detail="Сотрудник не найден")
    return {"ok": True, **dict(row)}

@app.patch("/api/admin/staff/{staff_id}/permissions")
async def admin_set_staff_permissions(staff_id: int, cid: int = Depends(_get_admin_cid),
    can_edit_items:       bool = Body(True,  embed=True),
    can_measure:          bool = Body(False, embed=True),
    can_approve_measure:  bool = Body(False, embed=True),
    can_override_measure: bool = Body(False, embed=True),
    can_create_order:     bool = Body(True,  embed=True),
    can_confirm_order:    bool = Body(True,  embed=True),
    can_edit_confirmed:   bool = Body(False, embed=True),
    can_send_pickup:      bool = Body(False, embed=True),
    can_edit_delivery:    bool = Body(False, embed=True),
    can_accept_payment:   bool = Body(False, embed=True),
    can_manage_cash:      bool = Body(False, embed=True),
    notify_new_users:     bool = Body(False, embed=True),
    can_approve_debt:     bool = Body(False, embed=True),
    can_drive:            bool = Body(False, embed=True),
    can_view_timesheet:   bool = Body(False, embed=True),
    hide_client_phone:    bool = Body(False, embed=True),
    order_stages:         str  = Body(None,  embed=True)):
    if not db.pool: raise HTTPException(status_code=503, detail="DB unavailable")
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            """UPDATE staff
               SET can_edit_items=$2, can_measure=$3, can_approve_measure=$4,
                   can_override_measure=$5,
                   can_create_order=$6, can_confirm_order=$7, order_stages=$8,
                   can_edit_confirmed=$9, can_send_pickup=$10, can_edit_delivery=$11,
                   can_accept_payment=$12, can_manage_cash=$13, notify_new_users=$14,
                   can_approve_debt=$15, can_drive=$16, can_view_timesheet=$17,
                   hide_client_phone=$18
               WHERE id=$1 AND company_id=$19
               RETURNING id, can_edit_items, can_measure, can_approve_measure,
                         can_override_measure,
                         can_create_order, can_confirm_order, order_stages,
                         can_edit_confirmed, can_send_pickup, can_edit_delivery,
                         can_accept_payment, can_manage_cash, notify_new_users,
                         can_approve_debt, can_drive, can_view_timesheet, hide_client_phone""",
            staff_id, can_edit_items, can_measure, can_approve_measure,
            can_override_measure,
            can_create_order, can_confirm_order, order_stages or None,
            can_edit_confirmed, can_send_pickup, can_edit_delivery,
            can_accept_payment, can_manage_cash, notify_new_users, can_approve_debt,
            can_drive, can_view_timesheet, hide_client_phone, cid)
    if not row:
        raise HTTPException(status_code=404, detail="Сотрудник не найден")
    return {"ok": True, **dict(row)}


@app.post("/api/staff/orders/{order_id}/mark-delivered")
async def staff_mark_delivered(
    order_id: int,
    due_date: str | None = Body(None, embed=True),
    staff=Depends(get_current_staff),
):
    role = staff.get("role")
    if role not in ("admin", "manager") and not staff.get("can_approve_debt"):
        raise HTTPException(403, "Нет доступа")
    debt = await db.get_order_debt_amount(order_id)
    if debt > 0 and not due_date:
        from datetime import date, timedelta
        due_date = (date.today() + timedelta(days=7)).isoformat()
    by_name = " ".join(filter(None, [staff.get("last_name"), staff.get("first_name")])) or staff.get("login", "")
    ok = await db.mark_order_delivered_with_debt(order_id, staff["id"], due_date, by_name)
    if not ok:
        raise HTTPException(500, "Ошибка обновления")
    asyncio.create_task(_update_api_channel_stop(order_id))
    return {"ok": True}

@app.get("/api/admin/orders/{order_id}/debt-history")
async def get_debt_history(order_id: int, _=Depends(get_current_staff)):
    rows = await db.get_order_debt_history(order_id)
    return {"ok": True, "history": rows}

@app.patch("/api/admin/orders/{order_id}/debt-due-date")
async def patch_debt_due_date(order_id: int, due_date: str = Body(..., embed=True), note: str = Body('', embed=True), me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403)
    ok = await db.extend_debt_due_date(order_id, due_date, note)
    if not ok:
        raise HTTPException(status_code=404, detail="Заказ не найден или не является долгом")
    return {"ok": True}

# ── Salary ledger ─────────────────────────────────────────────────────────────

@app.get("/api/admin/salary/ledger")
async def salary_ledger_list(staff_id: int, year: int, month: int, _=Depends(_get_admin)):
    rows = await db.get_salary_ledger(staff_id, year, month)
    return {"ok": True, "entries": rows}

@app.post("/api/admin/salary/ledger")
async def salary_ledger_add(body: dict = Body(...), me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    row = await db.add_salary_ledger_entry(
        staff_id   = int(body["staff_id"]),
        period_str = body["period"],
        type_      = body.get("type", "accrual"),
        amount     = float(body["amount"]),
        note       = body.get("note", ""),
        expense_id = body.get("expense_id"),
        created_by = me["id"],
        fine_reason= body.get("fine_reason"),
    )
    if not row:
        raise HTTPException(status_code=400, detail="Ошибка создания записи")
    return {"ok": True, "entry": row}

@app.get("/api/admin/salary/daily")
async def salary_daily_breakdown(staff_id: int, year: int, month: int, _=Depends(_get_admin)):
    data = await db.get_salary_daily_breakdown(staff_id, year, month)
    return {"ok": True, **data}

@app.delete("/api/admin/salary/accruals")
async def delete_month_accruals(staff_id: int, year: int, month: int, _=Depends(_get_admin)):
    deleted = await db.delete_month_accruals(staff_id, year, month)
    return {"ok": True, "deleted": deleted}

@app.post("/api/admin/salary/opening-balance")
async def set_opening_balance(body: dict = Body(...), me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(403)
    result = await db.set_opening_balance(
        staff_id   = int(body["staff_id"]),
        year       = int(body["year"]),
        month      = int(body["month"]),
        target     = float(body["amount"]),
        created_by = me["id"],
    )
    return {"ok": True, **result}

@app.patch("/api/admin/salary/ledger/{entry_id}")
async def salary_ledger_update(entry_id: int, body: dict = Body(...), _=Depends(_get_admin)):
    row = await db.update_salary_ledger_entry(entry_id, float(body["amount"]), body.get("note",""))
    if not row:
        raise HTTPException(status_code=404)
    return {"ok": True, "entry": row}

@app.delete("/api/admin/salary/ledger/{entry_id}")
async def salary_ledger_delete(entry_id: int, _=Depends(_get_admin)):
    ok = await db.delete_salary_ledger_entry(entry_id)
    if not ok:
        raise HTTPException(status_code=404)
    return {"ok": True}

@app.get("/api/admin/salary/balance/{staff_id}")
async def salary_balance(staff_id: int, _=Depends(get_current_staff)):
    data = await db.get_salary_balance(staff_id)
    return {"ok": True, **data}

@app.get("/api/admin/salary/advance-percent")
async def get_advance_pct(_=Depends(_get_admin)):
    pct = await db.get_advance_max_percent()
    return {"ok": True, "advance_max_percent": pct}

@app.patch("/api/admin/salary/advance-percent")
async def save_advance_pct(body: dict = Body(...), _=Depends(_get_admin)):
    await db.save_advance_max_percent(float(body["advance_max_percent"]))
    return {"ok": True}

@app.post("/api/admin/salary/accrue")
async def manual_accrue(cid: int = Depends(_get_admin_cid)):
    count = await db.auto_accrue_monthly_salaries(company_id=cid)
    return {"ok": True, "count": count}

# ── discount requests ──────────────────────────────────────────────────────────

@app.get("/api/discount-requests/pending")
async def get_pending_discount_requests(staff=Depends(get_current_staff)):
    role = staff.get("role")
    if role not in ("admin", "manager"):
        raise HTTPException(403, "Нет доступа")
    rows = await db.get_pending_discount_requests()
    return {"ok": True, "requests": rows}

@app.post("/api/discount-requests/{request_id}/approve")
async def approve_discount_request(
    request_id: int,
    approved_amount: float = Body(..., embed=True),
    staff=Depends(get_current_staff)
):
    role = staff.get("role")
    if role not in ("admin", "manager"):
        raise HTTPException(403, "Нет доступа")
    row = await db.resolve_discount_request(request_id, approved_amount, staff["id"])
    if not row:
        raise HTTPException(404, "Запрос не найден или уже обработан")
    return {"ok": True, "request": row}

@app.post("/api/discount-requests/{request_id}/reject")
async def reject_discount_request(
    request_id: int,
    staff=Depends(get_current_staff)
):
    role = staff.get("role")
    if role not in ("admin", "manager"):
        raise HTTPException(403, "Нет доступа")
    row = await db.reject_discount_request(request_id, staff["id"])
    if not row:
        raise HTTPException(404, "Запрос не найден или уже обработан")
    return {"ok": True, "request": row}

# ── driver delivery (web) ──────────────────────────────────────────────────────

_NOT_TAKEN_REASONS = ["🚗 Нет места в машине","⏳ Заказ ещё не готов","📅 Клиент перенёс","📦 Хрупкий/негабаритный","✏️ Другое"]
_RETURNED_REASONS  = ["🚪 Клиента нет дома","📍 Не нашёл адрес","📵 Не дозвонился","🚫 Клиент отказался","📅 Клиент перенёс","✏️ Другое"]

def _driver_name(staff: dict) -> str:
    return " ".join(filter(None,[staff.get("last_name"), staff.get("first_name")])) or staff.get("login","Водитель")

def _can_drive(staff: dict) -> bool:
    return bool(staff.get("can_drive")) or staff.get("role") == "driver"

@app.get("/api/staff/my-route")
async def get_my_route(staff=Depends(get_current_staff)):
    if not _can_drive(staff):
        raise HTTPException(403, "Нет доступа")
    await db.roll_forward_stale_routes()
    routes = await db.get_routes_today(staff["company_id"], staff.get("branch"))
    payment_events = []
    try:
        async with db.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT op.id, op.order_id, op.amount, op.method,
                       op.confirmed, op.reject_note,
                       CASE WHEN op.confirmed=TRUE THEN 'confirmed' ELSE 'rejected' END AS action
                FROM order_payments op
                WHERE op.created_by_staff_id = $1
                  AND op.confirmed_at IS NOT NULL
                  AND op.confirmed_at > NOW() - INTERVAL '3 minutes'
                ORDER BY op.confirmed_at DESC
            """, staff["id"])
        payment_events = [dict(r) for r in rows]
    except Exception as _pe:
        logging.warning(f"payment_events: {_pe}")
    return {"ok": True, "routes": routes, "payment_events": payment_events}

# ── Забор у клиента (pickup) ────────────────────────────────────────────────

@app.get("/api/staff/pickup/lookup")
async def staff_pickup_lookup(phone: str, staff=Depends(get_current_staff)):
    """Проверка по телефону: есть ли уже заказ в статусе new/confirmed (создан
    менеджером/колл-центром) — решает, открыть найденный заказ или дать создать новый."""
    if not _can_drive(staff):
        raise HTTPException(403, "Нет доступа")
    normalized = normalize_phone(phone)
    order = await db.get_order_by_phone_pending(normalized)
    if not order:
        return {"ok": True, "found": False}
    return {"ok": True, "found": True, "order": order}


class PickupTakeRequest(BaseModel):
    mode: str = "count"       # "count" | "by_service"
    count: int = 0
    items: dict[str, int] = {}
    sms_lang: str = "ru"

@app.post("/api/staff/pickup/{order_id}/take")
async def staff_pickup_take(order_id: int, req: PickupTakeRequest, staff=Depends(get_current_staff)):
    """Водитель забрал вещи у клиента: создаёт пустые позиции (общим счётом или по
    услугам), переводит заказ в 'pickup', шлёт клиенту SMS (если включено)."""
    if not _can_drive(staff):
        raise HTTPException(403, "Нет доступа")
    order = await db.get_order_by_id(order_id)
    if not order:
        raise HTTPException(404, "Заказ не найден")
    if order.get("status") not in ("new", "confirmed"):
        raise HTTPException(400, f"Заказ уже в статусе: {order.get('status')}")
    name = _driver_name(staff)
    if req.mode == "by_service" and req.items:
        created = await db.create_empty_items_by_service(order_id, req.items)
    else:
        created = await db.create_empty_items(order_id, max(0, req.count))
    await db.update_order_status(order_id, "pickup", note=f"Забор ({name}): {len(created)} поз.")
    asyncio.create_task(_send_sms_notification("pickup", order_id, order.get("client_phone"), order.get("order_num"),
                                                len(created), req.sms_lang, staff["company_id"], staff["id"], name))
    return {"ok": True, "items": created}


@app.post("/api/staff/pickup/{order_id}/handoff")
async def staff_pickup_handoff(order_id: int, staff=Depends(get_current_staff)):
    """Сдал забранные вещи в мастерскую."""
    if not _can_drive(staff):
        raise HTTPException(403, "Нет доступа")
    order = await db.get_order_by_id(order_id)
    if not order:
        raise HTTPException(404, "Заказ не найден")
    if order.get("status") != "pickup":
        raise HTTPException(400, f"Заказ уже в статусе: {order.get('status')}")
    name = _driver_name(staff)
    await db.update_order_status(order_id, "received", note=f"Забор ({name}): сдал в мастерскую")
    return {"ok": True}


@app.post("/api/staff/pickup/{order_id}/not-taken")
async def staff_pickup_not_taken(order_id: int, reason: str = Body("", embed=True),
                                  staff=Depends(get_current_staff)):
    """Не забрал (клиента нет дома и т.д.) — без смены статуса, только отметка в истории."""
    if not _can_drive(staff):
        raise HTTPException(403, "Нет доступа")
    order = await db.get_order_by_id(order_id)
    if not order:
        raise HTTPException(404, "Заказ не найден")
    name = _driver_name(staff)
    await db.add_order_activity(order_id, staff["id"], name, "pickup_not_taken", reason or "Не забрал")
    return {"ok": True}


class ManualSmsRequest(BaseModel):
    lang: str = "ru"
    kind: str = "pickup"   # "pickup" | "ready"

@app.post("/api/admin/orders/{order_id}/send-sms")
async def admin_send_pickup_sms(order_id: int, req: ManualSmsRequest,
                                 staff=Depends(require_perm("orders"))):
    """Ручная/повторная отправка SMS — кнопка «📲 SMS» во вкладке «Уведомления» карточки
    заказа. Уважает тогл «Включить SMS при заборе/Готово» — если выключен, возвращает
    ok:false с причиной вместо молчаливого no-op."""
    order = await db.get_order_by_id(order_id)
    if not order:
        raise HTTPException(404, "Заказ не найден")
    kind = req.kind if req.kind in ("pickup", "ready") else "pickup"
    items = await db.get_order_items(order_id)
    name = _driver_name(staff) if staff.get("role") == "driver" else (staff.get("first_name") or staff.get("login", "Сотрудник"))
    sent, reason = await _send_sms_notification(kind, order_id, order.get("client_phone"), order.get("order_num"),
                                                 len(items), req.lang, staff["company_id"], staff["id"], name)
    return {"ok": True, "sent": sent, "reason": reason}


@app.get("/api/staff/settings/sms-toggles")
async def staff_sms_toggles(staff=Depends(require_perm("orders"))):
    """Токены сотрудников (staff.html) не проходят _get_admin для не-admin ролей,
    поэтому /admin/sms/settings им недоступен. Отдельный лёгкий staff-эндпоинт —
    только два тоглера, нужных модалке подтверждения SMS при смене статуса.
    Портировано из прода."""
    return {
        "sms_pickup_enabled": (await _get_cfg("sms_pickup_enabled")) == "true",
        "sms_ready_enabled":  (await _get_cfg("sms_ready_enabled"))  == "true",
    }


async def _driver_take_delivery_core(order_id: int, staff: dict, source: str = "web") -> dict:
    """Общая логика «Взял для доставки» — вынесена, чтобы SaaS-бот заказов
    (order_bot_handlers.py, тот же процесс) могла вызывать её напрямую с уже
    резолвленным staff (через get_staff_by_tg_id_and_company), без HTTP-петли
    на себя же (тот же паттерн, что _agent_status_for_bot/_agent_apply_for_bot)."""
    if not _can_drive(staff): raise HTTPException(403, "Нет доступа")
    name = _driver_name(staff)
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT status FROM orders WHERE id=$1", order_id)
        if not row: raise HTTPException(404)
        cur = row["status"]
        if cur not in ("ready", "delivery"):
            raise HTTPException(400, f"Статус заказа: {cur}")
        if cur == "ready":
            await conn.execute("UPDATE orders SET status='delivery', updated_at=NOW() WHERE id=$1", order_id)
            await conn.execute(
                "INSERT INTO order_activity (order_id, staff_id, staff_name, action, details) VALUES ($1,$2,$3,$4,$5)",
                order_id, staff["id"], name, "status_delivery", f"Маршрут ({source}): забрал для доставки клиенту")
        await conn.execute("UPDATE route_orders SET driver_confirmed=TRUE WHERE order_id=$1", order_id)
    asyncio.create_task(_update_api_channel_stop(order_id))
    return {"ok": True}

@app.post("/api/staff/my-route/stops/{order_id}/take-pickup")
async def driver_route_take_pickup(order_id: int, req: PickupTakeRequest, staff=Depends(get_current_staff)):
    """«Забрал» на маршруте типа «Забор» — new/confirmed → pickup, создаёт пустые
    позиции (доизмерит мойщик), шлёт SMS клиенту. В отличие от /api/staff/pickup/{id}/take
    (тот же ad-hoc флоу без маршрута) — здесь ещё синхронизируем route_orders,
    иначе точка маршрута не отметится выполненной в списке. Портировано из прода."""
    if not _can_drive(staff): raise HTTPException(403, "Нет доступа")
    order = await db.get_order_by_id(order_id)
    if not order: raise HTTPException(404, "Заказ не найден")
    if order.get("status") not in ("new", "confirmed"):
        raise HTTPException(400, f"Статус заказа: {order.get('status')}")
    name = _driver_name(staff)

    if req.mode == "by_service" and req.items:
        items = await db.create_empty_items_by_service(order_id, req.items)
    else:
        items = await db.create_empty_items(order_id, max(1, req.count or 1))

    await db.update_order_status(order_id, "pickup",
        note=f"Забрал у клиента (маршрут): {name} ({len(items)} поз.)")
    async with db.pool.acquire() as conn:
        await conn.execute(
            "UPDATE route_orders SET stop_status='done', driver_confirmed=TRUE WHERE order_id=$1", order_id)
    asyncio.create_task(_send_sms_notification("pickup", order_id, order.get("client_phone"), order.get("order_num"),
                                                len(items), req.sms_lang, staff["company_id"], staff["id"], name))
    asyncio.create_task(_update_api_channel_stop(order_id))
    return {"ok": True, "items": items, "count": len(items)}

@app.post("/api/staff/my-route/stops/{order_id}/undo-pickup")
async def driver_route_undo_pickup(order_id: int, staff=Depends(get_current_staff)):
    """«Не забирал» после ошибочного «Забрал» на маршруте типа «Забор» — pickup → confirmed,
    удаляет созданные пустые позиции, возвращает точку маршрута в pending. Портировано из прода."""
    if not _can_drive(staff): raise HTTPException(403, "Нет доступа")
    order = await db.get_order_by_id(order_id)
    if not order: raise HTTPException(404, "Заказ не найден")
    if order.get("status") != "pickup":
        raise HTTPException(400, f"Статус заказа: {order.get('status')}")
    name = _driver_name(staff)
    async with db.pool.acquire() as conn:
        await conn.execute("DELETE FROM order_items WHERE order_id=$1", order_id)
        await conn.execute(
            "UPDATE route_orders SET stop_status='pending', driver_confirmed=FALSE WHERE order_id=$1", order_id)
    await db.update_order_status(order_id, "confirmed", note=f"Маршрут: не забирал — позиции удалены ({name})")
    asyncio.create_task(_update_api_channel_stop(order_id))
    return {"ok": True}

@app.post("/api/staff/my-route/stops/{order_id}/take-delivery")
async def driver_take_delivery(order_id: int, staff=Depends(get_current_staff)):
    return await _driver_take_delivery_core(order_id, staff)

@app.post("/api/staff/my-route/stops/{order_id}/undo-delivery")
async def driver_undo_delivery(order_id: int, staff=Depends(get_current_staff)):
    if not _can_drive(staff): raise HTTPException(403, "Нет доступа")
    name = _driver_name(staff)
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT status FROM orders WHERE id=$1", order_id)
        if not row: raise HTTPException(404)
        if row["status"] == "delivery":
            await conn.execute("UPDATE orders SET status='ready', updated_at=NOW() WHERE id=$1", order_id)
        await conn.execute("UPDATE route_orders SET driver_confirmed=FALSE WHERE order_id=$1", order_id)
        await conn.execute(
            "INSERT INTO order_activity (order_id, staff_id, staff_name, action, details) VALUES ($1,$2,$3,$4,$5)",
            order_id, staff["id"], name, "undo_delivery", "↩️ Отменил доставку (web)")
    asyncio.create_task(_update_api_channel_stop(order_id))
    return {"ok": True}

@app.post("/api/staff/my-route/stops/{order_id}/not-taken")
async def driver_not_taken(order_id: int, reason_index: int = Body(..., embed=True), staff=Depends(get_current_staff)):
    if not _can_drive(staff): raise HTTPException(403, "Нет доступа")
    name = _driver_name(staff)
    idx = reason_index if 0 <= reason_index < len(_NOT_TAKEN_REASONS) else len(_NOT_TAKEN_REASONS)-1
    reason = _NOT_TAKEN_REASONS[idx]
    async with db.pool.acquire() as conn:
        await conn.execute("UPDATE route_orders SET stop_status='skipped', driver_confirmed=FALSE WHERE order_id=$1", order_id)
        await conn.execute(
            "INSERT INTO order_activity (order_id, staff_id, staff_name, action, details) VALUES ($1,$2,$3,$4,$5)",
            order_id, staff["id"], name, "not_taken", f"❌ Не забрал: {reason}")
    asyncio.create_task(_update_api_channel_stop(order_id))
    return {"ok": True}

@app.post("/api/staff/my-route/stops/{order_id}/returned")
async def driver_returned(order_id: int, reason_index: int = Body(..., embed=True), staff=Depends(get_current_staff)):
    if not _can_drive(staff): raise HTTPException(403, "Нет доступа")
    name = _driver_name(staff)
    idx = reason_index if 0 <= reason_index < len(_RETURNED_REASONS) else len(_RETURNED_REASONS)-1
    reason = _RETURNED_REASONS[idx]
    async with db.pool.acquire() as conn:
        await conn.execute("UPDATE orders SET status='ready', updated_at=NOW() WHERE id=$1", order_id)
        await conn.execute("UPDATE route_orders SET stop_status='skipped', driver_confirmed=FALSE WHERE order_id=$1", order_id)
        await conn.execute(
            "INSERT INTO order_activity (order_id, staff_id, staff_name, action, details) VALUES ($1,$2,$3,$4,$5)",
            order_id, staff["id"], name, "returned", f"🔙 Вернул в мастерскую: {reason}")
    asyncio.create_task(_update_api_channel_stop(order_id))
    return {"ok": True}

async def _driver_deliver_core(order_id: int, staff: dict, method: str = "", amount: float = 0, source: str = "web") -> dict:
    """Общая логика «Доставлено» (опционально с оплатой) — см. _driver_take_delivery_core."""
    if not _can_drive(staff): raise HTTPException(403, "Нет доступа")
    name = _driver_name(staff)
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT status FROM orders WHERE id=$1", order_id)
        if not row: raise HTTPException(404)
        if row["status"] not in ("ready", "delivery"):
            raise HTTPException(400, f"Статус: {row['status']}")
    if amount > 0 and method in ("cash","card","transfer"):
        await db.add_order_payment(order_id, amount, method, "delivery",
                                   f"Оплата при доставке ({source})", name,
                                   created_by_staff_id=staff["id"])
    debt = await db.get_order_debt_amount(order_id)
    note = f"Маршрут ({source}): доставлен клиенту{f', долг {debt:.0f} сум' if debt > 0 else ''}"
    async with db.pool.acquire() as conn:
        await conn.execute(
            "UPDATE orders SET status='delivered', updated_at=NOW() WHERE id=$1", order_id)
        await conn.execute(
            "UPDATE route_orders SET stop_status='done' WHERE order_id=$1 AND stop_status!='done'", order_id)
        await conn.execute(
            "INSERT INTO order_activity (order_id, staff_id, staff_name, action, details) VALUES ($1,$2,$3,$4,$5)",
            order_id, staff["id"], name, "status_delivered", note)
    asyncio.create_task(_update_api_channel_stop(order_id))
    return {"ok": True, "debt": float(debt)}

@app.post("/api/staff/my-route/stops/{order_id}/deliver")
async def driver_deliver(
    order_id: int,
    method: str = Body(..., embed=True),
    amount: float = Body(0, embed=True),
    staff=Depends(get_current_staff)
):
    return await _driver_deliver_core(order_id, staff, method, amount)

@app.post("/api/staff/my-route/stops/{order_id}/close-with-debt")
async def driver_close_with_debt(order_id: int, staff=Depends(get_current_staff)):
    if not _can_drive(staff): raise HTTPException(403, "Нет доступа")
    # Проверить нет ли уже pending-запроса
    async with db.pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT id FROM debt_approval_requests WHERE order_id=$1 AND status='pending'", order_id)
        if existing:
            return {"ok": True, "already_pending": True, "debt": 0}
    # Данные заказа + вычисляем долг так же как фронт (items_total - discount - paid)
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT o.order_num, o.client_first_name, o.client_last_name, o.client_phone,
                   o.short_address, o.address,
                   COALESCE((SELECT SUM(COALESCE(sqm*price_per_sqm,0)) FROM order_items WHERE order_id=o.id),
                            COALESCE(o.total_price,0)) AS items_total,
                   COALESCE(o.discount_sum,0)+COALESCE(o.delivery_discount,0)+COALESCE(o.manual_discount,0) AS disc,
                   COALESCE((SELECT SUM(amount) FROM order_payments WHERE order_id=$1
                              AND ((method='cash' AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL))
                                   OR (method<>'cash' AND confirmed=TRUE))),0) AS paid
            FROM orders o WHERE o.id=$1
        """, order_id)
    if not row: raise HTTPException(404)
    # К оплате округляется вниз до 1000 (та же формула, что в get_orders_with_debt
    # и staff.html _drvStopCard) — портировано из прода 2026-08-07.
    total_raw = max(0.0, float(row["items_total"]) - float(row["disc"]))
    total = total_raw - (total_raw % 1000)
    paid = float(row["paid"])
    debt = max(0.0, total - paid)
    if debt <= 0: raise HTTPException(400, "Нет долга")
    order_num = row["order_num"] or str(order_id)
    client = " ".join(filter(None,[row["client_first_name"], row["client_last_name"]])) or "—"
    phone = row["client_phone"] or ""
    addr = row["short_address"] or row["address"] or "—"
    driver_name = _driver_name(staff)

    def _fmtd(v): return f"{int(v):,}".replace(",", " ")
    lines = ["⚠️ <b>Запрос: закрыть заказ в долг</b>",
             f"📋 Заказ: <b>{order_num}</b>",
             f"👤 Клиент: <b>{client}</b>"]
    if phone: lines.append(f"📞 {phone}")
    lines.append(f"📍 {addr}")
    lines.append("")
    if total > 0: lines.append(f"💰 Сумма: <b>{_fmtd(total)} с</b>")
    if paid > 0:  lines.append(f"✅ Оплачено: <b>{_fmtd(paid)} с</b>")
    lines.append(f"❗ <b>Долг: {_fmtd(debt)} сум</b>")
    lines.append(f"\n🚚 Водитель: {driver_name} (web)")
    text = "\n".join(lines)

    approve_kb = {"inline_keyboard": [
        [{"text": "✅ Разрешить закрыть в долг", "callback_data": f"debt_approve:{order_id}"}],
        [{"text": "❌ Отказать",                 "callback_data": f"debt_reject:{order_id}"}],
    ]}
    # Все approvers (с или без tg_id)
    async with db.pool.acquire() as conn:
        all_approvers = await conn.fetch("""
            SELECT s.id, s.first_name, s.last_name, s.tg_id FROM staff s
            JOIN orders o ON o.company_id = s.company_id
            WHERE s.can_approve_debt=TRUE AND s.active=TRUE AND o.id=$1
        """, order_id)
    mgr_msgs = {}
    if BOT_TOKEN:
        async with aiohttp.ClientSession() as _s:
            for mgr in all_approvers:
                tg_id = mgr["tg_id"]
                if not tg_id: continue
                try:
                    resp = await _s.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                        json={"chat_id": tg_id, "text": text, "parse_mode": "HTML",
                              "reply_markup": approve_kb, "disable_web_page_preview": True},
                        timeout=aiohttp.ClientTimeout(total=5))
                    d = await resp.json()
                    if d.get("ok"):
                        mgr_msgs[str(tg_id)] = d["result"]["message_id"]
                except Exception as _e:
                    logging.warning(f"debt DM to {tg_id} failed: {_e}")
    driver_tg_id = staff.get("tg_id")
    await db.create_debt_approval_request(order_id, order_num, driver_tg_id, debt,
                                          _json.dumps(mgr_msgs))
    # Web push
    for mgr in all_approvers:
        sid = mgr["id"]
        if sid:
            asyncio.create_task(send_web_push(sid,
                title="❗ Закрытие в долг",
                body=f"Заказ {order_num} · долг {_fmtd(debt)} сум · {driver_name}",
                push_type="debt_approval",
                order_id=order_id))
    return {"ok": True, "debt": float(debt), "notified": len(mgr_msgs)}

@app.post("/api/staff/my-route/stops/{order_id}/pay")
async def driver_pay(
    order_id: int,
    method: str = Body(..., embed=True),
    amount: float = Body(..., embed=True),
    staff=Depends(get_current_staff)
):
    if not _can_drive(staff): raise HTTPException(403, "Нет доступа")
    if method not in ("cash","card","transfer"): raise HTTPException(400, "Неверный метод")
    name = _driver_name(staff)
    row = await db.add_order_payment(order_id, amount, method, "delivery",
                                     "Оплата при доставке (web)", name,
                                     created_by_staff_id=staff["id"])
    if method == "cash":
        asyncio.create_task(_update_api_channel_stop(order_id))
    # card/transfer: push к менеджерам отправляется из upload_payment_receipt — после сохранения чека
    return {"ok": True, "payment": row}

# ── debt approval requests ─────────────────────────────────────────────────────

@app.post("/api/debt-approvals/push-managers")
async def push_debt_approval_managers_ep(
    order_num: str = Body(..., embed=True),
    debt_amount: float = Body(..., embed=True),
    bot_token_check: str = Body(..., embed=True)
):
    if not BOT_TOKEN or bot_token_check != BOT_TOKEN:
        raise HTTPException(403, "Forbidden")
    # Вызывается ботом (не через staff JWT) — company_id_middleware не может его выставить
    # из токена, поэтому резолвим компанию по заказу явно перед scoped-запросом approvers.
    if db.pool:
        async with db.pool.acquire() as _c:
            order_cid = await _c.fetchval("SELECT company_id FROM orders WHERE order_num=$1", order_num)
        if order_cid:
            db.set_request_company(order_cid)
    approvers = await db.get_debt_approvers()
    title = "❗ Закрытие в долг"
    body_txt = f"Заказ {order_num} · долг {int(debt_amount):,} с"
    for appr in approvers:
        sid = appr.get("id")
        if sid:
            asyncio.create_task(send_web_push(sid, title, body_txt, push_type="debt_approval"))
    return {"ok": True, "notified": len(approvers)}

async def _get_driver_staff_id(order_id: int, payment_row: dict) -> int | None:
    """Возвращает staff.id водителя: сначала из платежа, потом из route_orders."""
    sid = payment_row.get("created_by_staff_id")
    if sid:
        return int(sid)
    try:
        async with db.pool.acquire() as _c:
            r = await _c.fetchrow("""
                SELECT s.id FROM route_orders ro
                JOIN routes rt ON rt.id = ro.route_id
                JOIN staff s  ON s.id  = rt.driver_id
                WHERE ro.order_id = $1 AND s.can_drive = TRUE
                LIMIT 1
            """, order_id)
        return r["id"] if r else None
    except Exception:
        return None

async def _notify_debt_result(order_id: int, order_num: str, driver_tg_id, result: str):
    """Send web push to driver + all approvers when debt request approved/rejected."""
    is_approved = result == "approved"
    push_type = "debt_approved" if is_approved else "debt_rejected"
    title_drv = "✅ Долг одобрен" if is_approved else "❌ Запрос отклонён"
    title_mgr = title_drv
    body_drv = (f"Заказ {order_num} — долг одобрен." if is_approved
                else f"Заказ {order_num} — долг отклонён. Необходимо принять оплату.")
    body_mgr = (f"Заказ {order_num} — долг одобрен" if is_approved
                else f"Заказ {order_num} — запрос на долг отклонён")
    drv_staff_id = None
    if driver_tg_id:
        try:
            async with db.pool.acquire() as _c:
                drv = await _c.fetchrow("SELECT id FROM staff WHERE tg_id=$1 LIMIT 1", str(driver_tg_id))
            if drv:
                drv_staff_id = drv["id"]
                await send_web_push(drv["id"], title_drv, body_drv, push_type=push_type, order_id=order_id,
                                    driver_staff_id=drv["id"])
        except Exception as _e:
            logging.warning(f"_notify_debt_result driver push: {_e}")
    try:
        async with db.pool.acquire() as _c:
            approvers = await _c.fetch("""
                SELECT s.id FROM staff s
                JOIN orders o ON o.company_id = s.company_id
                WHERE s.can_approve_debt=TRUE AND s.active=TRUE AND o.id=$1
            """, order_id)
        for mgr in approvers:
            await send_web_push(mgr["id"], title_mgr, body_mgr, push_type=push_type, order_id=order_id,
                                driver_staff_id=drv_staff_id)
    except Exception as _e:
        logging.warning(f"_notify_debt_result approvers push: {_e}")

@app.post("/api/debt-approvals/notify-rejected")
async def notify_debt_rejected_ep(
    order_id:        int        = Body(..., embed=True),
    order_num:       str        = Body("",  embed=True),
    driver_tg_id:    int | None = Body(None, embed=True),
    bot_token_check: str        = Body(..., embed=True)
):
    if not BOT_TOKEN or bot_token_check != BOT_TOKEN:
        raise HTTPException(403, "Forbidden")
    body_drv = f"Закрытие в долг по заказу {order_num} отклонено. Необходимо принять оплату."
    body_mgr = f"Заказ {order_num} — запрос на долг отклонён"
    if driver_tg_id:
        async with db.pool.acquire() as _c:
            drv = await _c.fetchrow("SELECT id FROM staff WHERE tg_id=$1 LIMIT 1", str(driver_tg_id))
        if drv:
            asyncio.create_task(send_web_push(drv["id"], "❌ Запрос отклонён", body_drv,
                                              push_type="debt_rejected", order_id=order_id))
    async with db.pool.acquire() as _c:
        approvers = await _c.fetch("""
            SELECT s.id FROM staff s
            JOIN orders o ON o.company_id = s.company_id
            WHERE s.can_approve_debt=TRUE AND s.active=TRUE AND o.id=$1
        """, order_id)
    for mgr in approvers:
        asyncio.create_task(send_web_push(mgr["id"], "❌ Запрос отклонён", body_mgr,
                                          push_type="debt_rejected", order_id=order_id))
    return {"ok": True}

@app.post("/api/debt-approvals/notify-approved")
async def notify_debt_approved_ep(
    order_id:        int        = Body(..., embed=True),
    order_num:       str        = Body("",  embed=True),
    driver_tg_id:    int | None = Body(None, embed=True),
    bot_token_check: str        = Body(..., embed=True)
):
    if not BOT_TOKEN or bot_token_check != BOT_TOKEN:
        raise HTTPException(403, "Forbidden")
    body_drv = f"Закрытие в долг по заказу {order_num} одобрено."
    body_mgr = f"Заказ {order_num} — долг одобрен"
    if driver_tg_id:
        async with db.pool.acquire() as _c:
            drv = await _c.fetchrow("SELECT id FROM staff WHERE tg_id=$1 LIMIT 1", str(driver_tg_id))
        if drv:
            asyncio.create_task(send_web_push(drv["id"], "✅ Долг одобрен", body_drv,
                                              push_type="debt_approved", order_id=order_id))
    async with db.pool.acquire() as _c:
        approvers = await _c.fetch("""
            SELECT s.id FROM staff s
            JOIN orders o ON o.company_id = s.company_id
            WHERE s.can_approve_debt=TRUE AND s.active=TRUE AND o.id=$1
        """, order_id)
    for mgr in approvers:
        asyncio.create_task(send_web_push(mgr["id"], "✅ Долг одобрен", body_mgr,
                                          push_type="debt_approved", order_id=order_id))
    return {"ok": True}

@app.get("/api/debt-approvals/pending")
async def get_pending_debt_approvals_ep(staff=Depends(get_current_staff)):
    if not staff.get("can_approve_debt") and staff.get("role") not in ("admin", "manager"):
        raise HTTPException(403, "Нет доступа")
    rows = await db.get_pending_debt_approvals()
    approvers = await db.get_debt_approvers()
    return {"ok": True, "approvals": rows, "approvers": approvers}

def _fmt_stop_text_api(info: dict, num: int) -> str:
    import html as _h
    def h(s): return _h.escape(str(s)) if s else ""
    order_num  = (info.get("order_num") or "").replace("ARTEZ-", "")
    item_count = info.get("item_count", 0) or 0
    addr  = info.get("short_address") or info.get("address") or "—"
    first = (info.get("client_first_name") or "").strip()
    last  = (info.get("client_last_name")  or "").strip()
    client = f"{first} {last}".strip() or "—"
    phone  = info.get("client_phone") or ""
    total  = float(info.get("items_total") or info.get("total_price") or 0)
    disc   = (float(info.get("discount_sum") or 0) + float(info.get("delivery_discount") or 0)
              + float(info.get("manual_discount") or 0))
    net    = max(0.0, total - disc)
    paid   = float(info.get("paid_amount") or 0)
    debt   = max(0.0, net - paid)
    def _f(n): return f"{int(n):,}".replace(",", " ") + " с" if n > 0 else "—"
    pay_line = f"💰 {_f(net)} · Опл: {_f(paid)} · Долг: {_f(debt)}"
    contact = f"👤 {h(client)}"
    if phone: contact += f" 📞{h(phone)}"
    count_str = f" / {item_count}" if item_count else ""
    return f"📦 #{num}·{h(order_num)}{count_str} 📍{h(addr)}\n{contact}\n{pay_line}"

@app.post("/api/debt-approvals/{request_id}/approve")
async def approve_debt_approval_ep(
    request_id: int,
    responsible_id: int = Body(..., embed=True),
    staff=Depends(get_current_staff)
):
    if not staff.get("can_approve_debt") and staff.get("role") not in ("admin", "manager"):
        raise HTTPException(403, "Нет доступа")
    row = await db.resolve_debt_approval(request_id, "approved", staff["id"], responsible_id)
    if not row:
        raise HTTPException(404, "Запрос не найден или уже обработан")
    if BOT_TOKEN:
        order_id = row.get("order_id")
        order_num = row.get("order_num", "")
        driver_tg_id = row.get("driver_tg_id")
        _mgr_raw = row.get("mgr_msgs") or {}
        if isinstance(_mgr_raw, str):
            try: mgr_msgs = _json.loads(_mgr_raw)
            except: mgr_msgs = {}
        else:
            mgr_msgs = _mgr_raw
        approver_name = f"{staff.get('first_name', '')} {staff.get('last_name', '')}".strip() or "Менеджер"
        result_text = f"✅ Закрыт в долг · {order_num}\nОдобрил: {approver_name}"
        async with aiohttp.ClientSession() as s:
            # Уведомить водителя
            if driver_tg_id:
                try:
                    await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                                 json={"chat_id": int(driver_tg_id),
                                       "text": f"✅ Запрос на закрытие долга по заказу <b>{order_num}</b> одобрён.\nЗаказ закрыт в долг.",
                                       "parse_mode": "HTML"})
                except Exception: pass
            # Удалить сообщения менеджеров в TG
            for tg_id_str, msg_id in mgr_msgs.items():
                try:
                    await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage",
                                 json={"chat_id": int(tg_id_str), "message_id": int(msg_id)})
                except Exception: pass
            # Обновить канальное сообщение водителя
            if order_id:
                try:
                    ch_info = await db.get_order_channel_info(order_id)
                    if not ch_info:
                        logging.warning(f"debt approve: get_order_channel_info returned None for order_id={order_id}")
                    elif not ch_info.get("channel_id"):
                        logging.warning(f"debt approve: no channel_id in ch_info order_id={order_id} ch_info={ch_info}")
                    elif not ch_info.get("msg_id"):
                        logging.warning(f"debt approve: no msg_id in ch_info order_id={order_id} ch_info={ch_info}")
                    else:
                        new_text = _fmt_stop_text_api(ch_info, ch_info["stop_num"])
                        kb = {"inline_keyboard": [
                            [{"text": "📦 Позиции", "callback_data": f"rp:{order_id}:items"},
                             {"text": "📋 История", "callback_data": f"rp:{order_id}:history"},
                             {"text": "🔄 Обновить", "callback_data": f"rp:{order_id}:refresh"}],
                        ]}
                        resp = await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
                                     json={"chat_id": ch_info["channel_id"], "message_id": ch_info["msg_id"],
                                           "text": new_text, "parse_mode": "HTML",
                                           "disable_web_page_preview": True,
                                           "reply_markup": kb})
                        if resp.status != 200:
                            body = await resp.text()
                            logging.warning(f"debt approve: editMessageText {resp.status} order_id={order_id}: {body}")
                except Exception as e:
                    logging.warning(f"debt approve: channel update failed order_id={order_id}: {e}")
        # Web push водителю + approvers
        asyncio.create_task(_notify_debt_result(order_id, order_num, driver_tg_id, "approved"))
    return {"ok": True}

@app.post("/api/debt-approvals/{request_id}/reject")
async def reject_debt_approval_ep(
    request_id: int,
    staff=Depends(get_current_staff)
):
    if not staff.get("can_approve_debt") and staff.get("role") not in ("admin", "manager"):
        raise HTTPException(403, "Нет доступа")
    row = await db.resolve_debt_approval(request_id, "rejected", staff["id"])
    if not row:
        raise HTTPException(404, "Запрос не найден или уже обработан")
    if BOT_TOKEN:
        order_id = row.get("order_id")
        order_num = row.get("order_num", "")
        driver_tg_id = row.get("driver_tg_id")
        _mgr_raw = row.get("mgr_msgs") or {}
        if isinstance(_mgr_raw, str):
            try: mgr_msgs = _json.loads(_mgr_raw)
            except: mgr_msgs = {}
        else:
            mgr_msgs = _mgr_raw
        async with aiohttp.ClientSession() as s:
            if driver_tg_id:
                try:
                    await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                                 json={"chat_id": int(driver_tg_id),
                                       "text": f"❌ Запрос на закрытие долга по заказу <b>{order_num}</b> отклонён.\nНеобходимо принять оплату от клиента.",
                                       "parse_mode": "HTML"})
                except Exception: pass
            for tg_id_str, msg_id in mgr_msgs.items():
                try:
                    await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage",
                                 json={"chat_id": int(tg_id_str), "message_id": int(msg_id)})
                except Exception as e:
                    logging.warning(f"debt reject deleteMessage tg_id={tg_id_str}: {e}")
            # Обновить канальное сообщение — вернуть кнопки доставки
            if order_id:
                try:
                    ch_info = await db.get_order_channel_info(order_id)
                    if ch_info and ch_info.get("channel_id") and ch_info.get("msg_id"):
                        new_text = _fmt_stop_text_api(ch_info, ch_info["stop_num"])
                        kb = _route_pickup_kb(order_id, "delivery", closed=await _is_route_closed_for_order(order_id))
                        await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
                                     json={"chat_id": ch_info["channel_id"], "message_id": ch_info["msg_id"],
                                           "text": new_text, "parse_mode": "HTML",
                                           "disable_web_page_preview": True,
                                           "reply_markup": kb})
                except Exception as e:
                    logging.warning(f"debt reject: channel update failed order_id={order_id}: {e}")
    # Web push водителю + approvers
    if BOT_TOKEN:
        asyncio.create_task(_notify_debt_result(order_id, order_num, driver_tg_id, "rejected"))
    return {"ok": True}



# ── Скидка при самовывозе ─────────────────────────────────────
@app.get("/api/admin/settings/self-pickup-discount")
async def get_self_pickup_discount(_=Depends(get_admin)):
    val = await db.get_config("self_pickup_discount")
    return {"ok": True, "discount": float(val) if val else 0.0}

@app.get("/api/settings/self-pickup-discount")
async def get_self_pickup_discount_public(company_slug: str = None):
    cid = await _resolve_client_company_id(company_slug)
    db.set_request_company(cid)
    val = await db.get_config("self_pickup_discount")
    return {"ok": True, "discount": float(val) if val else 0.0}

@app.put("/api/admin/settings/self-pickup-discount")
async def save_self_pickup_discount(discount: float = Body(..., embed=True), _=Depends(get_admin)):
    await db.set_config("self_pickup_discount", str(discount))
    return {"ok": True, "discount": discount}

# ── Скидка при самовывозе (клиент забирает) ──────────────────
@app.get("/api/admin/settings/delivery-discount")
async def get_delivery_discount(_=Depends(get_admin)):
    val = await db.get_config("delivery_discount_pct")
    return {"ok": True, "discount": float(val) if val else 0.0}

@app.get("/api/settings/delivery-discount")
async def get_delivery_discount_public(company_slug: str = None):
    cid = await _resolve_client_company_id(company_slug)
    db.set_request_company(cid)
    val = await db.get_config("delivery_discount_pct")
    return {"ok": True, "discount": float(val) if val else 0.0}

@app.put("/api/admin/settings/delivery-discount")
async def save_delivery_discount(discount: float = Body(..., embed=True), _=Depends(get_admin)):
    await db.set_config("delivery_discount_pct", str(discount))
    return {"ok": True, "discount": discount}


# ── Настройки сайта ──────────────────────────────────────────
# Fallback: если в БД пусто — берём env-переменную, затем хардкод
SITE_SETTINGS_DEFAULTS = {
    # Соцсети (каждая компания заполняет свои)
    "social_instagram":    "",
    "social_tg_group":     "",
    # Контакты
    "contact_short":       "",
    "contact_main":        "",
    "contact_email":       "",
    "contact_zarafshan_1":         "",
    "contact_zarafshan_2":         "",
    "contact_zarafshan_telegram":  "",
    "contact_zarafshan_admin_tg":  "",
    "contact_zarafshan_whatsapp":  "",
    "contact_zarafshan_instagram": "",
    "branch_zarafshan_location":   "",
    "contact_navoi_1":             "",
    "contact_navoi_2":             "",
    "contact_navoi_telegram":      "",
    "contact_navoi_admin_tg":      "",
    "contact_navoi_whatsapp":      "",
    "contact_navoi_instagram":     "",
    "branch_navoi_location":       "",
    # Telegram бот (настраивается каждой компанией)
    "tg_group_id":         "",
    "tg_group_zarafshan":  "",
    "tg_group_navoi":      "",
    "tg_group_sms_id":     "",
    "order_bot_username":  "",
    # Яндекс Карты — fallback из env (общий ключ платформы)
    "yandex_maps_key":     os.getenv("YANDEX_MAPS_KEY", ""),
    # Eskiz SMS (каждая компания подключает свой аккаунт)
    "eskiz_email":         "",
    "eskiz_password":      "",
    "eskiz_from":          "4546",
    "sms_text_register":   "Kod podtverzhdeniya: {code}",
    "sms_text_login":      "Kod podtverzhdeniya dlya vhoda: {code}",
    "sms_text_reset":      "Kod vosstanovleniya parolya: {code}",
    "sms_pickup_enabled":       "false",
    "sms_pickup_template_ru":   "Zakaz {order_num} prinyat kurerom ({count} poz.). Voprosy: {phones} bot {bot_link} Status na sayte {site_link}",
    "sms_pickup_template_uz":   "{order_num} buyurtma kuryer tomonidan qabul qilindi ({count} dona). Savollar: {phones} bot {bot_link} Holat: {site_link}",
    "sms_ready_enabled":        "false",
    "sms_ready_template_ru":    "Zakaz {order_num} gotov. Voprosy: {phones} bot {bot_link} Status na sayte {site_link}",
    "sms_ready_template_uz":    "{order_num} buyurtma tayyor. Savollar: {phones} bot {bot_link} Holat: {site_link}",
    # Google Sheets
    "sheets_url":          "",
    # Группа уведомлений
    "new_clients_group_id":    "",
    # Канал водителей/доставщиков (маршруты)
    "delivery_channel_zarafshan_id":    "",
    "delivery_channel_navoi_id":        "",
    "delivery_channel_zarafshan_link":  "",
    "delivery_channel_navoi_link":      "",
    # Лиды
    "leads_group_id":          "",
    "leads_group_zarafshan":   "",
    "leads_group_navoi":       "",
    "leads_group_enabled": "0",
    "lead_notify_ru": (
        "🎯 {lead_code} · {source_full}\n"
        "👤 {client_name}  📞 {client_phone}\n"
        "🏢 {branch}{note_inline}\n"
        "{location_link}"
    ),
    "lead_notify_uz": (
        "🎯 {lead_code} · {source_full}\n"
        "👤 {client_name}  📞 {client_phone}\n"
        "🏢 {branch}{note_inline}\n"
        "{location_link}"
    ),
    "callback_overdue_minutes": "10",
    # Комиссия агентов за лиды
    "agent_commission_type":    "percent",
    "agent_commission_percent": "5.0",
    "agent_commission_fixed":   "0",
    # Текст чека (компания заполняет под себя)
    "receipt_header_text": "",
    "receipt_slogan":      "",
    "receipt_footer_note": "",
    # "О компании" в футере сайта
    "footer_about_ru": "",
    "footer_about_uz": "",
    # Секция "Стать агентом" на главной (card1 = % берётся из agent_commission_*, не дублируется)
    "agent_title_ru": "Станьте нашим агентом",
    "agent_title_uz": "Bizning agentimiz bo'ling",
    "agent_text_ru": "Рекомендуйте наши услуги и получайте вознаграждение за каждый заказ. Без вложений, без опыта — только ваша сеть знакомых.",
    "agent_text_uz": "Xizmatlarimizni tavsiya qiling va har bir buyurtma uchun mukofot oling. Sarmoyasiz, tajribasiz — faqat sizning tanishlaringiz doirasi.",
    "agent_card2_value":    "CRM",
    "agent_card2_label_ru": "личный кабинет",
    "agent_card2_label_uz": "shaxsiy kabinet",
    "agent_card3_value":    "∞",
    "agent_card3_label_ru": "без лимита",
    "agent_card3_label_uz": "cheklovsiz",
    "agent_cta_text_ru":       "🤝 Стать агентом",
    "agent_cta_text_uz":       "🤝 Agent bo'lish",
    "agent_learnmore_text_ru": "Подробнее о программе",
    "agent_learnmore_text_uz": "Dastur haqida batafsil",
    # Видео-карточка на главной странице сайта (своё видео компании)
    "site_video_enabled":      "false",
    "site_video_placement":    "hero",  # hero | how_it_works | reviews | floating
    # Правила приёма и выдачи заказов — JSON-массив [{emoji,title_ru,text_ru,title_uz,text_uz}].
    # Пусто по умолчанию: реальный дефолт живёт в config company_id=0 и копируется
    # в новую компанию при регистрации (см. seed_company_order_rules).
    "order_rules": "",
    # Режим "техработ" сайта/бота — ГЛОБАЛЬНЫЙ дефолт "false" (не ломает уже живые
    # компании, у которых просто нет этой строки в config). Для НОВЫХ компаний
    # явно ставится "true" в _provision_company — см. комментарий там.
    "site_maintenance_mode": "false",
    "bot_maintenance_mode": "false",
}

async def _get_cfg(key: str) -> str:
    """БД → env-fallback из SITE_SETTINGS_DEFAULTS."""
    val = await db.get_config(key)
    if val:
        return val
    return SITE_SETTINGS_DEFAULTS.get(key, "")

@app.get("/api/settings/site")
async def get_site_settings(company_slug: str = None):
    # Публичный эндпоинт — соцсети, контакты и ключ карты (не секреты)
    cid = await _resolve_client_company_id(company_slug)
    db.set_request_company(cid)
    PUBLIC_KEYS = [
        "social_instagram", "social_tg_group", "order_bot_username",
        "contact_short", "contact_main", "contact_email",
        "contact_zarafshan_1", "contact_zarafshan_2", "contact_zarafshan_telegram", "contact_zarafshan_admin_tg", "contact_zarafshan_whatsapp", "contact_zarafshan_instagram",
        "contact_navoi_1", "contact_navoi_2", "contact_navoi_telegram", "contact_navoi_admin_tg", "contact_navoi_whatsapp", "contact_navoi_instagram",
        "yandex_maps_key",
        "branch_zarafshan_location", "branch_navoi_location",
        "footer_about_ru", "footer_about_uz",
        "agent_commission_type", "agent_commission_percent", "agent_commission_fixed",
        "agent_title_ru", "agent_title_uz", "agent_text_ru", "agent_text_uz",
        "agent_card2_value", "agent_card2_label_ru", "agent_card2_label_uz",
        "agent_card3_value", "agent_card3_label_ru", "agent_card3_label_uz",
        "agent_cta_text_ru", "agent_cta_text_uz",
        "agent_learnmore_text_ru", "agent_learnmore_text_uz",
        "site_video_enabled", "site_video_placement",
        "order_rules", "site_maintenance_mode",
    ]
    result = {}
    for key in PUBLIC_KEYS:
        result[key] = await _get_cfg(key)

    # Доступность SMS-регистрации на сайте: ручной тогл + реально настроен Eskiz
    # + на балансе хватает хотя бы на 1 SMS (~1000 сум). Сам баланс/логин/пароль
    # Eskiz наружу не отдаём — только готовый булев флаг.
    sms_reg_enabled = (await _get_cfg("sms_registration_enabled")) != "false"
    eskiz_configured = bool(await _get_cfg("eskiz_email")) and bool(await _get_cfg("eskiz_password"))
    sms_registration_available = False
    if sms_reg_enabled and eskiz_configured:
        balance = await get_eskiz_balance_cached()
        sms_registration_available = balance is not None and balance >= 1000
    result["sms_registration_available"] = sms_registration_available

    return {"ok": True, "settings": result}


class SiteSettings(BaseModel):
    social_instagram:    str | None = None
    social_tg_group:     str | None = None
    contact_short:       str | None = None
    contact_main:        str | None = None
    contact_email:       str | None = None
    contact_zarafshan_1:        str | None = None
    contact_zarafshan_2:        str | None = None
    contact_zarafshan_telegram: str | None = None
    contact_zarafshan_admin_tg: str | None = None
    contact_zarafshan_whatsapp: str | None = None
    contact_zarafshan_instagram:str | None = None
    branch_zarafshan_location:  str | None = None
    contact_navoi_1:            str | None = None
    contact_navoi_2:            str | None = None
    contact_navoi_telegram:     str | None = None
    contact_navoi_admin_tg:     str | None = None
    contact_navoi_whatsapp:     str | None = None
    contact_navoi_instagram:    str | None = None
    branch_navoi_location:      str | None = None
    delivery_channel_zarafshan_id:    str | None = None
    delivery_channel_navoi_id:        str | None = None
    delivery_channel_zarafshan_link:  str | None = None
    delivery_channel_navoi_link:      str | None = None
    tg_group_id:         str | None = None
    tg_group_zarafshan:  str | None = None
    tg_group_navoi:      str | None = None
    tg_group_sms_id:     str | None = None
    yandex_maps_key:     str | None = None
    eskiz_email:         str | None = None
    eskiz_password:      str | None = None
    eskiz_from:          str | None = None
    sms_text_register:   str | None = None
    sms_text_login:      str | None = None
    sms_text_reset:      str | None = None
    sms_pickup_enabled:      str | None = None
    sms_pickup_template_ru:  str | None = None
    sms_pickup_template_uz:  str | None = None
    sms_ready_enabled:       str | None = None
    sms_ready_template_ru:   str | None = None
    sms_ready_template_uz:   str | None = None
    sms_registration_enabled: str | None = None
    sheets_url:          str | None = None
    leads_group_id:          str | None = None
    leads_group_zarafshan:   str | None = None
    leads_group_navoi:       str | None = None
    leads_group_enabled:         str | None = None
    lead_notify_ru:              str | None = None
    callback_overdue_minutes:    str | None = None
    agent_commission_type:       str | None = None
    agent_commission_percent:    str | None = None
    agent_commission_fixed:      str | None = None
    receipt_header_text: str | None = None
    receipt_slogan:      str | None = None
    receipt_footer_note: str | None = None
    footer_about_ru:     str | None = None
    footer_about_uz:     str | None = None
    agent_title_ru:          str | None = None
    agent_title_uz:          str | None = None
    agent_text_ru:           str | None = None
    agent_text_uz:           str | None = None
    agent_card2_value:       str | None = None
    agent_card2_label_ru:    str | None = None
    agent_card2_label_uz:    str | None = None
    agent_card3_value:       str | None = None
    agent_card3_label_ru:    str | None = None
    agent_card3_label_uz:    str | None = None
    agent_cta_text_ru:       str | None = None
    agent_cta_text_uz:       str | None = None
    agent_learnmore_text_ru: str | None = None
    agent_learnmore_text_uz: str | None = None
    site_video_enabled:      str | None = None
    site_video_placement:    str | None = None
    order_rules:             str | None = None
    site_maintenance_mode:   str | None = None
    bot_maintenance_mode:    str | None = None

@app.get("/api/admin/settings/site")
async def get_admin_site_settings(_=Depends(get_admin)):
    result = {key: await _get_cfg(key) for key in SITE_SETTINGS_DEFAULTS}
    return {"ok": True, "settings": result}

@app.put("/api/admin/settings/site")
async def save_site_settings(body: SiteSettings, _=Depends(get_admin)):
    data = {k: v for k, v in body.dict().items() if v is not None}
    for key, val in data.items():
        await db.set_config(key, val)
    return {"ok": True}


# ── Telegram: шаблоны уведомлений ──────────────────────────────────────
@app.get("/api/admin/settings/tg-messages")
async def get_tg_messages(_=Depends(get_admin)):
    rows = await db.get_tg_status_messages()
    return rows

@app.put("/api/admin/settings/tg-messages/{status}")
async def save_tg_message(status: str, body: dict, _=Depends(get_admin)):
    ALL_STATUSES = {"new","confirmed","pickup","received","washing","drying","packing","ready","delivery","delivered","cancelled"}
    if status not in ALL_STATUSES:
        raise HTTPException(status_code=400, detail="Неизвестный статус")
    row = await db.upsert_tg_status_message(
        status=status,
        enabled=bool(body.get("enabled", True)),
        message_ru=body.get("message_ru", ""),
        message_uz=body.get("message_uz", ""),
    )
    return row

@app.get("/api/admin/tg-clients")
async def get_tg_clients(search: str = "", _=Depends(get_admin)):
    rows = await db.get_tg_clients(search=search)
    return {"clients": rows, "total": len(rows)}

@app.patch("/api/admin/tg-clients/{tg_id}")
async def tg_client_update(tg_id: int, body: dict, _=Depends(get_admin)):
    allowed = {"first_name", "last_name", "phone"}
    data = {k: v for k, v in body.items() if k in allowed}
    if not data:
        raise HTTPException(status_code=400, detail="Нет полей для обновления")
    await db.update_tg_client(tg_id, data)
    return {"ok": True}

@app.patch("/api/admin/tg-clients/{tg_id}/block")
async def tg_client_block(tg_id: int, body: dict, _=Depends(get_admin)):
    if not (apass := await get_admin_pass()) or body.get("admin_password") != apass:
        raise HTTPException(status_code=403, detail="Неверный пароль администратора")
    blocked = bool(body.get("blocked", True))
    await db.block_tg_client(tg_id, blocked)
    return {"ok": True, "blocked": blocked}

@app.delete("/api/admin/tg-clients/{tg_id}")
async def tg_client_delete(tg_id: int, body: dict, _=Depends(get_admin)):
    if not (apass := await get_admin_pass()) or body.get("admin_password") != apass:
        raise HTTPException(status_code=403, detail="Неверный пароль администратора")
    await db.delete_tg_client(tg_id)
    return {"ok": True}


@app.post("/api/callback")
async def site_callback_request(body: dict = Body(...)):
    """Обратный звонок с сайта — создаёт лид и запускает автодозвон.

    Та же уязвимость, что и в /api/orders (см. коммент там): анонимные запросы
    без company_slug молча уходили в company_id=1 (дефолт middleware)."""
    company_slug = (body.get("company_slug") or "").strip()
    if company_slug:
        db.set_request_company(await _resolve_client_company_id(company_slug))
    phone        = (body.get("phone") or "").strip()
    profile_phone = (body.get("profile_phone") or "").strip()
    name         = (body.get("name") or "").strip()
    branch       = (body.get("branch") or "").strip()

    # Звоним на зарегистрированный номер если есть, иначе на введённый
    raw_phone = profile_phone or phone
    cb_phone  = _ami_phone(raw_phone)
    logging.info(f"Callback request: name={name!r} raw={raw_phone!r} ami={cb_phone!r} branch={branch!r}")

    if not cb_phone or len(cb_phone) < 9:
        raise HTTPException(400, detail="Неверный номер телефона")

    # Создаём лид
    lead = None
    try:
        lead = await db.create_lead({
            "client_name":  name or phone,
            "client_phone": phone or profile_phone,
            "service":      "Обратный звонок",
            "branch":       branch,
            "note":         "Обратный звонок с сайта",
            "status":       "new",
            "source":       "site",
        })
    except Exception as e:
        logging.warning(f"Callback lead create failed: {e}")

    lead_code = (lead or {}).get("lead_code") or "?"

    # Запускаем автодозвон
    try:
        async with db.pool.acquire() as conn:
            camp = await conn.fetchrow(
                "INSERT INTO autodial_campaigns (name,ivr_exten,max_parallel,source_type,status) "
                "VALUES ($1,'7000',1,'callback','running') RETURNING id",
                f"Обратный звонок {lead_code}"
            )
            await conn.execute(
                "INSERT INTO autodial_calls (campaign_id,source_type,phone,name) VALUES ($1,'callback',$2,$3)",
                camp["id"], cb_phone, name or phone
            )
            await conn.execute(
                "UPDATE autodial_campaigns SET total_count=1 WHERE id=$1", camp["id"]
            )
        logging.info(f"Callback campaign created: id={camp['id']} phone={cb_phone}")
    except Exception as e:
        logging.error(f"Callback autodial failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Ошибка создания звонка")

    # TG-уведомление о новом лиде
    if lead:
        site_staff = {"role": "site", "first_name": "Сайт", "last_name": "", "login": "site"}
        asyncio.create_task(_notify_new_lead(lead, site_staff))
        await db.upsert_crm_client(phone=phone or profile_phone, first_name=name or phone, last_name="", source="callback")
        await db.refresh_crm_client_stats(phone or profile_phone)

    return {"ok": True, "lead_code": lead_code}


@app.post("/api/orders")
async def create_order_from_site(order: OrderRequest, user=Depends(get_optional_user)):
    """Заявка с сайта/бота → сохраняется как лид для обработки сотрудниками.

    company_id: для анонимных посетителей (нет JWT) глобальный company_id_middleware
    дефолтит на 1 (ARTEZ) — без явного company_slug заявки ЛЮБОЙ другой компании
    молча уходили бы в company_id=1 (реальный баг, найден 2026-07-29). Если slug
    передан — переопределяем явно; если нет (старый закэшированный фронтенд или
    уже аутентифицированный пользователь) — доверяем тому, что уже поставил
    middleware, ничего не ломаем для авторизованных запросов."""
    if order.company_slug:
        db.set_request_company(await _resolve_client_company_id(order.company_slug))
    full_name = f"{order.first_name} {order.last_name}".strip()
    note_parts = []
    if order.service_type: note_parts.append(f"Тип: {order.service_type}")
    if order.pickup_date:  note_parts.append(f"Дата: {order.pickup_date}")
    if order.pickup_time:  note_parts.append(f"Время: {order.pickup_time}")
    if order.is_quick:     note_parts.append("Быстрая заявка")
    note = " · ".join(note_parts) if note_parts else None

    # Определяем агента: сначала по авторизованному пользователю, затем по телефону
    volunteer_id = None
    agent_staff = None
    if user:
        agent_staff = await db.get_staff_by_site_user(user["id"])
        if not agent_staff:
            agent_staff = await db.get_staff_by_login(user["phone"])
    if not agent_staff:
        agent_staff = await db.get_staff_by_login(order.phone)
    if agent_staff and agent_staff.get("role") == "agent" and agent_staff.get("active"):
        volunteer_id = agent_staff["id"]

    lead_source = order.source if order.source in ("site", "bot") else "site"
    lead = await db.create_lead({
        "client_name":   full_name,
        "client_phone":  order.phone,
        "service":       order.service,
        "branch":        order.branch,
        "city":          order.city,
        "address":       order.address,
        "short_address": order.address,
        "note":          note,
        "status":        "new",
        "created_by":    None,
        "volunteer_id":  volunteer_id,
        "location":      order.location,
        "location_address": order.location_address,
        "source":        lead_source,
        "client_tg_id":  order.client_tg_id,
        "pickup_date":   order.pickup_date or "",
        "pickup_time":   order.pickup_time or "",
    })
    lead_code = (lead or {}).get("lead_code") or f"#{(lead or {}).get('id','?')}"
    if lead:
        src_label = "Telegram-бот" if lead_source == "bot" else "сайта"
        await db.add_lead_call(lead["id"], None, action="created",
                               note=f"Лид создан с {src_label} ({lead_code})")

    creator_role = lead_source if lead_source in ("site", "bot") else "site"
    creator_staff = {"role": creator_role, "first_name": "Сайт" if creator_role == "site" else "Telegram", "last_name": "", "login": creator_role}
    asyncio.create_task(_notify_new_lead(lead or {}, creator_staff))
    await notify_sheets_new_order(lead_code, order)

    # Автозвонок при быстрой заявке (обратный звонок)
    if order.is_quick and lead:
        try:
            raw_phone = (user["phone"] if user and user.get("phone") else order.phone) or ""
            cb_phone = _ami_phone(raw_phone)
            logging.info(f"Callback autodial: raw={raw_phone!r} → ami={cb_phone!r} lead={lead_code}")
            if cb_phone and len(cb_phone) >= 9:
                async with db.pool.acquire() as conn:
                    camp = await conn.fetchrow(
                        "INSERT INTO autodial_campaigns (name,ivr_exten,max_parallel,source_type,status) "
                        "VALUES ($1,'7000',1,'callback','running') RETURNING id",
                        f"Обратный звонок {lead_code}"
                    )
                    await conn.execute(
                        "INSERT INTO autodial_calls (campaign_id,source_type,phone,name) VALUES ($1,'callback',$2,$3)",
                        camp["id"], cb_phone, full_name or raw_phone
                    )
                    await conn.execute(
                        "UPDATE autodial_campaigns SET total_count=1 WHERE id=$1", camp["id"]
                    )
                    logging.info(f"Callback campaign created: id={camp['id']} phone={cb_phone}")
            else:
                logging.warning(f"Callback: телефон слишком короткий {raw_phone!r}")
        except Exception as e:
            logging.error(f"Автозвонок callback failed: {e}", exc_info=True)

    await db.upsert_crm_client(
        phone=order.phone,
        first_name=order.first_name,
        last_name=order.last_name,
        source="site",
    )
    await db.refresh_crm_client_stats(order.phone)

    return {"ok": True, "order_num": lead_code}


class BotLeadRequest(BaseModel):
    client_name: str
    client_phone: str
    branch: str = ""
    city: str = ""
    address: str = ""
    service: str = ""
    service_type: str = ""
    pickup_date: str = ""
    pickup_time: str = ""
    note: str = ""
    location: str = ""
    location_address: str = ""
    client_tg_id: int | None = None
    is_quick: bool = False

    @field_validator("client_phone")
    @classmethod
    def validate_phone(cls, v):
        v = normalize_phone(v)
        if not PHONE_RE.match(v):
            raise ValueError("Неверный формат номера. Используйте +998XXXXXXXXX")
        return v

@app.post("/api/bot/lead")
async def create_bot_lead(req: BotLeadRequest, x_bot_token: str = Header(None, alias="X-Bot-Token")):
    """Заявка из Telegram-бота → лид."""
    if not BOT_TOKEN or x_bot_token != BOT_TOKEN:
        raise HTTPException(status_code=403, detail="Нет доступа")

    note_parts = []
    if req.is_quick:        note_parts.append("Быстрая заявка (бот)")
    if req.service_type:    note_parts.append(f"Тип: {req.service_type}")
    if req.pickup_date:     note_parts.append(f"Дата: {req.pickup_date}")
    if req.pickup_time:     note_parts.append(f"Время: {req.pickup_time}")
    if req.note:            note_parts.append(req.note)
    note = " · ".join(note_parts) if note_parts else None

    lead = await db.create_lead({
        "client_name":     req.client_name,
        "client_phone":    req.client_phone,
        "service":         req.service,
        "branch":          req.branch,
        "city":            req.city,
        "address":         req.address,
        "short_address":   req.address,
        "note":            note,
        "status":          "new",
        "created_by":      None,
        "volunteer_id":    None,
        "location":        req.location,
        "location_address": req.location_address,
        "source":          "bot",
        "client_tg_id":    req.client_tg_id,
        "pickup_date":     req.pickup_date or "",
        "pickup_time":     req.pickup_time or "",
    })
    lead_code = (lead or {}).get("lead_code") or f"#{(lead or {}).get('id','?')}"
    if lead:
        await db.add_lead_call(lead["id"], None, action="created",
                               note=f"Лид создан через Telegram-бот ({lead_code})")

    bot_staff = {"role": "bot", "first_name": "Telegram", "last_name": "", "login": "bot"}
    asyncio.create_task(_notify_new_lead(lead or {}, bot_staff))

    if req.client_phone:
        await db.upsert_crm_client(
            phone=req.client_phone,
            first_name=req.client_name,
            last_name="",
            tg_id=req.client_tg_id,
            source="bot",
        )
        await db.refresh_crm_client_stats(req.client_phone)

    return {"ok": True, "lead_code": lead_code, "lead_id": (lead or {}).get("id")}



async def _notify_group_site_lead(lead_code: str, data: "OrderRequest", lead_id: int = None):
    """Telegram: новый лид с сайта — кнопка Взять лид прямо в группе."""
    if not BOT_TOKEN:
        return
    chat_id = await _group_id_for_branch(data.branch or "")
    if not chat_id:
        return

    def he(s):
        return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;") if s else "—"

    full_name = f"{data.first_name} {data.last_name}".strip()

    if data.is_quick:
        text = (
            f"🎯 Новый лид <b>{lead_code}</b> — быстрая заявка (сайт)\n"
            f"━━━━━━━━━━\n"
            f"👤 {he(full_name)}\n"
            f"📞 {he(data.phone)}\n"
            f"━━━━━━━━━━"
        )
    else:
        lines = [
            f"🎯 Новый лид <b>{lead_code}</b> (сайт)",
            f"━━━━━━━━━━",
            f"👤 {he(full_name)}",
            f"📞 {he(data.phone)}",
        ]
        if data.branch:      lines.append(f"🏢 {he(branch_ru(data.branch))}")
        if data.city:        lines.append(f"📍 {he(data.city)}")
        if data.address:     lines.append(f"🏠 {he(data.address)}")
        if data.service:     lines.append(f"🧺 {he(data.service)}")
        if data.pickup_date: lines.append(f"📅 {he(data.pickup_date)} {he(data.pickup_time)}".rstrip())
        lines.append("━━━━━━━━━━")
        text = "\n".join(lines)

    keyboard = None
    if lead_id:
        keyboard = {"inline_keyboard": [[
            {"text": "✋ Взять лид", "callback_data": f"take_lead_{lead_id}"}
        ]]}

    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {"chat_id": str(chat_id), "text": text, "parse_mode": "HTML"}
        if keyboard:
            payload["reply_markup"] = keyboard
        async with aiohttp.ClientSession() as s:
            await s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8))
    except Exception as e:
        logging.warning(f"_notify_group_site_lead error: {e}")


# ── ОБСЛУЖИВАНИЕ БД ──────────────────────────────────────────────────────────
@app.post("/api/admin/db-maintenance")
async def db_maintenance(op: str = Body(..., embed=True), cid: int = Depends(_get_admin_cid)):
    if not db.pool:
        raise HTTPException(status_code=503, detail="DB unavailable")
    async with db.pool.acquire() as conn:
        if op == "purge_deleted_leads_data":
            r1 = await conn.execute(
                "DELETE FROM agent_notifications WHERE lead_id NOT IN (SELECT id FROM leads) AND company_id=$1", cid)
            r2 = await conn.execute(
                "DELETE FROM lead_reminders       WHERE lead_id NOT IN (SELECT id FROM leads) AND company_id=$1", cid)
            r3 = await conn.execute(
                "DELETE FROM lead_calls           WHERE lead_id NOT IN (SELECT id FROM leads) AND company_id=$1", cid)
            total = sum(int(r.split()[-1]) for r in [r1, r2, r3] if r)
            return {"ok": True, "message": f"Удалено {total} записей (уведомления: {r1.split()[-1]}, напоминания: {r2.split()[-1]}, журнал: {r3.split()[-1]})"}

        elif op == "purge_deleted_history":
            result = await conn.execute("""
                DELETE FROM order_status_history
                WHERE order_num NOT IN (
                    SELECT order_num FROM orders WHERE order_num IS NOT NULL
                )
                AND order_num IN (SELECT order_num FROM orders WHERE company_id=$1)
            """, cid)
            count = result.split()[-1] if result else "0"
            return {"ok": True, "message": f"Удалено {count} записей истории удалённых заказов"}

        elif op == "truncate_history":
            # order_status_history не имеет своей колонки company_id (ключ — order_num),
            # TRUNCATE стирал бы историю ВСЕХ компаний разом — заменено на DELETE по своим заказам.
            result = await conn.execute(
                "DELETE FROM order_status_history WHERE order_num IN (SELECT order_num FROM orders WHERE company_id=$1)",
                cid)
            count = result.split()[-1] if result else "0"
            return {"ok": True, "message": f"Удалено {count} записей истории (своей компании)"}

        elif op == "vacuum":
            # VACUUM — физическая операция над всей таблицей, не может быть ограничена одной
            # компанией. Недоступно тенантскому админу — это платформенная операция.
            raise HTTPException(status_code=403, detail="VACUUM недоступен для админа компании — обратитесь в поддержку платформы")

        elif op == "purge_old_leads":
            result = await conn.execute("""
                DELETE FROM leads
                WHERE status IN ('closed','cancelled')
                  AND created_at < NOW() - INTERVAL '90 days'
                  AND company_id=$1
            """, cid)
            count = result.split()[-1] if result else "0"
            return {"ok": True, "message": f"Удалено {count} старых лидов"}

        else:
            raise HTTPException(status_code=400, detail=f"Неизвестная операция: {op}")


# ══════════════════════════════════════════════════════════════════════════════
# CHAT
# ══════════════════════════════════════════════════════════════════════════════

def _tpl(text: str, session: dict) -> str:
    """Подставляет {name} → первое слово имени клиента. Без имени — убирает {name} вместе с соседней запятой."""
    if not text:
        return text
    raw = (session.get('client_name') or '').strip()
    first = raw.split()[0] if raw else ''
    if first:
        return text.replace('{name}', first)
    # убираем «, {name}», «{name},» и просто «{name}»
    for pat in (', {name}', ' {name},', '{name}, ', '{name}'):
        text = text.replace(pat, '')
    return text

class _ChatMgr:
    def __init__(self):
        self.clients: dict[str, set] = {}   # code → set of WebSocket (multi-device)
        self.staff:   dict[int,  WebSocket] = {}   # staff_id → ws
        self.staff_company: dict[int, int] = {}    # staff_id → company_id (изоляция broadcast между компаниями)

    async def connect_client(self, code: str, ws: WebSocket):
        await ws.accept()
        if code not in self.clients:
            self.clients[code] = set()
        self.clients[code].add(ws)

    async def connect_staff(self, staff_id: int, ws: WebSocket, company_id: int = 1):
        await ws.accept()
        self.staff[staff_id] = ws
        self.staff_company[staff_id] = company_id

    def disconnect_client(self, code: str, ws: WebSocket = None):
        if ws is not None:
            self.clients.get(code, set()).discard(ws)
            if not self.clients.get(code):
                self.clients.pop(code, None)
        else:
            self.clients.pop(code, None)

    def disconnect_staff(self, staff_id: int):
        self.staff.pop(staff_id, None)
        self.staff_company.pop(staff_id, None)

    async def send_client(self, code: str, data: dict):
        dead = []
        for ws in list(self.clients.get(code, set())):
            try: await ws.send_json(data)
            except: dead.append(ws)
        for ws in dead: self.disconnect_client(code, ws)

    async def send_staff(self, staff_id: int, data: dict):
        ws = self.staff.get(staff_id)
        if ws:
            try: await ws.send_json(data)
            except: self.disconnect_staff(staff_id)

    async def broadcast_staff(self, data: dict, exclude: int = None, company_id: int = None):
        """company_id обязателен для межтенантной изоляции — без него шлёт ВСЕМ подключённым сотрудникам всех компаний."""
        dead = []
        for sid, ws in list(self.staff.items()):
            if sid == exclude: continue
            if company_id is not None and self.staff_company.get(sid) != company_id: continue
            try: await ws.send_json(data)
            except: dead.append(sid)
        for sid in dead: self.disconnect_staff(sid)

    def staff_online_ids(self, company_id: int = None) -> set:
        if company_id is None:
            return set(self.staff.keys())
        return {sid for sid in self.staff if self.staff_company.get(sid) == company_id}

_chat = _ChatMgr()


async def _chat_timeout_worker():
    """Каждые 60 сек проверяет неактивные чаты и закрывает их."""
    await asyncio.sleep(60)
    while True:
        try:
            # 1. Предупредить
            to_warn = await db.get_sessions_to_warn()
            for s in to_warn:
                lang = s.get('lang') or 'uz'
                warn_text = _tpl(await db.get_chat_template_text('warn_timeout', lang) or \
                    "⏰ Вы давно не отвечаете. Чат будет автоматически закрыт через 1 минуту.", s)
                msg = await db.add_chat_message(s['id'], 'bot', 'ARTEZ', warn_text)
                if msg:
                    await _chat.send_client(s['code'], {"type": "message", "msg": _msg_json(msg)})
                    claimed = s.get('claimed_by')
                    if claimed:
                        await _chat.send_staff(claimed, {"type": "message", "code": s['code'], "msg": _msg_json(msg)})
                    else:
                        await _chat.broadcast_staff({"type": "message", "code": s['code'], "msg": _msg_json(msg)},
                                                     company_id=s.get('company_id'))
                await db.set_chat_warned(s['code'])

            # 2. Закрыть
            to_close = await db.get_sessions_to_close()
            for s in to_close:
                lang   = s.get('lang') or 'uz'
                gender = (s.get('staff_gender') or 'M').upper()
                bye_key = 'bye_f' if gender == 'F' else 'bye_m'
                bye = _tpl(await db.get_chat_template_text(bye_key, lang) or \
                    ("Я рада, что смогла вам помочь! 😊" if gender == 'F' else "Я рад, что смог вам помочь! 😊"), s)
                msg = await db.add_chat_message(s['id'], 'bot', 'ARTEZ', bye)
                if msg:
                    await _chat.send_client(s['code'], {"type": "message", "msg": _msg_json(msg)})
                    claimed = s.get('claimed_by')
                    if claimed:
                        await _chat.send_staff(claimed, {"type": "message", "code": s['code'], "msg": _msg_json(msg)})
                    else:
                        await _chat.broadcast_staff({"type": "message", "code": s['code'], "msg": _msg_json(msg)},
                                                     company_id=s.get('company_id'))
                await asyncio.sleep(1)
                closed = await db.close_chat_session(s['code'])
                if closed:
                    await _chat.send_client(s['code'], {"type": "chat_closed"})
                    await _chat.broadcast_staff({"type": "chat_closed", "code": s['code']}, company_id=s.get('company_id'))
        except Exception as e:
            logging.warning(f"_chat_timeout_worker error: {e}")
        await asyncio.sleep(60)

def _gen_chat_code() -> str:
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))


def _msg_json(msg: dict) -> dict:
    m = dict(msg)
    if hasattr(m.get('created_at'), 'isoformat'):
        m['created_at'] = m['created_at'].isoformat()
    return m


@app.post("/api/chat/start")
async def chat_start(body: dict = Body(...)):
    client_phone = (body.get("client_phone") or "").strip()
    client_name  = (body.get("client_name")  or "").strip()
    branch       = (body.get("branch")       or "").strip()
    lang         = (body.get("lang")         or "uz").strip().lower()[:5]
    cid = await _resolve_client_company_id(body.get("company_slug"))
    db.set_request_company(cid)

    code = _gen_chat_code()
    session = await db.create_chat_session(code, client_phone, client_name, branch, lang)
    if not session:
        raise HTTPException(500, "Не удалось создать сессию")

    welcome = _tpl(await db.get_chat_template_text('welcome', lang) or \
        "Здравствуйте! 👋 Спасибо, что обратились в ARTEZ. Менеджер ответит вам в ближайшее время.", session)
    await db.add_chat_message(session['id'], 'bot', 'ARTEZ', welcome)

    # Уведомить подключённых сотрудников той же компании через WS
    await _chat.broadcast_staff({
        "type": "new_chat",
        "code": code,
        "client_name": client_name or client_phone or "Клиент",
        "client_phone": client_phone,
        "branch": branch,
        "created_at": session['created_at'].isoformat() if hasattr(session.get('created_at'), 'isoformat') else str(session.get('created_at','')),
    }, company_id=cid)

    # Push сотрудникам той же компании, которые не подключены
    staff_ids = await db.get_staff_for_chat_push(cid)
    online    = _chat.staff_online_ids(cid)
    for sid in staff_ids:
        if sid not in online:
            asyncio.create_task(send_web_push(
                sid,
                title="💬 Новый чат",
                body=f"Клиент {client_name or client_phone or 'с сайта'} ждёт ответа",
                push_type="new_chat",
            ))

    return {"ok": True, "code": code}


@app.get("/api/chat/sessions")
async def chat_sessions(staff=Depends(get_current_staff)):
    sessions = await db.get_active_chat_sessions()
    result = []
    for s in sessions:
        msgs = await db.get_chat_messages(s['id'])
        s2 = {k: (v.isoformat() if hasattr(v, 'isoformat') else v) for k, v in s.items()}
        s2['message_count'] = len(msgs)
        s2['last_text'] = msgs[-1]['text'] if msgs else ''
        result.append(s2)
    return result


@app.get("/api/chat/{code}/messages")
async def chat_get_messages(code: str, staff=Depends(get_current_staff)):
    session = await db.get_chat_session(code)
    if not session or session.get('company_id') != (staff.get('company_id') or 1):
        raise HTTPException(404, "Сессия не найдена")
    msgs = await db.get_chat_messages(session['id'])
    s = {k: (v.isoformat() if hasattr(v, 'isoformat') else v) for k, v in session.items()}
    return {"session": s, "messages": [_msg_json(m) for m in msgs]}


@app.post("/api/chat/{code}/claim")
async def chat_claim(code: str, staff=Depends(get_current_staff)):
    existing = await db.get_chat_session(code)
    if not existing or existing.get('company_id') != (staff.get('company_id') or 1):
        raise HTTPException(404, "Сессия не найдена")
    name = f"{staff.get('first_name','')} {staff.get('last_name','')}".strip() or "Менеджер"
    session = await db.claim_chat_session(code, staff['id'], name)
    if not session:
        raise HTTPException(400, "Чат уже занят другим сотрудником")

    await _chat.broadcast_staff({"type": "chat_claimed", "code": code,
                                  "claimed_by": staff['id'], "claimed_name": name},
                                 company_id=session.get('company_id'))
    await _chat.send_client(code, {"type": "staff_joined", "staff_name": name})
    s = {k: (v.isoformat() if hasattr(v, 'isoformat') else v) for k, v in session.items()}
    return {"ok": True, "session": s}


@app.post("/api/chat/{code}/close")
async def chat_close(code: str, staff=Depends(get_current_staff)):
    existing = await db.get_chat_session(code)
    if not existing or existing.get('company_id') != (staff.get('company_id') or 1):
        raise HTTPException(404, "Сессия не найдена")
    session = await db.close_chat_session(code)
    if not session:
        raise HTTPException(404, "Сессия не найдена")
    await _chat.broadcast_staff({"type": "chat_closed", "code": code}, company_id=session.get('company_id'))
    await _chat.send_client(code, {"type": "chat_closed",
                                    "text": "Чат завершён. Спасибо, что обратились в ARTEZ!"})
    return {"ok": True}


@app.post("/api/chat/templates/seed")
async def seed_templates(staff=Depends(get_current_staff)):
    if staff.get('role') not in ('admin',):
        raise HTTPException(403, "Только для admin")
    # принудительно засеять (даже если таблица не пустая)
    await db.seed_chat_templates_forced()
    return {"ok": True}

@app.get("/api/chat/active-by-phone")
async def chat_active_by_phone(phone: str, company_slug: str = None):
    """Публичный эндпоинт — проверить есть ли активный чат для этого номера."""
    if not phone:
        return {"session": None}
    cid = await _resolve_client_company_id(company_slug)
    db.set_request_company(cid)
    session = await db.get_active_chat_by_phone(phone)
    if not session:
        return {"session": None}
    for k, v in session.items():
        if hasattr(v, 'isoformat'): session[k] = v.isoformat()
    return {"session": {"code": session["code"], "status": session["status"]}}

@app.get("/api/chat/history")
async def chat_history(limit: int = 50, offset: int = 0, filter: str = "own",
                       staff=Depends(get_current_staff)):
    role = staff.get("role", "")
    can_see_all = role in ("admin", "manager")
    own_only = not can_see_all or filter == "own"
    rows = await db.get_closed_chat_sessions(limit, offset,
                                              staff_id=staff["id"], own_only=own_only)
    for r in rows:
        for k, v in r.items():
            if hasattr(v, 'isoformat'): r[k] = v.isoformat()
    return {"rows": rows, "can_see_all": can_see_all}

@app.get("/api/chat/templates")
async def get_templates(staff=Depends(get_current_staff)):
    rows = await db.get_all_chat_templates()
    return rows

@app.get("/api/chat/templates/quick")
async def get_quick_templates(lang: str = "uz", staff=Depends(get_current_staff)):
    rows = await db.get_chat_templates(lang=lang, key='quick')
    return rows

@app.post("/api/chat/templates")
async def create_template(body: dict = Body(...), staff=Depends(get_current_staff)):
    if staff.get('role') not in ('admin', 'manager'):
        raise HTTPException(403, "Недостаточно прав")
    row = await db.upsert_chat_template(body)
    return row or {}

@app.put("/api/chat/templates/{tid}")
async def update_template(tid: int, body: dict = Body(...), staff=Depends(get_current_staff)):
    if staff.get('role') not in ('admin', 'manager'):
        raise HTTPException(403, "Недостаточно прав")
    body['id'] = tid
    row = await db.upsert_chat_template(body)
    return row or {}

@app.delete("/api/chat/templates/{tid}")
async def del_template(tid: int, staff=Depends(get_current_staff)):
    if staff.get('role') not in ('admin', 'manager'):
        raise HTTPException(403, "Недостаточно прав")
    await db.delete_chat_template(tid)
    return {"ok": True}


@app.post("/api/admin/bot/broadcast-restart")
async def bot_broadcast_restart(cid: int = Depends(_get_admin_cid)):
    """Рассылает всем клиентам бота заказов компании сообщение «Нажмите /start»."""
    token = await db.get_config_for_company("order_bot_token", cid)
    if not token:
        raise HTTPException(status_code=400, detail="Бот для клиентов не подключён")
    tg_ids = await db.get_all_bot_client_tg_ids()
    if not tg_ids:
        return {"ok": True, "sent": 0, "failed": 0, "total": 0}
    text = (
        "🔄 <b>Бот обновлён!</b>\n\n"
        "Для продолжения нажмите /start"
    )
    sent = failed = 0
    async with aiohttp.ClientSession() as s:
        for tg_id in tg_ids:
            try:
                r = await s.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": tg_id, "text": text, "parse_mode": "HTML"},
                    timeout=aiohttp.ClientTimeout(total=5))
                if (await r.json()).get("ok"):
                    sent += 1
                else:
                    failed += 1
            except Exception:
                failed += 1
            await asyncio.sleep(0.05)
    logging.info(f"Bot broadcast-restart: sent={sent}, failed={failed}, total={len(tg_ids)}")
    return {"ok": True, "sent": sent, "failed": failed, "total": len(tg_ids)}


@app.websocket("/ws/chat/client/{code}")
async def ws_chat_client(websocket: WebSocket, code: str):
    session = await db.get_chat_session(code)
    if not session or session['status'] == 'closed':
        await websocket.accept()
        await websocket.send_json({"type": "chat_closed"})
        await websocket.close()
        return

    await _chat.connect_client(code, websocket)  # accept() внутри
    msgs = await db.get_chat_messages(session['id'])
    await websocket.send_json({"type": "history", "messages": [_msg_json(m) for m in msgs]})

    try:
        while True:
            data = await websocket.receive_json()
            if data.get("type") != "message":
                continue
            text = (data.get("text") or "").strip()
            if not text:
                continue
            # Перечитать сессию (могли claim)
            session = await db.get_chat_session(code)
            if not session or session['status'] == 'closed':
                break
            cname = session.get('client_name') or session.get('client_phone') or "Клиент"
            is_first = await db.is_first_client_message(session['id'])
            msg = await db.add_chat_message(session['id'], 'client', cname, text)
            if not msg:
                continue
            asyncio.create_task(db.touch_chat_client_activity(code))
            payload = {"type": "message", "code": code, "msg": _msg_json(msg)}
            await websocket.send_json(payload)
            claimed = session.get('claimed_by')
            if claimed:
                await _chat.send_staff(claimed, payload)
            else:
                await _chat.broadcast_staff(payload, company_id=session.get('company_id'))
            # Авто-ответ на первое сообщение клиента (через 3 сек)
            if is_first:
                async def _send_auto_reply(c=code, sess=dict(session), cl=claimed):
                    await asyncio.sleep(3)
                    lang = sess.get('lang') or 'uz'
                    auto_text = _tpl(await db.get_chat_template_text('auto_reply', lang), sess)
                    if not auto_text:
                        return
                    auto_msg = await db.add_chat_message(sess['id'], 'bot', 'ARTEZ', auto_text)
                    if not auto_msg:
                        return
                    auto_payload = {"type": "message", "code": c, "msg": _msg_json(auto_msg)}
                    await _chat.send_client(c, auto_payload)
                    if cl:
                        await _chat.send_staff(cl, auto_payload)
                    else:
                        await _chat.broadcast_staff(auto_payload, company_id=sess.get('company_id'))
                asyncio.create_task(_send_auto_reply())
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        _chat.disconnect_client(code, websocket)


@app.websocket("/ws/chat/staff/{token}")
async def ws_chat_staff(websocket: WebSocket, token: str):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        staff_id = int(payload.get("sub"))
        staff_company_id = int(payload.get("company_id") or 1)
    except Exception:
        await websocket.close(code=4001)
        return

    await _chat.connect_staff(staff_id, websocket, staff_company_id)
    staff_row = await db.get_staff_by_id(staff_id)
    sname = f"{(staff_row or {}).get('first_name','')} {(staff_row or {}).get('last_name','')}".strip() or "Менеджер"

    try:
        while True:
            data = await websocket.receive_json()
            if data.get("type") != "message":
                continue
            code = (data.get("code") or "").strip()
            text = (data.get("text") or "").strip()
            if not code or not text:
                continue
            session = await db.get_chat_session(code)
            if not session or session['status'] == 'closed' or session.get('company_id') != staff_company_id:
                continue
            msg = await db.add_chat_message(session['id'], 'staff', sname, text)
            if not msg:
                continue
            payload = {"type": "message", "code": code, "msg": _msg_json(msg)}
            await _chat.send_client(code, {"type": "message", "msg": _msg_json(msg)})
            await _chat.broadcast_staff(payload, exclude=staff_id, company_id=staff_company_id)
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        _chat.disconnect_staff(staff_id)


# ══════════════════════════════════════════════════════════════════════════
# AUTODIAL — управление кампаниями (AMI вызовы делает autodial_agent.py локально)
# ══════════════════════════════════════════════════════════════════════════
import uuid as _uuid

class AMIClient:
    AMI_HOST = "192.168.1.6"
    AMI_PORT = 7777
    AMI_USER = "admin_ami"
    AMI_PASS  = "Kamila1984!"

    def __init__(self):
        self._reader = None
        self._writer = None
        self._connected = False
        self._event_futures: dict = {}   # action_id -> asyncio.Future
        self._event_cbs: list   = []
        self._loop_task = None

    async def connect(self):
        if self._connected:
            return
        self._reader, self._writer = await asyncio.open_connection(
            self.AMI_HOST, self.AMI_PORT, limit=2**20
        )
        await self._reader.readline()   # greeting line
        await self._raw_send({"Action": "Login", "Username": self.AMI_USER, "Secret": self.AMI_PASS})
        resp = await self._read_one()
        if resp.get("Response") != "Success":
            raise Exception(f"AMI login failed: {resp}")
        self._connected = True
        self._loop_task = asyncio.create_task(self._event_loop())
        logging.info("AMI connected")

    async def disconnect(self):
        try:
            if self._writer:
                await self._raw_send({"Action": "Logoff"})
                self._writer.close()
        except Exception:
            pass
        self._connected = False
        if self._loop_task:
            self._loop_task.cancel()

    async def _raw_send(self, fields: dict):
        msg = "\r\n".join(f"{k}: {v}" for k, v in fields.items()) + "\r\n\r\n"
        self._writer.write(msg.encode())
        await self._writer.drain()

    async def _read_one(self) -> dict:
        result = {}
        while True:
            line = (await self._reader.readline()).decode(errors="replace").strip()
            if not line:
                return result
            if ": " in line:
                k, v = line.split(": ", 1)
                result[k] = v
        return result

    async def _event_loop(self):
        buf = {}
        try:
            while True:
                line = (await self._reader.readline()).decode(errors="replace").strip()
                if line:
                    if ": " in line:
                        k, v = line.split(": ", 1); buf[k] = v
                else:
                    if buf:
                        await self._dispatch(dict(buf)); buf.clear()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logging.error(f"AMI event loop: {e}")
        finally:
            self._connected = False

    async def _dispatch(self, msg: dict):
        aid = msg.get("ActionID")
        if aid and aid in self._event_futures:
            fut = self._event_futures[aid]
            if not fut.done():
                fut.set_result(msg)
        for cb in list(self._event_cbs):
            try:
                await cb(msg)
            except Exception as e:
                logging.error(f"AMI cb error: {e}")

    async def originate(self, phone: str, exten: str, action_id: str):
        if not self._connected:
            await self.connect()
        loop = asyncio.get_event_loop()
        self._event_futures[action_id] = loop.create_future()
        await self._raw_send({
            "Action":   "Originate",
            "Channel":  f"Local/{phone}@outbound-allroutes",
            "Context":  "from-internal",
            "Exten":    exten,
            "Priority": "1",
            "CallerID": f"{exten} <{exten}>",
            "Timeout":  "30000",
            "Async":    "true",
            "ActionID": action_id,
        })

    async def wait_response(self, action_id: str, timeout: float = 40.0) -> dict:
        fut = self._event_futures.get(action_id)
        if not fut:
            return {}
        try:
            return await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
        except asyncio.TimeoutError:
            return {"Response": "Failure", "Reason": "timeout"}
        finally:
            self._event_futures.pop(action_id, None)


_ami = AMIClient()



# ── Endpoints ─────────────────────────────────────────────────────────────

class _AutodialCreate(BaseModel):
    name:            str
    ivr_exten:       str = "7000"
    max_parallel:    int = 4
    group_ids:       list = []
    sched_time_from: str = "09:00"
    sched_time_to:   str = "21:00"
    sched_days:      list = [0, 1, 2, 3, 4, 5, 6]
    sched_date_from: Optional[str] = None
    sched_date_to:   Optional[str] = None

@app.get("/api/admin/autodial/campaigns")
async def autodial_list(ccid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM autodial_campaigns WHERE company_id=$1 ORDER BY created_at DESC", ccid)
    return [dict(r) for r in rows]

def _parse_time(s):
    """'HH:MM' → datetime.time для asyncpg."""
    from datetime import time as _time
    if not s: return None
    try:
        h, m = s.split(':'); return _time(int(h), int(m))
    except Exception: return None

def _parse_date(s):
    """'YYYY-MM-DD' → datetime.date для asyncpg."""
    from datetime import date as _date
    if not s: return None
    try:
        y, mo, d = s.split('-'); return _date(int(y), int(mo), int(d))
    except Exception: return None

@app.post("/api/admin/autodial/campaigns")
async def autodial_create(body: _AutodialCreate, ccid: int = Depends(_get_admin_cid)):
    import json as _json
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO autodial_campaigns "
            "(name,ivr_exten,max_parallel,group_ids,sched_time_from,sched_time_to,sched_days,sched_date_from,sched_date_to,company_id) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *",
            body.name, body.ivr_exten, body.max_parallel,
            _json.dumps(body.group_ids),
            _parse_time(body.sched_time_from) or _parse_time("09:00"),
            _parse_time(body.sched_time_to)   or _parse_time("21:00"),
            body.sched_days or [0,1,2,3,4,5,6],
            _parse_date(body.sched_date_from),
            _parse_date(body.sched_date_to),
            ccid,
        )
    return dict(row)

@app.put("/api/admin/autodial/campaigns/{cid}")
async def autodial_update(cid: int, body: dict = Body(...), ccid: int = Depends(_get_admin_cid)):
    import json as _json
    allowed = {'name','ivr_exten','max_parallel','sched_time_from','sched_time_to',
               'sched_days','sched_date_from','sched_date_to','group_ids'}
    raw = {k: v for k, v in body.items() if k in allowed}
    if not raw: raise HTTPException(400, "no updatable fields")
    fields = {}
    for k, v in raw.items():
        if k in ('sched_time_from', 'sched_time_to'):
            fields[k] = _parse_time(v)
        elif k in ('sched_date_from', 'sched_date_to'):
            fields[k] = _parse_date(v)
        elif k == 'group_ids':
            fields[k] = _json.dumps(v or [])
        else:
            fields[k] = v or None
    sets = ', '.join(f"{k}=${i+2}" for i, k in enumerate(fields))
    cid_param = len(fields) + 2
    async with db.pool.acquire() as conn:
        await conn.execute(
            f"UPDATE autodial_campaigns SET {sets} WHERE id=$1 AND company_id=${cid_param}",
            cid, *fields.values(), ccid)
    return {"ok": True}

@app.get("/api/admin/autodial/campaigns/{cid}")
async def autodial_get(cid: int, ccid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM autodial_campaigns WHERE id=$1 AND company_id=$2", cid, ccid)
    if not row: raise HTTPException(404)
    return dict(row)

@app.post("/api/admin/autodial/campaigns/{cid}/start")
async def autodial_start(cid: int, ccid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        campaign = await conn.fetchrow("SELECT * FROM autodial_campaigns WHERE id=$1 AND company_id=$2", cid, ccid)
    if not campaign: raise HTTPException(404)
    if campaign["status"] == "running": raise HTTPException(400, "Already running")

    # Manual/test-кампании: сбрасываем все звонки в pending и перезапускаем
    if campaign["source_type"] == "manual":
        async with db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE autodial_calls SET status='pending', ami_action_id=NULL, started_at=NULL, hangup_at=NULL, hangup_cause=NULL WHERE campaign_id=$1", cid
            )
            cnt = await conn.fetchval("SELECT COUNT(*) FROM autodial_calls WHERE campaign_id=$1 AND status='pending'", cid)
            await conn.execute(
                "UPDATE autodial_campaigns SET status='running', dialed_count=0, answered_count=0, failed_count=0, total_count=$2, started_at=NOW(), finished_at=NULL WHERE id=$1",
                cid, cnt
            )
        return {"ok": True}

    # Обычные кампании: очищаем старые звонки если кампания уже завершалась
    if campaign["status"] in ("done", "stopped", "error"):
        async with db.pool.acquire() as conn:
            await conn.execute("DELETE FROM autodial_calls WHERE campaign_id=$1", cid)
            await conn.execute(
                "UPDATE autodial_campaigns SET dialed_count=0,answered_count=0,failed_count=0,total_count=0,started_at=NOW(),finished_at=NULL WHERE id=$1", cid
            )

    async with db.pool.acquire() as conn:
        cnt = await conn.fetchval("SELECT COUNT(*) FROM autodial_calls WHERE campaign_id=$1", cid)

    if cnt == 0:
        import json as _json
        phones_seen = set()
        rows_to_insert = []

        # Чёрный список своей компании — номера отсюда не должны попадать в автодозвон.
        async with db.pool.acquire() as conn:
            blacklisted_phones = {r["phone"] for r in await conn.fetch(
                "SELECT phone FROM blacklist WHERE company_id=$1", ccid)}

        group_ids = []
        try:
            gids = campaign["group_ids"]
            group_ids = _json.loads(gids) if isinstance(gids, str) else (gids or [])
        except Exception:
            group_ids = []

        if group_ids:
            async with db.pool.acquire() as conn:
                members = await conn.fetch(
                    "SELECT phone, name, source_type, source_id FROM autodial_group_members "
                    "WHERE group_id = ANY($1::int[]) ORDER BY id",
                    group_ids
                )
            for m in members:
                if normalize_phone(m["phone"] or "") in blacklisted_phones:
                    continue
                p = _ami_phone((m["phone"] or "").strip())
                if p and p not in phones_seen:
                    phones_seen.add(p)
                    rows_to_insert.append((m["source_type"], m["source_id"], p, m["name"] or ""))
        else:
            # Fallback: старая логика если группы не выбраны
            async with db.pool.acquire() as conn:
                crows = await conn.fetch(
                    "SELECT id, phone, first_name, last_name FROM crm_clients "
                    "WHERE phone IS NOT NULL AND phone != '' AND company_id=$1 ORDER BY id",
                    ccid
                )
            for r in crows:
                if normalize_phone(r["phone"] or "") in blacklisted_phones:
                    continue
                p = _ami_phone((r["phone"] or "").strip())
                if p and p not in phones_seen:
                    phones_seen.add(p)
                    rows_to_insert.append(("clients", r["id"], p, f"{r['first_name'] or ''} {r['last_name'] or ''}".strip()))

        if rows_to_insert:
            async with db.pool.acquire() as conn:
                await conn.executemany(
                    "INSERT INTO autodial_calls (campaign_id,source_type,source_id,phone,name,company_id) VALUES ($1,$2,$3,$4,$5,$6)",
                    [(cid, r[0], r[1], r[2], r[3], ccid) for r in rows_to_insert]
                )
                await conn.execute("UPDATE autodial_campaigns SET total_count=$1 WHERE id=$2", len(rows_to_insert), cid)

    async with db.pool.acquire() as conn:
        await conn.execute(
            "UPDATE autodial_campaigns SET status='running', started_at=COALESCE(started_at,NOW()) WHERE id=$1", cid
        )
    return {"ok": True}

@app.post("/api/admin/autodial/campaigns/{cid}/retry")
async def autodial_retry(cid: int, ccid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        campaign = await conn.fetchrow("SELECT * FROM autodial_campaigns WHERE id=$1 AND company_id=$2", cid, ccid)
    if not campaign: raise HTTPException(404)
    if campaign["status"] == "running": raise HTTPException(400, "Already running")
    async with db.pool.acquire() as conn:
        # Для manual (тест): сбрасываем все звонки; для обычных — только no_answer
        if campaign["source_type"] == "manual":
            await conn.execute(
                "UPDATE autodial_calls SET status='pending', ami_action_id=NULL, started_at=NULL, hangup_at=NULL, hangup_cause=NULL WHERE campaign_id=$1", cid
            )
        else:
            await conn.execute(
                "UPDATE autodial_calls SET status='pending', ami_action_id=NULL, started_at=NULL, hangup_at=NULL, hangup_cause=NULL "
                "WHERE campaign_id=$1 AND status='no_answer'", cid
            )
        pending = await conn.fetchval(
            "SELECT COUNT(*) FROM autodial_calls WHERE campaign_id=$1 AND status='pending'", cid
        )
        await conn.execute(
            "UPDATE autodial_campaigns SET status='running', dialed_count=0, answered_count=0, failed_count=0, "
            "total_count=$2, started_at=NOW(), finished_at=NULL WHERE id=$1", cid, pending
        )
    return {"ok": True}

@app.post("/api/admin/autodial/campaigns/{cid}/pause")
async def autodial_pause(cid: int, ccid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        await conn.execute("UPDATE autodial_campaigns SET status='paused' WHERE id=$1 AND company_id=$2", cid, ccid)
    return {"ok": True}

@app.post("/api/admin/autodial/campaigns/{cid}/stop")
async def autodial_stop(cid: int, ccid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        await conn.execute(
            "UPDATE autodial_campaigns SET status='stopped', finished_at=NOW() WHERE id=$1 AND company_id=$2", cid, ccid)
    return {"ok": True}

@app.get("/api/admin/autodial/campaigns/{cid}/calls")
async def autodial_calls(cid: int, ccid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        if not await conn.fetchrow("SELECT id FROM autodial_campaigns WHERE id=$1 AND company_id=$2", cid, ccid):
            raise HTTPException(404)
        rows = await conn.fetch("SELECT * FROM autodial_calls WHERE campaign_id=$1 ORDER BY id", cid)
    return [dict(r) for r in rows]

@app.post("/api/admin/autodial/campaigns/{cid}/create-group")
async def autodial_create_group_from_calls(cid: int, body: dict = Body(...), ccid: int = Depends(_get_admin_cid)):
    """Создать группу автодозвона из звонков с нужным статусом (answered / no_answer / all)"""
    import re as _re
    status_filter = body.get("status", "answered")
    group_name = (body.get("name") or "").strip()
    if not group_name:
        raise HTTPException(400, "name required")
    async with db.pool.acquire() as conn:
        if not await conn.fetchrow("SELECT id FROM autodial_campaigns WHERE id=$1 AND company_id=$2", cid, ccid):
            raise HTTPException(404)
        if status_filter == "all":
            calls = await conn.fetch(
                "SELECT DISTINCT phone, name FROM autodial_calls WHERE campaign_id=$1 AND status NOT IN ('pending','calling')", cid)
        else:
            calls = await conn.fetch(
                "SELECT DISTINCT phone, name FROM autodial_calls WHERE campaign_id=$1 AND status=$2", cid, status_filter)
        if not calls:
            raise HTTPException(400, "Нет звонков с таким статусом")
        group = await conn.fetchrow(
            "INSERT INTO autodial_groups (name,company_id) VALUES ($1,$2) RETURNING *", group_name, ccid)
        gid = group["id"]
        inserted = 0
        for c in calls:
            phone = _ami_phone(str(c["phone"]).strip())
            if not phone:
                continue
            await conn.execute(
                "INSERT INTO autodial_group_members (group_id,phone,name,source_type,company_id) "
                "VALUES ($1,$2,$3,'campaign',$4) ON CONFLICT (group_id,phone) DO NOTHING",
                gid, phone, c["name"] or "", ccid)
            inserted += 1
    return {"ok": True, "group_id": gid, "group_name": group_name, "inserted": inserted}


@app.post("/api/admin/autodial/groups/from-active-contacts")
async def autodial_create_group_from_active_contacts(body: dict = Body(...), ccid: int = Depends(_get_admin_cid)):
    """Создать группу автодозвона из базы «✅ Актуальные контакты» своей компании (без ЧС)."""
    group_name = (body.get("name") or "").strip()
    if not group_name:
        raise HTTPException(400, "name required")
    async with db.pool.acquire() as conn:
        contacts = await conn.fetch(
            "SELECT phone, first_name, last_name FROM active_contacts "
            "WHERE company_id=$1 AND NOT COALESCE(blacklisted, FALSE)", ccid)
        if not contacts:
            raise HTTPException(400, "Нет актуальных контактов")
        group = await conn.fetchrow(
            "INSERT INTO autodial_groups (name,company_id) VALUES ($1,$2) RETURNING *", group_name, ccid)
        gid = group["id"]
        inserted = 0
        for c in contacts:
            phone = _ami_phone(str(c["phone"]).strip())
            if not phone:
                continue
            name = f"{c['first_name'] or ''} {c['last_name'] or ''}".strip()
            await conn.execute(
                "INSERT INTO autodial_group_members (group_id,phone,name,source_type,company_id) "
                "VALUES ($1,$2,$3,'active_contacts',$4) ON CONFLICT (group_id,phone) DO NOTHING",
                gid, phone, name, ccid)
            inserted += 1
    return {"ok": True, "group_id": gid, "group_name": group_name, "inserted": inserted}


@app.post("/api/admin/autodial/campaigns/{cid}/fork")
async def autodial_fork(cid: int, ccid: int = Depends(_get_admin_cid)):
    """Создать повтор-кампанию /2 из недозвонившихся"""
    import re as _re
    async with db.pool.acquire() as conn:
        campaign = await conn.fetchrow("SELECT * FROM autodial_campaigns WHERE id=$1 AND company_id=$2", cid, ccid)
        if not campaign:
            raise HTTPException(404)
        calls = await conn.fetch(
            "SELECT DISTINCT phone, name FROM autodial_calls WHERE campaign_id=$1 AND status='no_answer'", cid)
        if not calls:
            raise HTTPException(400, "Нет недозвонившихся")
        base_name = _re.sub(r'\s*/\s*\d+$', '', campaign["name"]).strip()
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM autodial_campaigns WHERE name ~ $1 AND company_id=$2",
            r'^' + _re.escape(base_name) + r'(\s*/\s*\d+)?$', ccid)
        new_name = f"{base_name} / {count + 1}"
        new_camp = await conn.fetchrow(
            "INSERT INTO autodial_campaigns (name,source_type,ivr_exten,max_parallel,caller_id,"
            "sched_time_from,sched_time_to,sched_days,company_id) VALUES ($1,'fork',$2,$3,$4,$5,$6,$7,$8) RETURNING *",
            new_name, campaign["ivr_exten"], campaign["max_parallel"] or 1,
            campaign["caller_id"] or "1000", campaign["sched_time_from"],
            campaign["sched_time_to"], campaign["sched_days"], ccid)
        new_cid = new_camp["id"]
        for c in calls:
            phone = _ami_phone(str(c["phone"]).strip())
            if phone:
                await conn.execute(
                    "INSERT INTO autodial_calls (campaign_id,source_type,phone,name,company_id) VALUES ($1,'fork',$2,$3,$4)",
                    new_cid, phone, c["name"] or "", ccid)
        total = await conn.fetchval("SELECT COUNT(*) FROM autodial_calls WHERE campaign_id=$1", new_cid)
        await conn.execute("UPDATE autodial_campaigns SET total_count=$2 WHERE id=$1", new_cid, total)
    return {"ok": True, "campaign_id": new_cid, "name": new_name, "total": int(total)}


@app.post("/api/admin/autodial/campaigns/{cid}/clone")
async def autodial_clone(cid: int, ccid: int = Depends(_get_admin_cid)):
    """Клонировать кампанию со всеми контактами"""
    import re as _re
    async with db.pool.acquire() as conn:
        campaign = await conn.fetchrow("SELECT * FROM autodial_campaigns WHERE id=$1 AND company_id=$2", cid, ccid)
        if not campaign:
            raise HTTPException(404)
        calls = await conn.fetch("SELECT phone, name FROM autodial_calls WHERE campaign_id=$1", cid)
        base_name = _re.sub(r'\s*\(копия\s*\d*\)\s*$', '', campaign["name"]).strip()
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM autodial_campaigns WHERE name ~ $1 AND company_id=$2",
            r'^' + _re.escape(base_name) + r'(\s*\(копия(\s*\d+)?\))?$', ccid)
        suffix = "" if count == 1 else f" {count}"
        new_name = f"{base_name} (копия{suffix})"
        new_camp = await conn.fetchrow(
            "INSERT INTO autodial_campaigns (name,source_type,ivr_exten,max_parallel,caller_id,"
            "sched_time_from,sched_time_to,sched_days,company_id) VALUES ($1,'manual',$2,$3,$4,$5,$6,$7,$8) RETURNING *",
            new_name, campaign["ivr_exten"], campaign["max_parallel"] or 1,
            campaign["caller_id"] or "1000", campaign["sched_time_from"],
            campaign["sched_time_to"], campaign["sched_days"], ccid)
        new_cid = new_camp["id"]
        for c in calls:
            phone = _ami_phone(str(c["phone"]).strip())
            if phone:
                await conn.execute(
                    "INSERT INTO autodial_calls (campaign_id,source_type,phone,name,company_id) VALUES ($1,'clone',$2,$3,$4)",
                    new_cid, phone, c["name"] or "", ccid)
        total = await conn.fetchval("SELECT COUNT(*) FROM autodial_calls WHERE campaign_id=$1", new_cid)
        await conn.execute("UPDATE autodial_campaigns SET total_count=$2 WHERE id=$1", new_cid, total)
    return {"ok": True, "campaign_id": new_cid, "name": new_name}


@app.delete("/api/admin/autodial/campaigns/{cid}")
async def autodial_delete(cid: int, ccid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        await conn.execute("DELETE FROM autodial_campaigns WHERE id=$1 AND company_id=$2", cid, ccid)
    return {"ok": True}

@app.post("/api/admin/autodial/campaigns/{cid}/import-active")
async def autodial_import_active(cid: int, ccid: int = Depends(_get_admin_cid)):
    """Импортирует дозвонившихся (status='answered') из кампании своей компании
    в единую базу актуальных контактов (active_contacts)."""
    imported = await db.import_answered_autodial_calls(cid)
    return {"ok": True, "imported": imported}

@app.post("/api/admin/autodial/test")
async def autodial_test(body: dict = Body(...), ccid: int = Depends(_get_admin_cid)):
    """
    Создаёт одноразовую кампанию с одним номером — агент (autodial_agent.py)
    подхватит её и позвонит. Агент должен работать ЛОКАЛЬНО (в сети АТС 192.168.1.x).
    """
    exten = (body.get("exten") or "1000").strip()
    raw_phones = body.get("phones") or ([body.get("phone")] if body.get("phone") else [])
    phones = [p.strip() for p in raw_phones if p and str(p).strip()][:5]
    if not phones: raise HTTPException(400, "phone required")
    async with db.pool.acquire() as conn:
        label = phones[0] if len(phones) == 1 else f"{phones[0]} +{len(phones)-1}"
        camp = await conn.fetchrow(
            "INSERT INTO autodial_campaigns (name,ivr_exten,max_parallel,source_type,status,company_id) "
            "VALUES ($1,$2,$3,'manual','running',$4) RETURNING *",
            f"Тест {label}", exten, len(phones), ccid
        )
        for p in phones:
            await conn.execute(
                "INSERT INTO autodial_calls (campaign_id,source_type,phone,name,company_id) VALUES ($1,'manual',$2,'Тест',$3)",
                camp["id"], p, ccid
            )
        await conn.execute(
            "UPDATE autodial_campaigns SET total_count=$1 WHERE id=$2", len(phones), camp["id"]
        )
    return {"ok": True, "campaign_id": camp["id"],
            "note": "Агент autodial_agent.py должен быть запущен в локальной сети АТС"}


# ── Группы контактов автодозвона ───────────────────────────────────────────

@app.get("/api/admin/autodial/groups")
async def adg_list(cid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT g.*, COUNT(m.id) AS member_count FROM autodial_groups g "
            "LEFT JOIN autodial_group_members m ON m.group_id = g.id "
            "WHERE g.company_id=$1 "
            "GROUP BY g.id ORDER BY g.name",
            cid
        )
    return [dict(r) for r in rows]

@app.post("/api/admin/autodial/groups")
async def adg_create(body: dict = Body(...), cid: int = Depends(_get_admin_cid)):
    name  = (body.get("name") or "").strip()
    notes = (body.get("notes") or "").strip()
    if not name: raise HTTPException(400, "name required")
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO autodial_groups (name,notes,company_id) VALUES ($1,$2,$3) RETURNING *", name, notes, cid
        )
    return dict(row)

@app.put("/api/admin/autodial/groups/{gid}")
async def adg_update(gid: int, body: dict = Body(...), cid: int = Depends(_get_admin_cid)):
    name  = (body.get("name") or "").strip()
    notes = (body.get("notes") or "").strip()
    if not name: raise HTTPException(400, "name required")
    async with db.pool.acquire() as conn:
        await conn.execute("UPDATE autodial_groups SET name=$1,notes=$2 WHERE id=$3 AND company_id=$4", name, notes, gid, cid)
    return {"ok": True}

@app.delete("/api/admin/autodial/groups/{gid}")
async def adg_delete(gid: int, cid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        await conn.execute("DELETE FROM autodial_groups WHERE id=$1 AND company_id=$2", gid, cid)
    return {"ok": True}

@app.get("/api/admin/autodial/groups/{gid}/members")
async def adg_members(gid: int, cid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM autodial_group_members WHERE group_id=$1 AND company_id=$2 ORDER BY name, phone", gid, cid
        )
    return [dict(r) for r in rows]

def _ami_phone(phone: str) -> str:
    """Возвращает номер для AMI: только цифры, без 998 (9 цифр)."""
    import re
    p = re.sub(r'\D', '', phone)
    if p.startswith('998') and len(p) >= 11:
        p = p[3:]
    return p

@app.post("/api/admin/autodial/groups/{gid}/members")
async def adg_add_members(gid: int, body: dict = Body(...), cid: int = Depends(_get_admin_cid)):
    """Добавляет участников в группу. phones: [{phone,name,source_type,source_id}]"""
    members = body.get("members") or []
    if not members: raise HTTPException(400, "members required")
    inserted = 0
    async with db.pool.acquire() as conn:
        if not await conn.fetchrow("SELECT id FROM autodial_groups WHERE id=$1 AND company_id=$2", gid, cid):
            raise HTTPException(404, "Группа не найдена")
        for m in members:
            phone = _ami_phone((m.get("phone") or "").strip())
            if not phone: continue
            try:
                await conn.execute(
                    "INSERT INTO autodial_group_members (group_id,phone,name,source_type,source_id,company_id) "
                    "VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (group_id,phone) DO NOTHING",
                    gid, phone, (m.get("name") or "").strip(),
                    m.get("source_type") or "manual", m.get("source_id"), cid
                )
                inserted += 1
            except Exception:
                pass
    return {"ok": True, "inserted": inserted}

@app.post("/api/admin/autodial/groups/{gid}/members/import-all")
async def adg_import_all(gid: int, body: dict = Body(...), cid: int = Depends(_get_admin_cid)):
    """Серверный массовый импорт по фильтру — для любого объёма без загрузки в браузер."""
    src    = body.get("source", "both")
    q      = (body.get("q") or "").strip()
    prefix = (body.get("prefix") or "").strip()
    letter = (body.get("letter") or "").strip()
    async with db.pool.acquire() as conn:
        if not await conn.fetchrow("SELECT id FROM autodial_groups WHERE id=$1 AND company_id=$2", gid, cid):
            raise HTTPException(404, "Группа не найдена")
        where_parts = ["phone IS NOT NULL AND phone != ''"]
        params: list = []
        i = 1
        if q:
            where_parts.append(f"(phone ILIKE ${i} OR first_name ILIKE ${i} OR last_name ILIKE ${i})")
            params.append(f"%{q}%"); i += 1
        if prefix:
            where_parts.append(f"phone LIKE ${i}"); params.append(f"{prefix}%"); i += 1
        if letter:
            where_parts.append(f"(first_name ILIKE ${i} OR last_name ILIKE ${i})")
            params.append(f"{letter}%"); i += 1
        where_parts.append(f"company_id = ${i}"); params.append(cid); i += 1
        w = ' AND '.join(where_parts)
        # нормализация телефона: убрать не-цифры, срезать +998 если 11+ цифр
        phone_expr = (
            "CASE WHEN regexp_replace(phone,'[^0-9]','','g') ~ '^998' "
            "AND length(regexp_replace(phone,'[^0-9]','','g')) >= 11 "
            "THEN substring(regexp_replace(phone,'[^0-9]','','g'),4) "
            "ELSE regexp_replace(phone,'[^0-9]','','g') END"
        )
        parts = []
        if src in ("clients", "both"):
            parts.append(
                f"SELECT {phone_expr} AS np, "
                f"trim(coalesce(first_name,'')||' '||coalesce(last_name,'')) AS nm, "
                f"'clients'::text AS st, id AS sid FROM crm_clients WHERE {w}"
            )
        if src in ("contacts", "both"):
            parts.append(
                f"SELECT {phone_expr} AS np, "
                f"trim(coalesce(first_name,'')||' '||coalesce(last_name,'')) AS nm, "
                f"'contacts'::text AS st, id AS sid FROM contacts WHERE {w}"
            )
        union_sql = ' UNION ALL '.join(parts)
        gid_idx = i
        params.append(gid)
        cid_idx = i + 1
        params.append(cid)
        result = await conn.execute(
            f"INSERT INTO autodial_group_members (group_id,phone,name,source_type,source_id,company_id) "
            f"SELECT ${gid_idx}::int, np, nm, st, sid, ${cid_idx}::int FROM ({union_sql}) t WHERE length(np) >= 7 "
            f"ON CONFLICT (group_id,phone) DO NOTHING",
            *params
        )
    inserted = int(result.split()[-1]) if result else 0
    return {"ok": True, "inserted": inserted}

@app.delete("/api/admin/autodial/groups/{gid}/members/{mid}")
async def adg_del_member(gid: int, mid: int, cid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM autodial_group_members WHERE id=$1 AND group_id=$2 AND company_id=$3", mid, gid, cid)
    return {"ok": True}

@app.get("/api/admin/autodial/contacts-browse")
async def adg_contacts_browse(
    q: str = "", prefix: str = "", letter: str = "", source: str = "both",
    limit: int = 100, offset: int = 0, cid: int = Depends(_get_admin_cid)
):
    """Поиск клиентов/контактов для импорта — с пагинацией и общим счётчиком."""
    async with db.pool.acquire() as conn:
        where_parts = ["phone IS NOT NULL AND phone != ''"]
        params: list = []
        i = 1
        if q:
            where_parts.append(f"(phone ILIKE ${i} OR first_name ILIKE ${i} OR last_name ILIKE ${i})")
            params.append(f"%{q}%"); i += 1
        if prefix:
            where_parts.append(f"phone LIKE ${i}"); params.append(f"{prefix}%"); i += 1
        if letter:
            where_parts.append(f"(first_name ILIKE ${i} OR last_name ILIKE ${i})")
            params.append(f"{letter}%"); i += 1
        where_parts.append(f"company_id = ${i}"); params.append(cid); i += 1
        w = ' AND '.join(where_parts)

        parts = []
        if source in ("clients", "both"):
            parts.append(
                f"SELECT id, 'clients'::text AS source, phone, "
                f"trim(coalesce(first_name,'')||' '||coalesce(last_name,'')) AS name, "
                f"coalesce(company,'') AS company FROM crm_clients WHERE {w}"
            )
        if source in ("contacts", "both"):
            parts.append(
                f"SELECT id, 'contacts'::text AS source, phone, "
                f"trim(coalesce(first_name,'')||' '||coalesce(last_name,'')) AS name, "
                f"''::text AS company FROM contacts WHERE {w}"
            )

        union_sql = ' UNION ALL '.join(parts)
        rows = await conn.fetch(
            f"SELECT *, COUNT(*) OVER() AS total_count FROM ({union_sql}) t "
            f"ORDER BY name, phone LIMIT {limit} OFFSET {offset}",
            *params
        )

    total = int(rows[0]["total_count"]) if rows else 0
    return {
        "results": [
            {"id": r["id"], "source": r["source"], "phone": r["phone"],
             "name": r["name"], "company": r["company"]}
            for r in rows
        ],
        "total": total,
    }


# ── CallerID и IVR списки ──────────────────────────────────────────────────

@app.get("/api/admin/autodial/callerids")
async def ad_callerids(cid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM autodial_callerids WHERE company_id=$1 "
            "ORDER BY (regexp_replace(number,'[^0-9]','','g'))::bigint ASC, id", cid)
    return [dict(r) for r in rows]

@app.post("/api/admin/autodial/callerids")
async def ad_callerid_create(body: dict = Body(...), cid: int = Depends(_get_admin_cid)):
    num = (body.get("number") or "").strip()
    if not num: raise HTTPException(400, "number required")
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO autodial_callerids (number,label,sort_order,company_id) VALUES ($1,$2,$3,$4) RETURNING *",
            num, (body.get("label") or "").strip(), body.get("sort_order") or 0, cid
        )
    return dict(row)

@app.put("/api/admin/autodial/callerids/{cid}")
async def ad_callerid_update(cid: int, body: dict = Body(...), ccid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        await conn.execute(
            "UPDATE autodial_callerids SET number=$1,label=$2,sort_order=$3 WHERE id=$4 AND company_id=$5",
            (body.get("number") or "").strip(), (body.get("label") or "").strip(),
            body.get("sort_order") or 0, cid, ccid
        )
    return {"ok": True}

@app.delete("/api/admin/autodial/callerids/{cid}")
async def ad_callerid_delete(cid: int, ccid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        await conn.execute("DELETE FROM autodial_callerids WHERE id=$1 AND company_id=$2", cid, ccid)
    return {"ok": True}

@app.get("/api/admin/autodial/ivrs")
async def ad_ivrs(cid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM autodial_ivrs WHERE company_id=$1 "
            "ORDER BY ivr_group, (regexp_replace(exten,'[^0-9]','','g'))::bigint ASC, id", cid)
    return [dict(r) for r in rows]

@app.post("/api/admin/autodial/ivrs")
async def ad_ivr_create(body: dict = Body(...), cid: int = Depends(_get_admin_cid)):
    exten = (body.get("exten") or "").strip()
    if not exten: raise HTTPException(400, "exten required")
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO autodial_ivrs (exten,label,ivr_group,company_id) VALUES ($1,$2,$3,$4) RETURNING *",
            exten, (body.get("label") or "").strip(), (body.get("ivr_group") or "promo").strip(), cid
        )
    return dict(row)

@app.put("/api/admin/autodial/ivrs/{iid}")
async def ad_ivr_update(iid: int, body: dict = Body(...), cid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        await conn.execute(
            "UPDATE autodial_ivrs SET exten=$1,label=$2,ivr_group=$3 WHERE id=$4 AND company_id=$5",
            (body.get("exten") or "").strip(), (body.get("label") or "").strip(),
            (body.get("ivr_group") or "promo").strip(), iid, cid
        )
    return {"ok": True}

@app.delete("/api/admin/autodial/ivrs/{iid}")
async def ad_ivr_delete(iid: int, cid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        await conn.execute("DELETE FROM autodial_ivrs WHERE id=$1 AND company_id=$2", iid, cid)
    return {"ok": True}


# ══════════════════════════════════════
#  ADMIN SMS
# ══════════════════════════════════════

@app.get("/api/admin/sms/settings")
async def sms_settings_get(_=Depends(_get_admin)):
    token = await db.get_config("eskiz_token") or ""
    frm   = await db.get_config("eskiz_from") or "4546"
    return {"token": token, "from": frm}

@app.post("/api/admin/sms/settings")
async def sms_settings_save(body: dict = Body(...), cid: int = Depends(_get_admin_cid)):
    token = (body.get("token") or "").strip()
    frm   = (body.get("from") or "4546").strip()
    if token:
        await db.set_config("eskiz_token", token)
        _eskiz_tokens[cid] = token
    if frm:
        await db.set_config("eskiz_from", frm)
    return {"ok": True}

@app.get("/api/admin/sms/balance")
async def sms_balance(_=Depends(_get_admin)):
    token = await _eskiz_get_token()
    if not token:
        raise HTTPException(400, "Eskiz токен не настроен")
    async with aiohttp.ClientSession() as s:
        r = await s.get(
            "https://notify.eskiz.uz/api/user/get-limit",
            headers={"Authorization": f"Bearer {token}"},
            timeout=aiohttp.ClientTimeout(total=10),
        )
        data = await r.json()
    return data


# ── SMS ГРУППЫ И КОНТАКТЫ ─────────────────────────────────────────────────

@app.get("/api/admin/sms/groups")
async def sms_groups_list(cid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT g.id, g.name, g.description, g.created_at,
                   COUNT(c.id) AS member_count
            FROM sms_groups g
            LEFT JOIN sms_contacts c ON c.group_id = g.id
            WHERE g.company_id=$1
            GROUP BY g.id ORDER BY g.created_at DESC
        """, cid)
    return [dict(r) for r in rows]

@app.post("/api/admin/sms/groups")
async def sms_groups_create(body: dict = Body(...), cid: int = Depends(_get_admin_cid)):
    name = (body.get("name") or "").strip()
    desc = (body.get("description") or "").strip()
    if not name:
        raise HTTPException(400, "name обязателен")
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO sms_groups(name,description,company_id) VALUES($1,$2,$3) RETURNING *", name, desc, cid
        )
    return dict(row)

@app.put("/api/admin/sms/groups/{gid}")
async def sms_groups_update(gid: int, body: dict = Body(...), cid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE sms_groups SET name=$2, description=$3 WHERE id=$1 AND company_id=$4 RETURNING *",
            gid, (body.get("name") or "").strip(), (body.get("description") or "").strip(), cid
        )
    if not row:
        raise HTTPException(404, "Группа не найдена")
    return dict(row)

@app.delete("/api/admin/sms/groups/{gid}")
async def sms_groups_delete(gid: int, cid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        await conn.execute("DELETE FROM sms_groups WHERE id=$1 AND company_id=$2", gid, cid)
    return {"ok": True}

@app.post("/api/admin/sms/groups/{gid}/copy-from-autodial/{ad_gid}")
async def sms_groups_copy_autodial(gid: int, ad_gid: int, cid: int = Depends(_get_admin_cid)):
    """Скопировать участников из autodial-группы в SMS-группу."""
    async with db.pool.acquire() as conn:
        # Проверка
        grp = await conn.fetchrow("SELECT id FROM sms_groups WHERE id=$1 AND company_id=$2", gid, cid)
        if not grp:
            raise HTTPException(404, "SMS-группа не найдена")
        members = await conn.fetch(
            "SELECT phone, name FROM autodial_group_members WHERE group_id=$1 AND company_id=$2", ad_gid, cid
        )
        if not members:
            raise HTTPException(404, "Autodial-группа пуста")
        added = 0
        for m in members:
            try:
                await conn.execute(
                    "INSERT INTO sms_contacts(group_id,phone,name,company_id) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING",
                    gid, m["phone"], m["name"], cid
                )
                added += 1
            except Exception:
                pass
    return {"ok": True, "added": added, "total": len(members)}

@app.get("/api/admin/sms/groups/{gid}/contacts")
async def sms_contacts_list(gid: int, cid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM sms_contacts WHERE group_id=$1 AND company_id=$2 ORDER BY created_at DESC", gid, cid
        )
    return [dict(r) for r in rows]

@app.post("/api/admin/sms/groups/{gid}/contacts")
async def sms_contacts_add(gid: int, body: dict = Body(...), cid: int = Depends(_get_admin_cid)):
    phone = re.sub(r"\D", "", (body.get("phone") or "").strip())
    name  = (body.get("name") or "").strip()
    if not phone:
        raise HTTPException(400, "phone обязателен")
    if not phone.startswith("998"):
        phone = "998" + phone
    async with db.pool.acquire() as conn:
        if not await conn.fetchrow("SELECT id FROM sms_groups WHERE id=$1 AND company_id=$2", gid, cid):
            raise HTTPException(404, "Группа не найдена")
        try:
            row = await conn.fetchrow(
                "INSERT INTO sms_contacts(group_id,phone,name,company_id) VALUES($1,$2,$3,$4) "
                "ON CONFLICT(group_id,phone) DO UPDATE SET name=EXCLUDED.name RETURNING *",
                gid, phone, name, cid
            )
        except Exception as e:
            raise HTTPException(400, str(e))
    return dict(row)

@app.put("/api/admin/sms/contacts/{cid}")
async def sms_contacts_update(cid: int, body: dict = Body(...), ccid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE sms_contacts SET name=$2, status=$3 WHERE id=$1 AND company_id=$4 RETURNING *",
            cid, (body.get("name") or "").strip(), (body.get("status") or "active"), ccid
        )
    if not row:
        raise HTTPException(404, "Контакт не найден")
    return dict(row)

@app.delete("/api/admin/sms/contacts/{cid}")
async def sms_contacts_delete(cid: int, ccid: int = Depends(_get_admin_cid)):
    async with db.pool.acquire() as conn:
        await conn.execute("DELETE FROM sms_contacts WHERE id=$1 AND company_id=$2", cid, ccid)
    return {"ok": True}

@app.post("/api/admin/sms/groups/{gid}/contacts/bulk")
async def sms_contacts_bulk(gid: int, body: dict = Body(...), cid: int = Depends(_get_admin_cid)):
    """Массовое добавление: [{phone, name}, ...]"""
    contacts = body.get("contacts") or []
    added = 0
    async with db.pool.acquire() as conn:
        if not await conn.fetchrow("SELECT id FROM sms_groups WHERE id=$1 AND company_id=$2", gid, cid):
            raise HTTPException(404, "Группа не найдена")
        for c in contacts:
            phone = re.sub(r"\D", "", (c.get("phone") or "").strip())
            if not phone: continue
            if not phone.startswith("998"): phone = "998" + phone
            try:
                await conn.execute(
                    "INSERT INTO sms_contacts(group_id,phone,name,company_id) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING",
                    gid, phone, (c.get("name") or "").strip(), cid
                )
                added += 1
            except Exception:
                pass
    return {"ok": True, "added": added}


@app.get("/api/admin/sms/groups/{gid}/contacts/export")
async def sms_contacts_export(gid: int, cid: int = Depends(_get_admin_cid)):
    import csv, io
    from fastapi.responses import StreamingResponse
    async with db.pool.acquire() as conn:
        grp = await conn.fetchrow("SELECT name FROM sms_groups WHERE id=$1 AND company_id=$2", gid, cid)
        if not grp:
            raise HTTPException(404, "Группа не найдена")
        rows = await conn.fetch(
            "SELECT name, phone FROM sms_contacts WHERE group_id=$1 ORDER BY created_at DESC", gid
        )
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Имя", "Телефон"])
    for r in rows:
        w.writerow([r["name"] or "", r["phone"]])
    content = buf.getvalue().encode("utf-8-sig")
    grp_name = (grp["name"] if grp else f"group_{gid}").replace(" ", "_")
    return StreamingResponse(
        iter([content]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename=sms_contacts_{grp_name}.csv"},
    )

@app.get("/api/admin/sms/contacts/export")
async def sms_all_contacts_export(group_id: int = None, cid: int = Depends(_get_admin_cid)):
    import csv, io
    from fastapi.responses import StreamingResponse
    async with db.pool.acquire() as conn:
        if group_id:
            rows = await conn.fetch(
                "SELECT c.name, c.phone, g.name AS group_name FROM sms_contacts c "
                "JOIN sms_groups g ON c.group_id=g.id WHERE c.group_id=$1 AND c.company_id=$2 "
                "ORDER BY c.created_at DESC", group_id, cid
            )
        else:
            rows = await conn.fetch(
                "SELECT c.name, c.phone, g.name AS group_name FROM sms_contacts c "
                "JOIN sms_groups g ON c.group_id=g.id WHERE c.company_id=$1 ORDER BY g.name, c.created_at DESC",
                cid
            )
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Имя", "Телефон", "Группа"])
    for r in rows:
        w.writerow([r["name"] or "", r["phone"], r["group_name"] or ""])
    content = buf.getvalue().encode("utf-8-sig")
    return StreamingResponse(
        iter([content]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=sms_contacts.csv"},
    )

@app.get("/api/admin/sms/operator-prices")
async def sms_operator_prices_list(_=Depends(_get_admin)):
    return await db.get_sms_operator_prices()

@app.put("/api/admin/sms/operator-prices/{op_id}")
async def sms_operator_prices_update(op_id: int, body: dict = Body(...), _=Depends(_get_admin)):
    import json as _j
    pfx = body.get("prefixes", [])
    if isinstance(pfx, str):
        pfx = [int(x.strip()) for x in pfx.split(",") if x.strip().isdigit()]
    return await db.update_sms_operator_price(
        op_id,
        (body.get("display_name") or "").strip(),
        pfx,
        int(body.get("price_service") or 0),
        int(body.get("price_ad") or 0),
    )

@app.post("/api/admin/sms/groups/{gid}/cost-estimate")
async def sms_cost_estimate(gid: int, body: dict = Body({}), cid: int = Depends(_get_admin_cid)):
    """Расчёт стоимости рассылки по группе с разбивкой по операторам."""
    import json as _j
    sms_type = body.get("type", "service")  # service | ad
    price_field = "price_service" if sms_type == "service" else "price_ad"
    async with db.pool.acquire() as conn:
        if not await conn.fetchrow("SELECT id FROM sms_groups WHERE id=$1 AND company_id=$2", gid, cid):
            raise HTTPException(404, "Группа не найдена")
        contacts = await conn.fetch(
            "SELECT phone FROM sms_contacts WHERE group_id=$1 AND status='active'", gid
        )
        ops = await conn.fetch("SELECT * FROM sms_operator_prices")
    # Построить карту prefix → оператор
    prefix_map = {}
    for op in ops:
        pfx_list = _j.loads(op["prefixes"]) if isinstance(op["prefixes"], str) else op["prefixes"]
        for pfx in pfx_list:
            prefix_map[str(pfx)] = op
    breakdown = {}  # operator → {display_name, count, price, total}
    unknown = 0
    for c in contacts:
        phone = c["phone"]
        # phone = 998XXXXXXXXX
        pfx = phone[3:5] if len(phone) >= 5 else ""
        op = prefix_map.get(pfx)
        if not op:
            unknown += 1
            continue
        key = op["operator"]
        if key not in breakdown:
            breakdown[key] = {"operator": key, "display_name": op["display_name"],
                               "count": 0, "price": op[price_field], "total": 0}
        breakdown[key]["count"] += 1
        breakdown[key]["total"] += op[price_field]
    total = sum(v["total"] for v in breakdown.values())
    return {
        "total": total,
        "unknown": unknown,
        "type": sms_type,
        "breakdown": list(breakdown.values()),
    }

@app.get("/api/admin/sms/nicks")
async def sms_nicks(_=Depends(_get_admin)):
    """Список доступных ников/alpha-name из Eskiz."""
    try:
        data = await _eskiz_get("/nick/list")
        return data
    except Exception:
        return {"data": []}

@app.post("/api/admin/sms/send")
async def sms_send_admin(body: dict = Body(...), _=Depends(_get_admin)):
    token = await _eskiz_get_token()
    if not token:
        raise HTTPException(400, "Eskiz токен не настроен")
    phone   = re.sub(r"\D", "", (body.get("phone") or "").strip())
    message = (body.get("message") or "").strip()
    if not phone or not message:
        raise HTTPException(400, "phone и message обязательны")
    if not phone.startswith("998"):
        phone = "998" + phone
    frm = (body.get("nick") or "").strip() or await db.get_config("eskiz_from") or "4546"
    async with aiohttp.ClientSession() as s:
        r = await s.post(
            "https://notify.eskiz.uz/api/message/sms/send",
            headers={"Authorization": f"Bearer {token}"},
            data={"mobile_phone": phone, "message": message, "from": frm, "callback_url": ""},
            timeout=aiohttp.ClientTimeout(total=15),
        )
        data = await r.json()
    return data

@app.post("/api/admin/sms/send-group")
async def sms_send_group(body: dict = Body(...), cid: int = Depends(_get_admin_cid)):
    """Рассылка всем участникам autodial-группы (сейчас или по расписанию)."""
    token = await _eskiz_get_token()
    if not token:
        raise HTTPException(400, "Eskiz токен не настроен")
    gid           = int(body.get("group_id") or 0)
    message       = (body.get("message") or "").strip()
    name          = (body.get("name") or "Рассылка").strip()
    schedule_time = (body.get("schedule_time") or "").strip()  # "YYYY-MM-DD HH:MM"
    if not gid or not message:
        raise HTTPException(400, "group_id и message обязательны")
    frm = (body.get("nick") or "").strip() or await db.get_config("eskiz_from") or "4546"

    async with db.pool.acquire() as conn:
        if not await conn.fetchrow("SELECT id FROM sms_groups WHERE id=$1 AND company_id=$2", gid, cid):
            raise HTTPException(404, "Группа не найдена")
        members = await conn.fetch(
            "SELECT phone, id FROM sms_contacts WHERE group_id=$1 AND status != 'blacklist'", gid
        )
    if not members:
        raise HTTPException(404, "Группа пуста или все контакты в чёрном списке")

    phones_seen = set()
    phones = []
    messages = []
    for m in members:
        p = re.sub(r"\D", "", m["phone"] or "")
        if not p: continue
        if not p.startswith("998"): p = "998" + p
        if p in phones_seen: continue
        phones_seen.add(p)
        phones.append(p)
        messages.append({"user_sms_id": str(len(messages)+1), "to": p, "text": message})

    if not messages:
        raise HTTPException(400, "Нет валидных номеров в группе")

    if schedule_time:
        # Сохраняем в нашу БД, worker отправит в нужное время
        from datetime import datetime, timedelta
        st = schedule_time.strip().replace("T", " ")
        if len(st) == 16:
            st += ":00"
        tz5 = timezone(timedelta(hours=5))
        scheduled_at = datetime.fromisoformat(st).replace(tzinfo=tz5).astimezone(timezone.utc)
        dispatch_id = await db.create_sms_dispatch(name, message, frm, phones, scheduled_at)
        logging.info(f"SMS dispatch scheduled: id={dispatch_id}, time={st}, phones={len(phones)}")
        return {"ok": True, "total": len(phones), "scheduled": True, "dispatch_id": dispatch_id}
    else:
        # Отправить сейчас — индивидуальные запросы (send-batch ненадёжен)
        results = []
        async with aiohttp.ClientSession() as s:
            for m in messages:
                try:
                    r = await s.post(
                        "https://notify.eskiz.uz/api/message/sms/send",
                        headers={"Authorization": f"Bearer {token}"},
                        data={"mobile_phone": m["to"], "message": m["text"], "from": frm, "callback_url": ""},
                        timeout=aiohttp.ClientTimeout(total=15),
                    )
                    resp = await r.json(content_type=None)
                    results.append({"to": m["to"], "status": resp.get("status"), "id": (resp.get("data") or {}).get("id")})
                except Exception as e:
                    results.append({"to": m["to"], "error": str(e)})
        sent = sum(1 for r in results if r.get("status") == "waiting")
        return {"ok": True, "total": len(messages), "sent": sent, "scheduled": False, "results": results}


@app.get("/api/admin/sms/templates")
async def sms_templates_get(_=Depends(_get_admin)):
    raw = await db.get_config("sms_templates_v1") or "[]"
    try:
        return _json.loads(raw)
    except Exception:
        return []

@app.post("/api/admin/sms/templates")
async def sms_templates_create(body: dict = Body(...), _=Depends(_get_admin)):
    raw = await db.get_config("sms_templates_v1") or "[]"
    try:
        items = _json.loads(raw)
    except Exception:
        items = []
    new_id = (max((x.get("id", 0) for x in items), default=0) + 1)
    item = {
        "id": new_id,
        "category": (body.get("category") or "other").strip(),
        "title":    (body.get("title") or "").strip(),
        "text":     (body.get("text") or "").strip(),
    }
    items.append(item)
    await db.set_config("sms_templates_v1", _json.dumps(items))
    return item

@app.put("/api/admin/sms/templates/{tid}")
async def sms_templates_update(tid: int, body: dict = Body(...), _=Depends(_get_admin)):
    raw = await db.get_config("sms_templates_v1") or "[]"
    try:
        items = _json.loads(raw)
    except Exception:
        items = []
    for item in items:
        if item.get("id") == tid:
            if "category" in body: item["category"] = body["category"].strip()
            if "title"    in body: item["title"]    = body["title"].strip()
            if "text"     in body: item["text"]     = body["text"].strip()
            await db.set_config("sms_templates_v1", _json.dumps(items))
            return item
    raise HTTPException(404, "Шаблон не найден")

@app.delete("/api/admin/sms/templates/{tid}")
async def sms_templates_delete(tid: int, _=Depends(_get_admin)):
    raw = await db.get_config("sms_templates_v1") or "[]"
    try:
        items = _json.loads(raw)
    except Exception:
        items = []
    items = [x for x in items if x.get("id") != tid]
    await db.set_config("sms_templates_v1", _json.dumps(items))
    return {"ok": True}


# ── SMS ОТЧЁТЫ (прокси к Eskiz API) ──────────────────────────────────────

async def _eskiz_parse(r) -> dict:
    """Безопасно парсит ответ Eskiz (возвращает text/plain вместо JSON)."""
    raw = await r.text()
    try:
        return _json.loads(raw.strip())
    except Exception:
        return {"raw": raw.strip(), "status_code": r.status}

async def _eskiz_post(path: str, data: dict) -> dict:
    token = await _eskiz_get_token()
    if not token:
        raise HTTPException(400, "Eskiz токен не настроен")
    async with aiohttp.ClientSession() as s:
        r = await s.post(
            f"https://notify.eskiz.uz/api{path}",
            headers={"Authorization": f"Bearer {token}"},
            data=data,
            timeout=aiohttp.ClientTimeout(total=30),
        )
        return await _eskiz_parse(r)

async def _eskiz_get(path: str) -> dict:
    token = await _eskiz_get_token()
    if not token:
        raise HTTPException(400, "Eskiz токен не настроен")
    async with aiohttp.ClientSession() as s:
        r = await s.get(
            f"https://notify.eskiz.uz/api{path}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=aiohttp.ClientTimeout(total=30),
        )
        return await _eskiz_parse(r)

@app.post("/api/admin/sms/reports/messages")
async def sms_report_messages(body: dict = Body(...), _=Depends(_get_admin)):
    sd = (body.get("start_date") or "")
    ed = (body.get("end_date") or "")
    if len(sd) == 10: sd += " 00:00"
    if len(ed) == 10: ed += " 23:59"
    return await _eskiz_post("/message/sms/get-user-messages", {
        "start_date": sd,
        "end_date":   ed,
        "page_size":  str(body.get("page_size", 100)),
        "count":      str(body.get("count", 0)),
        "status":     body.get("status", ""),
        "smsc":       body.get("smsc", ""),
        "user":       body.get("user", ""),
    })

@app.post("/api/admin/sms/reports/export")
async def sms_report_export(body: dict = Body(...), _=Depends(_get_admin)):
    import csv, io
    from fastapi.responses import StreamingResponse
    sd = body.get("start_date", "")
    ed = body.get("end_date", "")
    rows = await db.get_sms_dispatches_for_export(sd, ed)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["ID","Название","Отправитель","Сообщение","Всего номеров","Отправлено","Статус","Запланировано","Отправлено в","Создано"])
    for r in rows:
        w.writerow([r["id"],r["name"],r["from_nick"],r["message"],
                    r["total_phones"],r["sent_count"],r["status"],
                    str(r["scheduled_at"] or ""),str(r["sent_at"] or ""),str(r["created_at"] or "")])
    content = buf.getvalue().encode("utf-8-sig")
    fname = f"sms_{sd}_{ed}.csv"
    return StreamingResponse(
        iter([content]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename={fname}"},
    )

@app.get("/api/admin/sms/reports/by-month")
async def sms_report_by_month(start_date: str = "", end_date: str = "", _=Depends(_get_admin)):
    if not start_date:
        from datetime import date, timedelta
        end_date = date.today().isoformat()
        start_date = (date.today().replace(day=1) - timedelta(days=365)).isoformat()
    rows = await db.get_sms_stats_by_month(start_date, end_date)
    return {"data": rows}

@app.post("/api/admin/sms/reports/by-date")
async def sms_report_by_date(body: dict = Body(...), _=Depends(_get_admin)):
    rows = await db.get_sms_stats_by_date(
        body.get("start_date", ""), body.get("end_date", ""))
    return {"data": rows}

@app.post("/api/admin/sms/reports/by-dispatch")
async def sms_report_by_dispatch(body: dict = Body(...), _=Depends(_get_admin)):
    rows = await db.get_sms_dispatches_report(
        body.get("start_date", ""), body.get("end_date", ""))
    return {"data": rows}

@app.get("/api/admin/sms/reports/status/{msg_id}")
async def sms_report_status(msg_id: str, _=Depends(_get_admin)):
    return await _eskiz_get(f"/message/sms/{msg_id}")

@app.get("/api/admin/sms/reports/prices")
async def sms_report_prices(_=Depends(_get_admin)):
    return {"data": None, "note": "Информация о ценах недоступна на текущем тарифе Eskiz. Обратитесь в поддержку Eskiz или проверьте личный кабинет."}


# ══════════════════════════════════════
#  SaaS — Суперадмин + Компании + Филиалы
# ══════════════════════════════════════

class SuperadminAuthRequest(BaseModel):
    secret: str

class CompanyCreateRequest(BaseModel):
    name:         str
    slug:         str
    plan:         str = "starter"
    max_branches: int = 5
    max_staff:    int = 50
    admin_password: str = "admin1234"
    subscription_mode: str = "demo"  # "demo" (14 дней, status=trial) | "full" (N месяцев, status=active)
    subscription_months: int = 1     # используется только при subscription_mode="full"
    city_ru:      str = ""
    city_uz:      str = ""
    workshop_lat: float | None = None
    workshop_lng: float | None = None
    contact_phone: str | None = None  # опционально — если уже подтверждён через @cleanouz_bot, привяжется сразу

class CompanyUpdateRequest(BaseModel):
    name:             str | None = None
    slug:             str | None = None
    plan:             str | None = None
    max_branches:     int | None = None
    max_staff:        int | None = None
    active:           bool | None = None
    timezone:         str | None = None
    contact_name:     str | None = None
    contact_phone:    str | None = None
    contact_email:    str | None = None
    inn:              str | None = None
    legal_name:       str | None = None
    address:          str | None = None
    notes:            str | None = None
    trial_days:       int | None = None
    whatsapp:         str | None = None
    instagram:        str | None = None
    tg_group_link:    str | None = None
    tg_group_id:      int | None = None
    tg_channel_link:  str | None = None
    tg_channel_id:    int | None = None
    tg_admin_link:    str | None = None
    tg_admin_id:      int | None = None

class BranchCreateRequest(BaseModel):
    slug:                      str
    name_ru:                   str
    name_uz:                   str = ""
    lat:                       float | None = None
    lon:                       float | None = None
    workshop_lat:              float | None = None
    workshop_lon:              float | None = None
    phones:                    list = []
    tg_orders_channel_id:      int | None = None
    tg_leads_group_id:         int | None = None
    tg_delivery_channel_id:    int | None = None
    tg_delivery_channel_link:  str | None = None
    telegram_link:             str | None = None
    admin_tg_link:             str | None = None
    whatsapp:                  str | None = None
    instagram:                 str | None = None
    tg_leads_group_link:       str | None = None
    tg_orders_channel_link:    str | None = None
    telegram_group_id:         int | None = None
    admin_tg_id:               int | None = None

class BranchUpdateRequest(BaseModel):
    name_ru:                   str | None = None
    name_uz:                   str | None = None
    lat:                       float | None = None
    lon:                       float | None = None
    workshop_lat:              float | None = None
    workshop_lon:              float | None = None
    phones:                    list | None = None
    tg_orders_channel_id:      int | None = None
    tg_leads_group_id:         int | None = None
    tg_delivery_channel_id:    int | None = None
    tg_delivery_channel_link:  str | None = None
    telegram_link:             str | None = None
    admin_tg_link:             str | None = None
    whatsapp:                  str | None = None
    instagram:                 str | None = None
    active:                    bool | None = None
    tg_leads_group_link:       str | None = None
    tg_orders_channel_link:    str | None = None
    telegram_group_id:         int | None = None
    admin_tg_id:               int | None = None


@app.post("/api/saas/auth")
async def saas_auth(req: SuperadminAuthRequest):
    """Суперадмин-логин по секретному ключу из SUPERADMIN_SECRET env."""
    if not SUPERADMIN_SECRET:
        raise HTTPException(status_code=503, detail="Суперадмин не настроен")
    if req.secret != SUPERADMIN_SECRET:
        raise HTTPException(status_code=401, detail="Неверный секрет")
    token = create_superadmin_token()
    return {"ok": True, "token": token}


@app.get("/api/saas/companies")
async def saas_list_companies(_=Depends(get_superadmin)):
    rows = await db.get_all_companies()
    return {"ok": True, "companies": [dict(r) for r in rows]}


async def _provision_company(name: str, slug: str, plan: str, max_branches: int, max_staff: int,
                              admin_password: str | None = None,
                              city_ru: str = "", city_uz: str = "",
                              workshop_lat: float | None = None, workshop_lng: float | None = None):
    """Полное создание компании: staff-аккаунты, первый филиал, seed всех каталогов.
    Общая логика для superadmin-создания и публичной self-service регистрации.
    admin_password=None сохраняет прежнее поведение (login=password='admin').
    city_ru/city_uz/workshop_lat/workshop_lng — данные из формы регистрации (город/район
    и локация цеха клиента), используются для первого филиала и дефолтов сайта вместо
    примеров ARTEZ (Навои/Зарафшан)."""
    secret_key = secrets.token_urlsafe(32)
    company = await db.create_company(
        name=name, slug=slug, secret_key=secret_key,
        plan=plan, max_branches=max_branches, max_staff=max_staff,
    )
    if not company:
        return None, [], secret_key
    credentials = []
    # Уровень компании: без привязки к филиалу, доступ в admin-панель
    for first_name, login, role, position in [
        ("Admin",      "admin",      "admin",      "Администратор"),
        ("Менеджер",   "manager",    "manager",    "Менеджер"),
        ("Колл-центр", "callcenter", "callcenter", "Оператор колл-центра"),
    ]:
        pw = admin_password if (login == "admin" and admin_password) else login
        hashed = pwd_context.hash(pw[:72])
        try:
            await db.create_staff({"first_name": first_name, "login": login,
                                   "password_hash": hashed, "plain_password": pw,
                                   "role": role, "position": position, "branch": None},
                                  company_id=company["id"])
            credentials.append({"level": "company", "role": role, "login": login, "password": pw})
        except Exception:
            pass
    # Создаём первый филиал — название филиала: город/район клиента, если указан, иначе название компании
    try:
        await db.create_branch(
            company_id=company["id"], slug=slug,
            name_ru=city_ru or name, name_uz=city_uz or city_ru or name,
            workshop_lat=workshop_lat, workshop_lon=workshop_lng,
        )
    except Exception:
        pass
    # Уровень филиала: привязаны к первому филиалу, без доступа в admin-панель
    for first_name, login, role, position in [
        ("Водитель",  "driver",    "driver",    "Водитель"),
        ("Упаковщик", "packer",    "packer",    "Упаковщик"),
        ("Логистика", "logistics", "logistics", "Логист"),
        ("Мойщик",    "washer",    "washer",    "Мойщик"),
    ]:
        pw = login
        hashed = pwd_context.hash(pw[:72])
        try:
            await db.create_staff({"first_name": first_name, "login": login,
                                   "password_hash": hashed, "plain_password": pw,
                                   "role": role, "position": position, "branch": slug},
                                  company_id=company["id"])
            credentials.append({"level": "branch", "role": role, "login": login, "password": pw, "branch": slug})
        except Exception:
            pass
    await db.seed_from_template(company["id"])
    await db.seed_company_services(company["id"])
    await db.seed_company_prices(company["id"])
    await db.seed_company_tg_messages(company["id"])
    await db.seed_company_expense_categories(company["id"])
    await db.seed_company_chat_templates(company["id"])
    await db.seed_company_site_stats(company["id"], city_ru=city_ru, city_uz=city_uz)
    await db.seed_company_site_slides(company["id"])
    await db.seed_company_site_reviews(company["id"], city_ru=city_ru, city_uz=city_uz)
    await db.seed_company_site_faq(company["id"])
    await db.seed_company_defaults(company["id"], name)
    await db.seed_company_sms_templates(company["id"])
    await db.seed_company_order_rules(company["id"])

    # Мастер-пароль для подтверждения опасных действий — свой уникальный на компанию,
    # а не общий ADMIN_PASS по умолчанию для всех.
    master_password = secrets.token_urlsafe(6)
    await db.set_config_for_company("admin_pass", master_password, company["id"])
    credentials.append({"level": "company", "role": "master_password", "login": "—", "password": master_password})

    # Новая компания включается в режим "техработ" на сайте И в боте по умолчанию —
    # настройки (контакты, бот, номера филиалов) ещё не заполнены, и без этого
    # реальные посетители видели бы то пустые данные, то (что хуже) чужие/дефолтные
    # ARTEZ-заглушки (см. фиксы order.successText, auth.tgVar3Html этой же сессии).
    # Владелец выключает тоглы сам, когда сайт/бот готовы к реальным клиентам.
    await db.set_config_for_company("site_maintenance_mode", "true", company["id"])
    await db.set_config_for_company("bot_maintenance_mode", "true", company["id"])

    return company, credentials, secret_key


@app.post("/api/saas/companies")
async def saas_create_company(req: CompanyCreateRequest, _=Depends(get_superadmin)):
    slug = req.slug.lower().strip()
    contact_phone = (req.contact_phone or "").strip()
    # Проверка ДО провижининга — иначе при конфликте телефона компания уже была бы
    # создана, а запрос упал бы с ошибкой (осиротевшая наполовину созданная компания).
    if contact_phone and await db.check_company_field_exists("contact_phone", contact_phone):
        raise HTTPException(status_code=409, detail="Этот телефон уже используется другой компанией")

    company, credentials, secret_key = await _provision_company(
        req.name, slug, req.plan, req.max_branches, req.max_staff,
        city_ru=req.city_ru.strip(), city_uz=req.city_uz.strip(),
        workshop_lat=req.workshop_lat, workshop_lng=req.workshop_lng)
    if not company:
        raise HTTPException(status_code=409, detail="Slug уже занят")

    # Если для этого номера УЖЕ есть свежее подтверждение через @cleanouz_bot (клиент
    # заранее поделился контактом с ботом) — привязываем Telegram сразу, как при обычной
    # публичной регистрации. Если подтверждения ещё нет — просто сохраняем номер,
    # суперадмин отправит клиенту персистентную Telegram-ссылку (telegram-link ниже),
    # чтобы тот подтвердил номер через бота уже после создания компании.
    phone_linked = False
    if contact_phone:
        normalized_phone = normalize_phone(contact_phone)
        verification = await db.get_cleano_phone_verification(normalized_phone)
        update_fields = {"contact_phone": contact_phone}
        if verification:
            update_fields["contact_tg_id"] = verification.get("tg_id")
            await db.consume_cleano_phone_verification(normalized_phone)
            phone_linked = True
        await db.update_company(company["id"], update_fields)
        if verification and verification.get("tg_id"):
            await db.clear_stale_cleano_tg_link(verification["tg_id"], company["id"])

    plan = await db.get_saas_plan_by_slug(req.plan)
    if plan:
        import calendar
        from datetime import date, timedelta
        start = date.today()
        if req.subscription_mode == "full":
            months = max(1, req.subscription_months)
            total = start.month - 1 + months
            year = start.year + total // 12
            month = total % 12 + 1
            day = min(start.day, calendar.monthrange(year, month)[1])
            end = date(year, month, day)
            await db.create_saas_subscription(company["id"], plan["id"], start, end,
                                               notes="Создано суперадмином — полный доступ", status="active")
        else:
            end = start + timedelta(days=14)
            await db.create_saas_subscription(company["id"], plan["id"], start, end,
                                               notes="Создано суперадмином — демо-доступ", status="trial")

    return {"ok": True, "company": dict(company), "secret_key": secret_key, "credentials": credentials, "phone_linked": phone_linked}


class PublicRegisterRequest(BaseModel):
    company_name:  str
    slug:          str
    contact_name:  str
    contact_phone: str
    contact_email: str | None = None
    admin_password: str
    city_ru:        str | None = None
    city_uz:        str | None = None
    workshop_lat:   float | None = None
    workshop_lng:   float | None = None

_CHECK_FIELD_MAP = {
    "company_name": "name", "slug": "slug",
    "phone": "contact_phone", "email": "contact_email",
}

@app.get("/api/saas/check-availability")
async def public_check_availability(field: str, value: str):
    """Живая проверка на дубликат при заполнении формы регистрации (cleano.uz)."""
    column = _CHECK_FIELD_MAP.get(field)
    if not column or not value.strip():
        return {"ok": True, "exists": False}
    exists = await db.check_company_field_exists(column, value.strip())
    return {"ok": True, "exists": exists}


@app.post("/api/saas/register")
async def public_register_company(req: PublicRegisterRequest):
    """Самостоятельная регистрация компании с лендинга cleano.uz — создаёт триал на 14 дней."""
    name = req.company_name.strip()
    slug = req.slug.lower().strip()
    if not name:
        raise HTTPException(status_code=400, detail="Укажите название компании")
    if not SLUG_RE.match(slug):
        raise HTTPException(status_code=400, detail="Адрес: только латиница, цифры и дефис, от 3 до 50 символов")
    if not req.contact_phone.strip():
        raise HTTPException(status_code=400, detail="Укажите телефон")
    if len(req.admin_password) < 6:
        raise HTTPException(status_code=400, detail="Пароль должен быть не короче 6 символов")

    if await db.check_company_field_exists("name", name):
        raise HTTPException(status_code=409, detail="Компания с таким названием уже зарегистрирована")
    if await db.check_company_field_exists("slug", slug):
        raise HTTPException(status_code=409, detail="Такой адрес уже занят, выберите другой")
    if await db.check_company_field_exists("contact_phone", req.contact_phone):
        raise HTTPException(status_code=409, detail="Этот телефон уже зарегистрирован")
    if req.contact_email and await db.check_company_field_exists("contact_email", req.contact_email):
        raise HTTPException(status_code=409, detail="Этот email уже зарегистрирован")

    normalized_phone = normalize_phone(req.contact_phone)
    verification = await db.get_cleano_phone_verification(normalized_phone)
    if not verification:
        raise HTTPException(status_code=400, detail="Подтвердите номер телефона перед регистрацией")

    company, credentials, secret_key = await _provision_company(
        name, slug, "starter", 1, 10, admin_password=req.admin_password,
        city_ru=(req.city_ru or "").strip(), city_uz=(req.city_uz or "").strip(),
        workshop_lat=req.workshop_lat, workshop_lng=req.workshop_lng)
    if not company:
        raise HTTPException(status_code=409, detail="Такой адрес уже занят, выберите другой")

    # Одноразовое использование: компания создана — это же подтверждение телефона
    # больше нельзя переиспользовать для другой регистрации (в течение 30-минутного окна).
    await db.consume_cleano_phone_verification(normalized_phone)

    await db.update_company(company["id"], {
        "contact_name":  req.contact_name.strip() or None,
        "contact_phone": req.contact_phone.strip(),
        "contact_email": (req.contact_email or "").strip() or None,
        "contact_tg_id": verification.get("tg_id"),
        "trial_days":    14,
    })
    trial_plan = await db.get_saas_plan_by_slug("starter")
    if trial_plan:
        from datetime import date, timedelta
        await db.create_saas_subscription(
            company["id"], trial_plan["id"], date.today(), date.today() + timedelta(days=14),
            notes="Автоматический триал — регистрация с cleano.uz", status="trial")

    # Мастер-пароль дублируем в Telegram-бот (верификация только через него) — на случай,
    # если пользователь не сохранит таблицу с экрана регистрации.
    tg_id = verification.get("tg_id")
    if tg_id:
        master_row = next((c for c in credentials if c.get("role") == "master_password"), None)
        bot_token = await db.get_config("cleano_bot_token")
        if master_row and bot_token:
            await _cleano_bot_send(bot_token, tg_id,
                f"🔐 <b>{name}</b>\n\nMaxfiy parol (o'chirishlarni tasdiqlash uchun admin-panelda): "
                f"<code>{master_row['password']}</code>\n\n"
                f"🔐 Мастер-пароль (для подтверждения удалений в админ-панели): "
                f"<code>{master_row['password']}</code>")

    return {"ok": True, "slug": slug, "admin_login": "admin", "credentials": credentials}


@app.post("/api/saas/companies/{company_id}/branches")
async def saas_create_branch(company_id: int, body: dict, _=Depends(get_superadmin)):
    name_ru = (body.get("name_ru") or "").strip()
    slug    = (body.get("slug") or "").strip().lower()
    if not name_ru or not slug:
        raise HTTPException(status_code=400, detail="Укажите название и slug")
    company  = await db.get_company(company_id)
    existing = await db.get_branches(company_id)
    if company and len(existing) >= (company["max_branches"] or 99):
        raise HTTPException(status_code=400, detail=f"Лимит филиалов: {company['max_branches']}")
    branch = await db.create_branch(company_id=company_id, slug=slug, name_ru=name_ru)
    if not branch:
        raise HTTPException(status_code=409, detail="Slug уже занят")
    return {"ok": True, "branch": dict(branch)}


@app.post("/api/saas/migrate/admin-logins")
async def saas_migrate_admin_logins(_=Depends(get_superadmin)):
    count = await db.migrate_admin_logins()
    return {"ok": True, "updated": count}


@app.get("/api/saas/companies/{company_id}")
async def saas_get_company(company_id: int, _=Depends(get_superadmin)):
    c = await db.get_company(company_id)
    if not c:
        raise HTTPException(status_code=404, detail="Компания не найдена")
    branches = await db.get_branches(company_id)
    admin = await db.get_company_admin_staff(company_id)
    return {"ok": True, "company": {**dict(c), "trial_days": c.get("trial_days") or 14},
            "branches": [dict(b) for b in branches],
            "admin_login":    admin["login"]          if admin else None,
            "admin_password": admin["plain_password"] if admin else None}


@app.post("/api/saas/companies/{company_id}/impersonate")
async def saas_impersonate_company(company_id: int, _=Depends(get_superadmin)):
    """Получить staff-токен для входа в admin.html как admin данной компании."""
    c = await db.get_company(company_id)
    if not c:
        raise HTTPException(status_code=404, detail="Компания не найдена")
    admin = await db.get_company_admin_staff(company_id)
    if not admin:
        raise HTTPException(status_code=404, detail="Admin-сотрудник не найден")
    token = create_staff_token(admin["id"], admin["login"], admin["role"], company_id)
    return {"ok": True, "token": token, "company_name": c["name"], "slug": c["slug"]}


@app.get("/api/saas/companies/{company_id}/telegram-link")
async def saas_company_telegram_link(company_id: int, _=Depends(get_superadmin)):
    """Персистентная ссылка t.me/<bot>?start=company_<id> — суперадмин отправляет её клиенту
    (WhatsApp/SMS/лично), клиент открывает бота и делится контактом, чтобы привязать Telegram
    к своей компании (без этого доступ к admin.html заблокирован)."""
    c = await db.get_company(company_id)
    if not c:
        raise HTTPException(status_code=404, detail="Компания не найдена")
    bot_username = await db.get_config("cleano_bot_username")
    if not bot_username:
        raise HTTPException(status_code=503, detail="Username Cleano-бота не настроен в Глобальных настройках")
    link = f"https://t.me/{bot_username}?start=company_{company_id}"
    return {"ok": True, "link": link, "linked": bool(c["contact_tg_id"])}


@app.get("/api/saas/companies/{company_id}/config")
async def saas_get_company_config(company_id: int, _=Depends(get_superadmin)):
    keys = ["contact_short", "contact_main"]
    result = {k: await db.get_config_for_company(k, company_id) or "" for k in keys}
    return {"ok": True, "config": result}


@app.put("/api/saas/companies/{company_id}/config")
async def saas_update_company_config(company_id: int, body: dict, _=Depends(get_superadmin)):
    allowed = {"contact_short", "contact_main"}
    for key, val in body.items():
        if key in allowed:
            await db.set_config_for_company(key, str(val), company_id)
    return {"ok": True}


@app.post("/api/saas/companies/{company_id}/admin-password")
async def saas_set_admin_password(company_id: int, body: dict, _=Depends(get_superadmin)):
    new_password = body.get("password", "").strip()
    if len(new_password) < 4:
        raise HTTPException(status_code=400, detail="Пароль минимум 4 символа")
    admin = await db.get_company_admin_staff(company_id)
    if not admin:
        raise HTTPException(status_code=404, detail="Admin-сотрудник не найден")
    hashed = pwd_context.hash(new_password[:72])
    await db.update_staff_password(admin["id"], hashed)
    return {"ok": True}


@app.post("/api/saas/companies/{company_id}/master-password")
async def saas_reset_master_password(company_id: int, _=Depends(get_superadmin)):
    company = await db.get_company(company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Компания не найдена")
    new_password = secrets.token_urlsafe(6)
    await db.set_config_for_company("admin_pass", new_password, company_id)
    sent_via_telegram = False
    if company["contact_tg_id"]:
        bot_token = await db.get_config("cleano_bot_token")
        if bot_token:
            await _cleano_bot_send(bot_token, company["contact_tg_id"],
                f"🔐 <b>{company['name']}</b>\n\nYangi maxfiy parol (o'chirishlarni tasdiqlash uchun admin-panelda): "
                f"<code>{new_password}</code>\n\n"
                f"🔐 Новый мастер-пароль (для подтверждения удалений в админ-панели): "
                f"<code>{new_password}</code>")
            sent_via_telegram = True
    return {"ok": True, "password": new_password, "sent_via_telegram": sent_via_telegram}


@app.put("/api/saas/companies/{company_id}")
async def saas_update_company(company_id: int, req: CompanyUpdateRequest, _=Depends(get_superadmin)):
    c = await db.get_company(company_id)
    if not c:
        raise HTTPException(status_code=404, detail="Компания не найдена")
    updates = {k: v for k, v in req.model_dump().items() if v is not None}
    if updates.get("contact_phone") and await db.check_company_field_exists(
            "contact_phone", updates["contact_phone"], exclude_company_id=company_id):
        raise HTTPException(status_code=409, detail="Этот телефон уже используется другой компанией")
    await db.update_company(company_id, updates)
    return {"ok": True}



@app.post("/api/saas/companies/{company_id}/regenerate-key")
async def saas_regen_key(company_id: int, _=Depends(get_superadmin)):
    """Сгенерировать новый secret_key для компании."""
    c = await db.get_company(company_id)
    if not c:
        raise HTTPException(status_code=404, detail="Компания не найдена")
    new_key = secrets.token_urlsafe(32)
    await db.update_company(company_id, {"secret_key": new_key})
    return {"ok": True, "secret_key": new_key}


class SaasStaffCreateRequest(BaseModel):
    first_name: str
    last_name:  str = ""
    login:      str
    password:   str
    role:       str = "admin"

@app.post("/api/saas/companies/{company_id}/staff")
async def saas_create_staff(company_id: int, req: SaasStaffCreateRequest, _=Depends(get_superadmin)):
    c = await db.get_company(company_id)
    if not c:
        raise HTTPException(status_code=404, detail="Компания не найдена")
    max_staff = c.get("max_staff") or 999
    current_count = await db.count_staff_by_company(company_id)
    if current_count >= max_staff:
        raise HTTPException(status_code=400, detail=f"Лимит сотрудников: {max_staff}")
    existing = await db.get_staff_by_login(req.login, company_id)
    if existing:
        raise HTTPException(status_code=400, detail="Логин уже занят")
    hashed = pwd_context.hash(req.password[:72])
    staff_id = await db.create_staff({
        "first_name":    req.first_name,
        "last_name":     req.last_name,
        "login":         req.login,
        "password_hash": hashed,
        "plain_password": req.password,
        "role":          req.role,
    }, company_id=company_id)
    return {"ok": True, "staff_id": staff_id}

@app.get("/api/saas/companies/{company_id}/staff")
async def saas_list_staff(company_id: int, _=Depends(get_superadmin)):
    if not await db.get_company(company_id):
        raise HTTPException(status_code=404, detail="Компания не найдена")
    rows = await db.get_staff_by_company(company_id)
    return {"ok": True, "staff": [dict(r) for r in rows]}


@app.put("/api/saas/companies/{company_id}/branches/{branch_id}")
async def saas_update_branch(company_id: int, branch_id: int, body: dict = Body(...), _=Depends(get_superadmin)):
    if not await db.get_company(company_id):
        raise HTTPException(status_code=404, detail="Компания не найдена")
    await db.update_branch(branch_id, company_id, body)
    return {"ok": True}


@app.put("/api/saas/companies/{company_id}/staff/{staff_id}")
async def saas_update_staff(company_id: int, staff_id: int, body: dict = Body(...), _=Depends(get_superadmin)):
    if not await db.get_company(company_id):
        raise HTTPException(status_code=404, detail="Компания не найдена")
    async with db.pool.acquire() as conn:
        s = await conn.fetchrow("SELECT id FROM staff WHERE id=$1 AND company_id=$2", staff_id, company_id)
        if not s:
            raise HTTPException(status_code=404, detail="Сотрудник не найден")
        allowed = {"first_name","last_name","login","role"}
        fields = {k: v for k, v in body.items() if k in allowed and v is not None}
        if fields:
            sets = ", ".join(f"{k}=${i+2}" for i, k in enumerate(fields))
            await conn.execute(
                f"UPDATE staff SET {sets}, updated_at=NOW() WHERE id=$1 AND company_id=${len(fields)+2}",
                staff_id, *fields.values(), company_id
            )
    password = body.get("password", "")
    if password and len(password) >= 4:
        hashed = pwd_context.hash(password[:72])
        await db.update_staff_password(staff_id, hashed, password)
    return {"ok": True}


@app.delete("/api/saas/companies/{company_id}/staff/{staff_id}")
async def saas_delete_staff(company_id: int, staff_id: int, _=Depends(get_superadmin)):
    if not await db.get_company(company_id):
        raise HTTPException(status_code=404, detail="Компания не найдена")
    async with db.pool.acquire() as conn:
        s = await conn.fetchrow("SELECT role FROM staff WHERE id=$1 AND company_id=$2", staff_id, company_id)
        if not s:
            raise HTTPException(status_code=404, detail="Сотрудник не найден")
        if s["role"] == "admin":
            cnt = await conn.fetchval(
                "SELECT COUNT(*) FROM staff WHERE company_id=$1 AND role='admin'", company_id
            )
            if cnt <= 1:
                raise HTTPException(status_code=400, detail="Нельзя удалить последнего admin")
        await conn.execute("DELETE FROM staff WHERE id=$1 AND company_id=$2", staff_id, company_id)
    return {"ok": True}


CLEANO_CONFIG_KEYS = [
    "cleano_phone", "cleano_telegram", "cleano_instagram", "cleano_facebook", "cleano_whatsapp",
    "cleano_office_lat", "cleano_office_lng", "cleano_office_address", "cleano_bot_username",
    "cleano_about_ru", "cleano_about_uz",
    "subscription_reminder_days_before", "subscription_reminder_days_after",
    "subscription_reminder_sms_enabled", "subscription_reminder_tg_enabled",
    "subscription_reminder_text_ru", "subscription_reminder_text_uz",
    "subscription_reminder_text_expired_ru", "subscription_reminder_text_expired_uz",
    "subscription_reminder_time",
    "platform_eskiz_email", "platform_eskiz_from",
]
# Секретные ключи Cleano — доступны только суперадмину, НИКОГДА не попадают в public-settings
CLEANO_SECRET_CONFIG_KEYS = ["cleano_bot_token", "platform_eskiz_password"]

class SaasGlobalSettingsRequest(BaseModel):
    yandex_maps_key: str | None = None
    cleano_phone:          str | None = None
    cleano_telegram:       str | None = None
    cleano_instagram:      str | None = None
    cleano_facebook:       str | None = None
    cleano_whatsapp:       str | None = None
    cleano_office_lat:     str | None = None
    cleano_office_lng:     str | None = None
    cleano_office_address: str | None = None
    cleano_bot_username:   str | None = None
    cleano_bot_token:      str | None = None
    cleano_about_ru:       str | None = None
    cleano_about_uz:       str | None = None
    subscription_reminder_days_before: str | None = None
    subscription_reminder_days_after:  str | None = None
    subscription_reminder_sms_enabled: str | None = None
    subscription_reminder_tg_enabled:  str | None = None
    subscription_reminder_text_ru:     str | None = None
    subscription_reminder_text_uz:     str | None = None
    subscription_reminder_text_expired_ru: str | None = None
    subscription_reminder_text_expired_uz: str | None = None
    subscription_reminder_time:        str | None = None
    platform_eskiz_email:    str | None = None
    platform_eskiz_password: str | None = None
    platform_eskiz_from:     str | None = None

@app.get("/api/saas/global-settings")
async def saas_get_global_settings(_=Depends(get_superadmin)):
    settings = {"yandex_maps_key": await db.get_config("yandex_maps_key") or ""}
    for k in CLEANO_CONFIG_KEYS + CLEANO_SECRET_CONFIG_KEYS:
        settings[k] = await db.get_config(k) or ""
    return {"ok": True, "settings": settings}

@app.put("/api/saas/global-settings")
async def saas_update_global_settings(req: SaasGlobalSettingsRequest, _=Depends(get_superadmin)):
    if req.yandex_maps_key is not None:
        await db.set_config("yandex_maps_key", req.yandex_maps_key)
    for k in CLEANO_CONFIG_KEYS + CLEANO_SECRET_CONFIG_KEYS:
        v = getattr(req, k)
        if v is not None:
            await db.set_config(k, v)

    webhook_result = None
    if req.cleano_bot_token is not None and req.cleano_bot_token.strip():
        webhook_result = await _cleano_bot_set_webhook(req.cleano_bot_token.strip())
    return {"ok": True, "webhook": webhook_result}


@app.get("/api/saas/platform-eskiz-balance")
async def saas_platform_eskiz_balance(_=Depends(get_superadmin)):
    balance = await get_platform_eskiz_balance()
    return {"ok": True, "balance": balance}


@app.post("/api/saas/subscription-reminders/run-now")
async def saas_run_subscription_reminders_now(_=Depends(get_superadmin)):
    """Ручной запуск рассылки, в обход расписания воркера — он спит до заранее
    вычисленной цели и не подхватывает смену subscription_reminder_time мгновенно
    (только на следующем витке после текущего сна). Возвращает результат по каждой
    компании в окне рассылки — видно, кому отправлено и почему нет (нет телефона/
    Telegram, канал выключен, ошибка отправки)."""
    results = await _send_subscription_reminders()
    return {"ok": True, "results": results}


@app.get("/api/saas/public-settings")
async def public_saas_settings():
    """Публичные контакты/локация платформы Cleano — для лендинга cleano.uz (без авторизации)."""
    settings = {}
    for k in CLEANO_CONFIG_KEYS:
        settings[k] = await db.get_config(k) or ""
    return {"ok": True, "settings": settings}


@app.get("/api/saas/cleano-bot-clients")
async def saas_cleano_bot_clients(_=Depends(get_superadmin)):
    """Список тех, кто подтверждал телефон через @cleanouz_bot — для карточки
    "Telegram-бот Cleano" в superadmin.html."""
    rows = await db.list_cleano_bot_clients()
    return {"ok": True, "clients": rows}


@app.post("/api/saas/cleano-bot-image")
async def sa_upload_cleano_bot_image(file: UploadFile = File(...), _=Depends(get_superadmin)):
    """Картинка главного меню @cleanouz_bot — хранится как data-URI (см. _cleano_bot_send_photo:
    единственный отправитель этой картинки — сам cleanouz_bot, поэтому Telegram file_id тут не
    нужен вообще, raw-байты из БД проще и без пробем валидности file_id между ботами."""
    if not (file.content_type or "").startswith("image/"):
        raise HTTPException(status_code=400, detail="Файл должен быть изображением")
    data = await file.read()
    if len(data) > 5_000_000:
        raise HTTPException(status_code=400, detail="Изображение слишком большое (макс. 5 МБ)")
    b64 = f"data:{file.content_type};base64,{base64.b64encode(data).decode('ascii')}"
    await db.set_config("cleano_bot_main_image", b64)
    return {"ok": True}


@app.delete("/api/saas/cleano-bot-image")
async def sa_delete_cleano_bot_image(_=Depends(get_superadmin)):
    await db.set_config("cleano_bot_main_image", "")
    return {"ok": True}


@app.get("/api/saas/cleano-bot-image-status")
async def sa_cleano_bot_image_status(_=Depends(get_superadmin)):
    img = await db.get_config("cleano_bot_main_image")
    return {"ok": True, "has_image": bool(img)}


# ══════════════════════════════════════
#  ПОДТВЕРЖДЕНИЕ ТЕЛЕФОНА ПРИ РЕГИСТРАЦИИ НА CLEANO.UZ (SMS или Telegram-бот)
#  Отдельный бот от artez_bot (клиенты ARTEZ) — свой токен, своя таблица привязки.
# ══════════════════════════════════════

def _cleano_bot_webhook_secret(token: str) -> str:
    return hashlib.sha256(f"cleano-webhook-{token}".encode()).hexdigest()[:32]


async def _cleano_bot_set_webhook(token: str):
    """Регистрирует webhook Cleano-бота в Telegram сразу при сохранении токена в суперадмине."""
    if not APP_URL:
        return {"ok": False, "error": "APP_URL не задан на сервере"}
    url = f"{APP_URL}/api/cleano/tg-webhook"
    secret = _cleano_bot_webhook_secret(token)
    try:
        async with aiohttp.ClientSession() as s:
            r = await s.post(
                f"https://api.telegram.org/bot{token}/setWebhook",
                json={"url": url, "secret_token": secret, "allowed_updates": ["message", "callback_query"]},
                timeout=aiohttp.ClientTimeout(total=10),
            )
            data = await r.json()
            # Кнопка "Menu" рядом со строкой ввода — список команд, чтобы не набирать /start вручную.
            await s.post(
                f"https://api.telegram.org/bot{token}/setMyCommands",
                json={"commands": [
                    {"command": "start", "description": "Bosh menyu / Главное меню"},
                    {"command": "menu", "description": "Menyu / Меню"},
                ]},
                timeout=aiohttp.ClientTimeout(total=10),
            )
            await s.post(
                f"https://api.telegram.org/bot{token}/setChatMenuButton",
                json={"menu_button": {"type": "commands"}},
                timeout=aiohttp.ClientTimeout(total=10),
            )
            return {"ok": bool(data.get("ok")), "description": data.get("description", "")}
    except Exception as e:
        return {"ok": False, "error": str(e)}


async def _cleano_bot_send(token: str, chat_id, text: str, reply_markup: dict | None = None):
    if not token or not chat_id:
        return
    try:
        payload = {"chat_id": str(chat_id), "text": text, "parse_mode": "HTML"}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        async with aiohttp.ClientSession() as s:
            await s.post(f"https://api.telegram.org/bot{token}/sendMessage", json=payload,
                         timeout=aiohttp.ClientTimeout(total=8))
    except Exception as e:
        logging.warning(f"_cleano_bot_send error: {e}")


async def _cleano_bot_answer_callback(token: str, callback_query_id: str):
    """Гасит "часики" на инлайн-кнопке в Telegram — обязательно после любого callback_query,
    иначе кнопка у клиента виснет в состоянии загрузки."""
    if not token or not callback_query_id:
        return
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(f"https://api.telegram.org/bot{token}/answerCallbackQuery",
                         json={"callback_query_id": callback_query_id},
                         timeout=aiohttp.ClientTimeout(total=8))
    except Exception as e:
        logging.warning(f"_cleano_bot_answer_callback error: {e}")


_CLN_T = {
    "ru": {
        "choose_lang": "🌐 Выберите язык интерфейса:",
        "lang_changed": "✅ Язык изменён на русский",
        "menu_title": "🏠 Главное меню",
        "about_fallback": "Cleano — SaaS-платформа для химчисток и клининговых компаний: сайт, Telegram-бот и CRM под ключ.",
        "btn_admin": "🌐 Админ-панель",
        "btn_company": "📋 Моя компания",
        "btn_support": "📞 Поддержка Cleano",
        "btn_settings": "⚙️ Настройки",
        "btn_confirm_phone": "📱 Подтвердить номер",
        "btn_change_lang": "🌐 Сменить язык",
        "btn_relink": "🔄 Перепривязать Telegram",
        "btn_back": "◀️ Назад",
        "settings_title": "⚙️ <b>Настройки</b>",
        "support_title": "📞 <b>Поддержка Cleano</b>",
        "support_empty": "Контакты пока не заполнены суперадмином.",
        "not_linked": "⚠️ Telegram ещё не привязан ни к одной компании.",
        "company_title": "🏠 <b>Моя компания</b>",
        "plan_label": "📦 Тариф",
        "trial_days": "⏳ Демо-доступ — осталось {n} дн.",
        "trial_generic": "⏳ Демо-доступ",
        "full_until": "✅ Полный доступ до {d}",
        "full_generic": "✅ Полный доступ",
        "period_label": "📅 Доступ с {s} по {e}",
        "contact_name_label": "👤 Ответственное лицо",
        "company_phone_label": "📞 Телефон компании",
        "branches_label": "🏢 Филиалы ({n})",
        "no_branches": "🏢 Филиалы не добавлены",
        "branch_no_phone": "без номера",
        "groups_label": "💬 Telegram-группы и каналы",
        "group_line": "Группа",
        "channel_line": "Канал",
        "admin_line": "Админ-группа",
        "share_prompt": "Поделитесь контактом кнопкой ниже, чтобы подтвердить/перепривязать номер.",
        "share_btn": "📱 Поделиться номером",
        "btn_register": "📝 Начать регистрацию",
        "need_phone_hint": "👉 Сначала подтвердите номер телефона кнопкой ниже — после этого станет доступна регистрация компании.",
    },
    "uz": {
        "choose_lang": "🌐 Interfeys tilini tanlang:",
        "lang_changed": "✅ Til o'zbekchaga o'zgartirildi",
        "menu_title": "🏠 Bosh menyu",
        "about_fallback": "Cleano — kimyoviy tozalash va klining kompaniyalari uchun SaaS-platforma: sayt, Telegram-bot va CRM.",
        "btn_admin": "🌐 Admin-panel",
        "btn_company": "📋 Mening kompaniyam",
        "btn_support": "📞 Cleano qo'llab-quvvatlash",
        "btn_settings": "⚙️ Sozlamalar",
        "btn_confirm_phone": "📱 Raqamni tasdiqlash",
        "btn_change_lang": "🌐 Tilni o'zgartirish",
        "btn_relink": "🔄 Telegramni qayta ulash",
        "btn_back": "◀️ Orqaga",
        "settings_title": "⚙️ <b>Sozlamalar</b>",
        "support_title": "📞 <b>Cleano qo'llab-quvvatlash</b>",
        "support_empty": "Superadmin hali kontaktlarni to'ldirmagan.",
        "not_linked": "⚠️ Telegram hali birorta kompaniyaga ulanmagan.",
        "company_title": "🏠 <b>Mening kompaniyam</b>",
        "plan_label": "📦 Tarif",
        "trial_days": "⏳ Demo-kirish — {n} kun qoldi.",
        "trial_generic": "⏳ Demo-kirish",
        "full_until": "✅ To'liq kirish {d} gacha",
        "full_generic": "✅ To'liq kirish",
        "period_label": "📅 Kirish muddati: {s} — {e}",
        "contact_name_label": "👤 Mas'ul shaxs",
        "company_phone_label": "📞 Kompaniya telefoni",
        "branches_label": "🏢 Filiallar ({n})",
        "no_branches": "🏢 Filiallar qo'shilmagan",
        "branch_no_phone": "raqamsiz",
        "groups_label": "💬 Telegram guruh va kanallar",
        "group_line": "Guruh",
        "channel_line": "Kanal",
        "admin_line": "Admin-guruh",
        "share_prompt": "Raqamni tasdiqlash/qayta ulash uchun pastdagi tugma orqali kontaktingizni ulashing.",
        "share_btn": "📱 Raqamni ulashish",
        "btn_register": "📝 Ro'yxatdan o'tishni boshlash",
        "need_phone_hint": "👉 Avval pastdagi tugma orqali telefon raqamingizni tasdiqlang — shundan keyin kompaniyani roʻyxatdan oʻtkazish mumkin boʻladi.",
    },
}


def _cleano_lang_kb() -> dict:
    return {"inline_keyboard": [
        [{"text": "🇷🇺 Русский язык", "callback_data": "cln_lang_ru"}],
        [{"text": "🇺🇿 O'zbek tili", "callback_data": "cln_lang_uz"}],
    ]}


def _cleano_share_kb(lang: str) -> dict:
    return {
        "keyboard": [[{"text": _CLN_T[lang]["share_btn"], "request_contact": True}]],
        "resize_keyboard": True, "one_time_keyboard": True,
    }


def _cleano_main_menu_kb(company: dict | None, lang: str, phone: str | None = None) -> dict:
    """Главное меню @cleanouz_bot — инлайн-кнопки. 'Поделиться номером' у Telegram работает
    только через reply-кнопку (request_contact), инлайн-кнопки этого не умеют — поэтому
    подтверждение/перепривязка (cln_relink) в ответ шлёт ОТДЕЛЬНОЕ сообщение с
    reply-клавиатурой (см. _cleano_share_kb). phone — уже подтверждённый через бота номер
    (если есть), пробрасывается в форму регистрации на сайте параметром ?phone=, чтобы
    не заставлять вводить и подтверждать его там ещё раз."""
    t = _CLN_T[lang]
    if company:
        rows = [
            [{"text": t["btn_admin"], "url": f"https://cleano.uz/admin.html?company_slug={company['slug']}&fresh=1"}],
            [{"text": t["btn_company"], "callback_data": "cln_company"}],
            [{"text": t["btn_support"], "callback_data": "cln_support"}],
            [{"text": t["btn_settings"], "callback_data": "cln_settings"}],
        ]
    elif phone:
        # Номер уже подтверждён (был "Поделиться номером") — предлагаем регистрацию,
        # кнопка повторного подтверждения больше не нужна.
        import urllib.parse
        reg_url = f"https://cleano.uz/cleano-landing.html?phone={urllib.parse.quote(phone)}"
        rows = [
            [{"text": t["btn_register"], "url": reg_url}],
            [{"text": t["btn_support"], "callback_data": "cln_support"}],
        ]
    else:
        # Номер ещё не подтверждён — регистрацию предлагать рано (пока некуда привязать
        # созданную компанию), только подтверждение номера + поддержка.
        rows = [
            [{"text": t["btn_confirm_phone"], "callback_data": "cln_relink"}],
            [{"text": t["btn_support"], "callback_data": "cln_support"}],
        ]
    return {"inline_keyboard": rows}


def _cleano_settings_kb(lang: str) -> dict:
    t = _CLN_T[lang]
    return {"inline_keyboard": [
        [{"text": t["btn_change_lang"], "callback_data": "cln_lang"}],
        [{"text": t["btn_relink"], "callback_data": "cln_relink"}],
        [{"text": t["btn_back"], "callback_data": "cln_menu"}],
    ]}


async def _cleano_resolve_lang(chat_id) -> str:
    lang = await db.get_cleano_tg_lang(chat_id)
    return lang if lang in ("ru", "uz") else "ru"


async def _cleano_bot_send_photo(token: str, chat_id, image_b64: str, caption: str, reply_markup: dict | None = None):
    """Картинка главного меню — хранится как data-URI base64 в конфиге (тот же приём, что и
    logo_url компании, см. company_upload_logo), НЕ как Telegram file_id: единственный
    отправитель — этот же бот, так что кросс-бот-проблема file_id тут не актуальна, а raw-байты
    проще и надёжнее (не нужно ничего кэшировать/восстанавливать при недействительном file_id)."""
    if not token or not chat_id or not image_b64:
        return
    import json
    try:
        b64data = image_b64.split(",", 1)[1] if "," in image_b64 else image_b64
        photo_bytes = base64.b64decode(b64data)
        form = aiohttp.FormData()
        form.add_field("chat_id", str(chat_id))
        form.add_field("caption", caption)
        form.add_field("parse_mode", "HTML")
        if reply_markup:
            form.add_field("reply_markup", json.dumps(reply_markup))
        form.add_field("photo", photo_bytes, filename="cleano.jpg", content_type="image/jpeg")
        async with aiohttp.ClientSession() as s:
            await s.post(f"https://api.telegram.org/bot{token}/sendPhoto", data=form,
                         timeout=aiohttp.ClientTimeout(total=15))
    except Exception as e:
        logging.warning(f"_cleano_bot_send_photo error: {e}")


async def _cleano_send_main_menu(token: str, chat_id, lang: str, clear_keyboard: bool = False):
    """clear_keyboard=True шлёт крошечное сообщение с remove_keyboard ПЕРЕД самим меню —
    Telegram не даёт одновременно убрать reply-клавиатуру и показать inline-кнопки в одном
    sendMessage/sendPhoto, поэтому это два отдельных сообщения."""
    if clear_keyboard:
        await _cleano_bot_send(token, chat_id, "🏠", reply_markup={"remove_keyboard": True})
    company = await db.get_company_by_contact_tg_id(chat_id)
    own_phone = None if company else await db.get_cleano_phone_by_tg_id(chat_id)
    if not company and own_phone:
        # Номер этого чата подтверждён через бота, но контакт компании (contact_tg_id)
        # мог быть проставлен ДРУГИМ путём — напр. суперадмин вписал этот же номер
        # вручную в contact_phone компании, минуя бота вообще (см. company-by-verified-
        # -phone для того же случая на cleano-landing.html). Раз номер совпал и он
        # подтверждён — это и есть владелец, довязываем contact_tg_id, чтобы дальше
        # компания узнавалась как обычно (get_company_by_contact_tg_id).
        matched = await db.get_company_by_phone(own_phone)
        if matched and not matched["contact_tg_id"]:
            await db.clear_stale_cleano_tg_link(chat_id, matched["id"])
            await db.update_company(matched["id"], {"contact_tg_id": chat_id})
            company = await db.get_company(matched["id"])
    t = _CLN_T[lang]
    about = await db.get_config(f"cleano_about_{lang}") or t["about_fallback"]
    support_phone = await db.get_config("cleano_phone") or ""
    lines = [t["menu_title"], "", about]
    if support_phone:
        lines.append(f"☎️ {support_phone}")
    if not company and not own_phone:
        lines += ["", t["need_phone_hint"]]
    text = "\n".join(lines)
    kb = _cleano_main_menu_kb(company, lang, phone=own_phone)
    image_b64 = await db.get_config("cleano_bot_main_image")
    if image_b64:
        await _cleano_bot_send_photo(token, chat_id, image_b64, caption=text, reply_markup=kb)
    else:
        await _cleano_bot_send(token, chat_id, text, reply_markup=kb)


async def _cleano_company_info_text(company: dict, lang: str) -> str:
    import json as _json
    from datetime import date
    t = _CLN_T[lang]
    sub = await db.get_saas_subscription(company["id"])
    lines = [t["company_title"], "", f"🏢 <b>{company['name']}</b>"]
    if sub:
        lines.append(f"{t['plan_label']}: {sub.get('plan_name') or sub.get('plan_slug') or company.get('plan')}")
        start_date, end_date = sub.get("start_date"), sub.get("end_date")
        if start_date and end_date:
            lines.append(t["period_label"].format(s=start_date.strftime('%d.%m.%Y'), e=end_date.strftime('%d.%m.%Y')))
        if sub.get("status") == "trial":
            days_left = (end_date - date.today()).days if end_date else None
            lines.append(t["trial_days"].format(n=max(days_left, 0)) if days_left is not None else t["trial_generic"])
        else:
            lines.append(t["full_until"].format(d=end_date.strftime('%d.%m.%Y')) if end_date else t["full_generic"])
    else:
        lines.append(f"{t['plan_label']}: {company.get('plan', '—')}")
    if company.get("contact_name"):
        lines.append(f"{t['contact_name_label']}: {company['contact_name']}")
    if company.get("contact_phone"):
        lines.append(f"{t['company_phone_label']}: {company['contact_phone']}")

    branches = await db.get_branches(company["id"])
    lines.append("")
    lines.append(t["branches_label"].format(n=len(branches)) if branches else t["no_branches"])
    for b in branches:
        name = (b["name_uz"] or b["name_ru"]) if lang == "uz" else b["name_ru"]
        raw_phones = b["phones"]
        try:
            raw_phones = _json.loads(raw_phones) if isinstance(raw_phones, str) else (raw_phones or [])
        except (TypeError, ValueError):
            raw_phones = []
        phone_nums = [p.get("n") for p in raw_phones if isinstance(p, dict) and p.get("n")]
        lines.append(f"• {name}: {', '.join(phone_nums) if phone_nums else t['branch_no_phone']}")

    tg_lines = []
    if company.get("tg_group_link"):
        tg_lines.append(f"{t['group_line']}: {company['tg_group_link']}")
    if company.get("tg_channel_link"):
        tg_lines.append(f"{t['channel_line']}: {company['tg_channel_link']}")
    if company.get("tg_admin_link"):
        tg_lines.append(f"{t['admin_line']}: {company['tg_admin_link']}")
    if tg_lines:
        lines.append("")
        lines.append(t["groups_label"])
        lines.extend(tg_lines)

    return "\n".join(lines)


async def _cleano_support_text(lang: str) -> str:
    t = _CLN_T[lang]
    phone = await db.get_config("cleano_phone") or ""
    tg = await db.get_config("cleano_telegram") or ""
    lines = [t["support_title"]]
    if phone: lines.append(f"☎️ {phone}")
    if tg: lines.append(f"✈️ {tg}")
    if not phone and not tg: lines.append(t["support_empty"])
    return "\n".join(lines)


@app.post("/api/cleano/tg-webhook")
async def cleano_tg_webhook(request: Request):
    """Webhook Telegram-бота Cleano — подтверждение телефона + главное меню (админ-панель,
    инфо о компании, поддержка Cleano)."""
    token = await db.get_config("cleano_bot_token")
    if not token:
        return {"ok": True}
    expected_secret = _cleano_bot_webhook_secret(token)
    got_secret = request.headers.get("x-telegram-bot-api-secret-token", "")
    if not secrets.compare_digest(got_secret, expected_secret):
        raise HTTPException(status_code=403, detail="Invalid secret token")

    update = await request.json()

    cq = update.get("callback_query")
    if cq:
        chat_id = ((cq.get("message") or {}).get("chat") or {}).get("id")
        await _cleano_bot_answer_callback(token, cq.get("id"))
        if not chat_id:
            return {"ok": True}
        data = cq.get("data") or ""
        lang = await _cleano_resolve_lang(chat_id)
        if data in ("cln_lang_ru", "cln_lang_uz"):
            new_lang = "ru" if data == "cln_lang_ru" else "uz"
            await db.set_cleano_tg_lang(chat_id, new_lang)
            await _cleano_bot_send(token, chat_id, _CLN_T[new_lang]["lang_changed"])
            await _cleano_send_main_menu(token, chat_id, new_lang)
        elif data == "cln_lang":
            await _cleano_bot_send(token, chat_id, _CLN_T[lang]["choose_lang"], reply_markup=_cleano_lang_kb())
        elif data == "cln_menu":
            await _cleano_send_main_menu(token, chat_id, lang)
        elif data == "cln_settings":
            await _cleano_bot_send(token, chat_id, _CLN_T[lang]["settings_title"], reply_markup=_cleano_settings_kb(lang))
        elif data == "cln_company":
            company = await db.get_company_by_contact_tg_id(chat_id)
            await _cleano_bot_send(token, chat_id,
                await _cleano_company_info_text(company, lang) if company else _CLN_T[lang]["not_linked"])
        elif data == "cln_support":
            await _cleano_bot_send(token, chat_id, await _cleano_support_text(lang))
        elif data == "cln_relink":
            await _cleano_bot_send(token, chat_id, _CLN_T[lang]["share_prompt"], reply_markup=_cleano_share_kb(lang))
        return {"ok": True}

    msg = update.get("message") or {}
    chat_id = (msg.get("chat") or {}).get("id")
    if not chat_id:
        return {"ok": True}

    contact = msg.get("contact")
    if contact and contact.get("phone_number"):
        phone = normalize_phone(contact["phone_number"])
        lang = await _cleano_resolve_lang(chat_id)
        # Если это переход по персистентной ссылке суперадмина (t.me/<bot>?start=company_<id>) —
        # привязываем Telegram НАПРЯМУЮ к указанной компании, а не через сопоставление по номеру.
        pending_company_id = await db.get_pending_company_link(chat_id)
        if pending_company_id:
            await db.clear_stale_cleano_tg_link(chat_id, pending_company_id)
            await db.update_company(pending_company_id, {"contact_tg_id": chat_id})
            await db.consume_pending_company_link(chat_id)
            company = await db.get_company(pending_company_id)
            name = company["name"] if company else ""
            await _cleano_bot_send(token, chat_id,
                f"✅ Telegram muvaffaqiyatli ulandi: <b>{name}</b>\n\n"
                f"✅ Telegram успешно привязан: <b>{name}</b>",
                reply_markup={"remove_keyboard": True})
            await _cleano_send_main_menu(token, chat_id, lang)
            return {"ok": True}
        await db.save_cleano_tg_link(phone, chat_id)
        await db.mark_cleano_phone_verified(phone, "telegram", tg_id=chat_id)
        await _cleano_bot_send(token, chat_id,
            "✅ Raqam tasdiqlandi! Endi cleano.uz saytiga qaytib, ro'yxatdan o'tishni yakunlashingiz mumkin."
            "\n\n✅ Номер подтверждён! Вернитесь на cleano.uz и завершите регистрацию.",
            reply_markup={"remove_keyboard": True})
        await _cleano_send_main_menu(token, chat_id, lang)
        return {"ok": True}

    text = (msg.get("text") or "").strip()

    # Персистентная ссылка суперадмина: /start company_<id> — редкий, инициированный
    # суперадмином путь (оставлен двуязычным как раньше, без гейта по выбору языка).
    if text.startswith("/start company_"):
        try:
            target_company_id = int(text.split("company_", 1)[1].strip())
        except (IndexError, ValueError):
            target_company_id = None
        company = await db.get_company(target_company_id) if target_company_id else None
        if not company:
            await _cleano_bot_send(token, chat_id, "⚠️ Havola noto'g'ri yoki eskirgan.\n\n⚠️ Ссылка неверна или устарела.")
            return {"ok": True}
        await db.save_pending_company_link(chat_id, target_company_id)
        lang = await _cleano_resolve_lang(chat_id)
        await _cleano_bot_send(token, chat_id,
            f"👋 <b>Cleano</b>\n\n<b>{company['name']}</b> nomidan Telegram-ni ulash uchun "
            f"pastdagi tugma orqali raqamingizni ulashing.\n\n"
            f"Поделитесь контактом кнопкой ниже, чтобы привязать Telegram к компании <b>{company['name']}</b>.",
            reply_markup=_cleano_share_kb(lang))
        return {"ok": True}

    # Первое обращение этого Telegram-аккаунта — язык ещё не выбран. Гейт ПЕРЕД чем угодно
    # ещё (даже перед меню/подтверждением номера) — выбор языка в две строки (RU/UZ).
    lang = await db.get_cleano_tg_lang(chat_id)
    if lang is None:
        # Чистим клавиатуру ДО выбора языка — иначе зависшая с прошлой сессии reply-кнопка
        # "Поделиться номером" оставалась бы висеть до первого выбора языка (эта ветка
        # раньше возвращала ответ до того, как код доходил до _cleano_send_main_menu, где
        # clear_keyboard обрабатывается для /start).
        if text in ("/start", "/menu"):
            await _cleano_bot_send(token, chat_id, "🌐", reply_markup={"remove_keyboard": True})
        await _cleano_bot_send(token, chat_id,
            f"{_CLN_T['ru']['choose_lang']}\n{_CLN_T['uz']['choose_lang']}",
            reply_markup=_cleano_lang_kb())
        return {"ok": True}

    # /start и любой другой текст — главное меню (не нужно помнить команды). Именно на
    # /start ВСЕГДА чистим клавиатуру — могла зависнуть reply-клавиатура «Поделиться номером»
    # с прошлого раза, если пользователь ушёл, не нажав её.
    await _cleano_send_main_menu(token, chat_id, lang, clear_keyboard=(text in ("/start", "/menu")))
    return {"ok": True}



# ══════════════════════════════════════
#  БОТ ЗАКАЗОВ ДЛЯ КЛИЕНТОВ КОМПАНИИ (свой токен на компанию, общий процесс)
#  "Вариант B2": каждая компания подключает СВОЙ Telegram-бот в admin.html —
#  один этот процесс обслуживает вебхуки всех компаний одновременно.
# ══════════════════════════════════════

def _order_bot_webhook_secret(token: str) -> str:
    return hashlib.sha256(f"order-bot-webhook-{token}".encode()).hexdigest()[:32]


_order_bot_instances: dict[str, "Bot"] = {}
_order_bot_dispatcher = None


def _get_order_bot_dispatcher():
    global _order_bot_dispatcher
    if _order_bot_dispatcher is None:
        from aiogram import Dispatcher
        from order_bot_storage import PostgresStorage
        from order_bot_handlers import router as order_bot_router
        _order_bot_dispatcher = Dispatcher(storage=PostgresStorage())
        _order_bot_dispatcher.include_router(order_bot_router)
    return _order_bot_dispatcher


def _get_order_bot_instance(token: str):
    from aiogram import Bot
    from aiogram.client.default import DefaultBotProperties
    bot = _order_bot_instances.get(token)
    if bot is None:
        bot = Bot(token=token, default=DefaultBotProperties(parse_mode="HTML"))
        _order_bot_instances[token] = bot
    return bot


class OrderBotSetupRequest(BaseModel):
    token: str


@app.post("/api/admin/order-bot/setup")
async def admin_order_bot_setup(req: OrderBotSetupRequest, cid: int = Depends(_get_admin_cid)):
    """Сохраняет токен бота заказов текущей компании и устанавливает вебхук в Telegram."""
    token = req.token.strip()
    if not token:
        raise HTTPException(status_code=400, detail="Укажите токен")
    if not APP_URL:
        raise HTTPException(status_code=500, detail="APP_URL не задан на сервере")
    secret = _order_bot_webhook_secret(token)
    url = f"{APP_URL}/api/order-bot/webhook/{secret}"
    try:
        async with aiohttp.ClientSession() as s:
            r = await s.post(f"https://api.telegram.org/bot{token}/getMe",
                              timeout=aiohttp.ClientTimeout(total=10))
            info = await r.json()
            if not info.get("ok"):
                raise HTTPException(status_code=400, detail="Неверный токен бота")
            username = info["result"]["username"]
            r2 = await s.post(
                f"https://api.telegram.org/bot{token}/setWebhook",
                json={"url": url, "secret_token": secret, "allowed_updates": ["message", "callback_query"]},
                timeout=aiohttp.ClientTimeout(total=10),
            )
            wh = await r2.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Ошибка Telegram API: {e}")
    await db.set_config_for_company("order_bot_token", token, cid)
    await db.set_config_for_company("order_bot_username", username, cid)
    await db.set_config_for_company("order_bot_webhook_secret", secret, cid)
    return {"ok": True, "username": username, "webhook_ok": bool(wh.get("ok"))}


@app.get("/api/admin/order-bot/status")
async def admin_order_bot_status(cid: int = Depends(_get_admin_cid)):
    username = await db.get_config_for_company("order_bot_username", cid)
    token = await db.get_config_for_company("order_bot_token", cid)
    return {"ok": True, "connected": bool(username), "username": username, "token": token or ""}


@app.post("/api/order-bot/webhook/{secret}")
async def order_bot_webhook(secret: str, request: Request):
    company_id = await db.get_company_id_by_order_bot_secret(secret)
    if not company_id:
        return {"ok": True}
    token = await db.get_config_for_company("order_bot_token", company_id)
    if not token:
        return {"ok": True}
    got_secret = request.headers.get("x-telegram-bot-api-secret-token", "")
    if not secrets.compare_digest(got_secret, secret):
        raise HTTPException(status_code=403, detail="Invalid secret token")
    db.set_request_company(company_id)
    body = await request.json()

    # Тогл "техработ" бота (см. site_maintenance_mode для сайта — тот же смысл, отдельный
    # ключ, т.к. сайт и бот компания может включать/выключать независимо). У новой компании
    # включён по умолчанию (_provision_company), пока не заполнены реальные настройки —
    # без этого клиент получал бы полноценный, но недонастроенный бот. Ответ шлём напрямую
    # через Telegram API, МИНУЯ диспетчер целиком — не хотим, чтобы хоть один хендлер
    # (FSM, БД-запросы) вообще выполнился, пока бот в этом режиме.
    maintenance = await db.get_config_for_company("bot_maintenance_mode", company_id)
    if maintenance == "true":
        chat_id = ((body.get("message") or {}).get("chat") or {}).get("id") \
            or (((body.get("callback_query") or {}).get("message") or {}).get("chat") or {}).get("id")
        if chat_id:
            try:
                async with aiohttp.ClientSession() as s:
                    await s.post(f"https://api.telegram.org/bot{token}/sendMessage", json={
                        "chat_id": chat_id,
                        "text": "🚧 Ведутся технические работы. Пожалуйста, попробуйте позже.\n\n"
                                "🚧 Texnik ishlar olib borilmoqda. Iltimos, keyinroq urinib ko'ring.",
                    }, timeout=aiohttp.ClientTimeout(total=8))
            except Exception as e:
                logging.warning(f"order_bot maintenance notice error: {e}")
        return {"ok": True}

    bot = _get_order_bot_instance(token)
    dp = _get_order_bot_dispatcher()
    result = await dp.feed_webhook_update(bot, body, company_id=company_id)
    return result.model_dump(exclude_none=True) if result else {"ok": True}


@app.get("/api/saas/phone-verify-status")
async def cleano_phone_verify_status(phone: str):
    phone = normalize_phone(phone)
    v = await db.get_cleano_phone_verification(phone)
    return {"ok": True, "verified": v is not None, "method": v["method"] if v else None}


@app.get("/api/saas/company-by-verified-phone")
async def company_by_verified_phone(phone: str):
    """Slug компании по номеру — ТОЛЬКО если он недавно (<30 мин) подтверждён через
    @cleanouz_bot этим же номером. Без этого условия был бы публичный неавторизованный
    оракул "номер → чья компания" (перебор чужих номеров). Используется на
    cleano-landing.html, когда телефон из ссылки бота уже занят другой компанией —
    чтобы предложить сразу перейти в её admin.html, а не молча отказать."""
    normalized = normalize_phone(phone)
    verification = await db.get_cleano_phone_verification(normalized)
    if not verification:
        return {"ok": True, "slug": None}
    company = await db.get_company_by_phone(phone)
    if not company:
        return {"ok": True, "slug": None}
    return {"ok": True, "slug": company["slug"], "name": company["name"]}


# ══════════════════════════════════════
#  ДЕМО-ВИДЕО НА ЛЕНДИНГЕ CLEANO.UZ (отдельно RU/UZ)
#  Хранится в Telegram-канале (тот же MEDIA_CHANNEL_ID, что и остальные медиа) —
#  в config сохраняется только file_id. Раздача публичная — файл ПРОКСИРУЕТСЯ
#  через бэкенд (не редиректом на api.telegram.org), чтобы BOT_TOKEN не утекал клиенту.
# ══════════════════════════════════════
MAX_CLEANO_VIDEO_BYTES = 19_000_000  # запас под лимит Telegram getFile (20 МБ)

@app.post("/api/saas/cleano-demo-video/{lang}")
async def sa_upload_cleano_demo_video(lang: str, file: UploadFile = File(...), _=Depends(get_superadmin)):
    if lang not in ("ru", "uz"):
        raise HTTPException(status_code=400, detail="lang должен быть ru или uz")
    if not (file.content_type or "").startswith("video/"):
        raise HTTPException(status_code=400, detail="Файл должен быть видео")
    media_ch = await _get_media_channel()
    if not BOT_TOKEN or not media_ch:
        raise HTTPException(status_code=503, detail="Медиа-хранилище не настроено")
    file_bytes = await file.read()
    if len(file_bytes) > MAX_CLEANO_VIDEO_BYTES:
        raise HTTPException(status_code=400, detail=f"Видео слишком большое (макс. {MAX_CLEANO_VIDEO_BYTES//1_000_000} МБ)")
    form = aiohttp.FormData()
    form.add_field("chat_id", str(media_ch))
    form.add_field("video", file_bytes, filename=file.filename or f"cleano-demo-{lang}.mp4", content_type=file.content_type)
    form.add_field("caption", f"Cleano demo video ({lang})")
    async with aiohttp.ClientSession() as s:
        async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendVideo", data=form,
                           timeout=aiohttp.ClientTimeout(total=60)) as r:
            result = await r.json()
    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=f"Telegram: {result.get('description','upload failed')}")
    file_id = result["result"]["video"]["file_id"]
    await db.set_config(f"cleano_demo_video_{lang}", file_id)
    return {"ok": True}


@app.get("/api/saas/cleano-demo-video-status")
async def sa_cleano_demo_video_status(_=Depends(get_superadmin)):
    ru = await db.get_config("cleano_demo_video_ru")
    uz = await db.get_config("cleano_demo_video_uz")
    return {"ok": True, "ru": bool(ru), "uz": bool(uz)}


def _video_range_response(content: bytes, request: Request, media_type: str = "video/mp4"):
    """Отдаёт видео с поддержкой HTTP Range. Без этого iOS Safari (и любой браузер
    на iOS, включая Chrome/Telegram-webview — там тот же WebKit) не проигрывает
    видео: он всегда шлёт Range-запрос в самом начале и ждёт 206, а получив 200 —
    рвёт воспроизведение через 1-2 секунды. Desktop-браузеры Range не требуют,
    поэтому баг был незаметен там."""
    file_size = len(content)
    range_header = request.headers.get("range")
    if range_header:
        try:
            range_value = range_header.strip().lower().replace("bytes=", "")
            start_str, _, end_str = range_value.partition("-")
            start = int(start_str) if start_str else 0
            end = int(end_str) if end_str else file_size - 1
            end = min(end, file_size - 1)
        except ValueError:
            start, end = 0, file_size - 1
        chunk = content[start:end + 1]
        return Response(content=chunk, status_code=206, media_type=media_type, headers={
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(len(chunk)),
            "Cache-Control": "public, max-age=3600",
        })
    return Response(content=content, media_type=media_type, headers={
        "Accept-Ranges": "bytes",
        "Content-Length": str(file_size),
        "Cache-Control": "public, max-age=3600",
        "Content-Disposition": "inline",
    })


@app.get("/api/cleano/demo-video/{lang}")
async def public_cleano_demo_video(lang: str, request: Request):
    """Публичная раздача демо-видео для лендинга cleano.uz — без авторизации."""
    if lang not in ("ru", "uz"):
        raise HTTPException(status_code=404)
    file_id = await db.get_config(f"cleano_demo_video_{lang}")
    if not file_id or not BOT_TOKEN:
        raise HTTPException(status_code=404, detail="Видео не загружено")
    async with aiohttp.ClientSession() as s:
        async with s.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile",
                          params={"file_id": file_id}, timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
        if not data.get("ok"):
            raise HTTPException(status_code=502, detail="Файл не найден в Telegram")
        file_path = data["result"]["file_path"]
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
        async with s.get(file_url, timeout=aiohttp.ClientTimeout(total=60)) as fr:
            content = await fr.read()
    return _video_range_response(content, request)


# ── Слайдер фото (лендинг cleano.uz) — показывается вместо видео-карточки после
# закрытия крестиком. От 1 до 5 фото, тот же паттерн хранения, что у демо-видео
# (Telegram как бесплатное файловое хранилище, file_id в config). ──
CLEANO_LANDING_SLIDE_MAX = 5

@app.post("/api/saas/cleano-landing-slides/{index}")
async def sa_upload_cleano_landing_slide(index: int, file: UploadFile = File(...), _=Depends(get_superadmin)):
    if not (1 <= index <= CLEANO_LANDING_SLIDE_MAX):
        raise HTTPException(status_code=400, detail=f"index должен быть от 1 до {CLEANO_LANDING_SLIDE_MAX}")
    if not (file.content_type or "").startswith("image/"):
        raise HTTPException(status_code=400, detail="Файл должен быть изображением")
    media_ch = await _get_media_channel()
    if not BOT_TOKEN or not media_ch:
        raise HTTPException(status_code=503, detail="Медиа-хранилище не настроено")
    file_bytes = await file.read()
    form = aiohttp.FormData()
    form.add_field("chat_id", str(media_ch))
    form.add_field("photo", file_bytes, filename=file.filename or f"cleano-slide-{index}.jpg", content_type=file.content_type)
    form.add_field("caption", f"Cleano landing slide {index}")
    async with aiohttp.ClientSession() as s:
        async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto", data=form,
                           timeout=aiohttp.ClientTimeout(total=30)) as r:
            result = await r.json()
    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=f"Telegram: {result.get('description','upload failed')}")
    # sendPhoto возвращает массив уменьшенных копий — берём последнюю (самое большое разрешение)
    file_id = result["result"]["photo"][-1]["file_id"]
    await db.set_config(f"cleano_landing_slide_{index}", file_id)
    return {"ok": True}


@app.delete("/api/saas/cleano-landing-slides/{index}")
async def sa_delete_cleano_landing_slide(index: int, _=Depends(get_superadmin)):
    if not (1 <= index <= CLEANO_LANDING_SLIDE_MAX):
        raise HTTPException(status_code=400, detail=f"index должен быть от 1 до {CLEANO_LANDING_SLIDE_MAX}")
    await db.set_config(f"cleano_landing_slide_{index}", "")
    return {"ok": True}


@app.get("/api/saas/cleano-landing-slides-status")
async def sa_cleano_landing_slides_status(_=Depends(get_superadmin)):
    result = {}
    for i in range(1, CLEANO_LANDING_SLIDE_MAX + 1):
        result[str(i)] = bool(await db.get_config(f"cleano_landing_slide_{i}"))
    return {"ok": True, "slides": result}


@app.get("/api/cleano/landing-slides-list")
async def public_cleano_landing_slides_list():
    """Публичный список индексов слайдов, у которых реально загружено фото — без авторизации."""
    indices = []
    for i in range(1, CLEANO_LANDING_SLIDE_MAX + 1):
        if await db.get_config(f"cleano_landing_slide_{i}"):
            indices.append(i)
    return {"ok": True, "indices": indices}


@app.get("/api/cleano/landing-slide/{index}")
async def public_cleano_landing_slide(index: int):
    """Публичная раздача фото слайдера лендинга cleano.uz — без авторизации."""
    from fastapi.responses import StreamingResponse
    if not (1 <= index <= CLEANO_LANDING_SLIDE_MAX):
        raise HTTPException(status_code=404)
    file_id = await db.get_config(f"cleano_landing_slide_{index}")
    if not file_id or not BOT_TOKEN:
        raise HTTPException(status_code=404, detail="Фото не загружено")
    async with aiohttp.ClientSession() as s:
        async with s.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile",
                          params={"file_id": file_id}, timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
        if not data.get("ok"):
            raise HTTPException(status_code=502, detail="Файл не найден в Telegram")
        file_path = data["result"]["file_path"]
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
        async with s.get(file_url, timeout=aiohttp.ClientTimeout(total=30)) as fr:
            content = await fr.read()
    return StreamingResponse(iter([content]), media_type="image/jpeg",
                              headers={"Cache-Control": "public, max-age=3600", "Content-Disposition": "inline"})


# ── SAAS PLANS ──────────────────────────────────────────────────────────────

@app.get("/api/saas/plans")
async def saas_list_plans(_=Depends(get_superadmin)):
    plans = await db.get_saas_plans()
    return {"ok": True, "plans": plans}

@app.put("/api/saas/plans/{plan_id}")
async def saas_update_plan(plan_id: int, body: dict = Body(...), _=Depends(get_superadmin)):
    allowed = {"display_name", "max_branches", "max_staff", "base_price", "active", "sort_order"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        raise HTTPException(400, "Нет данных для обновления")
    await db.update_saas_plan(plan_id, updates)
    return {"ok": True}

@app.put("/api/saas/plans/{plan_id}/pricing/{month}")
async def saas_update_plan_pricing(plan_id: int, month: int, body: dict = Body(...), _=Depends(get_superadmin)):
    price = body.get("price")
    if price is None or int(price) < 0:
        raise HTTPException(400, "Укажите price")
    await db.update_saas_plan_pricing(plan_id, month, int(price))
    return {"ok": True}


# ── SAAS SUBSCRIPTIONS ───────────────────────────────────────────────────────

@app.get("/api/saas/companies/{company_id}/subscription")
async def saas_get_subscription(company_id: int, _=Depends(get_superadmin)):
    sub = await db.get_saas_subscription(company_id)
    return {"ok": True, "subscription": dict(sub) if sub else None}

@app.get("/api/saas/companies/{company_id}/subscriptions")
async def saas_get_subscriptions_all(company_id: int, _=Depends(get_superadmin)):
    subs = await db.get_saas_subscriptions_all(company_id)
    return {"ok": True, "subscriptions": [dict(s) for s in subs]}

@app.post("/api/saas/companies/{company_id}/subscription")
async def saas_create_subscription(company_id: int, body: dict = Body(...), _=Depends(get_superadmin)):
    if not await db.get_company(company_id):
        raise HTTPException(404, "Компания не найдена")
    plan_id    = body.get("plan_id")
    start_date = body.get("start_date")
    end_date   = body.get("end_date")
    notes      = body.get("notes")
    status     = body.get("status", "active")
    if not plan_id or not start_date or not end_date:
        raise HTTPException(400, "Укажите plan_id, start_date, end_date")
    sub = await db.create_saas_subscription(company_id, plan_id, start_date, end_date, notes, status)
    return {"ok": True, "subscription": dict(sub)}

@app.put("/api/saas/companies/{company_id}/subscription/{sub_id}")
async def saas_update_subscription(company_id: int, sub_id: int, body: dict = Body(...), _=Depends(get_superadmin)):
    allowed = {"status", "end_date", "balance", "notes"}
    updates = {k: v for k, v in body.items() if k in allowed}
    await db.update_saas_subscription(sub_id, updates)
    return {"ok": True}


# ── SAAS PAYMENTS ────────────────────────────────────────────────────────────

@app.get("/api/saas/companies/{company_id}/payments")
async def saas_get_payments(company_id: int, _=Depends(get_superadmin)):
    payments = await db.get_saas_payments(company_id)
    return {"ok": True, "payments": [dict(p) for p in payments]}

@app.post("/api/saas/companies/{company_id}/payments")
async def saas_add_payment(company_id: int, body: dict = Body(...), _=Depends(get_superadmin)):
    amount          = body.get("amount")
    payment_date    = body.get("payment_date")
    note            = body.get("note")
    subscription_id = body.get("subscription_id")
    if not amount or int(amount) <= 0:
        raise HTTPException(400, "Укажите сумму")
    payment = await db.add_saas_payment(company_id, subscription_id, int(amount), payment_date or None, note)
    return {"ok": True, "payment": dict(payment)}


# ── SITE TEMPLATES & PALETTES (каталог дизайна сайта) ───────────────────────

@app.get("/api/saas/site-templates")
async def sa_get_site_templates(_=Depends(get_superadmin)):
    rows = await db.get_site_templates()
    return {"ok": True, "templates": rows}

@app.post("/api/saas/site-templates")
async def sa_create_site_template(body: dict = Body(...), _=Depends(get_superadmin)):
    key = (body.get("key") or "").strip()
    name_ru = (body.get("name_ru") or "").strip()
    name_uz = (body.get("name_uz") or "").strip()
    if not key or not name_ru or not name_uz:
        raise HTTPException(400, "Укажите key, name_ru, name_uz")
    row = await db.create_site_template(key, name_ru, name_uz, body.get("preview_url"), body.get("sort_order", 0))
    return {"ok": True, "template": row}

@app.patch("/api/saas/site-templates/{template_id}")
async def sa_update_site_template(template_id: int, body: dict = Body(...), _=Depends(get_superadmin)):
    await db.update_site_template(template_id, body)
    return {"ok": True}

@app.delete("/api/saas/site-templates/{template_id}")
async def sa_delete_site_template(template_id: int, _=Depends(get_superadmin)):
    await db.delete_site_template(template_id)
    return {"ok": True}

@app.get("/api/saas/site-palettes")
async def sa_get_site_palettes(_=Depends(get_superadmin)):
    rows = await db.get_site_palettes()
    return {"ok": True, "palettes": rows}

@app.post("/api/saas/site-palettes")
async def sa_create_site_palette(body: dict = Body(...), _=Depends(get_superadmin)):
    key = (body.get("key") or "").strip()
    name_ru = (body.get("name_ru") or "").strip()
    name_uz = (body.get("name_uz") or "").strip()
    colors = body.get("colors")
    if not key or not name_ru or not name_uz or not isinstance(colors, dict):
        raise HTTPException(400, "Укажите key, name_ru, name_uz, colors")
    row = await db.create_site_palette(key, name_ru, name_uz, colors, body.get("sort_order", 0))
    return {"ok": True, "palette": row}

@app.patch("/api/saas/site-palettes/{palette_id}")
async def sa_update_site_palette(palette_id: int, body: dict = Body(...), _=Depends(get_superadmin)):
    await db.update_site_palette(palette_id, body)
    return {"ok": True}

@app.delete("/api/saas/site-palettes/{palette_id}")
async def sa_delete_site_palette(palette_id: int, _=Depends(get_superadmin)):
    await db.delete_site_palette(palette_id)
    return {"ok": True}


# ── TEMPLATE DEPARTMENTS & POSITIONS ─────────────────────────────────────────
@app.get("/api/saas/template/departments")
async def sa_get_template_depts(_=Depends(get_superadmin)):
    await db.seed_departments_positions(db.TEMPLATE_CID)
    rows = await db.get_template_departments()
    return {"departments": [dict(r) for r in rows]}

@app.post("/api/saas/template/departments")
async def sa_create_template_dept(body: dict, _=Depends(get_superadmin)):
    row = await db.create_template_department(body["name"], body.get("description"), body.get("name_uz", ""), body.get("description_uz", ""))
    return {"department": dict(row)}

@app.patch("/api/saas/template/departments/{dept_id}")
async def sa_update_template_dept(dept_id: int, body: dict, _=Depends(get_superadmin)):
    row = await db.update_template_department(dept_id, **body)
    if not row: raise HTTPException(404)
    return {"department": dict(row)}

@app.delete("/api/saas/template/departments/{dept_id}")
async def sa_delete_template_dept(dept_id: int, _=Depends(get_superadmin)):
    await db.delete_template_department(dept_id)
    return {"ok": True}

@app.get("/api/saas/template/positions")
async def sa_get_template_pos(_=Depends(get_superadmin)):
    rows = await db.get_template_positions()
    return {"positions": [dict(r) for r in rows]}

@app.post("/api/saas/template/positions")
async def sa_create_template_pos(body: dict, _=Depends(get_superadmin)):
    row = await db.create_template_position(body)
    return {"position": dict(row)}

@app.patch("/api/saas/template/positions/{pos_id}")
async def sa_update_template_pos(pos_id: int, body: dict, _=Depends(get_superadmin)):
    row = await db.update_template_position(pos_id, **body)
    if not row: raise HTTPException(404)
    return {"position": dict(row)}

@app.delete("/api/saas/template/positions/{pos_id}")
async def sa_delete_template_pos(pos_id: int, _=Depends(get_superadmin)):
    await db.delete_template_position(pos_id)
    return {"ok": True}

@app.post("/api/saas/template/import/{company_id}")
async def sa_import_template(company_id: int, _=Depends(get_superadmin)):
    await db.import_template_from_company(company_id)
    return {"ok": True}

@app.post("/api/saas/companies/{company_id}/import-departments")
async def sa_import_depts_to_company(company_id: int, _=Depends(get_superadmin)):
    await db.import_departments_from_template(company_id)
    return {"ok": True}

@app.post("/api/saas/companies/{company_id}/import-positions")
async def sa_import_pos_to_company(company_id: int, _=Depends(get_superadmin)):
    await db.import_positions_from_template(company_id)
    return {"ok": True}

@app.post("/api/saas/companies/{company_id}/import-prices")
async def sa_import_prices_to_company(company_id: int, _=Depends(get_superadmin)):
    await db.seed_company_prices(company_id, force=True)
    return {"ok": True}

# ── Superadmin: catalog services & prices ────────────────────────────────

@app.get("/api/saas/catalog/services")
async def sa_catalog_services(_=Depends(get_superadmin)):
    svcs = await db.get_catalog_services()
    return {"ok": True, "services": svcs}

@app.put("/api/saas/catalog/services")
async def sa_catalog_upsert_service(req: ServiceRequest, _=Depends(get_superadmin)):
    if not req.key.strip():
        raise HTTPException(status_code=400, detail="Укажите ключ услуги")
    if not req.name_ru.strip():
        raise HTTPException(status_code=400, detail="Укажите название на RU")
    if not req.name_uz.strip():
        raise HTTPException(status_code=400, detail="Укажите название на UZ")
    await db.upsert_service(req.key.strip(), req.name_ru.strip(), req.name_uz.strip(),
                            req.emoji.strip(), req.order_idx, company_id=0)
    return {"ok": True}

@app.delete("/api/saas/catalog/services/{key}")
async def sa_catalog_delete_service(key: str, _=Depends(get_superadmin)):
    ok = await db.delete_service(key, company_id=0)
    if not ok:
        raise HTTPException(status_code=404, detail="Услуга не найдена")
    return {"ok": True}

@app.post("/api/saas/companies/{company_id}/import-services")
async def sa_import_services_to_company(company_id: int, _=Depends(get_superadmin)):
    await db.seed_company_services(company_id, force=True)
    return {"ok": True}

@app.post("/api/saas/catalog/seed-from-company")
async def sa_seed_catalog_from_company(_=Depends(get_superadmin)):
    """Reseed company_id=0 template from company_id=1 (force)."""
    if not db.pool:
        raise HTTPException(status_code=503, detail="DB unavailable")
    async with db.pool.acquire() as conn:
        await conn.execute("DELETE FROM services WHERE company_id=0")
        await conn.execute("""
            INSERT INTO services (company_id, key, name_ru, name_uz, emoji, order_idx)
            SELECT 0, key, name_ru, name_uz, emoji, order_idx
            FROM services WHERE company_id=1
        """)
        await conn.execute("DELETE FROM prices WHERE company_id=0")
        await conn.execute("""
            INSERT INTO prices (company_id, service_key, type_key, price, unit_key, min_order, updated_at)
            SELECT 0, service_key, type_key, price, unit_key, min_order, NOW()
            FROM prices WHERE company_id=1
        """)
    return {"ok": True, "message": "Template reseeded from company_id=1"}

@app.post("/api/saas/catalog/expense-categories/seed-from-company")
async def sa_seed_expense_cats_from_company(_=Depends(get_superadmin)):
    await db.resync_expense_category_template_from_company1()
    return {"ok": True, "message": "Expense categories template reseeded from company_id=1"}

@app.get("/api/saas/catalog/prices")
async def sa_catalog_prices(_=Depends(get_superadmin)):
    prices = await db.get_catalog_prices()
    return {"ok": True, "prices": prices}

@app.put("/api/saas/catalog/prices")
async def sa_catalog_set_price(req: SetPriceRequest, _=Depends(get_superadmin)):
    if not req.service_key.strip() or not req.type_key.strip():
        raise HTTPException(status_code=400, detail="Укажите ключи услуги и типа")
    if req.price <= 0:
        raise HTTPException(status_code=400, detail="Цена должна быть > 0")
    await db.set_price(req.service_key, req.type_key, req.price,
                       unit_key=req.unit_key, min_order=req.min_order,
                       min_order_total=req.min_order_total, company_id=0)
    return {"ok": True}

@app.get("/api/saas/catalog/units")
async def sa_catalog_units(_=Depends(get_superadmin)):
    units = await db.get_all_units()
    return {"ok": True, "units": [dict(u) for u in units]}

@app.put("/api/saas/catalog/units")
async def sa_catalog_upsert_unit(req: UnitRequest, _=Depends(get_superadmin)):
    if not req.key.strip():
        raise HTTPException(status_code=400, detail="Укажите ключ")
    await db.add_unit(req.key.strip(), req.name_ru.strip(), req.name_uz.strip(),
                      req.symbol_ru.strip(), req.symbol_uz.strip())
    return {"ok": True}

@app.delete("/api/saas/catalog/units/{key}")
async def sa_catalog_delete_unit(key: str, _=Depends(get_superadmin)):
    ok = await db.delete_unit(key)
    if not ok:
        raise HTTPException(status_code=404, detail="Единица не найдена")
    return {"ok": True}


# ── Superadmin: expense categories catalog ────────────────────────────

class ExpCatRequest(BaseModel):
    name_ru: str
    name_uz: str = ""
    icon: str = "🧾"
    parent_id: int | None = None
    approve_level: str = "manager"
    receipt_required: bool = False
    amount_threshold: float | None = None
    sort_order: int = 0
    for_staff: bool = False
    active: bool = True

@app.get("/api/saas/catalog/expense-categories")
async def sa_catalog_expense_cats(_=Depends(get_superadmin)):
    cats = await db.get_expense_categories_for_company(0)
    return {"ok": True, "categories": cats}

@app.post("/api/saas/catalog/expense-categories")
async def sa_catalog_create_expense_cat(req: ExpCatRequest, _=Depends(get_superadmin)):
    row = await db.create_expense_category_for_company(
        0, req.name_ru.strip(), req.name_uz.strip(), req.icon.strip(),
        req.parent_id, req.approve_level, req.receipt_required,
        req.amount_threshold, req.sort_order, req.for_staff)
    return {"ok": True, "category": row}

@app.put("/api/saas/catalog/expense-categories/{cat_id}")
async def sa_catalog_update_expense_cat(cat_id: int, req: ExpCatRequest, _=Depends(get_superadmin)):
    row = await db.update_expense_category_for_company(
        0, cat_id, req.name_ru.strip(), req.name_uz.strip(), req.icon.strip(),
        req.parent_id, req.approve_level, req.receipt_required,
        req.amount_threshold, req.sort_order, req.active, req.for_staff)
    if not row:
        raise HTTPException(404, "Категория не найдена")
    return {"ok": True, "category": row}

@app.delete("/api/saas/catalog/expense-categories/{cat_id}")
async def sa_catalog_delete_expense_cat(cat_id: int, _=Depends(get_superadmin)):
    res = await db.delete_expense_category_for_company(0, cat_id)
    if not res.get("ok"):
        raise HTTPException(400, res.get("error", "Ошибка удаления"))
    return {"ok": True}

@app.post("/api/saas/companies/{company_id}/import-expense-categories")
async def sa_import_expense_cats(company_id: int, _=Depends(get_superadmin)):
    if company_id <= 0:
        raise HTTPException(400, "Нельзя применить к шаблону")
    await db.seed_company_expense_categories(company_id, force=True)
    return {"ok": True}

# ── Superadmin: chat templates catalog ────────────────────────────────

class ChatTplRequest(BaseModel):
    id: int | None = None
    key: str = "quick"
    lang: str = "ru"
    text: str
    sort_order: int = 0
    active: bool = True

@app.get("/api/saas/catalog/chat-templates")
async def sa_catalog_chat_tpls(_=Depends(get_superadmin)):
    rows = await db.get_chat_templates_for_company(0)
    return {"ok": True, "templates": rows}

@app.post("/api/saas/catalog/chat-templates")
async def sa_catalog_upsert_chat_tpl(req: ChatTplRequest, _=Depends(get_superadmin)):
    row = await db.upsert_chat_template_for_company(0, req.model_dump())
    return {"ok": True, "template": row}

@app.delete("/api/saas/catalog/chat-templates/{tid}")
async def sa_catalog_delete_chat_tpl(tid: int, _=Depends(get_superadmin)):
    await db.delete_chat_template_for_company(0, tid)
    return {"ok": True}

@app.post("/api/saas/catalog/chat-templates/seed")
async def sa_catalog_seed_chat_tpls(_=Depends(get_superadmin)):
    await db.seed_chat_templates_for_company0()
    return {"ok": True}

@app.post("/api/saas/companies/{company_id}/import-chat-templates")
async def sa_import_chat_tpls(company_id: int, _=Depends(get_superadmin)):
    if company_id <= 0:
        raise HTTPException(400, "Нельзя применить к шаблону")
    await db.seed_company_chat_templates(company_id, force=True)
    return {"ok": True}


MAX_SLIDE_IMAGE_BYTES = 1_500_000
MAX_SLIDES_PER_COMPANY = 5

class SiteSlideText(BaseModel):
    eyebrow_ru: str = ""
    eyebrow_uz: str = ""
    title_ru: str = ""
    title_uz: str = ""
    text_ru: str = ""
    text_uz: str = ""
    sort_order: int = 0

class SiteStatIn(BaseModel):
    value_ru: str = ""
    value_uz: str = ""
    label_ru: str = ""
    label_uz: str = ""
    sort_order: int = 0

class SiteReviewIn(BaseModel):
    author_name: str = ""
    rating: int = 5
    text_ru: str = ""
    text_uz: str = ""
    city_ru: str = ""
    city_uz: str = ""
    sort_order: int = 0

class SiteFaqIn(BaseModel):
    question_ru: str = ""
    question_uz: str = ""
    answer_ru: str = ""
    answer_uz: str = ""
    sort_order: int = 0

# ── Superadmin: шаблоны слайдера/статистики/отзывов/FAQ (company_id=0) ──────

@app.get("/api/saas/catalog/site-slides")
async def sa_catalog_site_slides(_=Depends(get_superadmin)):
    return {"ok": True, "slides": await db.get_site_slides(0)}

@app.post("/api/saas/catalog/site-slides")
async def sa_catalog_create_site_slide(
    file: UploadFile = File(...),
    eyebrow_ru: str = Form(""), eyebrow_uz: str = Form(""),
    title_ru: str = Form(""), title_uz: str = Form(""),
    text_ru: str = Form(""), text_uz: str = Form(""),
    sort_order: int = Form(0),
    _=Depends(get_superadmin),
):
    if not (file.content_type or "").startswith("image/"):
        raise HTTPException(status_code=400, detail="Файл должен быть изображением")
    data = await file.read()
    if len(data) > MAX_SLIDE_IMAGE_BYTES:
        raise HTTPException(status_code=400, detail=f"Изображение слишком большое (макс. {MAX_SLIDE_IMAGE_BYTES//1000} КБ)")
    image_url = f"data:{file.content_type};base64,{base64.b64encode(data).decode('ascii')}"
    slide = await db.create_site_slide(0, image_url, eyebrow_ru, eyebrow_uz, title_ru, title_uz, text_ru, text_uz, sort_order)
    return {"ok": True, "slide": slide}

@app.put("/api/saas/catalog/site-slides/{slide_id}")
async def sa_catalog_update_site_slide(slide_id: int, data: SiteSlideText, _=Depends(get_superadmin)):
    slide = await db.update_site_slide(slide_id, 0, data.model_dump())
    if not slide:
        raise HTTPException(status_code=404, detail="Слайд не найден")
    return {"ok": True, "slide": slide}

@app.post("/api/saas/catalog/site-slides/{slide_id}/image")
async def sa_catalog_update_site_slide_image(slide_id: int, file: UploadFile = File(...), _=Depends(get_superadmin)):
    if not (file.content_type or "").startswith("image/"):
        raise HTTPException(status_code=400, detail="Файл должен быть изображением")
    data = await file.read()
    if len(data) > MAX_SLIDE_IMAGE_BYTES:
        raise HTTPException(status_code=400, detail=f"Изображение слишком большое (макс. {MAX_SLIDE_IMAGE_BYTES//1000} КБ)")
    image_url = f"data:{file.content_type};base64,{base64.b64encode(data).decode('ascii')}"
    slide = await db.update_site_slide_image(slide_id, 0, image_url)
    if not slide:
        raise HTTPException(status_code=404, detail="Слайд не найден")
    return {"ok": True, "slide": slide}

@app.delete("/api/saas/catalog/site-slides/{slide_id}")
async def sa_catalog_delete_site_slide(slide_id: int, _=Depends(get_superadmin)):
    ok = await db.delete_site_slide(slide_id, 0)
    if not ok:
        raise HTTPException(status_code=404, detail="Слайд не найден")
    return {"ok": True}

@app.post("/api/saas/companies/{company_id}/import-site-slides")
async def sa_import_site_slides(company_id: int, _=Depends(get_superadmin)):
    if company_id <= 0:
        raise HTTPException(400, "Нельзя применить к шаблону")
    await db.seed_company_site_slides(company_id, force=True)
    return {"ok": True}


@app.get("/api/saas/catalog/site-reviews")
async def sa_catalog_site_reviews(_=Depends(get_superadmin)):
    return {"ok": True, "reviews": await db.get_site_reviews(0)}

@app.post("/api/saas/catalog/site-reviews")
async def sa_catalog_create_site_review(data: SiteReviewIn, _=Depends(get_superadmin)):
    review = await db.create_site_review(0, data.model_dump())
    return {"ok": True, "review": review}

@app.put("/api/saas/catalog/site-reviews/{review_id}")
async def sa_catalog_update_site_review(review_id: int, data: SiteReviewIn, _=Depends(get_superadmin)):
    review = await db.update_site_review(review_id, 0, data.model_dump())
    if not review:
        raise HTTPException(status_code=404, detail="Отзыв не найден")
    return {"ok": True, "review": review}

@app.delete("/api/saas/catalog/site-reviews/{review_id}")
async def sa_catalog_delete_site_review(review_id: int, _=Depends(get_superadmin)):
    ok = await db.delete_site_review(review_id, 0)
    if not ok:
        raise HTTPException(status_code=404, detail="Отзыв не найден")
    return {"ok": True}

@app.post("/api/saas/companies/{company_id}/import-site-reviews")
async def sa_import_site_reviews(company_id: int, _=Depends(get_superadmin)):
    if company_id <= 0:
        raise HTTPException(400, "Нельзя применить к шаблону")
    await db.seed_company_site_reviews(company_id, force=True)
    return {"ok": True}


@app.get("/api/saas/catalog/site-faq")
async def sa_catalog_site_faq(_=Depends(get_superadmin)):
    return {"ok": True, "faq": await db.get_site_faq(0)}

@app.post("/api/saas/catalog/site-faq")
async def sa_catalog_create_site_faq(data: SiteFaqIn, _=Depends(get_superadmin)):
    item = await db.create_site_faq_item(0, data.model_dump())
    return {"ok": True, "faq": item}

@app.put("/api/saas/catalog/site-faq/{faq_id}")
async def sa_catalog_update_site_faq(faq_id: int, data: SiteFaqIn, _=Depends(get_superadmin)):
    item = await db.update_site_faq_item(faq_id, 0, data.model_dump())
    if not item:
        raise HTTPException(status_code=404, detail="Вопрос не найден")
    return {"ok": True, "faq": item}

@app.delete("/api/saas/catalog/site-faq/{faq_id}")
async def sa_catalog_delete_site_faq(faq_id: int, _=Depends(get_superadmin)):
    ok = await db.delete_site_faq_item(faq_id, 0)
    if not ok:
        raise HTTPException(status_code=404, detail="Вопрос не найден")
    return {"ok": True}

@app.post("/api/saas/companies/{company_id}/import-site-faq")
async def sa_import_site_faq(company_id: int, _=Depends(get_superadmin)):
    if company_id <= 0:
        raise HTTPException(400, "Нельзя применить к шаблону")
    await db.seed_company_site_faq(company_id, force=True)
    return {"ok": True}


@app.get("/api/saas/catalog/site-stats")
async def sa_catalog_site_stats(_=Depends(get_superadmin)):
    return {"ok": True, "stats": await db.get_site_stats(0)}

@app.put("/api/saas/catalog/site-stats/{stat_id}")
async def sa_catalog_update_site_stat(stat_id: int, data: SiteStatIn, _=Depends(get_superadmin)):
    stat = await db.update_site_stat(stat_id, 0, data.model_dump())
    if not stat:
        raise HTTPException(status_code=404, detail="Не найдено")
    return {"ok": True, "stat": stat}


class TgTemplateRequest(BaseModel):
    status: str
    enabled: bool = True
    message_ru: str = ""
    message_uz: str = ""

@app.get("/api/saas/catalog/tg-messages")
async def sa_catalog_tg_messages(_=Depends(get_superadmin)):
    rows = await db.get_tg_template_messages()
    return {"ok": True, "messages": rows}

@app.put("/api/saas/catalog/tg-messages/{status}")
async def sa_catalog_upsert_tg_message(status: str, req: TgTemplateRequest, _=Depends(get_superadmin)):
    ALL_STATUSES = {"new","confirmed","pickup","received","washing","drying","packing","ready","delivery","delivered","cancelled"}
    if status not in ALL_STATUSES:
        raise HTTPException(status_code=400, detail="Неизвестный статус")
    row = await db.upsert_tg_template_message(status, req.enabled, req.message_ru, req.message_uz)
    return {"ok": True, "message": row}

@app.post("/api/saas/catalog/tg-messages/seed")
async def sa_catalog_seed_tg_messages(_=Depends(get_superadmin)):
    """Принудительно засеять шаблоны TG-уведомлений для company_id=0."""
    if not db.pool:
        raise HTTPException(503, "DB unavailable")
    # Убедиться что template company существует
    async with db.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO companies (id, name, slug, secret_key, plan, active)
            VALUES (0, 'Template', '__template__', '__template_secret__', 'starter', false)
            ON CONFLICT (id) DO NOTHING
        """)
        inserted = 0
        for status, enabled, msg_ru, msg_uz in db._TG_STATUS_DEFAULTS:
            r = await conn.execute("""
                INSERT INTO tg_status_messages (status, enabled, message_ru, message_uz, company_id)
                SELECT $1::varchar, $2, $3, $4, 0
                WHERE NOT EXISTS (
                    SELECT 1 FROM tg_status_messages WHERE status=$1::varchar AND company_id=0
                )
            """, status, enabled, msg_ru, msg_uz)
            if r == "INSERT 0 1":
                inserted += 1
    return {"ok": True, "inserted": inserted}

@app.post("/api/saas/companies/{company_id}/import-tg-messages")
async def sa_import_tg_messages(company_id: int, _=Depends(get_superadmin)):
    if company_id <= 0:
        raise HTTPException(status_code=400, detail="Нельзя применить к шаблону")
    await db.seed_company_tg_messages(company_id, force=True)
    return {"ok": True}


@app.get("/api/settings/site-design-catalog")
async def public_site_design_catalog():
    """Публичный каталог активных шаблонов/палитр — для превью в admin.html (iframe)."""
    templates = await db.get_site_templates(active_only=True)
    palettes = await db.get_site_palettes(active_only=True)
    return {"templates": templates, "palettes": palettes}


@app.get("/api/company/resolve")
async def company_resolve(slug: str):
    """Публичный эндпоинт: slug → {name} для формы входа."""
    company_id = await db.get_company_id_by_slug(slug.strip().lower())
    if not company_id:
        raise HTTPException(status_code=404, detail="Компания не найдена")
    company = await db.get_company(company_id)
    if not company or not company.get("active"):
        raise HTTPException(status_code=404, detail="Компания не найдена или отключена")
    design = await db.get_company_public_design(company_id)
    return {
        "name": company["name"], "slug": company["slug"], "logo_url": company.get("logo_url"),
        "site_template_key": (design or {}).get("site_template_key") or "template-01",
        "site_palette_key":  (design or {}).get("site_palette_key") or "teal-amber",
        "palette_colors":    (design or {}).get("colors") or None,
    }


@app.get("/api/company/info")
async def company_info(staff=Depends(get_current_staff)):
    company_id = staff.get("company_id") or 1
    company = await db.get_company(company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Компания не найдена")
    return {
        "ok": True, "id": company["id"], "name": company["name"], "slug": company["slug"],
        "logo_url":        company.get("logo_url"),
        "whatsapp":        company.get("whatsapp"),
        "instagram":       company.get("instagram"),
        "tg_group_link":   company.get("tg_group_link"),
        "tg_group_id":     company.get("tg_group_id"),
        "tg_channel_link": company.get("tg_channel_link"),
        "tg_channel_id":   company.get("tg_channel_id"),
        "tg_admin_link":   company.get("tg_admin_link"),
        "tg_admin_id":     company.get("tg_admin_id"),
    }


SLUG_RE = re.compile(r"^[a-z0-9-]{3,50}$")

class CompanyProfileRequest(BaseModel):
    name: str | None = None
    slug: str | None = None

@app.put("/api/company/profile")
async def company_update_profile(req: CompanyProfileRequest, staff=Depends(get_current_staff)):
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только admin")
    company_id = staff.get("company_id") or 1
    updates = {}
    if req.name is not None:
        name = req.name.strip()
        if not name:
            raise HTTPException(status_code=400, detail="Название не может быть пустым")
        updates["name"] = name
    if req.slug is not None:
        slug = req.slug.strip().lower()
        if not SLUG_RE.match(slug):
            raise HTTPException(status_code=400, detail="Slug: только латиница, цифры и дефис, от 3 до 50 символов")
        updates["slug"] = slug
    if not updates:
        return {"ok": True}
    ok = await db.update_company(company_id, updates)
    if not ok:
        raise HTTPException(status_code=400, detail="Этот slug уже занят другой компанией")
    return {"ok": True, **updates}


@app.get("/api/admin/company/site-design")
async def admin_get_site_design(staff=Depends(get_current_staff)):
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только admin")
    company_id = staff.get("company_id") or 1
    current = await db.get_company_site_design(company_id)
    templates = await db.get_site_templates(active_only=True)
    palettes = await db.get_site_palettes(active_only=True)
    return {
        "ok": True,
        "site_template_key": (current or {}).get("site_template_key") or "template-01",
        "site_palette_key":  (current or {}).get("site_palette_key") or "teal-amber",
        "templates": templates,
        "palettes": palettes,
    }


@app.put("/api/admin/company/site-design")
async def admin_update_site_design(body: dict = Body(...), staff=Depends(get_current_staff)):
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только admin")
    company_id = staff.get("company_id") or 1
    template_key = (body.get("site_template_key") or "").strip()
    palette_key  = (body.get("site_palette_key") or "").strip()
    if not template_key or not palette_key:
        raise HTTPException(400, "Укажите site_template_key и site_palette_key")
    try:
        await db.update_company_site_design(company_id, template_key, palette_key)
    except ValueError:
        raise HTTPException(400, "Неизвестный или отключённый шаблон/палитра")
    return {"ok": True}


MAX_LOGO_BYTES = 400_000

@app.post("/api/company/logo")
async def company_upload_logo(file: UploadFile = File(...), staff=Depends(get_current_staff)):
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только admin")
    if not (file.content_type or "").startswith("image/"):
        raise HTTPException(status_code=400, detail="Файл должен быть изображением")
    data = await file.read()
    if len(data) > MAX_LOGO_BYTES:
        raise HTTPException(status_code=400, detail=f"Логотип слишком большой (макс. {MAX_LOGO_BYTES//1000} КБ)")
    company_id = staff.get("company_id") or 1
    logo_url = f"data:{file.content_type};base64,{base64.b64encode(data).decode('ascii')}"
    await db.update_company(company_id, {"logo_url": logo_url})
    return {"ok": True, "logo_url": logo_url}


# ── Слайдер главной страницы (до 5 слайдов) ──────────────────────────────────

@app.get("/api/admin/site-slides")
async def admin_list_site_slides(cid: int = Depends(_get_admin_cid)):
    return {"ok": True, "slides": await db.get_site_slides(cid)}

@app.post("/api/admin/site-slides")
async def admin_create_site_slide(
    file: UploadFile = File(...),
    eyebrow_ru: str = Form(""), eyebrow_uz: str = Form(""),
    title_ru: str = Form(""), title_uz: str = Form(""),
    text_ru: str = Form(""), text_uz: str = Form(""),
    sort_order: int = Form(0),
    cid: int = Depends(_get_admin_cid),
):
    existing = await db.get_site_slides(cid)
    if len(existing) >= MAX_SLIDES_PER_COMPANY:
        raise HTTPException(status_code=400, detail=f"Достигнут лимит слайдов ({MAX_SLIDES_PER_COMPANY})")
    if not (file.content_type or "").startswith("image/"):
        raise HTTPException(status_code=400, detail="Файл должен быть изображением")
    data = await file.read()
    if len(data) > MAX_SLIDE_IMAGE_BYTES:
        raise HTTPException(status_code=400, detail=f"Изображение слишком большое (макс. {MAX_SLIDE_IMAGE_BYTES//1000} КБ)")
    image_url = f"data:{file.content_type};base64,{base64.b64encode(data).decode('ascii')}"
    slide = await db.create_site_slide(cid, image_url, eyebrow_ru, eyebrow_uz, title_ru, title_uz, text_ru, text_uz, sort_order)
    return {"ok": True, "slide": slide}

@app.put("/api/admin/site-slides/{slide_id}")
async def admin_update_site_slide(slide_id: int, data: SiteSlideText, cid: int = Depends(_get_admin_cid)):
    slide = await db.update_site_slide(slide_id, cid, data.model_dump())
    if not slide:
        raise HTTPException(status_code=404, detail="Слайд не найден")
    return {"ok": True, "slide": slide}

@app.post("/api/admin/site-slides/{slide_id}/image")
async def admin_update_site_slide_image(slide_id: int, file: UploadFile = File(...), cid: int = Depends(_get_admin_cid)):
    if not (file.content_type or "").startswith("image/"):
        raise HTTPException(status_code=400, detail="Файл должен быть изображением")
    data = await file.read()
    if len(data) > MAX_SLIDE_IMAGE_BYTES:
        raise HTTPException(status_code=400, detail=f"Изображение слишком большое (макс. {MAX_SLIDE_IMAGE_BYTES//1000} КБ)")
    image_url = f"data:{file.content_type};base64,{base64.b64encode(data).decode('ascii')}"
    slide = await db.update_site_slide_image(slide_id, cid, image_url)
    if not slide:
        raise HTTPException(status_code=404, detail="Слайд не найден")
    return {"ok": True, "slide": slide}

@app.delete("/api/admin/site-slides/{slide_id}")
async def admin_delete_site_slide(slide_id: int, cid: int = Depends(_get_admin_cid)):
    ok = await db.delete_site_slide(slide_id, cid)
    if not ok:
        raise HTTPException(status_code=404, detail="Слайд не найден")
    return {"ok": True}

@app.get("/api/site-slides")
async def get_site_slides_public(company_slug: str = None):
    """Публичный список слайдов для автослайдера на сайте."""
    cid = await _resolve_client_company_id(company_slug)
    return {"ok": True, "slides": await db.get_site_slides(cid)}


# ── Видео-карточка на главной странице сайта компании ────────────────────
# Своё видео каждой компании (не путать с демо-видео Cleano выше — то показывает
# ЛЕНДИНГ Cleano, это показывает САЙТ КОМПАНИИ-клиента). Отдельное видео на RU/UZ,
# тот же приём хранения: Telegram как бесплатное файловое хранилище, file_id
# в company-scoped config (по ключу на язык, как cleano_demo_video_{lang}).
MAX_SITE_VIDEO_BYTES = 19_000_000  # запас под лимит Telegram getFile (20 МБ)

@app.post("/api/admin/site-video/{lang}")
async def admin_upload_site_video(lang: str, file: UploadFile = File(...), _=Depends(get_admin)):
    if lang not in ("ru", "uz"):
        raise HTTPException(status_code=400, detail="lang должен быть ru или uz")
    if not (file.content_type or "").startswith("video/"):
        raise HTTPException(status_code=400, detail="Файл должен быть видео")
    media_ch = await _get_media_channel()
    if not BOT_TOKEN or not media_ch:
        raise HTTPException(status_code=503, detail="Медиа-хранилище не настроено")
    file_bytes = await file.read()
    if len(file_bytes) > MAX_SITE_VIDEO_BYTES:
        raise HTTPException(status_code=400, detail=f"Видео слишком большое (макс. {MAX_SITE_VIDEO_BYTES//1_000_000} МБ)")
    form = aiohttp.FormData()
    form.add_field("chat_id", str(media_ch))
    form.add_field("video", file_bytes, filename=file.filename or f"site-video-{lang}.mp4", content_type=file.content_type)
    form.add_field("caption", f"Site video card ({lang})")
    async with aiohttp.ClientSession() as s:
        async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendVideo", data=form,
                           timeout=aiohttp.ClientTimeout(total=60)) as r:
            result = await r.json()
    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=f"Telegram: {result.get('description','upload failed')}")
    file_id = result["result"]["video"]["file_id"]
    await db.set_config(f"site_video_file_id_{lang}", file_id)
    return {"ok": True}


@app.delete("/api/admin/site-video/{lang}")
async def admin_delete_site_video(lang: str, _=Depends(get_admin)):
    if lang not in ("ru", "uz"):
        raise HTTPException(status_code=400, detail="lang должен быть ru или uz")
    await db.set_config(f"site_video_file_id_{lang}", "")
    return {"ok": True}


@app.get("/api/admin/site-video-status")
async def admin_site_video_status(_=Depends(get_admin)):
    ru = await db.get_config("site_video_file_id_ru")
    uz = await db.get_config("site_video_file_id_uz")
    return {"ok": True, "ru": bool(ru), "uz": bool(uz)}


# ── Видео-инструкция при первом входе в Telegram-бот компании (отдельно от
# видео-карточки сайта выше). Хранится тем же приёмом — через ГЛОБАЛЬНЫЙ
# Cleano-бот (BOT_TOKEN) в media_ch, — но клиенту её шлёт КОНКРЕТНЫЙ бот
# компании (order_bot_token), а file_id одного бота не работает у другого.
# Поэтому раздача — не reuse file_id, а скачивание сырых байт глобальным ботом
# и пересылка через message.bot самой компании — см. fetch_bot_welcome_video_bytes,
# вызывается из order_bot_handlers.set_language.
@app.post("/api/admin/bot-welcome-video/{lang}")
async def admin_upload_bot_welcome_video(lang: str, file: UploadFile = File(...), _=Depends(get_admin)):
    if lang not in ("ru", "uz"):
        raise HTTPException(status_code=400, detail="lang должен быть ru или uz")
    if not (file.content_type or "").startswith("video/"):
        raise HTTPException(status_code=400, detail="Файл должен быть видео")
    media_ch = await _get_media_channel()
    if not BOT_TOKEN or not media_ch:
        raise HTTPException(status_code=503, detail="Медиа-хранилище не настроено")
    file_bytes = await file.read()
    if len(file_bytes) > MAX_SITE_VIDEO_BYTES:
        raise HTTPException(status_code=400, detail=f"Видео слишком большое (макс. {MAX_SITE_VIDEO_BYTES//1_000_000} МБ)")
    form = aiohttp.FormData()
    form.add_field("chat_id", str(media_ch))
    form.add_field("video", file_bytes, filename=file.filename or f"bot-welcome-{lang}.mp4", content_type=file.content_type)
    form.add_field("caption", f"Bot welcome video ({lang})")
    async with aiohttp.ClientSession() as s:
        async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendVideo", data=form,
                           timeout=aiohttp.ClientTimeout(total=60)) as r:
            result = await r.json()
    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=f"Telegram: {result.get('description','upload failed')}")
    file_id = result["result"]["video"]["file_id"]
    await db.set_config(f"bot_welcome_video_file_id_{lang}", file_id)
    return {"ok": True}


@app.delete("/api/admin/bot-welcome-video/{lang}")
async def admin_delete_bot_welcome_video(lang: str, _=Depends(get_admin)):
    if lang not in ("ru", "uz"):
        raise HTTPException(status_code=400, detail="lang должен быть ru или uz")
    await db.set_config(f"bot_welcome_video_file_id_{lang}", "")
    return {"ok": True}


@app.get("/api/admin/bot-welcome-video-status")
async def admin_bot_welcome_video_status(_=Depends(get_admin)):
    ru = await db.get_config("bot_welcome_video_file_id_ru")
    uz = await db.get_config("bot_welcome_video_file_id_uz")
    return {"ok": True, "ru": bool(ru), "uz": bool(uz)}


async def fetch_global_bot_file_bytes(file_id: str) -> bytes | None:
    """Скачивает сырые байты любого файла, загруженного через глобальный Cleano-бот
    (BOT_TOKEN) — фото/видео заказа, медиа замера, видео-инструкция. file_id этого
    бота не работает у бота конкретной компании (order_bot_token), поэтому раздача
    клиенту — не reuse file_id, а скачивание сюда и пересылка через message.bot
    самой компании. Общий хелпер для fetch_bot_welcome_video_bytes и
    order_bot_handlers.show_order_photos/show_order_measure_media."""
    if not BOT_TOKEN or not file_id:
        return None
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile",
                              params={"file_id": file_id}, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
            if not data.get("ok"):
                return None
            file_path = data["result"]["file_path"]
            file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
            async with s.get(file_url, timeout=aiohttp.ClientTimeout(total=60)) as fr:
                return await fr.read()
    except Exception as e:
        logging.warning(f"fetch_global_bot_file_bytes error: {e}")
        return None


async def fetch_bot_welcome_video_bytes(company_id: int, lang: str) -> bytes | None:
    """Скачивает сырые байты видео-инструкции компании через глобальный Cleano-бот
    (тем же file_id, которым его туда когда-то загрузили). Вызывается из
    order_bot_handlers.set_language — там видео пересылается через СВОЙ бот
    компании (message.bot), т.к. file_id одного бота не работает у другого."""
    file_id = await db.get_config_for_company(f"bot_welcome_video_file_id_{lang}", company_id)
    if not file_id:
        return None
    return await fetch_global_bot_file_bytes(file_id)


async def bot_register_client(phone: str, first_name: str, lang: str, company_id: int, tg_id: int) -> dict:
    """Регистрация сайтового аккаунта, инициированная ИЗ бота (кнопка «Зарегистрироваться»
    в главном меню — order_bot_handlers.RegisterForm). Отдельный путь от
    /api/register-via-tg (тот вызывается САЙТОМ после подтверждения телефона через бота,
    там пароль выбирает сам клиент). Здесь пароль генерируется сервером — клиент уже
    подтвердил владение номером в реальном времени, поделившись контактом в боте,
    поэтому дополнительный код подтверждения не нужен (тот же довод, что в проде)."""
    existing = await db.get_user_by_phone(phone, company_id)
    if existing and existing.get("is_verified"):
        return {"ok": False, "already_registered": True}

    import secrets, string
    password = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(10))
    password_hash = pwd_context.hash(password[:72])

    await db.create_user(phone, password_hash, first_name, company_id)
    await db.verify_user(phone, company_id)
    await db.link_user_tg_id(phone, tg_id, company_id)

    user = await db.get_user_by_phone(phone, company_id)
    if not user:
        return {"ok": False}
    await db.set_user_must_change_password(user["id"], True)
    asyncio.create_task(db.update_user_last_login(user["id"]))
    asyncio.create_task(db.upsert_crm_client(phone=phone, first_name=first_name, source="bot_register"))
    asyncio.create_task(_notify_new_site_user(first_name, phone, "tg"))

    return {"ok": True, "password": password, "user": dict(user)}


@app.get("/api/site-video/{lang}")
async def public_site_video(lang: str, request: Request, company_slug: str = None):
    """Публичная раздача видео-карточки сайта компании — без авторизации."""
    if lang not in ("ru", "uz"):
        raise HTTPException(status_code=404)
    cid = await _resolve_client_company_id(company_slug)
    db.set_request_company(cid)
    file_id = await db.get_config(f"site_video_file_id_{lang}")
    if not file_id or not BOT_TOKEN:
        raise HTTPException(status_code=404, detail="Видео не загружено")
    async with aiohttp.ClientSession() as s:
        async with s.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile",
                          params={"file_id": file_id}, timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
        if not data.get("ok"):
            raise HTTPException(status_code=502, detail="Файл не найден в Telegram")
        file_path = data["result"]["file_path"]
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
        async with s.get(file_url, timeout=aiohttp.ClientTimeout(total=60)) as fr:
            content = await fr.read()
    return _video_range_response(content, request)


# ── Статистика на главной странице (ровно 4 карточки — фиксированная сетка) ──
@app.get("/api/admin/site-stats")
async def admin_list_site_stats(cid: int = Depends(_get_admin_cid)):
    return {"ok": True, "stats": await db.get_site_stats(cid)}

@app.put("/api/admin/site-stats/{stat_id}")
async def admin_update_site_stat(stat_id: int, data: SiteStatIn, cid: int = Depends(_get_admin_cid)):
    stat = await db.update_site_stat(stat_id, cid, data.model_dump())
    if not stat:
        raise HTTPException(status_code=404, detail="Не найдено")
    return {"ok": True, "stat": stat}

@app.get("/api/site-stats")
async def get_site_stats_public(company_slug: str = None):
    """Публичная статистика для секции после автослайдера."""
    cid = await _resolve_client_company_id(company_slug)
    return {"ok": True, "stats": await db.get_site_stats(cid)}


# ── Отзывы на главной странице ───────────────────────────────────────────────
@app.get("/api/admin/site-reviews")
async def admin_list_site_reviews(cid: int = Depends(_get_admin_cid)):
    return {"ok": True, "reviews": await db.get_site_reviews(cid)}

@app.post("/api/admin/site-reviews")
async def admin_create_site_review(data: SiteReviewIn, cid: int = Depends(_get_admin_cid)):
    review = await db.create_site_review(cid, data.model_dump())
    return {"ok": True, "review": review}

@app.put("/api/admin/site-reviews/{review_id}")
async def admin_update_site_review(review_id: int, data: SiteReviewIn, cid: int = Depends(_get_admin_cid)):
    review = await db.update_site_review(review_id, cid, data.model_dump())
    if not review:
        raise HTTPException(status_code=404, detail="Отзыв не найден")
    return {"ok": True, "review": review}

@app.delete("/api/admin/site-reviews/{review_id}")
async def admin_delete_site_review(review_id: int, cid: int = Depends(_get_admin_cid)):
    ok = await db.delete_site_review(review_id, cid)
    if not ok:
        raise HTTPException(status_code=404, detail="Отзыв не найден")
    return {"ok": True}

@app.get("/api/site-reviews")
async def get_site_reviews_public(company_slug: str = None):
    cid = await _resolve_client_company_id(company_slug)
    return {"ok": True, "reviews": await db.get_site_reviews(cid)}


# ── FAQ на главной странице ──────────────────────────────────────────────────
@app.get("/api/admin/site-faq")
async def admin_list_site_faq(cid: int = Depends(_get_admin_cid)):
    return {"ok": True, "faq": await db.get_site_faq(cid)}

@app.post("/api/admin/site-faq")
async def admin_create_site_faq(data: SiteFaqIn, cid: int = Depends(_get_admin_cid)):
    item = await db.create_site_faq_item(cid, data.model_dump())
    return {"ok": True, "faq": item}

@app.put("/api/admin/site-faq/{faq_id}")
async def admin_update_site_faq(faq_id: int, data: SiteFaqIn, cid: int = Depends(_get_admin_cid)):
    item = await db.update_site_faq_item(faq_id, cid, data.model_dump())
    if not item:
        raise HTTPException(status_code=404, detail="Вопрос не найден")
    return {"ok": True, "faq": item}

@app.delete("/api/admin/site-faq/{faq_id}")
async def admin_delete_site_faq(faq_id: int, cid: int = Depends(_get_admin_cid)):
    ok = await db.delete_site_faq_item(faq_id, cid)
    if not ok:
        raise HTTPException(status_code=404, detail="Вопрос не найден")
    return {"ok": True}

@app.get("/api/site-faq")
async def get_site_faq_public(company_slug: str = None):
    cid = await _resolve_client_company_id(company_slug)
    return {"ok": True, "faq": await db.get_site_faq(cid)}


class CompanySocialRequest(BaseModel):
    whatsapp:        str | None = None
    instagram:       str | None = None
    tg_group_link:   str | None = None
    tg_group_id:     int | None = None
    tg_channel_link: str | None = None
    tg_channel_id:   int | None = None
    tg_admin_link:   str | None = None
    tg_admin_id:     int | None = None

@app.put("/api/company/contacts")
async def company_update_contacts(req: CompanySocialRequest, staff=Depends(get_current_staff)):
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только admin")
    company_id = staff.get("company_id") or 1
    updates = {k: v for k, v in req.model_dump().items()}
    await db.update_company(company_id, updates)
    return {"ok": True}


# ── Филиалы (управляются company admin или superadmin) ──────────────────

@app.get("/api/branches")
async def branches_list(staff=Depends(get_current_staff)):
    company_id = staff.get("company_id") or 1
    rows = await db.get_branches(company_id)
    return {"ok": True, "branches": [dict(r) for r in rows]}


@app.post("/api/branches")
async def branches_create(req: BranchCreateRequest, staff=Depends(get_current_staff)):
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только admin")
    company_id = staff.get("company_id") or 1
    # Проверяем лимит
    company = await db.get_company(company_id)
    existing = await db.get_branches(company_id)
    if company and len(existing) >= (company["max_branches"] or 99):
        raise HTTPException(status_code=400, detail=f"Достигнут лимит филиалов ({company['max_branches']})")
    branch = await db.create_branch(
        company_id=company_id, slug=req.slug.lower().strip(),
        name_ru=req.name_ru, name_uz=req.name_uz,
        lat=req.lat, lon=req.lon, phones=req.phones,
        workshop_lat=req.workshop_lat, workshop_lon=req.workshop_lon,
        tg_orders_channel_id=req.tg_orders_channel_id,
        tg_leads_group_id=req.tg_leads_group_id,
        tg_delivery_channel_id=req.tg_delivery_channel_id,
        tg_delivery_channel_link=req.tg_delivery_channel_link,
        telegram_link=req.telegram_link,
        admin_tg_link=req.admin_tg_link,
        whatsapp=req.whatsapp,
        instagram=req.instagram,
        tg_leads_group_link=req.tg_leads_group_link,
        tg_orders_channel_link=req.tg_orders_channel_link,
        telegram_group_id=req.telegram_group_id,
        admin_tg_id=req.admin_tg_id,
    )
    if not branch:
        raise HTTPException(status_code=409, detail="Slug уже занят в этой компании")
    return {"ok": True, "branch": dict(branch)}


@app.put("/api/branches/{branch_id}")
async def branches_update(branch_id: int, req: BranchUpdateRequest, staff=Depends(get_current_staff)):
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только admin")
    company_id = staff.get("company_id") or 1
    updates = {k: v for k, v in req.model_dump().items() if v is not None}
    if "phones" in req.model_dump() and req.phones is not None:
        updates["phones"] = req.phones  # включаем пустой список тоже
    ok = await db.update_branch(branch_id, company_id, updates)
    if not ok:
        raise HTTPException(status_code=404, detail="Филиал не найден")
    return {"ok": True}


@app.delete("/api/branches/{branch_id}")
async def branches_delete(branch_id: int, staff=Depends(get_current_staff)):
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только admin")
    company_id = staff.get("company_id") or 1
    branches = await db.get_branches(company_id)
    if len(branches) <= 1:
        raise HTTPException(status_code=400, detail="Нельзя удалить единственный филиал")
    min_id = min(b["id"] for b in branches)
    if branch_id == min_id:
        raise HTTPException(status_code=400, detail="Нельзя удалить основной филиал")
    ok = await db.delete_branch(branch_id, company_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Филиал не найден")
    return {"ok": True}


@app.delete("/api/saas/companies/{company_id}/branches/{branch_id}")
async def saas_delete_branch(company_id: int, branch_id: int, _=Depends(get_superadmin)):
    branches = await db.get_branches(company_id)
    if len(branches) <= 1:
        raise HTTPException(status_code=400, detail="Нельзя удалить единственный филиал")
    min_id = min(b["id"] for b in branches)
    if branch_id == min_id:
        raise HTTPException(status_code=400, detail="Нельзя удалить основной филиал")
    ok = await db.delete_branch(branch_id, company_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Филиал не найден")
    return {"ok": True}


@app.delete("/api/saas/companies/{company_id}")
async def saas_delete_company(company_id: int, _=Depends(get_superadmin)):
    """Мягкое удаление (архивация) — компания и все её данные остаются в БД, просто
    перестают быть активными и помечаются архивной. Физическое удаление (delete_company_cascade)
    больше не вызывается отсюда: тестовые компании удалялись и переиспользовали id
    следующей созданной (см. фикс setval в database.create_tables)."""
    if company_id <= 1:
        raise HTTPException(status_code=400, detail="Эту компанию удалить нельзя")
    company = await db.get_company(company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Компания не найдена")
    ok = await db.archive_company(company_id)
    if not ok:
        raise HTTPException(status_code=400, detail="Компания уже архивирована")
    return {"ok": True}


@app.post("/api/saas/companies/{company_id}/restore")
async def saas_restore_company(company_id: int, _=Depends(get_superadmin)):
    company = await db.get_company(company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Компания не найдена")
    await db.restore_company(company_id)
    return {"ok": True}


# ── Обслуживание БД ──────────────────────────────────────────────────────────

_CLEANUP_TABLES = [
    "chat_messages", "chat_sessions", "chat_templates",
    "sms_contacts", "sms_dispatches", "sms_groups",
    "autodial_group_members", "autodial_calls", "autodial_campaigns",
    "autodial_groups", "autodial_callerids", "autodial_ivrs",
    "route_orders", "routes",
    "discount_requests", "debt_approval_requests", "order_receipt_log",
    "order_activity", "order_payments", "order_photos",
    "order_item_media", "order_items", "orders",
    "lead_reminders", "lead_calls", "leads",
    "salary_ledger", "staff_salary_percents", "staff_salary_per_unit",
    "staff_salary_kpi", "staff_commissions",
    "timesheet", "staff_attendance_events", "staff_personal", "staff",
    "branches",
    "prices", "services",
    "positions", "departments",
    "cash_handovers", "cash_shifts", "expense_categories", "expenses",
    "crm_clients", "contacts", "site_contacts",
    "promo_user_state", "promotions",
    "push_subscriptions", "agent_notifications", "washer_notifications",
    "tg_status_messages", "tg_phone_links",
    "config", "settings", "users", "plans",
]

@app.get("/api/saas/maintenance/orphans")
async def sa_orphans_preview(_=Depends(get_superadmin)):
    """Подсчитать строки-сироты (company_id отсутствует в companies)."""
    if not db.pool:
        raise HTTPException(status_code=503, detail="DB unavailable")
    result = {}
    async with db.pool.acquire() as conn:
        for t in _CLEANUP_TABLES:
            try:
                n = await conn.fetchval(
                    f"SELECT COUNT(*) FROM {t} "
                    f"WHERE company_id IS NOT NULL "
                    f"AND company_id NOT IN (SELECT id FROM companies)"
                )
                if n:
                    result[t] = n
            except Exception:
                pass
    return {"ok": True, "orphans": result, "total": sum(result.values())}

@app.post("/api/saas/maintenance/cleanup")
async def sa_cleanup_orphans(_=Depends(get_superadmin)):
    """Удалить все строки-сироты."""
    if not db.pool:
        raise HTTPException(status_code=503, detail="DB unavailable")
    deleted = {}
    async with db.pool.acquire() as conn:
        for t in _CLEANUP_TABLES:
            try:
                r = await conn.execute(
                    f"DELETE FROM {t} "
                    f"WHERE company_id IS NOT NULL "
                    f"AND company_id NOT IN (SELECT id FROM companies)"
                )
                n = int(r.split()[-1])
                if n:
                    deleted[t] = n
            except Exception:
                pass
    return {"ok": True, "deleted": deleted, "total": sum(deleted.values())}


# ── Настройки компании (timezone и прочее, управляется company admin) ────

_SUPPORTED_TIMEZONES = [
    {"name": "Asia/Tashkent",   "label": "Ташкент / Навои / Зарафшан (UTC+5)"},
    {"name": "Asia/Almaty",     "label": "Алматы / Нур-Султан (UTC+5)"},
    {"name": "Asia/Bishkek",    "label": "Бишкек (UTC+6)"},
    {"name": "Asia/Dushanbe",   "label": "Душанбе (UTC+5)"},
    {"name": "Asia/Ashgabat",   "label": "Ашхабад (UTC+5)"},
    {"name": "Asia/Baku",       "label": "Баку (UTC+4)"},
    {"name": "Asia/Yerevan",    "label": "Ереван (UTC+4)"},
    {"name": "Asia/Tbilisi",    "label": "Тбилиси (UTC+4)"},
    {"name": "Europe/Moscow",   "label": "Москва (UTC+3)"},
    {"name": "Europe/Istanbul", "label": "Стамбул (UTC+3)"},
    {"name": "Europe/Kiev",     "label": "Киев (UTC+2/3)"},
    {"name": "UTC",             "label": "UTC+0"},
]


@app.get("/api/timezones")
async def list_timezones():
    """Список поддерживаемых таймзон для выпадающего списка в настройках."""
    return {"ok": True, "timezones": _SUPPORTED_TIMEZONES}


@app.get("/api/company/settings")
async def company_settings_get(staff=Depends(get_current_staff)):
    """Настройки текущей компании (timezone и лимиты)."""
    company_id = staff.get("company_id") or 1
    c = await db.get_company(company_id)
    if not c:
        raise HTTPException(status_code=404, detail="Компания не найдена")
    return {"ok": True, "settings": {
        "timezone": c["timezone"] or db.DEFAULT_TZ_NAME,
        "name":     c["name"],
        "plan":     c["plan"],
        "max_branches": c["max_branches"],
        "max_staff":    c["max_staff"],
    }}


class CompanySettingsRequest(BaseModel):
    timezone: str | None = None


@app.put("/api/company/settings")
async def company_settings_update(req: CompanySettingsRequest, staff=Depends(get_current_staff)):
    """Company admin обновляет настройки своей компании."""
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только admin")
    company_id = staff.get("company_id") or 1
    updates = {}
    if req.timezone is not None:
        allowed_names = {t["name"] for t in _SUPPORTED_TIMEZONES}
        if req.timezone not in allowed_names:
            raise HTTPException(status_code=400, detail=f"Неизвестный timezone: {req.timezone}")
        updates["timezone"] = req.timezone
    if updates:
        await db.update_company(company_id, updates)
    return {"ok": True}


# ── Тарифы компании (самообслуживание админа) ────────────────────────────────
@app.get("/api/company/plans")
async def company_list_plans(staff=Depends(get_current_staff)):
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только admin")
    plans = await db.get_saas_plans()
    return {"ok": True, "plans": [p for p in plans if p.get("active")]}


@app.get("/api/company/subscription")
async def company_get_subscription(staff=Depends(get_current_staff)):
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только admin")
    company_id = staff.get("company_id") or 1
    sub = await db.get_saas_subscription(company_id)
    return {"ok": True, "subscription": dict(sub) if sub else None}


@app.get("/api/company/subscriptions")
async def company_get_subscriptions(staff=Depends(get_current_staff)):
    """История подписок компании — то же, что видит суперадмин
    (GET /saas/companies/{id}/subscriptions), но своим доступом и без поля
    notes (внутренние пометки суперадмина не для владельца компании)."""
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только admin")
    company_id = staff.get("company_id") or 1
    subs = await db.get_saas_subscriptions_all(company_id)
    return {"ok": True, "subscriptions": [
        {k: v for k, v in dict(s).items() if k != "notes"} for s in subs
    ]}


@app.get("/api/company/payments")
async def company_get_payments(staff=Depends(get_current_staff)):
    """История оплат компании — аналогично, без поля note."""
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только admin")
    company_id = staff.get("company_id") or 1
    payments = await db.get_saas_payments(company_id)
    return {"ok": True, "payments": [
        {k: v for k, v in dict(p).items() if k != "note"} for p in payments
    ]}


class CheckoutRequest(BaseModel):
    plan_id: int
    months: int = 1


@app.post("/api/company/checkout")
async def company_checkout(req: CheckoutRequest, staff=Depends(get_current_staff)):
    """ТЕСТОВЫЙ режим оплаты — платёжный шлюз (Click/Payme) ещё не подключён,
    поэтому подтверждение сразу применяет тариф к компании."""
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только admin")
    company_id = staff.get("company_id") or 1
    result = await db.checkout_plan_test(company_id, req.plan_id, req.months)
    if not result:
        raise HTTPException(status_code=400, detail="Тариф не найден")
    return {"ok": True, **result}


@app.get("/api/company/support-contact")
async def company_support_contact(staff=Depends(get_current_staff)):
    """Контакты поддержки SaaS-платформы (настраивает суперадмин, company_id=0)."""
    tg = await db.get_config_for_company("saas_support_tg", 0)
    phone = await db.get_config_for_company("saas_support_phone", 0)
    return {"ok": True, "telegram": tg or "", "phone": phone or ""}


class SupportContactRequest(BaseModel):
    telegram: str = ""
    phone: str = ""


@app.get("/api/saas/support-contact")
async def saas_get_support_contact(_=Depends(get_superadmin)):
    tg = await db.get_config_for_company("saas_support_tg", 0)
    phone = await db.get_config_for_company("saas_support_phone", 0)
    return {"ok": True, "telegram": tg or "", "phone": phone or ""}


@app.put("/api/saas/support-contact")
async def saas_update_support_contact(req: SupportContactRequest, _=Depends(get_superadmin)):
    await db.set_config_for_company("saas_support_tg", req.telegram, 0)
    await db.set_config_for_company("saas_support_phone", req.phone, 0)
    return {"ok": True}


_SMS_TEMPLATE_KEYS = ["sms_text_register", "sms_text_login", "sms_text_reset",
                      "sms_pickup_enabled", "sms_pickup_template_ru", "sms_pickup_template_uz",
                      "sms_ready_enabled", "sms_ready_template_ru", "sms_ready_template_uz"]

@app.get("/api/saas/catalog/sms-templates")
async def saas_get_sms_templates(_=Depends(get_superadmin)):
    """Шаблон SMS-текстов (company_id=0) — копируется в новые компании при создании."""
    result = {}
    for key in _SMS_TEMPLATE_KEYS:
        val = await db.get_config_for_company(key, 0)
        result[key] = val if val else SITE_SETTINGS_DEFAULTS.get(key, "")
    return {"ok": True, "templates": result}


class SmsTemplatesRequest(BaseModel):
    sms_text_register:      str | None = None
    sms_text_login:         str | None = None
    sms_text_reset:         str | None = None
    sms_pickup_enabled:     str | None = None
    sms_pickup_template_ru: str | None = None
    sms_pickup_template_uz: str | None = None
    sms_ready_enabled:      str | None = None
    sms_ready_template_ru:  str | None = None
    sms_ready_template_uz:  str | None = None

@app.put("/api/saas/catalog/sms-templates")
async def saas_save_sms_templates(body: SmsTemplatesRequest, _=Depends(get_superadmin)):
    data = {k: v for k, v in body.dict().items() if v is not None}
    for key, val in data.items():
        await db.set_config_for_company(key, val, 0)
    return {"ok": True}


@app.post("/api/saas/catalog/sms-templates/seed-to-companies")
async def saas_reseed_sms_templates(_=Depends(get_superadmin)):
    """Принудительно засеять недостающие ключи SMS-шаблонов во ВСЕХ компаниях
    (для уже созданных компаний, у которых их ещё нет — новые ключи типа sms_pickup_*)."""
    companies = await db.get_all_companies()
    for c in companies:
        await db.seed_company_sms_templates(c["id"])
    return {"ok": True, "companies": len(companies)}


@app.get("/api/saas/catalog/order-rules")
async def saas_get_order_rules_template(_=Depends(get_superadmin)):
    """Шаблон правил приёма/выдачи (company_id=0) — копируется в новые компании при создании."""
    val = await db.get_config_for_company("order_rules", 0)
    return {"ok": True, "order_rules": val or db._DEFAULT_ORDER_RULES_JSON}


class OrderRulesTemplateRequest(BaseModel):
    order_rules: str

@app.put("/api/saas/catalog/order-rules")
async def saas_save_order_rules_template(body: OrderRulesTemplateRequest, _=Depends(get_superadmin)):
    await db.set_config_for_company("order_rules", body.order_rules, 0)
    return {"ok": True}


@app.post("/api/saas/catalog/order-rules/seed-from-company")
async def sa_seed_order_rules_from_company(_=Depends(get_superadmin)):
    """Копирует текущие правила приёма компании #1 в шаблон (company_id=0)."""
    val = await db.get_config_for_company("order_rules", 1)
    if val:
        await db.set_config_for_company("order_rules", val, 0)
    return {"ok": True}


@app.post("/api/saas/catalog/order-rules/seed-to-companies")
async def saas_reseed_order_rules(_=Depends(get_superadmin)):
    """Засеять order_rules во ВСЕХ компаниях, у которых он ещё пуст (компании,
    созданные до появления этой фичи — ON CONFLICT DO NOTHING не тронет тех,
    кто уже сам отредактировал свои правила)."""
    companies = await db.get_all_companies()
    for c in companies:
        await db.seed_company_order_rules(c["id"])
    return {"ok": True, "companies": len(companies)}


_ROLE_PERMS_DEFAULT = {
    "manager": ["dashboard","plans","leads","orders","routes","cash","promotions",
                "clients","contacts","site-users",
                "autodial-groups","autodial-campaigns","autodial-greetings","autodial-settings",
                "timesheet","workon","salary-calc","agent-monitoring",
                "prices","units","expenses-admin","chat-templates"],
    "callcenter": ["dashboard","leads","orders","clients","contacts"]
}


@app.get("/api/settings/role-permissions")
async def get_role_permissions(staff=Depends(get_current_staff)):
    company_id = staff.get("company_id") or 1
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT role_permissions FROM settings WHERE company_id=$1", company_id
        )
    if row and row["role_permissions"]:
        return {"ok": True, "permissions": dict(row["role_permissions"])}
    return {"ok": True, "permissions": _ROLE_PERMS_DEFAULT}


@app.put("/api/settings/role-permissions")
async def save_role_permissions(body: dict = Body(...), staff=Depends(get_current_staff)):
    if staff.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Только admin")
    company_id = staff.get("company_id") or 1
    import json as _json
    async with db.pool.acquire() as conn:
        await conn.execute(
            "UPDATE settings SET role_permissions=$1::jsonb WHERE company_id=$2",
            _json.dumps(body), company_id
        )
    return {"ok": True}
