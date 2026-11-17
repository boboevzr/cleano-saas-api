"""Мобильное приложение — ОДНО на все компании Cleano (решение владельца 15–16.11.2026).

Перенос из ARTEZ (cad5f53 вход/PIN, 7126720 выход/удаление, b8f25d9 push, 1fe71b8 push по заявке) + выбор компании:

Вход клиента по номеру:
  1) POST /api/app/auth/code {phone, company_slug?}  — код в Telegram (ботом компании) или SMS.
  2) POST /api/app/auth/verify {phone, code, company_slug?, first_name?}
       • номер — клиент одной компании → сразу сессия;
       • нескольких → {choose:[{slug,name,logo_url}], ticket} → 3) POST /api/app/auth/pick {ticket, company_slug};
       • ни одной: без company_slug → 404 company_required (приложение спросит код/ссылку компании —
         GET /api/app/company/{slug}); с company_slug → новый клиент этой компании (нужно имя).
  Список компаний номера отдаётся ТОЛЬКО после верного кода (по номеру нельзя узнать, чей он клиент).
  Пароль сайта: POST /api/app/auth/password {phone, password, company_slug?} — так же (выбор, если несколько).
  Сотрудник: POST /api/app/auth/staff {company_slug, login, password}.
  Сменить компанию без кода: POST /api/app/auth/switch {company_slug} (тот же номер — клиент и там).

Сессии/устройства (PIN, Face ID)/push-токены хранят company_id; в пропуске (JWT) — company_id, поэтому
company_id_middleware сам ставит компанию для всех остальных эндпоинтов.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import logging
import secrets
from datetime import datetime, timedelta, timezone

import aiohttp
from fastapi import APIRouter, Body, Depends, Header, HTTPException, Request
from jose import JWTError, jwt

import database as db

log = logging.getLogger("app_api")
router = APIRouter()

APP_ACCESS_MIN = 15
APP_REFRESH_DAYS = 30
APP_GRACE_SEC = 60            # старый ключ принимается повторно только сразу после замены (ответ потерялся)
APP_DEVICE_IDLE_DAYS = 30
APP_DEVICE_MAX_DAYS = 180
APP_PIN_MAX_FAILS = 5
APP_CODE_MAX_FAILS = 5        # неверных кодов за 15 минут
PICK_TTL_MIN = 10
EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send"


def _m():
    import main   # поздний импорт: main импортирует этот модуль
    return main


# ══════════════════════════════════════
#  Таблицы
# ══════════════════════════════════════
async def ensure_tables() -> None:
    if not db.pool:
        return
    stmts = [
        """CREATE TABLE IF NOT EXISTS app_sessions (
            id           BIGSERIAL PRIMARY KEY,
            company_id   INTEGER NOT NULL,
            kind         VARCHAR(10) NOT NULL,
            subject_id   INTEGER NOT NULL,
            staff_sid    INTEGER,
            device_id    BIGINT,
            method       VARCHAR(16) NOT NULL,
            refresh_hash TEXT NOT NULL UNIQUE,
            prev_hash    TEXT,
            rotated_at   TIMESTAMPTZ,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_used_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            expires_at   TIMESTAMPTZ NOT NULL,
            revoked_at   TIMESTAMPTZ,
            user_agent   TEXT,
            ip           TEXT
        )""",
        "CREATE INDEX IF NOT EXISTS idx_app_sessions_prev ON app_sessions(prev_hash) WHERE prev_hash IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS idx_app_sessions_subject ON app_sessions(kind, subject_id) WHERE revoked_at IS NULL",
        """CREATE TABLE IF NOT EXISTS app_devices (
            id              BIGSERIAL PRIMARY KEY,
            company_id      INTEGER NOT NULL,
            kind            VARCHAR(10) NOT NULL,
            subject_id      INTEGER NOT NULL,
            token_hash      TEXT NOT NULL UNIQUE,
            prev_token_hash TEXT,
            rotated_at      TIMESTAMPTZ,
            pin_hash        TEXT NOT NULL,
            pin_failed      INTEGER NOT NULL DEFAULT 0,
            biometric       BOOLEAN NOT NULL DEFAULT FALSE,
            name            TEXT,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_used_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            revoked_at      TIMESTAMPTZ
        )""",
        "CREATE INDEX IF NOT EXISTS idx_app_devices_prev ON app_devices(prev_token_hash) WHERE prev_token_hash IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS idx_app_devices_subject ON app_devices(kind, subject_id) WHERE revoked_at IS NULL",
        # push: токены Expo телефонов; subject_id = users.id (у клиента users уже по компании)
        """CREATE TABLE IF NOT EXISTS app_push_tokens (
            id           BIGSERIAL PRIMARY KEY,
            company_id   INTEGER NOT NULL,
            kind         VARCHAR(10) NOT NULL,
            subject_id   INTEGER NOT NULL,
            token        TEXT NOT NULL UNIQUE,
            platform     VARCHAR(10),
            lang         VARCHAR(4),
            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            revoked_at   TIMESTAMPTZ
        )""",
        "CREATE INDEX IF NOT EXISTS idx_app_push_subject ON app_push_tokens(kind, subject_id) WHERE revoked_at IS NULL",
        # события смены статуса заказа пишет триггер БД (статус меняют и API, и бот) — рассылает push_worker
        """CREATE TABLE IF NOT EXISTS order_status_events (
            id           BIGSERIAL PRIMARY KEY,
            company_id   INTEGER,
            order_id     INTEGER NOT NULL,
            order_num    TEXT,
            client_phone TEXT,
            old_status   TEXT,
            new_status   TEXT,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            sent_at      TIMESTAMPTZ,
            result       TEXT
        )""",
        "CREATE INDEX IF NOT EXISTS idx_order_status_events_unsent ON order_status_events(id) WHERE sent_at IS NULL",
        """CREATE OR REPLACE FUNCTION cleano_order_status_event() RETURNS trigger AS $$
           BEGIN
             IF NEW.status IS DISTINCT FROM OLD.status THEN
               INSERT INTO order_status_events (company_id, order_id, order_num, client_phone, old_status, new_status)
               VALUES (NEW.company_id, NEW.id, NEW.order_num, NEW.client_phone, OLD.status, NEW.status);
             END IF;
             RETURN NEW;
           END $$ LANGUAGE plpgsql""",
        """DO $$ BEGIN
             IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_cleano_order_status_event') THEN
               CREATE TRIGGER trg_cleano_order_status_event AFTER UPDATE OF status ON orders
               FOR EACH ROW EXECUTE FUNCTION cleano_order_status_event();
             END IF;
           END $$""",
    ]
    async with db.pool.acquire() as c:
        for s in stmts:
            await c.execute(s)


# ══════════════════════════════════════
#  Помощники
# ══════════════════════════════════════
def _sha(s: str) -> str:
    return hashlib.sha256((s or "").encode()).hexdigest()


def _scrypt_pin(pin: str) -> str:
    salt = secrets.token_bytes(16)
    dk = hashlib.scrypt(pin.encode(), salt=salt, n=2**14, r=8, p=1, dklen=32)
    return f"scrypt$16384$8$1${base64.b64encode(salt).decode()}${base64.b64encode(dk).decode()}"


def _scrypt_check(pin: str, stored: str) -> bool:
    try:
        _, n, r, p, salt, dk = stored.split("$")
        got = hashlib.scrypt(pin.encode(), salt=base64.b64decode(salt), n=int(n), r=int(r), p=int(p), dklen=32)
        return hmac.compare_digest(got, base64.b64decode(dk))
    except (ValueError, TypeError):
        return False


def _pin_problem(pin: str) -> tuple[str, str] | None:
    if not (isinstance(pin, str) and len(pin) == 4 and pin.isdigit()):
        return "PIN — 4 цифры", "PIN — 4 ta raqam"
    d = [int(x) for x in pin]
    steps = {d[i + 1] - d[i] for i in range(3)}
    if len(set(d)) == 1 or steps in ({1}, {-1}):
        return "Слишком простой PIN (1111, 1234)", "Juda oddiy PIN (1111, 1234)"
    return None


def _err(status: int, code: str, ru: str, uz: str, **extra):
    """Ошибка с машинным кодом для приложения: detail = {code, message, ...}."""
    raise HTTPException(status_code=status, detail={"code": code, "message": _m().bi(ru, uz), **extra})


def _ip(request: Request) -> str:
    # последний из X-Forwarded-For (его дописал прокси Railway; левые клиент может подделать)
    return (request.headers.get("x-forwarded-for") or "").split(",")[-1].strip() \
        or (request.client.host if request.client else "")


async def _company(slug: str | None) -> dict | None:
    """Действующая компания по slug (не архив, не шаблон)."""
    if not slug or not db.pool:
        return None
    row = await db.pool.fetchrow(
        "SELECT id, slug, name, logo_url FROM companies WHERE slug=$1 AND id > 0 AND archived_at IS NULL "
        "AND COALESCE(active, TRUE)", str(slug).strip().lower())
    return dict(row) if row else None


def _company_pub(c: dict) -> dict:
    return {"slug": c["slug"], "name": c["name"], "logo_url": c.get("logo_url")}


async def _client_companies(phone: str) -> list[dict]:
    """Компании, где номер — подтверждённый клиент (users)."""
    rows = await db.pool.fetch(
        "SELECT c.id, c.slug, c.name, c.logo_url, u.id AS user_id FROM users u JOIN companies c ON c.id=u.company_id "
        "WHERE u.phone=$1 AND u.is_verified AND c.id > 0 AND c.archived_at IS NULL AND COALESCE(c.active, TRUE) "
        "ORDER BY c.name", phone)
    return [dict(r) for r in rows]


def _client_public(u) -> dict:
    return {"id": u["id"], "phone": u["phone"], "first_name": u.get("first_name"),
            "address": u.get("address"), "tg_linked": bool(u.get("tg_id"))}


def _pick_secret() -> str:
    # отдельный ключ: «билет выбора компании» нельзя использовать как обычный пропуск (и наоборот)
    return _m().JWT_SECRET + ":app_pick"


def _pick_ticket(phone: str, company_ids: list[int]) -> str:
    return jwt.encode({"phone": phone, "cids": company_ids, "purpose": "app_pick",
                       "exp": datetime.now(timezone.utc) + timedelta(minutes=PICK_TTL_MIN)}, _pick_secret(), algorithm="HS256")


async def _choose_or_issue(companies: list[dict], phone: str, method: str, request: Request) -> dict:
    """Одна компания — сразу сессия; несколько — список и билет выбора (10 минут)."""
    if len(companies) == 1:
        c = companies[0]
        user = await db.get_user_by_phone(phone, c["id"])
        return await _issue("client", dict(user), c["id"], method, request)
    return {"ok": True, "choose": [_company_pub(c) for c in companies],
            "ticket": _pick_ticket(phone, [c["id"] for c in companies]), "method": method}


# ══════════════════════════════════════
#  Сессии
# ══════════════════════════════════════
async def _issue(kind: str, subject: dict, company_id: int, method: str, request: Request,
                 device_id: int | None = None) -> dict:
    """Новая сессия приложения (+ строка staff_sessions для сотрудника) и пара ключей."""
    ua, ip = request.headers.get("user-agent"), _ip(request)
    staff_sid = None
    if kind == "staff":
        staff_sid = await db.create_staff_session(subject["id"], f"📱 app · {ua or ''}", ip)
    refresh = secrets.token_urlsafe(48)
    asid = await db.pool.fetchval(
        "INSERT INTO app_sessions (company_id, kind, subject_id, staff_sid, device_id, method, refresh_hash, expires_at, "
        "user_agent, ip) VALUES ($1,$2,$3,$4,$5,$6,$7, NOW() + make_interval(days => $8), $9, $10) RETURNING id",
        company_id, kind, subject["id"], staff_sid, device_id, method, _sha(refresh), APP_REFRESH_DAYS,
        (ua or "")[:300] or None, (ip or "")[:64] or None)
    return await _pair(kind, subject, company_id, asid, staff_sid, refresh)


async def _pair(kind: str, subject: dict, company_id: int, asid: int, staff_sid: int | None, refresh: str) -> dict:
    m = _m()
    exp = datetime.now(timezone.utc) + timedelta(minutes=APP_ACCESS_MIN)
    if kind == "staff":
        payload = {"sub": str(subject["id"]), "login": subject["login"], "role": subject["role"], "type": "staff",
                   "company_id": company_id, "sid": staff_sid, "asid": asid, "exp": exp}
        who = {"staff": m._staff_public(dict(subject))}
    else:
        payload = {"sub": str(subject["id"]), "phone": subject["phone"], "type": "client",
                   "company_id": company_id, "asid": asid, "exp": exp}
        who = {"user": _client_public(subject)}
    comp = await db.pool.fetchrow("SELECT id, slug, name, logo_url FROM companies WHERE id=$1", company_id)
    return {"ok": True, "kind": kind, "access_token": jwt.encode(payload, m.JWT_SECRET, algorithm=m.JWT_ALGORITHM),
            "refresh_token": refresh, "expires_in": APP_ACCESS_MIN * 60,
            "company": _company_pub(dict(comp)) if comp else None, **who}


async def _subject(kind: str, subject_id: int, company_id: int):
    """Действующий клиент/сотрудник ЭТОЙ компании или None (удалён, не подтверждён, уволен, другая компания)."""
    if kind == "staff":
        s = await db.get_staff_by_id(subject_id)
        return dict(s) if s and s["active"] and (s.get("company_id") or 1) == company_id else None
    u = await db.get_user_by_id(subject_id)
    return dict(u) if u and u["is_verified"] and (u.get("company_id") or 1) == company_id else None


async def _session_get(session_id: int) -> dict | None:
    row = await db.pool.fetchrow(
        "SELECT *, created_at > NOW() - INTERVAL '15 minutes' AS fresh FROM app_sessions "
        "WHERE id=$1 AND revoked_at IS NULL AND expires_at > NOW()", session_id)
    return dict(row) if row else None


async def current(authorization: str = Header(None)) -> dict:
    """Пропуск приложения (с asid) клиента или сотрудника → {kind, subject, session, company_id}."""
    m = _m()
    if not authorization or not authorization.startswith("Bearer "):
        _err(401, "auth_required", "Не авторизован", "Avtorizatsiya kerak")
    try:
        payload = jwt.decode(authorization.removeprefix("Bearer ").strip(), m.JWT_SECRET, algorithms=[m.JWT_ALGORITHM])
    except JWTError:
        _err(401, "auth_expired", "Сессия истекла", "Sessiya tugadi")
    asid = payload.get("asid")
    sess = await _session_get(int(asid)) if asid else None
    kind = "staff" if payload.get("type") == "staff" else "client"
    if (not sess or sess["kind"] != kind or str(sess["subject_id"]) != str(payload.get("sub"))
            or int(payload.get("company_id") or 0) != sess["company_id"]):
        _err(401, "auth_expired", "Сессия истекла", "Sessiya tugadi")
    db.set_request_company(sess["company_id"])
    subject = await m._staff_from_payload(payload) if kind == "staff" else await _subject("client", sess["subject_id"], sess["company_id"])
    if not subject:
        _err(401, "auth_expired", "Сессия истекла", "Sessiya tugadi")
    return {"kind": kind, "subject": dict(subject), "session": sess, "company_id": sess["company_id"]}


async def revoke_subject(kind: str, subject_id: int) -> None:
    """Закрыть все сессии приложения и сбросить все PIN/Face ID человека (смена/сброс пароля, «выйти везде»)."""
    if not db.pool:
        return
    async with db.pool.acquire() as c:
        try:
            await c.execute("UPDATE app_sessions SET revoked_at=NOW() WHERE kind=$1 AND subject_id=$2 AND revoked_at IS NULL",
                            kind, subject_id)
            await c.execute("UPDATE app_devices SET revoked_at=NOW() WHERE kind=$1 AND subject_id=$2 AND revoked_at IS NULL",
                            kind, subject_id)
        except Exception:
            pass   # таблиц ещё нет
    if kind == "client" and hasattr(db, "passkeys_revoke_user"):
        await db.passkeys_revoke_user(subject_id)


# ══════════════════════════════════════
#  Коды из SMS / Telegram
# ══════════════════════════════════════
async def _attempt_count(phone: str) -> int:
    async with db.pool.acquire() as c:
        await c.execute("INSERT INTO sms_codes (phone, code, purpose, expires_at, used) VALUES ($1,'-','app_attempt',NOW(),TRUE)", phone)
        return int(await c.fetchval("SELECT COUNT(*) FROM sms_codes WHERE phone=$1 AND purpose='app_attempt' "
                                    "AND created_at > NOW() - INTERVAL '15 minutes'", phone) or 0)


@router.get("/api/app/company/{slug}")
async def app_company(slug: str):
    """Карточка компании для экрана «Код компании» (сайт компании и так публичный)."""
    c = await _company(slug)
    if not c:
        _err(404, "company_not_found", "Компания не найдена — проверьте код", "Kompaniya topilmadi — kodni tekshiring")
    return {"ok": True, "company": _company_pub(c)}


@router.post("/api/app/auth/code")
async def app_auth_code(body: dict = Body({})):
    """Шаг 1: код в Telegram (ботом компании клиента) или SMS. new=true — номер ещё не клиент этой компании."""
    m = _m()
    phone = m.normalize_phone(body.get("phone") or "")
    if not m.PHONE_RE.match(phone):
        _err(400, "phone_format", "Номер в формате +998XXXXXXXXX", "Raqam +998XXXXXXXXX formatida")
    slug_c = await _company(body.get("company_slug")) if body.get("company_slug") else None
    if body.get("company_slug") and not slug_c:
        _err(404, "company_not_found", "Компания не найдена — проверьте код", "Kompaniya topilmadi — kodni tekshiring")
    mine = await _client_companies(phone)
    targets = [slug_c] if slug_c else mine
    if not targets:
        # номер нигде не клиент — приложение спросит код/ссылку компании (ответ одинаковый для любого номера)
        _err(404, "company_required", "Укажите код компании или откройте её ссылку",
             "Kompaniya kodini kiriting yoki uning havolasini oching")
    is_new = bool(slug_c) and not any(c["id"] == slug_c["id"] for c in mine)
    ok, err = await db.check_sms_rate_limit(phone, "app_login")
    if not ok:
        _err(429, "rate_limited", err, err)
    code = m.generate_code()
    await db.save_sms_code(phone, code, "app_login", datetime.utcnow() + timedelta(minutes=m.SMS_CODE_TTL_MIN))
    uz = (body.get("lang") or "ru") != "ru"
    # Telegram: первая компания, где у номера привязан Telegram, — её ботом
    for c in targets:
        tg = await db.pool.fetchval("SELECT tg_id FROM users WHERE phone=$1 AND company_id=$2 AND tg_id IS NOT NULL", phone, c["id"])
        if not tg and c is slug_c:
            tg = await db.get_tg_id_by_phone(phone)
        if not tg:
            continue
        brand, _ = await m._company_brand(c["id"])
        import html as _h
        b = _h.escape(brand)
        text = (f"🔐 <b>{b}</b> — ilovaga kirish kodi:\n\n<code>{code}</code>\n\n⏱ {m.SMS_CODE_TTL_MIN} daqiqa amal qiladi. "
                f"Kodni hech kimga bermang." if uz else
                f"🔐 <b>{b}</b> — код для входа в приложение:\n\n<code>{code}</code>\n\n⏱ Действителен "
                f"{m.SMS_CODE_TTL_MIN} минут. Никому не сообщайте код.")
        res = await m._send_client_tg(c["id"], int(tg), text)
        if res.get("ok"):
            return {"ok": True, "via": "tg", "new": is_new, "phone": phone}
    first = targets[0]
    if is_new and await db.get_config_for_company("sms_registration_enabled", first["id"]) == "false":
        _err(403, "tg_required", "Регистрация — через Telegram-бота компании: нажмите /start и поделитесь номером",
             "Ro'yxatdan o'tish — kompaniya Telegram-boti orqali: /start bosing va raqamingizni ulashing")
    db.set_request_company(first["id"])   # SMS — настройками Eskiz этой компании
    await m.send_sms(phone, await m.sms_text(code, "login"))
    return {"ok": True, "via": "sms", "new": is_new, "phone": phone}


@router.post("/api/app/auth/verify")
async def app_auth_verify(request: Request, body: dict = Body({})):
    """Шаг 2: код → сессия (или выбор компании). Новому клиенту компании нужно имя — аккаунт создаётся здесь."""
    m = _m()
    phone = m.normalize_phone(body.get("phone") or "")
    code = str(body.get("code") or "").strip()
    name = (body.get("first_name") or "").strip()[:60]
    slug_c = await _company(body.get("company_slug")) if body.get("company_slug") else None
    if body.get("company_slug") and not slug_c:
        _err(404, "company_not_found", "Компания не найдена — проверьте код", "Kompaniya topilmadi — kodni tekshiring")
    if slug_c:
        existing = await db.get_user_by_phone(phone, slug_c["id"])
        if not (existing and existing["is_verified"]) and not name:
            _err(400, "name_required", "Укажите имя", "Ismingizni kiriting")
    # попытку записываем ДО проверки: одновременные запросы не обходят лимит; лимит исчерпан — код сгорает
    if await _attempt_count(phone) > APP_CODE_MAX_FAILS:
        await db.pool.execute("UPDATE sms_codes SET used=TRUE WHERE phone=$1 AND purpose='app_login' AND used=FALSE", phone)
        _err(429, "rate_limited", "Много неверных кодов. Попробуйте через 15 минут",
             "Ko'p noto'g'ri kod. 15 daqiqadan keyin urinib ko'ring")
    if not code or not await db.check_sms_code(phone, code, "app_login"):
        _err(400, "code_invalid", "Неверный или просроченный код", "Kod noto'g'ri yoki eskirgan")
    await db.pool.execute("DELETE FROM sms_codes WHERE phone=$1 AND purpose='app_attempt'", phone)
    if slug_c:
        cid = slug_c["id"]
        user = await db.get_user_by_phone(phone, cid)
        if not user:
            await db.create_user(phone, m.pwd_context.hash(secrets.token_urlsafe(24)[:72]), name, cid)
        if not (user and user["is_verified"]):
            await db.verify_user(phone, cid)
            db.set_request_company(cid)
            asyncio.create_task(m._notify_new_site_user(name or (user or {}).get("first_name") or "", phone, "app", cid))
            asyncio.create_task(db.upsert_crm_client(phone=phone, first_name=name, source="app_register"))
        user = await db.get_user_by_phone(phone, cid)
        return await _issue("client", dict(user), cid, "code", request)
    companies = await _client_companies(phone)
    if not companies:
        _err(404, "company_required", "Укажите код компании или откройте её ссылку",
             "Kompaniya kodini kiriting yoki uning havolasini oching")
    return await _choose_or_issue(companies, phone, "code", request)


@router.post("/api/app/auth/pick")
async def app_auth_pick(request: Request, body: dict = Body({})):
    """Выбор компании после кода/пароля (билет — 10 минут, только компании из списка)."""
    try:
        t = jwt.decode(str(body.get("ticket") or ""), _pick_secret(), algorithms=["HS256"])
    except JWTError:
        _err(401, "ticket_expired", "Время выбора вышло — войдите заново", "Tanlash vaqti tugadi — qayta kiring")
    c = await _company(body.get("company_slug"))
    if not c or t.get("purpose") != "app_pick" or c["id"] not in (t.get("cids") or []):
        _err(403, "company_forbidden", "Эта компания недоступна", "Bu kompaniya mavjud emas")
    user = await db.get_user_by_phone(t["phone"], c["id"])
    if not user or not user["is_verified"]:
        _err(403, "company_forbidden", "Эта компания недоступна", "Bu kompaniya mavjud emas")
    return await _issue("client", dict(user), c["id"], str(body.get("method") or "code")[:16], request)


@router.post("/api/app/auth/password")
async def app_auth_password(request: Request, body: dict = Body({})):
    """Вход клиента по номеру и паролю сайта компании (если пароль подходит в нескольких — выбор)."""
    m = _m()
    phone = m.normalize_phone(body.get("phone") or "")
    pw = str(body.get("password") or "")[:72]
    slug_c = await _company(body.get("company_slug")) if body.get("company_slug") else None
    pool = [slug_c] if slug_c else await _client_companies(phone)
    matched = []
    for c in pool:
        u = await db.get_user_by_phone(phone, c["id"])
        if u and u["is_verified"] and await asyncio.to_thread(m.pwd_context.verify, pw, u["password_hash"]):
            matched.append(c)
    if not matched:
        _err(401, "bad_credentials", "Неверный номер или пароль", "Noto'g'ri telefon yoki parol")
    return await _choose_or_issue(matched, phone, "password", request)


@router.post("/api/app/auth/staff")
async def app_auth_staff(request: Request, body: dict = Body({})):
    """Вход сотрудника — компания, логин и пароль, как в staff.html."""
    m = _m()
    c = await _company(body.get("company_slug"))
    if not c:
        _err(401, "bad_credentials", "Неверная компания, логин или пароль", "Kompaniya, login yoki parol noto'g'ri")
    staff = await db.get_staff_by_login(str(body.get("login") or "").strip(), c["id"])
    pw = str(body.get("password") or "")[:72]
    valid = bool(staff) and m.pwd_context.verify(pw, staff["password_hash"])
    if staff and not valid and staff.get("temp_password_hash") and staff.get("temp_password_expires"):
        if datetime.now(timezone.utc) < staff["temp_password_expires"]:
            valid = m.pwd_context.verify(pw, staff["temp_password_hash"])
    if not valid:
        _err(401, "bad_credentials", "Неверная компания, логин или пароль", "Kompaniya, login yoki parol noto'g'ri")
    await m._reject_if_subscription_closed(c["id"])
    if not staff.get("active", True):
        _err(401, "staff_inactive", "Сотрудник деактивирован", "Xodim o'chirilgan")
    out = await _issue("staff", dict(staff), c["id"], "password", request)
    out["staff"]["must_change_password"] = bool(staff.get("must_change_password"))
    return out


@router.post("/api/app/auth/switch")
async def app_auth_switch(request: Request, body: dict = Body({}), cur=Depends(current)):
    """Клиент переключается на другую свою компанию (тот же номер там — подтверждённый клиент)."""
    if cur["kind"] != "client":
        _err(403, "staff_account", "Сотрудник входит в компанию заново", "Xodim kompaniyaga qayta kiradi")
    c = await _company(body.get("company_slug"))
    user = await db.get_user_by_phone(cur["subject"]["phone"], c["id"]) if c else None
    if not c or not user or not user["is_verified"]:
        _err(403, "company_forbidden", "Эта компания недоступна", "Bu kompaniya mavjud emas")
    return await _issue("client", dict(user), c["id"], "switch", request)


@router.get("/api/app/me/companies")
async def app_my_companies(cur=Depends(current)):
    """Компании, где этот номер — клиент (для переключателя в профиле)."""
    if cur["kind"] != "client":
        return {"ok": True, "companies": []}
    rows = await _client_companies(cur["subject"]["phone"])
    return {"ok": True, "companies": [{**_company_pub(r), "current": r["id"] == cur["company_id"]} for r in rows]}


@router.post("/api/app/auth/refresh")
async def app_auth_refresh(body: dict = Body({})):
    new = secrets.token_urlsafe(48)
    old_hash, new_hash = _sha(str(body.get("refresh_token") or "")), _sha(new)
    async with db.pool.acquire() as c, c.transaction():
        row = await c.fetchrow(
            "SELECT id, refresh_hash, revoked_at IS NULL AND expires_at > NOW() AS alive, "
            "rotated_at > NOW() - make_interval(secs => $2) AS in_grace "
            "FROM app_sessions WHERE refresh_hash=$1 OR prev_hash=$1 ORDER BY id DESC LIMIT 1 FOR UPDATE", old_hash, APP_GRACE_SEC)
        sess = None
        if row and row["alive"]:
            if row["refresh_hash"] != old_hash and not row["in_grace"]:   # старый ключ всплыл позже — признак кражи
                await c.execute("UPDATE app_sessions SET revoked_at=NOW() WHERE id=$1", row["id"])
            else:
                sess = dict(await c.fetchrow(
                    "UPDATE app_sessions SET prev_hash=refresh_hash, refresh_hash=$2, rotated_at=NOW(), last_used_at=NOW() "
                    "WHERE id=$1 RETURNING *", row["id"], new_hash))
    if not sess:
        _err(401, "auth_expired", "Сессия истекла — войдите заново", "Sessiya tugadi — qayta kiring")
    subject = await _subject(sess["kind"], sess["subject_id"], sess["company_id"])
    alive = subject is not None
    if alive and sess["kind"] == "staff":
        alive = bool(sess["staff_sid"]) and await db.check_staff_session(sess["staff_sid"], subject["id"])
    if not alive:
        await db.pool.execute("UPDATE app_sessions SET revoked_at=NOW() WHERE id=$1", sess["id"])
        _err(401, "auth_expired", "Сессия истекла — войдите заново", "Sessiya tugadi — qayta kiring")
    return await _pair(sess["kind"], subject, sess["company_id"], sess["id"], sess["staff_sid"], new)


@router.post("/api/app/auth/logout")
async def app_auth_logout(body: dict = Body({})):
    rh = _sha(str(body.get("refresh_token") or ""))
    sess = await db.pool.fetchrow("UPDATE app_sessions SET revoked_at=NOW() WHERE (refresh_hash=$1 OR prev_hash=$1) "
                                  "AND revoked_at IS NULL RETURNING *", rh)
    if sess and sess["kind"] == "staff" and sess["staff_sid"]:
        await db.revoke_staff_sessions(sess["subject_id"], "app_logout", sid=sess["staff_sid"])
    if body.get("device_token"):
        await db.pool.execute("UPDATE app_devices SET revoked_at=NOW() WHERE token_hash=$1 AND revoked_at IS NULL",
                              _sha(str(body["device_token"])))
    if body.get("push_token"):
        await db.pool.execute("UPDATE app_push_tokens SET revoked_at=NOW() WHERE token=$1 AND revoked_at IS NULL",
                              str(body["push_token"]))
    return {"ok": True}


# ══════════════════════════════════════
#  PIN / Face ID на телефоне
# ══════════════════════════════════════
async def _device_find(token_hash: str) -> dict | None:
    """Устройство по ключу (текущему или предыдущему). Истёкшее — отключается. Предыдущий ключ вне окна — кража."""
    async with db.pool.acquire() as c:
        row = await c.fetchrow(
            "SELECT *, token_hash=$1 AS is_current, rotated_at > NOW() - make_interval(secs => $2) AS in_grace, "
            "(last_used_at < NOW() - make_interval(days => $3) OR created_at < NOW() - make_interval(days => $4)) AS expired "
            "FROM app_devices WHERE (token_hash=$1 OR prev_token_hash=$1) AND revoked_at IS NULL ORDER BY id DESC LIMIT 1",
            token_hash, APP_GRACE_SEC, APP_DEVICE_IDLE_DAYS, APP_DEVICE_MAX_DAYS)
        if not row:
            return None
        if row["expired"] or (not row["is_current"] and not row["in_grace"]):
            await c.execute("UPDATE app_devices SET revoked_at=NOW() WHERE id=$1", row["id"])
            return None
        return dict(row)


@router.post("/api/app/pin/enroll")
async def app_pin_enroll(body: dict = Body({}), cur=Depends(current)):
    """Придумать PIN → ключ устройства (выдаётся один раз; телефон хранит его в защищённом хранилище)."""
    sess = cur["session"]
    if sess["method"] in ("pin", "bio") or not sess["fresh"]:
        _err(403, "login_again", "Для смены PIN войдите заново по коду или паролю",
             "PIN o'zgartirish uchun kod yoki parol bilan qayta kiring")
    pin = str(body.get("pin") or "")
    if (problem := _pin_problem(pin)):
        _err(400, "pin_weak", *problem)
    token = secrets.token_urlsafe(32)
    pin_hash = await asyncio.to_thread(_scrypt_pin, pin)
    async with db.pool.acquire() as c, c.transaction():
        await c.execute(   # не больше 5 устройств: самые давние отключаются
            "UPDATE app_devices SET revoked_at=NOW() WHERE id IN (SELECT id FROM app_devices WHERE kind=$1 AND subject_id=$2 "
            "AND revoked_at IS NULL ORDER BY last_used_at DESC OFFSET 4)", cur["kind"], cur["subject"]["id"])
        dev_id = await c.fetchval(
            "INSERT INTO app_devices (company_id, kind, subject_id, token_hash, pin_hash, name) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id",
            cur["company_id"], cur["kind"], cur["subject"]["id"], _sha(token), pin_hash, str(body.get("device_name") or "")[:80] or None)
    return {"ok": True, "device_token": token, "device_id": dev_id}


async def _device_login(device_token: str, pin: str | None, request: Request) -> dict:
    gone = ("device_unknown", "Вход по PIN сброшен — войдите по коду", "PIN bilan kirish o'chirildi — kod bilan kiring")
    dev = await _device_find(_sha(device_token or ""))
    if not dev:
        _err(401, *gone)
    subject = await _subject(dev["kind"], dev["subject_id"], dev["company_id"])
    if not subject:
        await db.pool.execute("UPDATE app_devices SET revoked_at=NOW() WHERE id=$1", dev["id"])
        _err(401, *gone)
    if pin is None:
        if not dev["biometric"]:
            _err(403, "biometric_off", "Face ID выключен — введите PIN", "Face ID o'chirilgan — PIN kiriting")
    else:
        # попытку занимаем ДО проверки — одновременные запросы не переберут больше 5 PIN
        used = await db.pool.fetchval("UPDATE app_devices SET pin_failed = pin_failed + 1 WHERE id=$1 AND revoked_at IS NULL "
                                      "AND pin_failed < $2 RETURNING pin_failed", dev["id"], APP_PIN_MAX_FAILS)
        locked = ("pin_locked", "PIN введён неверно 5 раз — войдите по коду", "PIN 5 marta noto'g'ri — kod bilan kiring")
        if used is None:
            await db.pool.execute("UPDATE app_devices SET revoked_at=NOW() WHERE id=$1 AND revoked_at IS NULL", dev["id"])
            _err(401, *locked)
        if not await asyncio.to_thread(_scrypt_check, pin, dev["pin_hash"]):
            left = APP_PIN_MAX_FAILS - used
            if left <= 0:
                await db.pool.execute("UPDATE app_devices SET revoked_at=NOW() WHERE id=$1", dev["id"])
                _err(401, *locked)
            _err(400, "pin_invalid", f"Неверный PIN, осталось попыток: {left}", f"PIN noto'g'ri, qolgan urinishlar: {left}",
                 attempts_left=left)
    new_token = secrets.token_urlsafe(32)
    res = await db.pool.execute(
        "UPDATE app_devices SET prev_token_hash = CASE WHEN $3 THEN NULL ELSE token_hash END, token_hash=$2, pin_failed=0, "
        "last_used_at=NOW(), rotated_at=NOW() WHERE id=$1 AND revoked_at IS NULL", dev["id"], _sha(new_token), not dev["is_current"])
    if not res.endswith(" 1"):
        _err(401, *gone)
    for old in await db.pool.fetch("UPDATE app_sessions SET revoked_at=NOW() WHERE device_id=$1 AND revoked_at IS NULL RETURNING *",
                                   dev["id"]):   # одна живая сессия на устройство
        if old["kind"] == "staff" and old["staff_sid"]:
            await db.revoke_staff_sessions(old["subject_id"], "app_relogin", sid=old["staff_sid"])
    out = await _issue(dev["kind"], subject, dev["company_id"], "pin" if pin is not None else "bio", request, device_id=dev["id"])
    out["device_token"] = new_token
    return out


@router.post("/api/app/pin/unlock")
async def app_pin_unlock(request: Request, body: dict = Body({})):
    return await _device_login(str(body.get("device_token") or ""), str(body.get("pin") or ""), request)


@router.post("/api/app/pin/biometric-unlock")
async def app_pin_bio_unlock(request: Request, body: dict = Body({})):
    return await _device_login(str(body.get("device_token") or ""), None, request)


@router.post("/api/app/pin/biometric")
async def app_pin_biometric(body: dict = Body({}), cur=Depends(current)):
    """Включить/выключить Face ID для этого устройства."""
    dev = await _device_find(_sha(str(body.get("device_token") or "")))
    if not dev or dev["kind"] != cur["kind"] or dev["subject_id"] != cur["subject"]["id"]:
        _err(404, "device_unknown", "Устройство не найдено", "Qurilma topilmadi")
    await db.pool.execute("UPDATE app_devices SET biometric=$2 WHERE id=$1", dev["id"], bool(body.get("on")))
    return {"ok": True, "biometric": bool(body.get("on"))}


@router.post("/api/app/pin/status")
async def app_pin_status(body: dict = Body({})):
    """Без входа: есть ли вход по PIN на этом телефоне, чей он (маска номера) и какой компании."""
    dev = await _device_find(_sha(str(body.get("device_token") or "")))
    if not dev or not dev["is_current"]:
        return {"ok": True, "active": False}
    subject = await _subject(dev["kind"], dev["subject_id"], dev["company_id"])
    if not subject:
        return {"ok": True, "active": False}
    label = (subject.get("first_name") or subject["login"]) if dev["kind"] == "staff" else f"{subject['phone'][:6]} *** {subject['phone'][-2:]}"
    comp = await db.pool.fetchrow("SELECT slug, name, logo_url FROM companies WHERE id=$1", dev["company_id"])
    return {"ok": True, "active": True, "kind": dev["kind"], "biometric": dev["biometric"], "label": label,
            "first_name": subject.get("first_name"), "company": _company_pub(dict(comp)) if comp else None}


@router.post("/api/app/me/logout-all")
async def app_logout_all(cur=Depends(current)):
    """«Выйти на всех устройствах»: все сессии приложения и все PIN/Face ID этого аккаунта (в этой компании)."""
    if cur["kind"] == "staff":
        await db.revoke_staff_sessions(cur["subject"]["id"], "app_logout_all")
    await revoke_subject(cur["kind"], cur["subject"]["id"])
    return {"ok": True}


@router.post("/api/app/me/delete")
async def app_delete_account(cur=Depends(current)):
    """Удаление аккаунта клиента в ЭТОЙ компании (требование App Store / Google Play).
    Удаляется вход (users, сессии, PIN, Face ID сайта); заказы остаются у компании — они по номеру телефона."""
    import pay
    if cur["kind"] != "client":
        _err(403, "staff_account", "Аккаунт сотрудника удаляет администратор", "Xodim akkauntini administrator o'chiradi")
    uid = cur["subject"]["id"]
    await revoke_subject("client", uid)
    await db.pool.execute("UPDATE app_push_tokens SET revoked_at=NOW() WHERE kind='client' AND subject_id=$1 AND revoked_at IS NULL", uid)
    if hasattr(pay, "delete_all_cards"):   # сохранённые карты PLUM — убрать и у нас, и в банке
        try:
            await pay.delete_all_cards(cur["company_id"], uid)
        except Exception as e:
            log.warning("app delete account %s: cards: %s", uid, type(e).__name__)
    try:
        await db.delete_site_user(uid)
    except Exception as e:
        log.error("app delete account %s: %s: %s", uid, type(e).__name__, e)
        _err(500, "server", "Не удалось удалить аккаунт. Напишите в поддержку", "Akkauntni o'chirib bo'lmadi. Qo'llab-quvvatlashga yozing")
    return {"ok": True}


# ══════════════════════════════════════
#  Push (Expo → APNs / FCM)
# ══════════════════════════════════════
APP_PUSH_STATUS = {   # сушка/упаковка — без уведомления, чтобы не надоедать
    "confirmed": ("Заказ подтверждён — скоро заберём ваши вещи", "Buyurtma tasdiqlandi — tez orada buyumlaringizni olib ketamiz"),
    "pickup":    ("Курьер едет к вам", "Kuryer sizga kelmoqda"),
    "received":  ("Вещи в мастерской — скоро замер и чистка", "Buyumlar ustaxonada — tez orada o'lchash va tozalash"),
    "washing":   ("Ваш заказ в чистке", "Buyurtmangiz tozalanmoqda"),
    "ready":     ("Заказ готов! Скоро привезём", "Buyurtma tayyor! Tez orada yetkazamiz"),
    "delivery":  ("Курьер везёт ваш заказ", "Kuryer buyurtmangizni olib kelmoqda"),
    "delivered": ("Заказ доставлен. Спасибо!", "Buyurtma yetkazildi. Rahmat!"),
    "cancelled": ("Заказ отменён", "Buyurtma bekor qilindi"),
}
APP_PUSH_LEAD = {
    "taken":     ("Менеджер взял вашу заявку и скоро позвонит", "Menejer arizangizni oldi va tez orada qo'ng'iroq qiladi"),
    "contacted": ("Менеджер работает над вашей заявкой", "Menejer arizangiz ustida ishlamoqda"),
    "callback":  ("Менеджер перезвонит вам в удобное время", "Menejer sizga qulay vaqtda qo'ng'iroq qiladi"),
    "no_answer": ("Не смогли до вас дозвониться — перезвоните нам", "Sizga qo'ng'iroq qila olmadik — bizga qo'ng'iroq qiling"),
}


def _lang_text(code: str | None, ru: str, uz: str) -> str:
    """Язык токена: ru | uz | uzc (Ўзбекча, кирилл — узбекский текст через транслит)."""
    if code == "uzc":
        from translit import to_cyrl
        return to_cyrl(uz)
    return uz if code == "uz" else ru


async def expo_send(messages: list[dict]) -> list[dict]:
    """Отправка в Expo пачками по 100; квитанции в том же порядке. Сбой сети → []."""
    out: list[dict] = []
    try:
        async with aiohttp.ClientSession() as s:
            for i in range(0, len(messages), 100):
                chunk = messages[i:i + 100]
                async with s.post(EXPO_PUSH_URL, json=chunk, timeout=aiohttp.ClientTimeout(total=15),
                                  headers={"Accept": "application/json"}) as r:
                    data = await r.json(content_type=None)
                    out.extend((data or {}).get("data") or [{"status": "error"}] * len(chunk))
    except Exception as e:
        log.warning("expo push: %s: %s", type(e).__name__, e)
    return out


async def _tokens_for_phone(company_id: int, phone: str) -> list[dict]:
    """Токены клиента ЭТОЙ компании по номеру заказа/заявки."""
    rows = await db.pool.fetch(
        "SELECT t.token, t.lang FROM app_push_tokens t JOIN users u ON u.id = t.subject_id "
        "WHERE t.kind='client' AND t.revoked_at IS NULL AND t.company_id=$1 AND u.company_id=$1 AND u.phone=$2",
        company_id, phone)
    return [dict(r) for r in rows]


async def _send_to(tokens: list[dict], title: tuple[str, str], text: tuple[str, str], data: dict) -> int:
    receipts = await expo_send([{
        "to": t["token"], "sound": "default", "priority": "high", "channelId": "default",
        "title": _lang_text(t["lang"], *title), "body": _lang_text(t["lang"], *text), "data": data,
    } for t in tokens])
    ok = 0
    for t, rc in zip(tokens, receipts):
        if rc.get("status") == "ok":
            ok += 1
        elif (rc.get("details") or {}).get("error") == "DeviceNotRegistered":
            await db.pool.execute("UPDATE app_push_tokens SET revoked_at=NOW() WHERE token=$1", t["token"])
    return ok


async def push_chat(company_id: int, session: dict, staff_name: str, text: str) -> None:
    """Ответ сотрудника в чате из приложения → push клиенту ЭТОЙ компании. Текст сообщения — как есть
    (без транслита: в нём могут быть ссылки, номера). Перенос из ARTEZ e7a4c12."""
    try:
        phone = session.get("client_phone")
        if not phone or not db.pool or session.get("company_id") != company_id:
            return
        tokens = await _tokens_for_phone(company_id, phone)
        if not tokens:
            return
        body = text if len(text) <= 160 else text[:157] + "…"
        receipts = await expo_send([{
            "to": t["token"], "sound": "default", "priority": "high", "channelId": "default",
            "title": f"💬 {staff_name}", "body": body, "data": {"kind": "chat", "code": session.get("code")},
        } for t in tokens])
        for t, rc in zip(tokens, receipts):
            if (rc.get("details") or {}).get("error") == "DeviceNotRegistered":
                await db.pool.execute("UPDATE app_push_tokens SET revoked_at=NOW() WHERE token=$1", t["token"])
    except Exception as e:
        log.warning("push_chat %s: %s: %s", session.get("code"), type(e).__name__, e)


@router.post("/api/app/chat/start")
async def app_chat_start(body: dict = Body({}), cur=Depends(current)):
    """Чат с компанией из приложения (перенос из ARTEZ e7a4c12/3e4dedb): клиент — по своему входу (телефон/имя
    из аккаунта, компания — из пропуска). Продолжает только свой открытый чат source='app' в своей компании,
    иначе — новый. Дальше — тот же WebSocket, что у сайта: /ws/chat/client/{code}; ответ сотрудника — push_chat."""
    m = _m()
    if cur["kind"] != "client":
        _err(403, "forbidden", "Чат — для клиентов", "Chat — mijozlar uchun")
    cid = cur["company_id"]
    u = cur["subject"]
    phone = (u.get("phone") or "").strip()
    name = (u.get("first_name") or "").strip()
    lang = {"ru": "ru", "uz": "uz", "uz_cyrl": "uz"}.get(str(body.get("lang") or ""), "uz")
    active = await db.get_active_app_chat_by_phone(phone) if phone else None
    if active and active.get("company_id") == cid:
        return {"ok": True, "code": active["code"], "status": active["status"]}
    code = m._gen_chat_code()
    session = await db.create_chat_session(code, phone, name, "", lang, source="app")
    if not session or session.get("company_id") != cid:
        raise HTTPException(500, "Не удалось создать сессию")
    welcome = m._tpl(await db.get_chat_template_text('welcome', lang) or
                     "Здравствуйте! 👋 Менеджер ответит вам в ближайшее время.", session)
    await db.add_chat_message(session['id'], 'bot', 'ARTEZ', welcome)
    label = m._chat_staff_name(session)
    await m._chat.broadcast_staff({
        "type": "new_chat", "code": code, "client_name": label, "client_phone": phone, "branch": "", "source": "app",
        "created_at": session['created_at'].isoformat() if hasattr(session.get('created_at'), 'isoformat') else str(session.get('created_at', '')),
    }, company_id=cid)
    online = m._chat.staff_online_ids(cid)
    for sid in await db.get_staff_for_chat_push(cid):
        if sid not in online:
            asyncio.create_task(m.send_web_push(sid, title="💬 Новый чат", body=f"{label} ждёт ответа", push_type="new_chat"))
    return {"ok": True, "code": code, "status": session["status"]}


# ── Push сотрудникам в приложение (перенос из ARTEZ b98f753/715c2f2/796bf7c/e45537a, 17.11.2026) ──
# Токен сотрудника — kind='staff', subject_id = staff.id; берём ТОЛЬКО токены этой компании и только
# активного сотрудника этой компании (уволенному не шлём).

async def _staff_tokens(company_id: int, staff_id: int | None = None, roles: list[str] | None = None) -> list[dict]:
    rows = await db.pool.fetch(
        "SELECT t.token, t.lang FROM app_push_tokens t JOIN staff s ON s.id = t.subject_id "
        "WHERE t.kind='staff' AND t.revoked_at IS NULL AND t.company_id=$1 AND s.company_id=$1 AND s.active IS TRUE "
        "AND ($2::int IS NULL OR s.id=$2) AND ($3::text[] IS NULL OR s.role = ANY($3::text[]))",
        company_id, staff_id, list(roles) if roles is not None else None)
    return [dict(r) for r in rows]


async def _send_plain(tokens: list[dict], title: str, body: str, data: dict) -> int:
    receipts = await expo_send([{
        "to": t["token"], "sound": "default", "priority": "high", "channelId": "default",
        "title": title, "body": body, "data": data,
    } for t in tokens])
    ok = 0
    for t, rc in zip(tokens, receipts):
        if rc.get("status") == "ok":
            ok += 1
        elif (rc.get("details") or {}).get("error") == "DeviceNotRegistered":
            await db.pool.execute("UPDATE app_push_tokens SET revoked_at=NOW() WHERE token=$1", t["token"])
    return ok


async def push_staff(company_id: int, staff_id: int, title: str, body: str, data: dict) -> None:
    """Push одному сотруднику своей компании; нет приложения — тихо ничего (в лог)."""
    try:
        if not db.pool or not company_id or not staff_id:
            return
        tokens = await _staff_tokens(company_id, staff_id=int(staff_id))
        if not tokens:
            log.info("app push staff=%s c=%s: нет токена приложения — «%s» не отправлен", staff_id, company_id, title)
            return
        await _send_plain(tokens, title, body, data)
    except Exception as e:
        log.warning("push_staff %s: %s: %s", staff_id, type(e).__name__, e)


async def push_roles(company_id: int, roles: list[str], title: str, body: str, data: dict,
                     only_id: int | None = None) -> None:
    """Push всем активным сотрудникам компании с ролью из roles (only_id — одному из них)."""
    try:
        if not db.pool or not company_id:
            return
        tokens = await _staff_tokens(company_id, staff_id=only_id, roles=roles)
        if tokens:
            await _send_plain(tokens, title, body, data)
    except Exception as e:
        log.warning("push_roles %s: %s: %s", roles, type(e).__name__, e)


async def push_route(company_id: int, route_id: int, added: int = 0) -> None:
    """Водителю: «Новый адрес в маршруте» (добавили остановки в уже отправленный/активный маршрут) или
    «Маршрут на сегодня» (отправили маршрут). Тап — экран «Маршрут»."""
    try:
        if not db.pool or not company_id:
            return
        async with db.pool.acquire() as c:
            r = await c.fetchrow(
                "SELECT driver_id, name, (app_sent_at IS NOT NULL OR status='active') AS sent, "
                "(SELECT COUNT(*) FROM route_orders WHERE route_id=routes.id)::int AS n "
                "FROM routes WHERE id=$1 AND company_id=$2", route_id, company_id)
        if not r or not r["driver_id"] or (added and not r["sent"]):
            return
        tokens = await _staff_tokens(company_id, staff_id=int(r["driver_id"]))
        if not tokens:
            return
        n, name = r["n"], r["name"] or ""
        if added:
            title = ("🚗 Новый адрес в маршруте", "🚗 Marshrutda yangi manzil")
            text = (f"Добавлено: {added} · всего остановок: {n}", f"Qo'shildi: {added} · jami to'xtash: {n}")
        else:
            title = ("🚗 Маршрут на сегодня", "🚗 Bugungi marshrut")
            text = (f"{name} · остановок: {n}", f"{name} · to'xtash: {n}")
        await _send_to(tokens, title, text, {"kind": "route", "route_id": route_id})
    except Exception as e:
        log.warning("push_route %s: %s: %s", route_id, type(e).__name__, e)


async def push_lead(company_id: int, lead_id: int, status: str) -> None:
    """Push клиенту, когда менеджер начал работу по заявке (contacted/callback/no_answer)."""
    text = APP_PUSH_LEAD.get(status)
    if not text or not db.pool:
        return
    try:
        lead = await db.pool.fetchrow("SELECT lead_code, client_phone FROM leads WHERE id=$1 AND company_id=$2", lead_id, company_id)
        if not lead or not lead["client_phone"]:
            return
        tokens = await _tokens_for_phone(company_id, lead["client_phone"])
        if tokens:
            code = lead["lead_code"] or ""
            await _send_to(tokens, (f"Заявка {code}", f"Ariza {code}"), text,
                           {"kind": "lead_status", "lead_code": code, "status": status})
    except Exception as e:
        log.warning("push_lead %s: %s: %s", lead_id, type(e).__name__, e)


async def push_worker() -> None:
    """Каждые 10 с: новые смены статуса заказов → push на телефоны клиента (своей компании)."""
    await asyncio.sleep(20)
    while True:
        try:
            async with db.pool.acquire() as c:
                await c.execute("UPDATE order_status_events SET sent_at=NOW(), result='stale' "
                                "WHERE sent_at IS NULL AND created_at < NOW() - INTERVAL '1 day'")
                events = [dict(r) for r in await c.fetch(
                    "UPDATE order_status_events SET sent_at=NOW() WHERE id IN (SELECT id FROM order_status_events "
                    "WHERE sent_at IS NULL ORDER BY id LIMIT 50 FOR UPDATE SKIP LOCKED) RETURNING *")]
            for ev in events:
                text = APP_PUSH_STATUS.get(ev["new_status"] or "")
                result = "skip"
                if text and ev["client_phone"] and ev["company_id"]:
                    tokens = await _tokens_for_phone(ev["company_id"], ev["client_phone"])
                    if not tokens:
                        result = "no_app"
                    else:
                        num = ev["order_num"]
                        ok = await _send_to(tokens, (f"Заказ №{num}", f"Buyurtma №{num}"), text,
                                            {"kind": "order_status", "order_num": num, "status": ev["new_status"]})
                        result = f"sent {ok}/{len(tokens)}"
                await db.pool.execute("UPDATE order_status_events SET result=$2 WHERE id=$1", ev["id"], result[:200])
        except Exception as e:
            log.warning("push_worker: %s: %s", type(e).__name__, e)
        await asyncio.sleep(10)


@router.post("/api/app/push/register")
async def app_push_register(body: dict = Body({}), cur=Depends(current)):
    import re
    token = str(body.get("token") or "")
    if not re.match(r"^Expo(nent)?PushToken\[[A-Za-z0-9_\-]{10,}\]$", token):
        _err(400, "validation", "Неверный токен уведомлений", "Bildirishnoma tokeni noto'g'ri")
    lang = {"uz": "uz", "uz_cyrl": "uzc"}.get(body.get("lang"), "ru")
    # токен телефона → этот человек этой компании (сменили аккаунт/компанию на телефоне — токен переходит)
    await db.pool.execute(
        "INSERT INTO app_push_tokens (company_id, kind, subject_id, token, platform, lang) VALUES ($1,$2,$3,$4,$5,$6) "
        "ON CONFLICT (token) DO UPDATE SET company_id=EXCLUDED.company_id, kind=EXCLUDED.kind, subject_id=EXCLUDED.subject_id, "
        "platform=EXCLUDED.platform, lang=EXCLUDED.lang, last_seen_at=NOW(), revoked_at=NULL",
        cur["company_id"], cur["kind"], cur["subject"]["id"], token[:200], str(body.get("platform") or "")[:10] or None, lang)
    return {"ok": True}


@router.post("/api/app/push/unregister")
async def app_push_unregister(body: dict = Body({})):
    """Выход: этот телефон больше не получает уведомления (токен знает только сам телефон)."""
    token = str(body.get("token") or "")
    if token:
        await db.pool.execute("UPDATE app_push_tokens SET revoked_at=NOW() WHERE token=$1 AND revoked_at IS NULL", token)
    return {"ok": True}
