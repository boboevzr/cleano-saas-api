import os
import re
import json
import asyncpg
import logging
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from contextvars import ContextVar

_FALLBACK_TZ = timezone(timedelta(hours=5))  # UTC+5 резерв если zoneinfo недоступна
DEFAULT_TZ_NAME = "Asia/Tashkent"

# ── SaaS: контекст company_id для текущего запроса ───────────────────────
# Устанавливается middleware в main.py из JWT-токена.
# Default=1 сохраняет обратную совместимость для любых функций, ещё не обновлённых.
_request_cid: ContextVar[int] = ContextVar("company_id", default=1)

def set_request_company(company_id: int) -> None:
    """Вызывается middleware перед каждым запросом."""
    _request_cid.set(company_id or 1)

def _cid() -> int:
    """Возвращает company_id текущего запроса (используется внутри DB-функций)."""
    return _request_cid.get()

# Кеш часовых поясов компаний: {company_id: "Asia/Tashkent"}
_company_tz_cache: dict[int, str] = {}


def _resolve_tz(tz_name: str):
    """Возвращает tzinfo объект по IANA-имени, фоллбек — UTC+5."""
    try:
        return ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, Exception):
        return _FALLBACK_TZ


def _tz_range(date_from: str, date_to: str, tz_name: str = DEFAULT_TZ_NAME):
    """Преобразует строки дат в UTC-границы для TIMESTAMPTZ-сравнения.

    tz_name — IANA timezone компании (например 'Asia/Tashkent').
    """
    tz = _resolve_tz(tz_name)
    df = datetime.fromisoformat(date_from).replace(tzinfo=tz)
    dt = datetime.fromisoformat(date_to).replace(tzinfo=tz) + timedelta(days=1)
    return df, dt


async def get_company_tz(company_id: int) -> str:
    """Возвращает IANA timezone компании (с кешем в памяти)."""
    if company_id in _company_tz_cache:
        return _company_tz_cache[company_id]
    if not pool:
        return DEFAULT_TZ_NAME
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT timezone FROM companies WHERE id=$1", company_id
        )
    tz = (row["timezone"] if row and row["timezone"] else DEFAULT_TZ_NAME)
    _company_tz_cache[company_id] = tz
    return tz


def invalidate_company_tz_cache(company_id: int):
    """Сбрасывает кеш после изменения timezone компании."""
    _company_tz_cache.pop(company_id, None)

DB_URL = os.getenv("DATABASE_URL", "")

pool = None

_TG_STATUS_DEFAULTS = [
    ("new",       True,  "🆕 Ваша заявка #{order_num} принята!\n\nУслуга: {service}\nДата вывоза: {pickup_date}\n\nМы свяжемся с вами для подтверждения.",
                         "🆕 #{order_num} raqamli arizangiz qabul qilindi!\n\nXizmat: {service}\nOlib ketish sanasi: {pickup_date}\n\nTasdiqlash uchun siz bilan bog'lanamiz."),
    ("confirmed", True,  "✅ Ваш заказ #{order_num} подтверждён!\n\nВодитель приедет: {pickup_date}\n📞 По вопросам: 1221",
                         "✅ #{order_num} raqamli buyurtmangiz tasdiqlandi!\n\nHaydovchi keladi: {pickup_date}\n📞 Savollar uchun: 1221"),
    ("pickup",    False, "🚗 Водитель выехал за вашим ковром #{order_num}.\n\nАдрес: {address}",
                         "🚗 Haydovchi #{order_num} gilamingiz uchun yo'lga chiqdi.\n\nManzil: {address}"),
    ("received",  True,  "📥 Ваш ковёр #{order_num} доставлен в мастерскую.\n\nНачинаем обработку. Сообщим о готовности!",
                         "📥 #{order_num} gilamingiz ustaxonaga yetkazildi.\n\nIshlashni boshladik. Tayyor bo'lganda xabar beramiz!"),
    ("washing",   False, "🧼 Ваш ковёр #{order_num} на мойке.",
                         "🧼 #{order_num} gilamingiz yuvish jarayonida."),
    ("drying",    False, "💨 Ваш ковёр #{order_num} на сушке.",
                         "💨 #{order_num} gilamingiz quritilmoqda."),
    ("packing",   False, "📦 Ваш ковёр #{order_num} упаковывается.",
                         "📦 #{order_num} gilamingiz qadoqlanmoqda."),
    ("ready",     True,  "✅ Ваш ковёр #{order_num} готов!\n\nМожем доставить или вы можете забрать сами.\n📞 Позвоните: 1221",
                         "✅ #{order_num} gilamingiz tayyor!\n\nYetkazib berishimiz yoki o'zingiz olib ketishingiz mumkin.\n📞 Qo'ng'iroq qiling: 1221"),
    ("delivery",  True,  "🚚 Ваш ковёр #{order_num} в пути!\n\nВодитель скоро будет у вас. Ждите звонка.",
                         "🚚 #{order_num} gilamingiz yo'lda!\n\nHaydovchi tez orada sizga etib keladi. Qo'ng'iroqni kuting."),
    ("delivered", True,  "🎉 Ваш ковёр #{order_num} доставлен!\n\nСпасибо что выбрали ARTEZ. Будем рады видеть вас снова! ⭐",
                         "🎉 #{order_num} gilamingiz yetkazildi!\n\nARTEZ ni tanlaganingiz uchun rahmat. Yana ko'rishishni xohlaymiz! ⭐"),
    ("cancelled", True,  "❌ Ваш заказ #{order_num} отменён.\n\nЕсли это ошибка — позвоните нам: 1221",
                         "❌ #{order_num} raqamli buyurtmangiz bekor qilindi.\n\nXato bo'lsa — qo'ng'iroq qiling: 1221"),
]

async def init_db():
    global pool
    if not DB_URL:
        logging.warning("DATABASE_URL not set, DB disabled")
        return
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    await create_tables()
    logging.info("✅ API: Database connected")


async def create_tables():
    # ── Шаг 0: SaaS — компании и филиалы ────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            id           SERIAL PRIMARY KEY,
            name         VARCHAR(200) NOT NULL,
            slug         VARCHAR(50)  UNIQUE NOT NULL,
            secret_key   VARCHAR(100) UNIQUE NOT NULL,
            plan         VARCHAR(20)  DEFAULT 'starter',
            max_branches INTEGER      DEFAULT 2,
            max_staff    INTEGER      DEFAULT 20,
            timezone     VARCHAR(50)  DEFAULT 'Asia/Tashkent',
            active       BOOLEAN      DEFAULT TRUE,
            created_at   TIMESTAMPTZ  DEFAULT NOW()
        );
        ALTER TABLE companies ADD COLUMN IF NOT EXISTS timezone      VARCHAR(50)  DEFAULT 'Asia/Tashkent';
        ALTER TABLE companies ADD COLUMN IF NOT EXISTS contact_name  VARCHAR(200) DEFAULT NULL;
        ALTER TABLE companies ADD COLUMN IF NOT EXISTS contact_phone VARCHAR(50)  DEFAULT NULL;
        ALTER TABLE companies ADD COLUMN IF NOT EXISTS contact_email VARCHAR(200) DEFAULT NULL;
        ALTER TABLE companies ADD COLUMN IF NOT EXISTS inn           VARCHAR(50)  DEFAULT NULL;
        ALTER TABLE companies ADD COLUMN IF NOT EXISTS legal_name    VARCHAR(300) DEFAULT NULL;
        ALTER TABLE companies ADD COLUMN IF NOT EXISTS address       TEXT         DEFAULT NULL;
        ALTER TABLE companies ADD COLUMN IF NOT EXISTS notes         TEXT         DEFAULT NULL;
        ALTER TABLE companies ADD COLUMN IF NOT EXISTS trial_days      INT          DEFAULT 14;
        ALTER TABLE companies ADD COLUMN IF NOT EXISTS whatsapp        TEXT         DEFAULT NULL;
        ALTER TABLE companies ADD COLUMN IF NOT EXISTS instagram       TEXT         DEFAULT NULL;
        ALTER TABLE companies ADD COLUMN IF NOT EXISTS tg_group_link   TEXT         DEFAULT NULL;
        ALTER TABLE companies ADD COLUMN IF NOT EXISTS tg_group_id     BIGINT       DEFAULT NULL;
        ALTER TABLE companies ADD COLUMN IF NOT EXISTS tg_channel_link TEXT         DEFAULT NULL;
        ALTER TABLE companies ADD COLUMN IF NOT EXISTS tg_channel_id   BIGINT       DEFAULT NULL;
        ALTER TABLE companies ADD COLUMN IF NOT EXISTS tg_admin_link   TEXT         DEFAULT NULL;
        ALTER TABLE companies ADD COLUMN IF NOT EXISTS tg_admin_id     BIGINT       DEFAULT NULL;
        ALTER TABLE companies ADD COLUMN IF NOT EXISTS logo_url        TEXT         DEFAULT NULL;
        CREATE TABLE IF NOT EXISTS branches (
            id                   SERIAL PRIMARY KEY,
            company_id           INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            slug                 VARCHAR(50)  NOT NULL,
            name_ru              VARCHAR(100) NOT NULL,
            name_uz              VARCHAR(100) NOT NULL DEFAULT '',
            lat                  NUMERIC(9,6) DEFAULT NULL,
            lon                  NUMERIC(9,6) DEFAULT NULL,
            phones               JSONB   DEFAULT '[]',
            tg_delivery_group_id BIGINT  DEFAULT NULL,
            tg_orders_channel_id BIGINT  DEFAULT NULL,
            active               BOOLEAN DEFAULT TRUE,
            created_at           TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(company_id, slug)
        );
        """)
        # Seed default company (id=1) — branches are managed via admin panel, not seeded here
        await c.execute("""
        INSERT INTO companies (id, name, slug, secret_key, plan, active)
        VALUES (1, 'default', 'default', 'change_me_before_use', 'starter', TRUE)
        ON CONFLICT DO NOTHING;
        """)
        await c.execute(
            "SELECT setval('companies_id_seq', GREATEST((SELECT MAX(id) FROM companies), 1), true)"
        )

    # ── Шаг 1: основные таблицы ──────────────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id              SERIAL PRIMARY KEY,
            phone           VARCHAR(20) UNIQUE NOT NULL,
            password_hash   VARCHAR(255) NOT NULL,
            first_name      VARCHAR(100),
            tg_id           BIGINT,
            is_verified     BOOLEAN DEFAULT FALSE,
            created_at      TIMESTAMP DEFAULT NOW(),
            updated_at      TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS sms_codes (
            id              SERIAL PRIMARY KEY,
            phone           VARCHAR(20) NOT NULL,
            code            VARCHAR(20) NOT NULL,
            purpose         VARCHAR(20) DEFAULT 'register'
                            CHECK (purpose IN ('register','login','reset','cleano_register','reset_attempt')),
            expires_at      TIMESTAMP NOT NULL,
            used            BOOLEAN DEFAULT FALSE,
            created_at      TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS config (
            key        VARCHAR(100) PRIMARY KEY,
            value      TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS units (
            id          SERIAL PRIMARY KEY,
            key         VARCHAR(20) UNIQUE NOT NULL,
            name_ru     VARCHAR(50) NOT NULL,
            name_uz     VARCHAR(50) NOT NULL,
            symbol_ru   VARCHAR(10) NOT NULL,
            symbol_uz   VARCHAR(10) NOT NULL,
            created_at  TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS prices (
            id              SERIAL PRIMARY KEY,
            service_key     VARCHAR(30) NOT NULL,
            type_key        VARCHAR(20) NOT NULL,
            price           INT NOT NULL,
            unit            VARCHAR(20) DEFAULT 'sum/m2',
            unit_key        VARCHAR(20) DEFAULT 'm2',
            min_order       NUMERIC(10,2) DEFAULT NULL,
            min_order_total NUMERIC(10,2) DEFAULT NULL,
            updated_at      TIMESTAMP DEFAULT NOW(),
            UNIQUE(service_key, type_key)
        );
        CREATE TABLE IF NOT EXISTS staff (
            id              SERIAL PRIMARY KEY,
            first_name      VARCHAR(100) NOT NULL,
            last_name       VARCHAR(100),
            middle_name     VARCHAR(100),
            phone           VARCHAR(20),
            login           VARCHAR(50),
            password_hash   TEXT,
            role            VARCHAR(30) DEFAULT 'callcenter',
            position        VARCHAR(100),
            branch          VARCHAR(50),
            tg_id           VARCHAR(50),
            tg_username     VARCHAR(100),
            salary_type     VARCHAR(20),
            salary_rate     NUMERIC(10,2),
            hire_date       DATE,
            note            TEXT,
            active          BOOLEAN DEFAULT TRUE,
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            updated_at      TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_users_phone     ON users(phone);
        CREATE INDEX IF NOT EXISTS idx_sms_codes_phone ON sms_codes(phone);
        """)

    # Опциональные миграции других таблиц — каждый отдельно чтобы не блокировать
    other_migrations = [
        """CREATE TABLE IF NOT EXISTS bot_fsm_state (
            key        TEXT PRIMARY KEY,
            state      TEXT,
            data       JSONB NOT NULL DEFAULT '{}',
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )""",
        # Клиенты бота заказов (order_bot_handlers.py). На проде (ARTEZ) эта таблица уже
        # существует с более старой эпохи (single-tenant artez_bot) и там UNIQUE(tg_id)
        # ГЛОБАЛЬНЫЙ (не per-company) — здесь CREATE IF NOT EXISTS её не трогает.
        # Для гринфилд-компаний (свежая БД) создаём сразу с company-scoped уникальностью.
        """CREATE TABLE IF NOT EXISTS clients (
            id           SERIAL PRIMARY KEY,
            company_id   INTEGER REFERENCES companies(id) DEFAULT 1,
            tg_id        BIGINT NOT NULL,
            tg_username  VARCHAR(100),
            first_name   VARCHAR(100),
            last_name    VARCHAR(100),
            phone        VARCHAR(20),
            lang         VARCHAR(5) DEFAULT 'ru',
            total_orders INT DEFAULT 0,
            created_at   TIMESTAMPTZ DEFAULT NOW(),
            updated_at   TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(company_id, tg_id)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_clients_company ON clients(company_id)",
        # На проде эта таблица уже существовала (создана Option A-ботом до этой миграции)
        # БЕЗ company_id и с ГЛОБАЛЬНЫМ UNIQUE(tg_id) — CREATE TABLE IF NOT EXISTS выше
        # был no-op. Дотягиваем существующую таблицу до company-scoped схемы вручную.
        "ALTER TABLE clients ADD COLUMN IF NOT EXISTS company_id INTEGER REFERENCES companies(id) DEFAULT 1",
        "UPDATE clients SET company_id = 1 WHERE company_id IS NULL",
        "ALTER TABLE clients DROP CONSTRAINT IF EXISTS clients_tg_id_key",
        "ALTER TABLE clients ADD CONSTRAINT clients_company_tg_uq UNIQUE (company_id, tg_id)",
        "ALTER TABLE orders ALTER COLUMN client_tg_id DROP NOT NULL",
        "ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_source_check",
        "ALTER TABLE orders ADD CONSTRAINT orders_source_check CHECK (source IN ('bot','site','staff'))",
        "ALTER TABLE prices  ADD COLUMN IF NOT EXISTS unit_key  VARCHAR(20)   DEFAULT 'm2'",
        "ALTER TABLE prices  ADD COLUMN IF NOT EXISTS min_order NUMERIC(10,2) DEFAULT NULL",
        "ALTER TABLE prices  ADD COLUMN IF NOT EXISTS min_order_total NUMERIC(10,2) DEFAULT NULL",
        "ALTER TABLE users   ADD COLUMN IF NOT EXISTS address    VARCHAR(200)  DEFAULT NULL",
        "ALTER TABLE users   ADD COLUMN IF NOT EXISTS car_plate  VARCHAR(20)   DEFAULT NULL",
        "ALTER TABLE users   ADD COLUMN IF NOT EXISTS osago_expiry DATE        DEFAULT NULL",
        "ALTER TABLE users   ADD COLUMN IF NOT EXISTS last_login  TIMESTAMPTZ  DEFAULT NULL",
        "UPDATE users SET last_login = updated_at WHERE last_login IS NULL AND updated_at IS NOT NULL",
        """CREATE TABLE IF NOT EXISTS tg_phone_links (
            phone      VARCHAR(20) PRIMARY KEY,
            tg_id      BIGINT      NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )""",
        "ALTER TABLE orders      ADD COLUMN IF NOT EXISTS total_price   INT          DEFAULT NULL",
        "ALTER TABLE orders      ADD COLUMN IF NOT EXISTS short_address VARCHAR(200) DEFAULT ''",
        "ALTER TABLE orders      ADD COLUMN IF NOT EXISTS discount_sum  NUMERIC(12,2) DEFAULT 0",
        "ALTER TABLE leads       ADD COLUMN IF NOT EXISTS short_address VARCHAR(200) DEFAULT ''",
        "ALTER TABLE crm_clients ADD COLUMN IF NOT EXISTS address       TEXT         DEFAULT ''",
        "ALTER TABLE crm_clients ADD COLUMN IF NOT EXISTS short_address VARCHAR(200) DEFAULT ''",
        "ALTER TABLE crm_clients ADD COLUMN IF NOT EXISTS lang          VARCHAR(2)   DEFAULT NULL",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_edit_items      BOOLEAN DEFAULT TRUE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_measure         BOOLEAN DEFAULT FALSE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_approve_measure BOOLEAN DEFAULT FALSE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_override_measure BOOLEAN DEFAULT FALSE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS order_stages        VARCHAR(100) DEFAULT NULL",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_create_order     BOOLEAN DEFAULT TRUE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_confirm_order    BOOLEAN DEFAULT TRUE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_edit_confirmed   BOOLEAN DEFAULT FALSE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_send_pickup      BOOLEAN DEFAULT FALSE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_edit_delivery    BOOLEAN DEFAULT FALSE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_accept_payment   BOOLEAN DEFAULT FALSE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_manage_cash      BOOLEAN DEFAULT FALSE",
        "UPDATE staff SET can_manage_cash=TRUE WHERE can_accept_payment=TRUE",
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS handed_to_staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL DEFAULT NULL",
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS created_by_staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL DEFAULT NULL",
        """CREATE TABLE IF NOT EXISTS order_activity (
            id          SERIAL PRIMARY KEY,
            order_id    INTEGER NOT NULL,
            staff_id    INTEGER REFERENCES staff(id) ON DELETE SET NULL,
            staff_name  VARCHAR(100) DEFAULT '',
            action      VARCHAR(50)  NOT NULL,
            details     TEXT         DEFAULT '',
            created_at  TIMESTAMPTZ  DEFAULT NOW()
        )""",
        "CREATE INDEX IF NOT EXISTS idx_order_activity_order ON order_activity(order_id)",
        """CREATE TABLE IF NOT EXISTS cash_handovers (
            id              SERIAL PRIMARY KEY,
            from_staff_id   INTEGER REFERENCES staff(id) ON DELETE SET NULL,
            to_staff_id     INTEGER REFERENCES staff(id) ON DELETE SET NULL,
            amount          NUMERIC(12,2) NOT NULL,
            note            TEXT DEFAULT '',
            created_at      TIMESTAMPTZ DEFAULT NOW()
        )""",
        "ALTER TABLE order_items ADD COLUMN IF NOT EXISTS review_claimed_by    INTEGER REFERENCES staff(id) ON DELETE SET NULL DEFAULT NULL",
        "ALTER TABLE order_items ADD COLUMN IF NOT EXISTS review_claimed_at    TIMESTAMPTZ DEFAULT NULL",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS gender             VARCHAR(1) DEFAULT 'M'",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS birth_date        DATE DEFAULT NULL",
        """CREATE TABLE IF NOT EXISTS staff_personal (
            staff_id         INTEGER PRIMARY KEY REFERENCES staff(id) ON DELETE CASCADE,
            passport_series  VARCHAR(10),
            passport_number  VARCHAR(20),
            pinfl            VARCHAR(20),
            home_address     TEXT,
            extra_phone      VARCHAR(20),
            children_count   INTEGER DEFAULT 0,
            marital_status   VARCHAR(20) DEFAULT 'single',
            spouse_name      VARCHAR(200),
            spouse_birth_date DATE,
            spouse_phone     VARCHAR(20),
            spouse_workplace VARCHAR(200),
            spouse_position  VARCHAR(200),
            updated_at       TIMESTAMPTZ DEFAULT NOW()
        )""",
        "ALTER TABLE leads       ADD COLUMN IF NOT EXISTS location           TEXT DEFAULT NULL",
        "ALTER TABLE leads       ADD COLUMN IF NOT EXISTS location_address   TEXT DEFAULT NULL",
        "ALTER TABLE leads       ADD COLUMN IF NOT EXISTS callback_at        TIMESTAMPTZ DEFAULT NULL",
        "ALTER TABLE leads       ADD COLUMN IF NOT EXISTS assigned_to        INTEGER REFERENCES staff(id) DEFAULT NULL",
        # Заполнить lead_code для лидов у которых он NULL
        """UPDATE leads SET lead_code = 'L-' || LPAD(id::text, 4, '0')
           WHERE lead_code IS NULL""",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS plain_password   VARCHAR(100) DEFAULT NULL",
        "ALTER TABLE order_items ADD COLUMN IF NOT EXISTS washer_login   VARCHAR(50)  DEFAULT NULL",
        "ALTER TABLE order_items ADD COLUMN IF NOT EXISTS actual_width_cm  NUMERIC(8,1) DEFAULT NULL",
        "ALTER TABLE order_items ADD COLUMN IF NOT EXISTS actual_length_cm NUMERIC(8,1) DEFAULT NULL",
        "ALTER TABLE order_items ADD COLUMN IF NOT EXISTS actual_sqm       NUMERIC(8,3) DEFAULT NULL",
        "ALTER TABLE order_items ADD COLUMN IF NOT EXISTS actual_total_sum NUMERIC(12,2) DEFAULT NULL",
        "ALTER TABLE order_items ADD COLUMN IF NOT EXISTS measure_status   VARCHAR(20)  DEFAULT 'pending'",
        # CRM leads расширение
        "ALTER TABLE leads ADD COLUMN IF NOT EXISTS converted_by   INTEGER REFERENCES staff(id)",
        "ALTER TABLE leads ADD COLUMN IF NOT EXISTS volunteer_id   INTEGER REFERENCES staff(id)",
        "ALTER TABLE leads ADD COLUMN IF NOT EXISTS converted_order VARCHAR(20)",
        "ALTER TABLE leads ADD COLUMN IF NOT EXISTS lead_code       VARCHAR(20) UNIQUE",
        "ALTER TABLE leads DROP CONSTRAINT IF EXISTS leads_status_check",
        "ALTER TABLE leads ADD CONSTRAINT leads_status_check CHECK (status IN ('new','contacted','callback','converted','lost','no_answer'))",
        # Агент: временный пароль
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS temp_password_hash    VARCHAR(255) DEFAULT NULL",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS temp_password_expires TIMESTAMPTZ  DEFAULT NULL",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS must_change_password  BOOLEAN DEFAULT FALSE",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS site_user_id          INTEGER REFERENCES users(id)",
        # Бот: верифицированный TG-номер клиента
        "ALTER TABLE clients ADD COLUMN IF NOT EXISTS tg_phone  VARCHAR(20) DEFAULT NULL",
        "ALTER TABLE clients ADD COLUMN IF NOT EXISTS language  VARCHAR(5)  DEFAULT 'ru'",
        # Users: привязка Telegram ID
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS tg_id BIGINT",
        # Users: обязательная смена пароля (для аккаунтов с сервер-сгенерированным
        # паролем — регистрация из бота, см. bot_register_client в main.py)
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS must_change_password BOOLEAN DEFAULT FALSE",
        # Staff: уникальный логин для ON CONFLICT
        "CREATE UNIQUE INDEX IF NOT EXISTS staff_login_unique ON staff(login)",
        # Orders: хранить текстовый адрес геолокации
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS location_address TEXT DEFAULT ''",
        # Orders: дедлайн (дата готовности)
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS deadline DATE DEFAULT NULL",
        # Замеры: причина отклонения
        "ALTER TABLE order_items ADD COLUMN IF NOT EXISTS reject_note TEXT DEFAULT NULL",
        # Название услуги на обоих языках (заполняется при сохранении из справочника —
        # раньше service хранил готовую строку на языке интерфейса сотрудника в момент
        # сохранения, из-за чего позиции показывались вперемешку RU/UZ клиенту)
        "ALTER TABLE order_items ADD COLUMN IF NOT EXISTS service_ru TEXT DEFAULT NULL",
        "ALTER TABLE order_items ADD COLUMN IF NOT EXISTS service_uz TEXT DEFAULT NULL",
        # Отсрочка доставки после статуса "Готов" — клиент просит подождать (ремонт,
        # переезд, отпуск). Заказ остаётся в статусе ready, поля работают поверх него.
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS postponed_until DATE DEFAULT NULL",
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS postpone_reason TEXT DEFAULT NULL",
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS postponed_by INTEGER REFERENCES staff(id) ON DELETE SET NULL DEFAULT NULL",
        # Видео-инструкция бота — отправляется один раз каждому новому клиенту
        # после выбора языка, флаг переживает рестарт бота (не in-memory)
        "ALTER TABLE clients ADD COLUMN IF NOT EXISTS welcome_video_sent BOOLEAN DEFAULT FALSE",
        # Маршруты: хранить TG message_id отправленных сообщений водителям
        "ALTER TABLE routes ADD COLUMN IF NOT EXISTS tg_delivery_msg_ids JSONB DEFAULT NULL",
        # Маршруты логистики
        """CREATE TABLE IF NOT EXISTS routes (
            id          SERIAL PRIMARY KEY,
            name        VARCHAR(200) NOT NULL,
            date        DATE NOT NULL,
            driver_id   INTEGER REFERENCES staff(id) ON DELETE SET NULL,
            branch      VARCHAR(50),
            type        VARCHAR(20) DEFAULT 'mixed',
            status      VARCHAR(20) DEFAULT 'planned',
            note        TEXT,
            created_at  TIMESTAMPTZ DEFAULT NOW(),
            updated_at  TIMESTAMPTZ DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS route_orders (
            id          SERIAL PRIMARY KEY,
            route_id    INTEGER NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
            order_id    INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
            sort_order  INTEGER DEFAULT 0,
            stop_status VARCHAR(20) DEFAULT 'pending',
            note        TEXT,
            created_at  TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(route_id, order_id)
        )""",
        # Заполнить created_by_staff_id для старых платежей по совпадению имени
        """UPDATE order_payments p
           SET created_by_staff_id = s.id
           FROM staff s
           WHERE p.created_by_staff_id IS NULL
             AND p.created_by IS NOT NULL
             AND p.created_by <> ''
             AND TRIM(COALESCE(s.last_name,'') || ' ' || COALESCE(s.first_name,'')) = TRIM(p.created_by)""",
        # Касса: статус передачи наличных
        "ALTER TABLE cash_handovers ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'pending'",
        "ALTER TABLE cash_handovers ADD COLUMN IF NOT EXISTS confirmed_at TIMESTAMPTZ DEFAULT NULL",
        "ALTER TABLE cash_handovers ADD COLUMN IF NOT EXISTS confirmed_by INTEGER REFERENCES staff(id) ON DELETE SET NULL DEFAULT NULL",
        "ALTER TABLE cash_handovers ADD COLUMN IF NOT EXISTS to_type VARCHAR(20) DEFAULT 'staff'",
        # Платежи: подтверждение и фото чека
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS confirmed BOOLEAN DEFAULT FALSE",
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS confirmed_by INTEGER REFERENCES staff(id) ON DELETE SET NULL DEFAULT NULL",
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS confirmed_at TIMESTAMPTZ DEFAULT NULL",
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS receipt_url TEXT DEFAULT NULL",
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS reject_note TEXT DEFAULT NULL",
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS receipt_file_id TEXT DEFAULT NULL",
        # Таблица настроек (создаём если не существует + гарантируем одну строку)
        "CREATE TABLE IF NOT EXISTS settings (id SERIAL PRIMARY KEY)",
        "INSERT INTO settings DEFAULT VALUES ON CONFLICT DO NOTHING",
        # Настройки: ТГ канал кассы
        "ALTER TABLE settings ADD COLUMN IF NOT EXISTS cash_tg_channel_id VARCHAR(50) DEFAULT NULL",
        # Настройки: канал медиафайлов (замеры, чеки и т.д.)
        "ALTER TABLE settings ADD COLUMN IF NOT EXISTS media_channel_id VARCHAR(50) DEFAULT NULL",
        # Уведомления о новых пользователях сайта
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS notify_new_users BOOLEAN DEFAULT FALSE",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS hide_client_phone BOOLEAN DEFAULT FALSE",
        "ALTER TABLE clients ADD COLUMN IF NOT EXISTS blocked BOOLEAN DEFAULT FALSE",
        # Автодозвон: кампании
        """CREATE TABLE IF NOT EXISTS autodial_campaigns (
            id             SERIAL PRIMARY KEY,
            name           VARCHAR(200) NOT NULL,
            status         VARCHAR(20) DEFAULT 'draft',
            ivr_exten      VARCHAR(20) DEFAULT '1000',
            max_parallel   INT DEFAULT 3,
            source_type    VARCHAR(20) DEFAULT 'both',
            manual_phones  TEXT DEFAULT '',
            created_at     TIMESTAMPTZ DEFAULT NOW(),
            started_at     TIMESTAMPTZ,
            finished_at    TIMESTAMPTZ,
            total_count    INT DEFAULT 0,
            dialed_count   INT DEFAULT 0,
            answered_count INT DEFAULT 0,
            failed_count   INT DEFAULT 0
        )""",
        # Автодозвон: записи звонков
        """CREATE TABLE IF NOT EXISTS autodial_calls (
            id            SERIAL PRIMARY KEY,
            campaign_id   INT REFERENCES autodial_campaigns(id) ON DELETE CASCADE,
            source_type   VARCHAR(20) DEFAULT 'manual',
            source_id     INT DEFAULT NULL,
            phone         VARCHAR(20) NOT NULL,
            name          VARCHAR(200) DEFAULT '',
            status        VARCHAR(30) DEFAULT 'pending',
            ami_action_id VARCHAR(100),
            started_at    TIMESTAMPTZ,
            answered_at   TIMESTAMPTZ,
            hangup_at     TIMESTAMPTZ,
            hangup_cause  VARCHAR(50),
            pressed_key   VARCHAR(5),
            created_at    TIMESTAMPTZ DEFAULT NOW()
        )""",
        "CREATE INDEX IF NOT EXISTS idx_autodial_calls_campaign ON autodial_calls(campaign_id)",
        # Автодозвон: группы контактов
        """CREATE TABLE IF NOT EXISTS autodial_groups (
            id         SERIAL PRIMARY KEY,
            name       VARCHAR(200) NOT NULL,
            notes      TEXT DEFAULT '',
            created_at TIMESTAMPTZ DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS autodial_group_members (
            id          SERIAL PRIMARY KEY,
            group_id    INT REFERENCES autodial_groups(id) ON DELETE CASCADE,
            phone       VARCHAR(20) NOT NULL,
            name        VARCHAR(200) DEFAULT '',
            source_type VARCHAR(20) DEFAULT 'manual',
            source_id   INT DEFAULT NULL,
            created_at  TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(group_id, phone)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_agm_group ON autodial_group_members(group_id)",
        "ALTER TABLE autodial_campaigns ADD COLUMN IF NOT EXISTS group_ids JSONB DEFAULT '[]'",
        "ALTER TABLE autodial_campaigns ADD COLUMN IF NOT EXISTS caller_id VARCHAR(20) DEFAULT '1000'",
        "ALTER TABLE autodial_campaigns ADD COLUMN IF NOT EXISTS sched_time_from TIME DEFAULT '09:00'",
        "ALTER TABLE autodial_campaigns ADD COLUMN IF NOT EXISTS sched_time_to   TIME DEFAULT '21:00'",
        "ALTER TABLE autodial_campaigns ADD COLUMN IF NOT EXISTS sched_days SMALLINT[] DEFAULT '{0,1,2,3,4,5,6}'",
        "ALTER TABLE autodial_campaigns ADD COLUMN IF NOT EXISTS sched_date_from DATE",
        "ALTER TABLE autodial_campaigns ADD COLUMN IF NOT EXISTS sched_date_to   DATE",
        """CREATE TABLE IF NOT EXISTS autodial_callerids (
            id         SERIAL PRIMARY KEY,
            number     VARCHAR(20) NOT NULL,
            label      VARCHAR(100) DEFAULT '',
            sort_order INT DEFAULT 0
        )""",
        """CREATE TABLE IF NOT EXISTS autodial_ivrs (
            id         SERIAL PRIMARY KEY,
            exten      VARCHAR(20) NOT NULL,
            label      VARCHAR(100) DEFAULT '',
            sort_order INT DEFAULT 0
        )""",
        "ALTER TABLE autodial_ivrs ADD COLUMN IF NOT EXISTS ivr_group VARCHAR(30) DEFAULT 'promo'",
        # SMS группы и контакты (отдельно от автодозвона)
        """CREATE TABLE IF NOT EXISTS sms_groups (
            id          SERIAL PRIMARY KEY,
            name        VARCHAR(200) NOT NULL,
            description TEXT DEFAULT '',
            created_at  TIMESTAMPTZ DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS sms_contacts (
            id               SERIAL PRIMARY KEY,
            group_id         INT REFERENCES sms_groups(id) ON DELETE CASCADE,
            phone            VARCHAR(20) NOT NULL,
            name             VARCHAR(200) DEFAULT '',
            status           VARCHAR(20) DEFAULT 'active',
            last_sms_at      TIMESTAMPTZ,
            last_sms_status  VARCHAR(20),
            created_at       TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(group_id, phone)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_sms_contacts_group ON sms_contacts(group_id)",
        "CREATE INDEX IF NOT EXISTS idx_sms_contacts_status ON sms_contacts(status)",
        # Смены: поддержка открытия смены и привязки операций
        "ALTER TABLE cash_shifts ADD COLUMN IF NOT EXISTS opened_at TIMESTAMPTZ DEFAULT NULL",
        "ALTER TABLE cash_shifts ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'closed'",
        "ALTER TABLE cash_shifts ADD COLUMN IF NOT EXISTS opened_by INTEGER REFERENCES staff(id) ON DELETE SET NULL DEFAULT NULL",
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS shift_id INTEGER REFERENCES cash_shifts(id) ON DELETE SET NULL DEFAULT NULL",
        "ALTER TABLE cash_handovers ADD COLUMN IF NOT EXISTS shift_id INTEGER REFERENCES cash_shifts(id) ON DELETE SET NULL DEFAULT NULL",
        "ALTER TABLE expenses ADD COLUMN IF NOT EXISTS shift_id INTEGER REFERENCES cash_shifts(id) ON DELETE SET NULL DEFAULT NULL",
        # Расходы: источник выплаты (сейф / банк / наличные)
        "ALTER TABLE expenses ADD COLUMN IF NOT EXISTS paid_from VARCHAR(20) DEFAULT NULL",
        "ALTER TABLE expenses ADD COLUMN IF NOT EXISTS paid_by INTEGER REFERENCES staff(id) ON DELETE SET NULL DEFAULT NULL",
        "ALTER TABLE expenses ADD COLUMN IF NOT EXISTS paid_at TIMESTAMPTZ DEFAULT NULL",
        # Передачи наличных: TG-сообщение для редактирования после confirm/reject
        "ALTER TABLE cash_handovers ADD COLUMN IF NOT EXISTS tg_chat_id BIGINT DEFAULT NULL",
        "ALTER TABLE cash_handovers ADD COLUMN IF NOT EXISTS tg_msg_id BIGINT DEFAULT NULL",
        # Расходы: получатель (сотрудник) — для зарплаты/аванса
        "ALTER TABLE expenses ADD COLUMN IF NOT EXISTS for_staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL DEFAULT NULL",
        # Категории расходов: флаг «требует указать сотрудника»
        "ALTER TABLE expense_categories ADD COLUMN IF NOT EXISTS for_staff BOOLEAN DEFAULT FALSE",
        # Зарплата сотрудников — рабочих дней в месяц
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS salary_work_days INTEGER DEFAULT 26",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS fired BOOLEAN DEFAULT FALSE",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS can_view_timesheet BOOLEAN DEFAULT FALSE",
        # Процентные ставки по типам работ
        """CREATE TABLE IF NOT EXISTS staff_salary_percents (
            id       SERIAL PRIMARY KEY,
            staff_id INTEGER REFERENCES staff(id) ON DELETE CASCADE,
            role     VARCHAR(20) NOT NULL,
            percent  NUMERIC(5,2) NOT NULL DEFAULT 0,
            UNIQUE(staff_id, role)
        )""",
        # Ставки за единицу измерения по услугам
        """CREATE TABLE IF NOT EXISTS staff_salary_per_unit (
            id          SERIAL PRIMARY KEY,
            staff_id    INTEGER REFERENCES staff(id) ON DELETE CASCADE,
            service_key VARCHAR(30) NOT NULL,
            type_key    VARCHAR(20) NOT NULL,
            total_rate  NUMERIC(10,2) DEFAULT 0,
            unit_rate   NUMERIC(10,2) DEFAULT 0,
            rate_type   VARCHAR(10) DEFAULT 'sum',
            UNIQUE(staff_id, service_key, type_key)
        )""",
        "ALTER TABLE staff_salary_per_unit ADD COLUMN IF NOT EXISTS rate_type VARCHAR(10) DEFAULT 'sum'",
        # KPI правила
        """CREATE TABLE IF NOT EXISTS staff_salary_kpi (
            id           SERIAL PRIMARY KEY,
            staff_id     INTEGER REFERENCES staff(id) ON DELETE CASCADE,
            metric       VARCHAR(30) NOT NULL,
            target_value NUMERIC(10,2) NOT NULL,
            bonus_type   VARCHAR(10) DEFAULT 'fixed',
            bonus_value  NUMERIC(10,2) NOT NULL DEFAULT 0
        )""",
        # Начисления агентам за лиды (комиссия)
        """CREATE TABLE IF NOT EXISTS staff_commissions (
            id           SERIAL PRIMARY KEY,
            staff_id     INTEGER REFERENCES staff(id) ON DELETE SET NULL,
            order_id     INTEGER REFERENCES orders(id) ON DELETE SET NULL,
            order_num    VARCHAR(20) DEFAULT '',
            lead_id      INTEGER REFERENCES leads(id) ON DELETE SET NULL,
            amount       NUMERIC(12,2) NOT NULL DEFAULT 0,
            percent      NUMERIC(5,2)  NOT NULL DEFAULT 0,
            order_total  NUMERIC(12,2) NOT NULL DEFAULT 0,
            note         TEXT DEFAULT '',
            status       VARCHAR(20) DEFAULT 'pending',
            paid_at      TIMESTAMPTZ DEFAULT NULL,
            created_at   TIMESTAMPTZ DEFAULT NOW()
        )""",
        "CREATE INDEX IF NOT EXISTS idx_staff_commissions_staff ON staff_commissions(staff_id)",
        "CREATE INDEX IF NOT EXISTS idx_staff_commissions_order ON staff_commissions(order_id)",
        # Табель рабочего времени
        """CREATE TABLE IF NOT EXISTS timesheet (
            id          SERIAL PRIMARY KEY,
            staff_id    INTEGER NOT NULL REFERENCES staff(id) ON DELETE CASCADE,
            date        DATE NOT NULL,
            hours       NUMERIC(4,1) DEFAULT 8,
            type        VARCHAR(20) DEFAULT 'work'
                        CHECK (type IN ('work','overtime','sick','vacation','dayoff')),
            note        TEXT DEFAULT '',
            created_at  TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(staff_id, date)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_timesheet_staff  ON timesheet(staff_id)",
        "CREATE INDEX IF NOT EXISTS idx_timesheet_date   ON timesheet(date)",
        # Отметки прихода/ухода (для почасовых сотрудников) — журнал событий (несколько пар в день)
        "DROP TABLE IF EXISTS staff_attendance",
        """CREATE TABLE IF NOT EXISTS staff_attendance_events (
            id          SERIAL PRIMARY KEY,
            staff_id    INTEGER NOT NULL REFERENCES staff(id) ON DELETE CASCADE,
            event_type  VARCHAR(10) NOT NULL CHECK (event_type IN ('in','out')),
            created_at  TIMESTAMPTZ DEFAULT NOW()
        )""",
        "CREATE INDEX IF NOT EXISTS idx_attendance_events_staff ON staff_attendance_events(staff_id)",
        "CREATE INDEX IF NOT EXISTS idx_attendance_events_created ON staff_attendance_events(created_at)",
        # Журнал отправок чека клиенту — для предупреждения о повторной отправке
        """CREATE TABLE IF NOT EXISTS order_receipt_log (
            id          SERIAL PRIMARY KEY,
            order_id    INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
            staff_id    INTEGER REFERENCES staff(id) ON DELETE SET NULL,
            staff_name  VARCHAR(100) DEFAULT '',
            note        TEXT DEFAULT '',
            tg_chat_id    BIGINT DEFAULT NULL,
            tg_message_id BIGINT DEFAULT NULL,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        )""",
        "CREATE INDEX IF NOT EXISTS idx_receipt_log_order ON order_receipt_log(order_id)",
        "ALTER TABLE order_receipt_log ADD COLUMN IF NOT EXISTS tg_chat_id    BIGINT DEFAULT NULL",
        "ALTER TABLE order_receipt_log ADD COLUMN IF NOT EXISTS tg_message_id BIGINT DEFAULT NULL",

        # ── Подтверждение телефона при регистрации компании на cleano.uz ──
        "ALTER TABLE companies ADD COLUMN IF NOT EXISTS contact_tg_id BIGINT DEFAULT NULL",
        "ALTER TABLE sms_codes DROP CONSTRAINT IF EXISTS sms_codes_purpose_check",
        "ALTER TABLE sms_codes ADD CONSTRAINT sms_codes_purpose_check "
        "CHECK (purpose IN ('register','login','reset','cleano_register'))",
        """CREATE TABLE IF NOT EXISTS cleano_tg_links (
            phone      VARCHAR(20) PRIMARY KEY,
            tg_id      BIGINT      NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS cleano_phone_verifications (
            phone       VARCHAR(20) PRIMARY KEY,
            verified_at TIMESTAMPTZ NOT NULL,
            method      VARCHAR(10) NOT NULL CHECK (method IN ('sms','telegram')),
            tg_id       BIGINT      DEFAULT NULL
        )""",
        # Персистентная ссылка суперадмина t.me/<bot>?start=company_<id> — между "/start company_X"
        # и последующим "поделиться контактом" нужно помнить, к какой компании привязываем этот chat_id.
        """CREATE TABLE IF NOT EXISTS cleano_pending_company_link (
            chat_id     BIGINT PRIMARY KEY,
            company_id  INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            expires_at  TIMESTAMPTZ NOT NULL
        )""",
        # forgot-password: попытки подтверждения кода пишут маркер purpose='reset_attempt'
        # с кодом-словом "attempt" (7 символов) — старая схема (code VARCHAR(6),
        # purpose CHECK без 'reset_attempt') валила это StringDataRightTruncationError
        # при каждой попытке ввести код сброса пароля (тот же баг, что и в проде).
        "ALTER TABLE sms_codes ALTER COLUMN code TYPE VARCHAR(20)",
        "ALTER TABLE sms_codes DROP CONSTRAINT IF EXISTS sms_codes_purpose_check",
        "ALTER TABLE sms_codes ADD CONSTRAINT sms_codes_purpose_check "
        "CHECK (purpose IN ('register','login','reset','cleano_register','reset_attempt'))",
    ]
    async with pool.acquire() as c:
        for sql in other_migrations:
            try:
                await c.execute(sql)
            except Exception as e:
                logging.warning(f"migration skipped ({sql[:80]!r}): {e}")
        # IVR 7000 — основной, всегда должен быть в списке
        await c.execute(
            "INSERT INTO autodial_ivrs (exten,label,ivr_group) "
            "SELECT '7000','Общее приветствие','promo' WHERE NOT EXISTS "
            "(SELECT 1 FROM autodial_ivrs WHERE exten='7000')"
        )
        # IVR: если нет записей с группами — чистим старые и засеваем новые
        cnt_grouped = await c.fetchval("SELECT COUNT(*) FROM autodial_ivrs WHERE ivr_group IS NOT NULL AND ivr_group != ''")
        if cnt_grouped == 0:
            await c.execute("DELETE FROM autodial_ivrs")
            ivr_seeds = [
                # 📢 Рекламные (7001–7005)
                ("7001","Реклама 1","promo"),("7002","Реклама 2","promo"),
                ("7003","Реклама 3","promo"),("7004","Реклама 4","promo"),("7005","Реклама 5","promo"),
                # 🎉 Поздравления (7011–7015)
                ("7011","Поздравление 1","greetings"),("7012","Поздравление 2","greetings"),
                ("7013","Поздравление 3","greetings"),("7014","Поздравление 4","greetings"),("7015","Поздравление 5","greetings"),
                # 🔔 Напоминания (7021–7025)
                ("7021","Напоминание 1","reminders"),("7022","Напоминание 2","reminders"),
                ("7023","Напоминание 3","reminders"),("7024","Напоминание 4","reminders"),("7025","Напоминание 5","reminders"),
            ]
            await c.executemany(
                "INSERT INTO autodial_ivrs (exten,label,ivr_group) VALUES ($1,$2,$3)",
                ivr_seeds
            )

    # ── Шаг 2: миграции staff (добавляем недостающие колонки) ────────────
    staff_migrations = [
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS phone         VARCHAR(20)",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS last_name     VARCHAR(100)",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS login         VARCHAR(50)",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS password_hash TEXT",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS role          VARCHAR(30) DEFAULT 'callcenter'",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS position      VARCHAR(100)",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS branch        VARCHAR(50)",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS tg_id         VARCHAR(50)",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS tg_username   VARCHAR(100)",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS salary_type   VARCHAR(20)",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS salary_rate   NUMERIC(10,2)",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS hire_date     DATE",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS note          TEXT",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS middle_name   VARCHAR(100)",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS active        BOOLEAN DEFAULT TRUE",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS updated_at    TIMESTAMPTZ DEFAULT NOW()",
        "CREATE INDEX IF NOT EXISTS idx_staff_login ON staff(login)",
        # Снять CHECK constraints на role — бот мог ограничить список ролей
        """DO $$ DECLARE r RECORD;
           BEGIN
             FOR r IN SELECT conname FROM pg_constraint
                      WHERE conrelid='staff'::regclass AND contype='c'
             LOOP EXECUTE format('ALTER TABLE staff DROP CONSTRAINT %I', r.conname);
             END LOOP;
           END $$""",
        # Снять NOT NULL с role и tg_id если был
        "ALTER TABLE staff ALTER COLUMN role   DROP NOT NULL",
        "ALTER TABLE staff ALTER COLUMN tg_id  DROP NOT NULL",
    ]
    async with pool.acquire() as c:
        for sql in staff_migrations:
            try:
                await c.execute(sql)
            except Exception:
                pass  # колонка или индекс уже существует

    # ── Шаг 3: таблица crm_clients ───────────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS crm_clients (
            id            SERIAL PRIMARY KEY,
            phone         VARCHAR(20) UNIQUE NOT NULL,
            phone2        VARCHAR(20),
            first_name    VARCHAR(100),
            last_name     VARCHAR(100),
            tg_id         BIGINT,
            tg_username   VARCHAR(100),
            source        VARCHAR(20) DEFAULT 'unknown',
            status        VARCHAR(20) DEFAULT 'new',
            note          TEXT,
            orders_count  INT DEFAULT 0,
            total_spent   NUMERIC(12,2) DEFAULT 0,
            last_order_at TIMESTAMPTZ,
            created_at    TIMESTAMPTZ DEFAULT NOW(),
            updated_at    TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_crm_clients_phone  ON crm_clients(phone);
        CREATE INDEX IF NOT EXISTS idx_crm_clients_tg_id  ON crm_clients(tg_id);
        CREATE INDEX IF NOT EXISTS idx_crm_clients_status ON crm_clients(status);
        """)

    # ── Push subscriptions ──────────────────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS push_subscriptions (
            id         SERIAL PRIMARY KEY,
            staff_id   INTEGER NOT NULL,
            endpoint   TEXT    NOT NULL,
            p256dh     TEXT    NOT NULL,
            auth       TEXT    NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_push_staff_id ON push_subscriptions(staff_id);
        -- UNIQUE был раньше только на endpoint (сама подписка браузера) — из-за
        -- этого один физический браузер мог "принадлежать" только ОДНОМУ staff_id
        -- одновременно: ON CONFLICT(endpoint) в upsert_push_subscription молча
        -- переписывал staff_id на того, кто подписался последним с этого устройства.
        -- Найдено 2026-08-21: push-уведомление для сотрудника компании id=3 пришло
        -- сотруднику компании id=1 — тот же браузер использовался для входа в оба
        -- staff-аккаунта, и второй "украл" подписку у первого. Теперь UNIQUE на
        -- пару (staff_id, endpoint) — один браузер может держать подписки сразу
        -- нескольких staff_id одновременно, без взаимной перезаписи.
        ALTER TABLE push_subscriptions DROP CONSTRAINT IF EXISTS push_subscriptions_endpoint_key;
        CREATE UNIQUE INDEX IF NOT EXISTS idx_push_staff_endpoint ON push_subscriptions(staff_id, endpoint);
        """)

    # ── Шаг 3б: таблица contacts (справочник) ───────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS contacts (
            id          SERIAL PRIMARY KEY,
            first_name  VARCHAR(100) DEFAULT '',
            last_name   VARCHAR(100) DEFAULT '',
            middle_name VARCHAR(100) DEFAULT '',
            phone       VARCHAR(20) NOT NULL,
            phone2      VARCHAR(20) DEFAULT '',
            address     TEXT DEFAULT '',
            source      VARCHAR(50) DEFAULT 'ARTEZ',
            created_at  TIMESTAMPTZ DEFAULT NOW(),
            updated_at  TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(phone)
        );
        CREATE INDEX IF NOT EXISTS idx_contacts_phone ON contacts(phone);
        CREATE INDEX IF NOT EXISTS idx_contacts_name  ON contacts(first_name, last_name);
        """)
        # Добавляем short_address если ещё нет (миграция)
        await c.execute("""
        ALTER TABLE contacts ADD COLUMN IF NOT EXISTS short_address VARCHAR(200) DEFAULT '';
        CREATE INDEX IF NOT EXISTS idx_contacts_short_addr ON contacts(short_address);
        """)

    # ── Шаг 4: таблица leads ─────────────────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            id              SERIAL PRIMARY KEY,
            lead_num        VARCHAR(20) UNIQUE,
            client_name     VARCHAR(200),
            client_phone    VARCHAR(20) NOT NULL,
            service         VARCHAR(100),
            branch          VARCHAR(50),
            city            VARCHAR(100),
            address         TEXT,
            note            TEXT,
            status          VARCHAR(30) DEFAULT 'new'
                            CHECK (status IN ('new','contacted','qualified','converted','lost')),
            assigned_to     INTEGER REFERENCES staff(id),
            created_by      INTEGER REFERENCES staff(id),
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            updated_at      TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_leads_phone  ON leads(client_phone);
        CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);
        """)

    # ── Шаг 4б: CRM — журнал звонков и напоминания ───────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS lead_calls (
            id          SERIAL PRIMARY KEY,
            lead_id     INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
            operator_id INTEGER REFERENCES staff(id),
            action      VARCHAR(50) NOT NULL,
            note        TEXT,
            scheduled_at TIMESTAMPTZ,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_lead_calls_lead ON lead_calls(lead_id);
        CREATE TABLE IF NOT EXISTS lead_reminders (
            id          SERIAL PRIMARY KEY,
            lead_id     INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
            staff_id    INTEGER REFERENCES staff(id),
            remind_at   TIMESTAMPTZ NOT NULL,
            message     TEXT,
            sent_browser BOOLEAN DEFAULT FALSE,
            sent_tg      BOOLEAN DEFAULT FALSE,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_lead_reminders_staff ON lead_reminders(staff_id, sent_browser);
        """)
        # Разовая чистка: напоминания, которые остались висеть (ещё не сработали)
        # по лидам, уже закрытым (конвертированы/потеряны) до этого фикса.
        # Идемпотентно — на следующих запусках просто ничего не находит.
        await c.execute("""
            DELETE FROM lead_reminders
            WHERE sent_tg = FALSE AND sent_browser = FALSE
              AND lead_id IN (SELECT id FROM leads WHERE status IN ('converted','lost'))
        """)

    await ensure_agent_notifications_table()
    # Разовая чистка: уведомления "Пора перезвонить", уже созданные по лидам,
    # которые с тех пор закрыли (конвертированы/потеряны) — до этого фикса
    # ничего их не убирало. Идемпотентно.
    async with pool.acquire() as c:
        await c.execute("""
            DELETE FROM agent_notifications
            WHERE action = 'callback'
              AND lead_id IN (SELECT id FROM leads WHERE status IN ('converted','lost'))
        """)
    await ensure_washer_notifications_table()

    # ── Шаг 4б: основная таблица заказов ────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id                          SERIAL PRIMARY KEY,
            order_num                   VARCHAR(20) UNIQUE NOT NULL,
            company_id                  INTEGER REFERENCES companies(id) DEFAULT 1,
            client_tg_id                BIGINT,
            client_tg_username          VARCHAR(100),
            client_first_name           VARCHAR(100),
            client_last_name            VARCHAR(100),
            client_phone                VARCHAR(20),
            source                      VARCHAR(20) DEFAULT 'bot',
            branch                      VARCHAR(30),
            city                        VARCHAR(100),
            address                     TEXT,
            short_address               VARCHAR(200) DEFAULT '',
            location                    VARCHAR(100),
            location_address            TEXT DEFAULT '',
            service                     VARCHAR(200),
            pickup_date                 VARCHAR(50),
            pickup_time                 VARCHAR(100),
            note                        TEXT,
            deadline                    DATE DEFAULT NULL,
            status                      VARCHAR(30) DEFAULT 'new',
            operator_tg_id              BIGINT,
            operator_username           VARCHAR(100),
            operator_first_name         VARCHAR(100),
            operator_last_name          VARCHAR(100),
            accepted_at                 TIMESTAMP,
            washer_tg_id                BIGINT,
            washer_username             VARCHAR(100),
            washer_first_name           VARCHAR(100),
            washer_last_name            VARCHAR(100),
            washing_started_at          TIMESTAMP,
            washing_done_at             TIMESTAMP,
            driver_pickup_tg_id         BIGINT,
            driver_pickup_username      VARCHAR(100),
            driver_pickup_first_name    VARCHAR(100),
            driver_pickup_last_name     VARCHAR(100),
            pickup_at                   TIMESTAMP,
            driver_delivery_tg_id       BIGINT,
            driver_delivery_username    VARCHAR(100),
            driver_delivery_first_name  VARCHAR(100),
            driver_delivery_last_name   VARCHAR(100),
            delivered_at                TIMESTAMP,
            total_price                 INT DEFAULT NULL,
            discount_sum                NUMERIC(12,2) DEFAULT 0,
            delivery_discount           NUMERIC(12,2) DEFAULT 0,
            manual_discount             NUMERIC(12,2) DEFAULT 0,
            payment_method              VARCHAR(20) DEFAULT NULL,
            prepaid_amount              NUMERIC(12,2) DEFAULT 0,
            payment_status              VARCHAR(20) DEFAULT 'unpaid',
            paid_at                     TIMESTAMPTZ DEFAULT NULL,
            created_at                  TIMESTAMP DEFAULT NOW(),
            updated_at                  TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS order_status_history (
            id              SERIAL PRIMARY KEY,
            order_num       VARCHAR(20) NOT NULL,
            old_status      VARCHAR(30),
            new_status      VARCHAR(30) NOT NULL,
            changed_by_tg_id      BIGINT,
            changed_by_name       VARCHAR(200),
            note            TEXT,
            created_at      TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_orders_company ON orders(company_id);
        CREATE INDEX IF NOT EXISTS idx_orders_status  ON orders(status);
        """)

    # ── Шаг 4в: позиции услуг в заказах ─────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS order_items (
            id              SERIAL PRIMARY KEY,
            order_id        INTEGER NOT NULL,
            service         VARCHAR(200) NOT NULL,
            width_cm        NUMERIC(8,1),
            length_cm       NUMERIC(8,1),
            sqm             NUMERIC(8,3) NOT NULL,
            price_per_sqm   NUMERIC(10,2) NOT NULL DEFAULT 0,
            total_sum       NUMERIC(12,2) GENERATED ALWAYS AS (sqm * price_per_sqm) STORED,
            created_at      TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
        """)

    # ── Шаг 5: дефолтные единицы измерения ───────────────────────────────
    async with pool.acquire() as c:
        units_count = await c.fetchval("SELECT COUNT(*) FROM units")
        if units_count == 0:
            default_units = [
                ("m2",  "Квадратный метр", "Kvadrat metr",  "м²",  "m²"),
                ("m",   "Метр",            "Metr",          "м",   "m"),
                ("pcs", "Штука",           "Dona",          "шт",  "dona"),
                ("cm",  "Сантиметр",       "Santimetr",     "см",  "sm"),
                ("cm2", "Кв. сантиметр",   "Kv. santimetr", "см²", "sm²"),
                ("kg",  "Килограмм",       "Kilogramm",     "кг",  "kg"),
            ]
            await c.executemany("""
                INSERT INTO units (key, name_ru, name_uz, symbol_ru, symbol_uz)
                VALUES ($1, $2, $3, $4, $5) ON CONFLICT (key) DO NOTHING
            """, default_units)

    # ── Шаг 5: таблица шаблонов Telegram-уведомлений ────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS tg_status_messages (
            status      VARCHAR(30) PRIMARY KEY,
            enabled     BOOLEAN     DEFAULT TRUE,
            message_ru  TEXT        DEFAULT '',
            message_uz  TEXT        DEFAULT ''
        );
        """)
        # Дефолтные шаблоны если таблица пустая
        await c.executemany("""
            INSERT INTO tg_status_messages (status, enabled, message_ru, message_uz)
            VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING
        """, _TG_STATUS_DEFAULTS)

    # ── Шаг 5: фото/видео заказов ────────────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS order_photos (
            id            SERIAL PRIMARY KEY,
            order_id      INTEGER      NOT NULL,
            tg_file_id    TEXT         NOT NULL,
            tg_file_type  VARCHAR(20)  NOT NULL DEFAULT 'photo',
            photo_type    VARCHAR(20)  NOT NULL DEFAULT 'before',
            note          TEXT         DEFAULT '',
            uploaded_by   VARCHAR(100) DEFAULT '',
            created_at    TIMESTAMPTZ  DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_order_photos_order ON order_photos(order_id);
        """)

    # ── Шаг 6: оплата и касса ────────────────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_method  VARCHAR(20)   DEFAULT NULL;
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS prepaid_amount  NUMERIC(12,2) DEFAULT 0;
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_status  VARCHAR(20)   DEFAULT 'unpaid';
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS paid_at         TIMESTAMPTZ   DEFAULT NULL;
        """)
        await c.execute("""
        CREATE TABLE IF NOT EXISTS order_payments (
            id          SERIAL PRIMARY KEY,
            order_id    INTEGER       NOT NULL,
            amount      NUMERIC(12,2) NOT NULL,
            method      VARCHAR(20)   NOT NULL,
            purpose     VARCHAR(50)   DEFAULT 'payment',
            note        TEXT          DEFAULT '',
            created_by  VARCHAR(100)  DEFAULT '',
            created_at  TIMESTAMPTZ   DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_order_payments_order ON order_payments(order_id);
        ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS purpose VARCHAR(50) DEFAULT 'payment';
        """)
        await c.execute("""
        CREATE TABLE IF NOT EXISTS order_item_media (
            id           SERIAL PRIMARY KEY,
            item_id      INTEGER       NOT NULL,
            order_id     INTEGER       NOT NULL,
            tg_file_id   VARCHAR(200)  NOT NULL,
            tg_file_type VARCHAR(20)   DEFAULT 'photo',
            created_by   VARCHAR(100)  DEFAULT '',
            created_at   TIMESTAMPTZ   DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_item_media_item ON order_item_media(item_id);
        """)
        await c.execute("""
        CREATE TABLE IF NOT EXISTS cash_shifts (
            id             SERIAL PRIMARY KEY,
            shift_date     DATE          NOT NULL,
            closed_by      VARCHAR(100)  DEFAULT '',
            closed_at      TIMESTAMPTZ   DEFAULT NOW(),
            cash_total     NUMERIC(12,2) DEFAULT 0,
            card_total     NUMERIC(12,2) DEFAULT 0,
            transfer_total NUMERIC(12,2) DEFAULT 0,
            grand_total    NUMERIC(12,2) DEFAULT 0,
            orders_count   INTEGER       DEFAULT 0,
            note           TEXT          DEFAULT ''
        );
        """)

    # ── Шаг 7: тип заказа (стандарт/экспресс) ───────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS service_type VARCHAR(20) DEFAULT 'standard';
        """)

    # ── Шаг 8: тип вывоза и скидка при самовывозе ────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS pickup_type VARCHAR(10) DEFAULT 'courier';
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS self_pickup_discount NUMERIC(5,2) DEFAULT 0;
        ALTER TABLE leads  ADD COLUMN IF NOT EXISTS pickup_type VARCHAR(10) DEFAULT 'courier';
        """)

    # ── Шаг 9: ручная скидка на заказ ────────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS manual_discount NUMERIC(12,2) DEFAULT 0;
        """)

    # ── Шаг 10: тип вывоза и скидка при самовывозе (из мастерской) ───────
    async with pool.acquire() as c:
        await c.execute("""
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_type VARCHAR(10) DEFAULT 'courier';
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_discount NUMERIC(12,2) DEFAULT 0;
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_discount_pct NUMERIC(5,2) DEFAULT 0;
        ALTER TABLE leads  ADD COLUMN IF NOT EXISTS delivery_type VARCHAR(10) DEFAULT 'courier';
        """)

    # ── Шаг 11: флаг pending position request ────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS pos_request_pending BOOLEAN DEFAULT FALSE;
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS pos_request_at TIMESTAMPTZ DEFAULT NULL;
        """)

    # ── Шаг 12: контакты филиалов ────────────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS site_contacts (
            branch      VARCHAR(50) PRIMARY KEY,
            branch_name VARCHAR(100) NOT NULL,
            phones      JSONB NOT NULL DEFAULT '[]',
            telegram    VARCHAR(200) DEFAULT '',
            whatsapp    VARCHAR(200) DEFAULT '',
            instagram   VARCHAR(200) DEFAULT ''
        );
        INSERT INTO site_contacts (branch, branch_name, phones, telegram, whatsapp, instagram)
        VALUES
          ('navoi',     'Навои',     '["1221","+998792221221"]', '', '', ''),
          ('zarafshan', 'Зарафшан',  '["1221","+998792221221"]', '', '', '')
        ON CONFLICT DO NOTHING;
        """)

    # ── Шаг 13: источник и tg_id клиента в лидах ───────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        ALTER TABLE leads ADD COLUMN IF NOT EXISTS source       VARCHAR(20) DEFAULT 'staff';
        ALTER TABLE leads ADD COLUMN IF NOT EXISTS client_tg_id BIGINT DEFAULT NULL;
        """)

    # ── Шаг 14: дата и время вывоза в лидах ─────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        ALTER TABLE leads ADD COLUMN IF NOT EXISTS pickup_date VARCHAR(20)  DEFAULT '';
        ALTER TABLE leads ADD COLUMN IF NOT EXISTS pickup_time VARCHAR(100) DEFAULT '';
        """)

    # ── Шаг 15b: tg_id водителя в платежах ──────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS driver_tg_id BIGINT;
        """)

    # ── Шаг 15: таблица услуг с именами RU/UZ ────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS services (
            key       VARCHAR(30) PRIMARY KEY,
            name_ru   VARCHAR(100) NOT NULL DEFAULT '',
            name_uz   VARCHAR(100) NOT NULL DEFAULT '',
            emoji     VARCHAR(10)  NOT NULL DEFAULT '',
            order_idx INTEGER      NOT NULL DEFAULT 0
        );
        INSERT INTO services (key, name_ru, name_uz, emoji, order_idx) VALUES
            ('carpet',      'Чистка ковра',         'Gilam tozalash',      '🧺', 1),
            ('carpet_home', 'Чистка ковра на дому', 'Uyda gilam tozalash', '🏠', 2),
            ('sofa',        'Диван, кресло',         'Divan, kreslo',       '🛋', 3),
            ('mattress',    'Матрас, одеяло',        'Matras, ko''rpa',     '🛏', 4),
            ('curtains',    'Шторы',                 'Pardalar',            '🪟', 5)
        ON CONFLICT DO NOTHING;
        """)

    # ── Шаг 16: учёт долгов по заказам ──────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        ALTER TABLE staff ADD COLUMN IF NOT EXISTS can_approve_debt BOOLEAN DEFAULT FALSE;
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS debt_responsible_id INTEGER REFERENCES staff(id) ON DELETE SET NULL;
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS debt_due_date DATE;
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS debt_approved_at TIMESTAMPTZ;
        ALTER TABLE route_orders ADD COLUMN IF NOT EXISTS driver_confirmed BOOLEAN DEFAULT FALSE;
        """)

    # ── Шаг 17: запросы скидок от водителей ──────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS discount_requests (
            id              SERIAL PRIMARY KEY,
            order_id        INTEGER REFERENCES orders(id) ON DELETE CASCADE,
            order_num       VARCHAR(50),
            driver_tg_id    BIGINT,
            requested_amount NUMERIC(12,2) NOT NULL,
            status          VARCHAR(20) DEFAULT 'pending',
            approved_amount NUMERIC(12,2),
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            resolved_at     TIMESTAMPTZ,
            resolved_by     INTEGER REFERENCES staff(id) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS idx_discount_requests_status ON discount_requests(status);
        """)

    # ── Шаг 18: запросы долгового одобрения ──────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS debt_approval_requests (
            id              SERIAL PRIMARY KEY,
            order_id        INTEGER REFERENCES orders(id) ON DELETE CASCADE,
            order_num       VARCHAR(50),
            driver_tg_id    BIGINT,
            debt_amount     NUMERIC(12,2),
            mgr_msgs        JSONB DEFAULT '{}',
            status          VARCHAR(20) DEFAULT 'pending',
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            resolved_at     TIMESTAMPTZ,
            resolved_by     INTEGER REFERENCES staff(id) ON DELETE SET NULL,
            resolution      VARCHAR(20),
            responsible_id  INTEGER REFERENCES staff(id) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS idx_debt_approval_status ON debt_approval_requests(status);
        """)

    # ── Шаг 19: флаг водителя ────────────────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("ALTER TABLE staff ADD COLUMN IF NOT EXISTS can_drive BOOLEAN DEFAULT FALSE;")

    # ── Шаг 20: промо-акции (generic-модель, одна кампания засеяна) ──────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS promotions (
            id              SERIAL PRIMARY KEY,
            code            VARCHAR(50) UNIQUE NOT NULL,
            title_ru        VARCHAR(200) NOT NULL,
            title_uz        VARCHAR(200) NOT NULL,
            text_ru         TEXT DEFAULT '',
            text_uz         TEXT DEFAULT '',
            discount_pct    NUMERIC(5,2) NOT NULL DEFAULT 0,
            starts_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            ends_at         TIMESTAMPTZ NOT NULL,
            window_hours    INTEGER NOT NULL DEFAULT 48,
            sound_enabled   BOOLEAN DEFAULT TRUE,
            target_new_only BOOLEAN DEFAULT TRUE,
            is_active       BOOLEAN DEFAULT TRUE,
            created_at      TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS promo_user_state (
            id              SERIAL PRIMARY KEY,
            promotion_id    INTEGER NOT NULL REFERENCES promotions(id) ON DELETE CASCADE,
            user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            shown_at        TIMESTAMPTZ,
            expires_at      TIMESTAMPTZ,
            channel         VARCHAR(10),
            used_order_id   INTEGER REFERENCES orders(id) ON DELETE SET NULL,
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(promotion_id, user_id)
        );
        ALTER TABLE leads  ADD COLUMN IF NOT EXISTS promo_id INTEGER REFERENCES promotions(id) ON DELETE SET NULL;
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS promo_id INTEGER REFERENCES promotions(id) ON DELETE SET NULL;
        """)
        # Единственная засеянная кампания: -20% до 31.08.2026 23:59 (Ташкент, UTC+5 → 18:59 UTC)
        await c.execute("""
        INSERT INTO promotions (code, title_ru, title_uz, text_ru, text_uz,
                                 discount_pct, ends_at, window_hours,
                                 sound_enabled, target_new_only, is_active)
        VALUES (
            'aug2026_20pct',
            'Акция: скидка 20%!',
            'Aksiya: 20% chegirma!',
            'Успей заказать до 31 августа и получи скидку 20%! Действует 48 часов.',
            '31 avgustgacha ulgurib buyurtma bering va 20% chegirma oling! 48 soat davomida amal qiladi.',
            20,
            '2026-08-31 18:59:00+00',
            48,
            TRUE, FALSE, TRUE
        )
        ON CONFLICT (code) DO NOTHING;
        """)
        # Условие "только для клиентов без заказов" убрано — акция теперь доступна
        # всем зарегистрированным клиентам. Правим уже засеянную строку тоже.
        await c.execute("""
        UPDATE promotions SET
            target_new_only = FALSE,
            text_ru = 'Успей заказать до 31 августа и получи скидку 20%! Действует 48 часов.',
            text_uz = '31 avgustgacha ulgurib buyurtma bering va 20% chegirma oling! 48 soat davomida amal qiladi.'
        WHERE code = 'aug2026_20pct';
        """)

    # ── Шаг 21: постоянная категория скидки клиента (пенсионер/инвалид) ──
    async with pool.acquire() as c:
        await c.execute("""
        ALTER TABLE crm_clients ADD COLUMN IF NOT EXISTS discount_category               VARCHAR(20);
        ALTER TABLE crm_clients ADD COLUMN IF NOT EXISTS discount_category_pct           NUMERIC(5,2);
        ALTER TABLE crm_clients ADD COLUMN IF NOT EXISTS discount_category_photo_file_id VARCHAR(200);
        ALTER TABLE crm_clients ADD COLUMN IF NOT EXISTS discount_category_verified_by   INTEGER REFERENCES staff(id) ON DELETE SET NULL;
        ALTER TABLE crm_clients ADD COLUMN IF NOT EXISTS discount_category_verified_at   TIMESTAMPTZ;
        """)

    # ── Шаг 22: SaaS — company_id для всех операционных таблиц ─────────
    _cid = "INTEGER REFERENCES companies(id) DEFAULT 1"
    company_id_migrations = [
        # Users / Staff
        f"ALTER TABLE users              ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE staff              ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE staff_personal     ADD COLUMN IF NOT EXISTS company_id {_cid}",
        # CRM
        f"ALTER TABLE crm_clients        ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE contacts           ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE site_contacts      ADD COLUMN IF NOT EXISTS company_id {_cid}",
        # Orders
        f"ALTER TABLE orders             ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE order_items        ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE order_payments     ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE order_photos       ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE order_item_media   ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE order_activity     ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE order_receipt_log  ADD COLUMN IF NOT EXISTS company_id {_cid}",
        # Leads
        f"ALTER TABLE leads              ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE lead_calls         ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE lead_reminders     ADD COLUMN IF NOT EXISTS company_id {_cid}",
        # Catalog
        f"ALTER TABLE prices             ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE services           ADD COLUMN IF NOT EXISTS company_id {_cid}",
        # Logistics
        f"ALTER TABLE routes             ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE route_orders       ADD COLUMN IF NOT EXISTS company_id {_cid}",
        # Cash / Expenses
        f"ALTER TABLE cash_shifts        ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE cash_handovers     ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE expense_categories ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE expenses           ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE salary_ledger      ADD COLUMN IF NOT EXISTS company_id {_cid}",
        # Settings (one row per company after Etap 2)
        f"ALTER TABLE settings           ADD COLUMN IF NOT EXISTS company_id {_cid}",
        # Autodial
        f"ALTER TABLE autodial_campaigns     ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE autodial_calls         ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE autodial_groups        ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE autodial_group_members ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE autodial_callerids     ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE autodial_ivrs          ADD COLUMN IF NOT EXISTS company_id {_cid}",
        # SMS
        f"ALTER TABLE sms_groups         ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE sms_contacts       ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE sms_dispatches     ADD COLUMN IF NOT EXISTS company_id {_cid}",
        # Salary detail tables
        f"ALTER TABLE staff_salary_percents  ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE staff_salary_per_unit  ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE staff_salary_kpi       ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE staff_commissions      ADD COLUMN IF NOT EXISTS company_id {_cid}",
        # Timesheet / Attendance
        f"ALTER TABLE timesheet              ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE staff_attendance_events ADD COLUMN IF NOT EXISTS company_id {_cid}",
        # Notifications / Push
        f"ALTER TABLE agent_notifications    ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE washer_notifications   ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE push_subscriptions     ADD COLUMN IF NOT EXISTS company_id {_cid}",
        # Approvals
        f"ALTER TABLE discount_requests      ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE debt_approval_requests ADD COLUMN IF NOT EXISTS company_id {_cid}",
        # Promotions
        f"ALTER TABLE promotions             ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE promo_user_state       ADD COLUMN IF NOT EXISTS company_id {_cid}",
        # Telegram / Plans / Chat
        f"ALTER TABLE tg_status_messages     ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE tg_phone_links         ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE plans                  ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE chat_sessions          ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE chat_messages          ADD COLUMN IF NOT EXISTS company_id {_cid}",
        f"ALTER TABLE chat_templates         ADD COLUMN IF NOT EXISTS company_id {_cid}",
        # Indexes on the most-queried tables
        "CREATE INDEX IF NOT EXISTS idx_staff_company       ON staff(company_id)",
        "CREATE INDEX IF NOT EXISTS idx_orders_company      ON orders(company_id)",
        "CREATE INDEX IF NOT EXISTS idx_leads_company       ON leads(company_id)",
        "CREATE INDEX IF NOT EXISTS idx_crm_clients_company ON crm_clients(company_id)",
        "CREATE INDEX IF NOT EXISTS idx_routes_company      ON routes(company_id)",
        "CREATE INDEX IF NOT EXISTS idx_expenses_company    ON expenses(company_id)",
    ]
    async with pool.acquire() as c:
        for sql in company_id_migrations:
            try:
                await c.execute(sql)
            except Exception:
                pass

    # ── Шаг 23: Расширение таблицы branches (SaaS — все поля per-branch) ─
    async with pool.acquire() as c:
        for sql in [
            "ALTER TABLE branches ADD COLUMN IF NOT EXISTS tg_leads_group_id      BIGINT  DEFAULT NULL",
            "ALTER TABLE branches ADD COLUMN IF NOT EXISTS tg_delivery_channel_id BIGINT  DEFAULT NULL",
            "ALTER TABLE branches ADD COLUMN IF NOT EXISTS tg_delivery_channel_link TEXT   DEFAULT NULL",
            "ALTER TABLE branches ADD COLUMN IF NOT EXISTS telegram_link           TEXT    DEFAULT NULL",
            "ALTER TABLE branches ADD COLUMN IF NOT EXISTS admin_tg_link           TEXT    DEFAULT NULL",
            "ALTER TABLE branches ADD COLUMN IF NOT EXISTS whatsapp                TEXT    DEFAULT NULL",
            "ALTER TABLE branches ADD COLUMN IF NOT EXISTS instagram               TEXT    DEFAULT NULL",
        ]:
            try:
                await c.execute(sql)
            except Exception:
                pass

    # ── Шаг 24: координаты цеха ───────────────────────────────────────────
    async with pool.acquire() as c:
        for sql in [
            "ALTER TABLE branches ADD COLUMN IF NOT EXISTS workshop_lat  FLOAT DEFAULT NULL",
            "ALTER TABLE branches ADD COLUMN IF NOT EXISTS workshop_lon  FLOAT DEFAULT NULL",
        ]:
            try:
                await c.execute(sql)
            except Exception:
                pass

    # ── Шаг 25: ссылки TG-групп + telegram_group_id + admin_tg_id ──────────
    async with pool.acquire() as c:
        for sql in [
            "ALTER TABLE branches ADD COLUMN IF NOT EXISTS tg_leads_group_link      TEXT    DEFAULT NULL",
            "ALTER TABLE branches ADD COLUMN IF NOT EXISTS tg_orders_channel_link   TEXT    DEFAULT NULL",
            "ALTER TABLE branches ADD COLUMN IF NOT EXISTS tg_delivery_group_link   TEXT    DEFAULT NULL",
            "ALTER TABLE branches ADD COLUMN IF NOT EXISTS telegram_group_id        BIGINT  DEFAULT NULL",
            "ALTER TABLE branches ADD COLUMN IF NOT EXISTS admin_tg_id              BIGINT  DEFAULT NULL",
        ]:
            try:
                await c.execute(sql)
            except Exception:
                pass

    # ── Шаг 26: role_permissions в settings ─────────────────────────────────────
    async with pool.acquire() as c:
        try:
            await c.execute("ALTER TABLE settings ADD COLUMN IF NOT EXISTS role_permissions JSONB DEFAULT NULL")
        except Exception:
            pass

    # ── Шаг 26б: отделы и должности ─────────────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS departments (
            id          SERIAL PRIMARY KEY,
            company_id  INTEGER NOT NULL,
            name        VARCHAR(100) NOT NULL,
            description VARCHAR(500),
            created_at  TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS positions (
            id          SERIAL PRIMARY KEY,
            company_id  INTEGER NOT NULL,
            dept_id     INTEGER REFERENCES departments(id) ON DELETE SET NULL,
            name        VARCHAR(100) NOT NULL,
            role        VARCHAR(30),
            salary_type VARCHAR(20),
            salary_rate BIGINT,
            description VARCHAR(500),
            sort_order  INTEGER NOT NULL DEFAULT 0,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        );
        ALTER TABLE positions ADD COLUMN IF NOT EXISTS sort_order INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE departments ADD COLUMN IF NOT EXISTS name_uz VARCHAR(100) NOT NULL DEFAULT '';
        ALTER TABLE departments ADD COLUMN IF NOT EXISTS description_uz VARCHAR(500) NOT NULL DEFAULT '';
        ALTER TABLE positions ADD COLUMN IF NOT EXISTS name_uz VARCHAR(100) NOT NULL DEFAULT '';
        ALTER TABLE positions ADD COLUMN IF NOT EXISTS description_uz VARCHAR(500) NOT NULL DEFAULT '';
        """)

    logging.info("✅ API: Tables created/verified")


async def ensure_saas_schema():
    """Шаг 27: таблицы планов/подписок/оплат SaaS."""
    if not pool:
        return
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS saas_plans (
            id           SERIAL PRIMARY KEY,
            slug         VARCHAR(50)  UNIQUE NOT NULL,
            display_name VARCHAR(100) NOT NULL,
            max_branches INT          NOT NULL DEFAULT 1,
            max_staff    INT          NOT NULL DEFAULT 7,
            base_price   INT          NOT NULL DEFAULT 500000,
            active       BOOLEAN      NOT NULL DEFAULT TRUE,
            sort_order   INT          NOT NULL DEFAULT 0,
            created_at   TIMESTAMPTZ  DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS saas_plan_pricing (
            id      SERIAL PRIMARY KEY,
            plan_id INT NOT NULL REFERENCES saas_plans(id) ON DELETE CASCADE,
            month   INT NOT NULL CHECK (month BETWEEN 1 AND 12),
            price   INT NOT NULL,
            UNIQUE(plan_id, month)
        );
        CREATE TABLE IF NOT EXISTS saas_subscriptions (
            id             SERIAL PRIMARY KEY,
            company_id     INT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            plan_id        INT NOT NULL REFERENCES saas_plans(id),
            start_date     DATE NOT NULL,
            end_date       DATE NOT NULL,
            status         VARCHAR(20) NOT NULL DEFAULT 'active',
            balance        INT         NOT NULL DEFAULT 0,
            notes          TEXT,
            created_at     TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS saas_payments (
            id              SERIAL PRIMARY KEY,
            company_id      INT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            subscription_id INT REFERENCES saas_subscriptions(id) ON DELETE SET NULL,
            amount          INT  NOT NULL,
            payment_date    DATE NOT NULL DEFAULT CURRENT_DATE,
            note            TEXT,
            created_at      TIMESTAMPTZ DEFAULT NOW()
        );
        """)
        # Засеять начальные планы только если таблица пустая
        count = await c.fetchval("SELECT COUNT(*) FROM saas_plans")
        if count == 0:
            await c.execute("""
            INSERT INTO saas_plans (slug, display_name, max_branches, max_staff, base_price, active, sort_order) VALUES
            ('starter',      'Starter',  1, 7,  500000,  true, 10),
            ('starter_plus', 'Starter+', 1, 15, 800000,  true, 20),
            ('basic',        'Basic',    2, 10, 1200000, true, 30),
            ('basic_plus',   'Basic+',   2, 20, 1500000, true, 40),
            ('pro',          'Pro',      5, 40, 2000000, true, 50)
            ON CONFLICT (slug) DO NOTHING;
            """)
            await c.execute("""
            INSERT INTO saas_plan_pricing (plan_id, month, price)
            SELECT id, m, base_price FROM saas_plans CROSS JOIN generate_series(1, 12) AS m
            ON CONFLICT (plan_id, month) DO NOTHING;
            """)
    logging.info("✅ API: SaaS schema (step 27) ready")

    # ── Шаг 28: цены per-company — исправить UNIQUE constraint ───────────
    async with pool.acquire() as c:
        for sql in [
            "UPDATE prices SET company_id=0 WHERE company_id IS NULL",
            "UPDATE services SET company_id=0 WHERE company_id IS NULL",
            "ALTER TABLE prices DROP CONSTRAINT IF EXISTS prices_service_key_type_key_key",
            """DO $$ BEGIN ALTER TABLE prices ADD CONSTRAINT prices_co_svc_tp UNIQUE (company_id, service_key, type_key); EXCEPTION WHEN duplicate_object THEN NULL; END $$""",
        ]:
            try:
                await c.execute(sql)
            except Exception:
                pass
    logging.info("✅ API: prices multi-tenancy constraint (step 28) ready")

    # ── Шаг 29: services per-company UNIQUE constraint ────────────────────
    async with pool.acquire() as c:
        # Drop all existing unique constraints on services.key (whatever they're named)
        try:
            rows = await c.fetch("""
                SELECT conname FROM pg_constraint
                WHERE conrelid='services'::regclass AND contype='u'
                  AND array_length(conkey,1)=1
                  AND conkey[1]=(SELECT attnum FROM pg_attribute
                                 WHERE attrelid='services'::regclass AND attname='key')
            """)
            for r in rows:
                try:
                    await c.execute(f'ALTER TABLE services DROP CONSTRAINT IF EXISTS "{r["conname"]}"')
                except Exception:
                    pass
        except Exception:
            pass
        try:
            await c.execute("""
                DO $$ BEGIN
                  ALTER TABLE services ADD CONSTRAINT services_co_key UNIQUE (company_id, key);
                EXCEPTION WHEN duplicate_object THEN NULL;
                END $$
            """)
        except Exception:
            pass
    logging.info("✅ API: services per-company constraint (step 29) ready")

    # ── Шаг 30: fix schema + sync template (company_id=0) from company_id=1 ──
    async with pool.acquire() as c:
        # 30a: ensure template company (id=0) exists so prices FK passes
        try:
            await c.execute("""
                INSERT INTO companies (id, name, slug, secret_key, plan, active)
                VALUES (0, 'Template', '__template__', '__template_secret__', 'starter', false)
                ON CONFLICT (id) DO NOTHING
            """)
        except Exception as e:
            logging.warning(f"step 30a template company error: {e}")
        # 30b: fix services PK — move from (key) to id BIGSERIAL so company_id=0 rows can coexist
        try:
            await c.execute("ALTER TABLE services ADD COLUMN IF NOT EXISTS id BIGSERIAL")
            await c.execute("ALTER TABLE services DROP CONSTRAINT IF EXISTS services_pkey")
            await c.execute("""
                DO $$ BEGIN
                  ALTER TABLE services ADD PRIMARY KEY (id);
                EXCEPTION WHEN duplicate_object THEN NULL;
                END $$
            """)
        except Exception as e:
            logging.warning(f"step 30b services PK error: {e}")
        # 30c: seed template services from company_id=1
        try:
            tpl_keys = {r["key"] for r in await c.fetch("SELECT key FROM services WHERE company_id=0")}
            src_keys = {r["key"] for r in await c.fetch("SELECT key FROM services WHERE company_id=1")}
            if tpl_keys != src_keys:
                await c.execute("DELETE FROM services WHERE company_id=0")
                await c.execute("""
                    INSERT INTO services (company_id, key, name_ru, name_uz, emoji, order_idx)
                    SELECT 0, key, name_ru, name_uz, emoji, order_idx
                    FROM services WHERE company_id=1
                """)
                logging.info("✅ API: template services resynced from company_id=1")
        except Exception as e:
            logging.warning(f"step 30 services seed error: {e}")
        # 30d: seed template prices from company_id=1
        try:
            tpl_pkeys = {(r["service_key"], r["type_key"]) for r in await c.fetch("SELECT service_key, type_key FROM prices WHERE company_id=0")}
            src_pkeys = {(r["service_key"], r["type_key"]) for r in await c.fetch("SELECT service_key, type_key FROM prices WHERE company_id=1")}
            if tpl_pkeys != src_pkeys:
                await c.execute("DELETE FROM prices WHERE company_id=0")
                await c.execute("""
                    INSERT INTO prices (company_id, service_key, type_key, price, unit_key, min_order, updated_at)
                    SELECT 0, service_key, type_key, price, unit_key, min_order, NOW()
                    FROM prices WHERE company_id=1
                """)
                logging.info("✅ API: template prices resynced from company_id=1")
        except Exception as e:
            logging.warning(f"step 30 prices seed error: {e}")
    logging.info("✅ API: template seeded from company_id=1 (step 30) ready")

    # ── Шаг 31: fix tg_status_messages PK for multi-tenant ──────────────────
    async with pool.acquire() as c:
        try:
            await c.execute("ALTER TABLE tg_status_messages ADD COLUMN IF NOT EXISTS id BIGSERIAL")
            await c.execute("ALTER TABLE tg_status_messages DROP CONSTRAINT IF EXISTS tg_status_messages_pkey")
            await c.execute("""
                DO $$ BEGIN
                  ALTER TABLE tg_status_messages ADD CONSTRAINT tg_status_messages_status_company_key
                    UNIQUE (status, company_id);
                EXCEPTION WHEN duplicate_object THEN NULL;
                END $$
            """)
            await c.execute("UPDATE tg_status_messages SET company_id=0 WHERE company_id IS NULL")
        except Exception as e:
            logging.warning(f"step 31 tg_status_messages PK fix error: {e}")
        try:
            existing = {r["status"] for r in await c.fetch("SELECT status FROM tg_status_messages WHERE company_id=0")}
            for status, enabled, msg_ru, msg_uz in _TG_STATUS_DEFAULTS:
                if status not in existing:
                    await c.execute("""
                        INSERT INTO tg_status_messages (status, enabled, message_ru, message_uz, company_id)
                        VALUES ($1, $2, $3, $4, 0)
                        ON CONFLICT DO NOTHING
                    """, status, enabled, msg_ru, msg_uz)
        except Exception as e:
            logging.warning(f"step 31 tg template seed error: {e}")
    logging.info("✅ API: tg_status_messages multi-tenant PK (step 31) ready")

    # ── Шаг 32: seed chat_templates into company_id=0 ───────────────────────
    try:
        await seed_chat_templates_for_company0()
        logging.info("✅ API: chat_templates template (company_id=0) seeded (step 32)")
    except Exception as e:
        logging.warning(f"step 32 chat template seed error: {e}")

    # ── Шаг 33: force-seed tg_status_messages + expense_categories for company_id=0 ──
    async with pool.acquire() as c:
        # 33a: TG messages — bypass PK issues with WHERE NOT EXISTS
        try:
            for status, enabled, msg_ru, msg_uz in _TG_STATUS_DEFAULTS:
                await c.execute("""
                    INSERT INTO tg_status_messages (status, enabled, message_ru, message_uz, company_id)
                    SELECT $1::varchar, $2, $3, $4, 0
                    WHERE NOT EXISTS (
                        SELECT 1 FROM tg_status_messages WHERE status=$1::varchar AND company_id=0
                    )
                """, status, enabled, msg_ru, msg_uz)
            logging.info("✅ API: tg_status_messages template (company_id=0) seeded (step 33a)")
        except Exception as e:
            logging.warning(f"step 33a tg template seed error: {e}")
        # 33b: expense_categories — copy from company_id=1 if template is empty
        try:
            tpl_count = await c.fetchval(
                "SELECT COUNT(*) FROM expense_categories WHERE company_id=0")
            if tpl_count == 0:
                src = await c.fetch(
                    "SELECT * FROM expense_categories WHERE company_id=1 ORDER BY sort_order, id")
                id_map: dict = {}
                for r in src:
                    new_parent = id_map.get(r['parent_id']) if r['parent_id'] else None
                    new_row = await c.fetchrow("""
                        INSERT INTO expense_categories
                            (name_ru, name_uz, icon, parent_id, approve_level, receipt_required,
                             amount_threshold, sort_order, for_staff, active, company_id)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,0) RETURNING id
                    """, r['name_ru'], r['name_uz'], r['icon'], new_parent,
                         r['approve_level'], r['receipt_required'], r['amount_threshold'],
                         r['sort_order'], r['for_staff'], r['active'])
                    if new_row:
                        id_map[r['id']] = new_row['id']
                logging.info(f"✅ API: expense_categories template seeded {len(src)} rows from company_id=1 (step 33b)")
        except Exception as e:
            logging.warning(f"step 33b expense_categories template seed error: {e}")

    # ── Шаг 34: users per-company UNIQUE constraint (был global UNIQUE(phone)) ──
    async with pool.acquire() as c:
        try:
            rows = await c.fetch("""
                SELECT conname FROM pg_constraint
                WHERE conrelid='users'::regclass AND contype='u'
                  AND array_length(conkey,1)=1
                  AND conkey[1]=(SELECT attnum FROM pg_attribute
                                 WHERE attrelid='users'::regclass AND attname='phone')
            """)
            for r in rows:
                try:
                    await c.execute(f'ALTER TABLE users DROP CONSTRAINT IF EXISTS "{r["conname"]}"')
                except Exception:
                    pass
        except Exception:
            pass
        try:
            await c.execute("""
                DO $$ BEGIN
                  ALTER TABLE users ADD CONSTRAINT users_co_phone UNIQUE (company_id, phone);
                EXCEPTION WHEN duplicate_object THEN NULL;
                END $$
            """)
        except Exception:
            pass
    logging.info("✅ API: users per-company constraint (step 34) ready — прод (ARTEZ PROJECT) не затронут, это отдельная БД SaaS")

    # ── Шаг 35: site_contacts per-company (был global PRIMARY KEY(branch)) ──
    async with pool.acquire() as c:
        try:
            rows = await c.fetch("""
                SELECT conname FROM pg_constraint
                WHERE conrelid='site_contacts'::regclass AND contype IN ('p','u')
                  AND array_length(conkey,1)=1
                  AND conkey[1]=(SELECT attnum FROM pg_attribute
                                 WHERE attrelid='site_contacts'::regclass AND attname='branch')
            """)
            for r in rows:
                try:
                    await c.execute(f'ALTER TABLE site_contacts DROP CONSTRAINT IF EXISTS "{r["conname"]}"')
                except Exception:
                    pass
        except Exception:
            pass
        try:
            await c.execute("""
                DO $$ BEGIN
                  ALTER TABLE site_contacts ADD CONSTRAINT site_contacts_co_branch UNIQUE (company_id, branch);
                EXCEPTION WHEN duplicate_object THEN NULL;
                END $$
            """)
        except Exception:
            pass
    logging.info("✅ API: site_contacts per-company constraint (step 35) ready")

    # ── Шаг 36: crm_clients per-company (был global UNIQUE(phone)) ──
    async with pool.acquire() as c:
        try:
            rows = await c.fetch("""
                SELECT conname FROM pg_constraint
                WHERE conrelid='crm_clients'::regclass AND contype='u'
                  AND array_length(conkey,1)=1
                  AND conkey[1]=(SELECT attnum FROM pg_attribute
                                 WHERE attrelid='crm_clients'::regclass AND attname='phone')
            """)
            for r in rows:
                try:
                    await c.execute(f'ALTER TABLE crm_clients DROP CONSTRAINT IF EXISTS "{r["conname"]}"')
                except Exception:
                    pass
        except Exception:
            pass
        try:
            await c.execute("""
                DO $$ BEGIN
                  ALTER TABLE crm_clients ADD CONSTRAINT crm_clients_co_phone UNIQUE (company_id, phone);
                EXCEPTION WHEN duplicate_object THEN NULL;
                END $$
            """)
        except Exception:
            pass
    logging.info("✅ API: crm_clients per-company constraint (step 36) ready")

    # ── Шаг 37: одноразовый перенос site_contacts → branches (branches — актуальная
    # таблица филиалов, используется ботом и admin.html; site_contacts был отдельным,
    # более простым источником для сайта — теперь сайт тоже читает branches) ──
    async with pool.acquire() as c:
        try:
            import json as _json
            old_rows = await c.fetch("SELECT * FROM site_contacts")
            for r in old_rows:
                exists = await c.fetchval(
                    "SELECT 1 FROM branches WHERE company_id=$1 AND slug=$2",
                    r["company_id"], r["branch"])
                if exists:
                    continue
                try:
                    raw_phones = r["phones"]
                    phones_list = _json.loads(raw_phones) if isinstance(raw_phones, str) else (raw_phones or [])
                except (TypeError, ValueError):
                    phones_list = []
                phones = [{"n": p, "receipt": i == 0, "site": True} for i, p in enumerate(phones_list) if p]
                await c.execute("""
                    INSERT INTO branches (company_id, slug, name_ru, name_uz, phones, telegram_link, whatsapp, instagram)
                    VALUES ($1,$2,$3,$3,$4::jsonb,$5,$6,$7)
                    ON CONFLICT (company_id, slug) DO NOTHING
                """, r["company_id"], r["branch"], r["branch_name"] or r["branch"],
                     _json.dumps(phones, ensure_ascii=False), r["telegram"] or None, r["whatsapp"] or None, r["instagram"] or None)
        except Exception as e:
            logging.warning(f"⚠️ API: перенос site_contacts→branches (step 37) частично не удался: {e}")
    logging.info("✅ API: site_contacts→branches перенос (step 37) готов")

    # ── Шаг 38: слайды автослайдера и статистика на главной странице сайта ──
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS site_slides (
            id         SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            image_url  TEXT NOT NULL DEFAULT '',
            eyebrow_ru VARCHAR(100) DEFAULT '',
            eyebrow_uz VARCHAR(100) DEFAULT '',
            title_ru   VARCHAR(200) DEFAULT '',
            title_uz   VARCHAR(200) DEFAULT '',
            text_ru    TEXT DEFAULT '',
            text_uz    TEXT DEFAULT '',
            sort_order INT DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_site_slides_company ON site_slides(company_id, sort_order);

        CREATE TABLE IF NOT EXISTS site_stats (
            id         SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            value_ru   VARCHAR(50) DEFAULT '',
            value_uz   VARCHAR(50) DEFAULT '',
            label_ru   VARCHAR(200) DEFAULT '',
            label_uz   VARCHAR(200) DEFAULT '',
            sort_order INT DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_site_stats_company ON site_stats(company_id, sort_order);
        """)
        # Засеять текущий (ARTEZ, company_id=1) контент — чтобы живой сайт не остался пустым
        count_slides = await c.fetchval("SELECT COUNT(*) FROM site_slides WHERE company_id=1")
        if count_slides == 0:
            await c.execute("""
                INSERT INTO site_slides (company_id, image_url, eyebrow_ru, eyebrow_uz, title_ru, title_uz, text_ru, text_uz, sort_order) VALUES
                (1, 'https://images.unsplash.com/photo-1581578731548-c64695cc6952?q=80&w=1400&auto=format&fit=crop',
                 'Услуга на дому', 'Uyga xizmat',
                 'Чистим ковры у вас дома', 'Gilamlarni uyingizda tozalaymiz',
                 'Приедем со своим оборудованием, почистим ковёр прямо в квартире — без вывоза и ожидания.',
                 'O''z jihozlarimiz bilan kelib, gilamni uyingizda tozalaymiz — olib ketishsiz va kutishsiz.', 0),
                (1, 'https://images.unsplash.com/photo-1493663284031-b7e3aefcae8e?q=80&w=1400&auto=format&fit=crop',
                 'Без хлопот', 'Muammosiz',
                 'Вывоз и доставка — бесплатно', 'Olib ketish va yetkazish — bepul',
                 'Заберём ковры, диваны и шторы, почистим в мастерской и привезём обратно — вы ничего не платите за логистику.',
                 'Gilam, divan va pardalarni olib ketamiz, ustaxonada tozalaymiz va qaytarib beramiz — logistika uchun to''lamaysiz.', 1),
                (1, 'https://images.unsplash.com/photo-1567016432779-094069958ea5?q=80&w=1400&auto=format&fit=crop',
                 'Мягкая мебель', 'Yumshoq mebel',
                 'Диваны, кресла, матрасы — как новые', 'Divan, kreslo, matraslar — yangidek',
                 'Глубокая чистка обивки от пятен, пыли и запахов с использованием профессиональной химии.',
                 'Mato qoplamasini dog'', chang va hidlardan professional kimyo yordamida chuqur tozalash.', 2)
                ON CONFLICT DO NOTHING;
            """)
        count_stats = await c.fetchval("SELECT COUNT(*) FROM site_stats WHERE company_id=1")
        if count_stats == 0:
            await c.execute("""
                INSERT INTO site_stats (company_id, value_ru, value_uz, label_ru, label_uz, sort_order) VALUES
                (1, '500+', '500+', 'клиентов в год', 'yiliga mijozlar', 0),
                (1, '3', '3', 'года на рынке', 'yil bozorda', 1),
                (1, '2 📍', '2 📍', 'города: Навои и Зарафшан', 'shahar: Navoiy va Zarafshon', 2),
                (1, 'Бесплатно', 'Bepul', 'вывоз и доставка', 'olib ketish va yetkazish', 3)
                ON CONFLICT DO NOTHING;
            """)
        count_stats_tpl = await c.fetchval("SELECT COUNT(*) FROM site_stats WHERE company_id=0")
        if count_stats_tpl == 0:
            await c.execute("""
                INSERT INTO site_stats (company_id, value_ru, value_uz, label_ru, label_uz, sort_order) VALUES
                (0, '', '', '', '', 0), (0, '', '', '', '', 1), (0, '', '', '', '', 2), (0, '', '', '', '', 3)
                ON CONFLICT DO NOTHING;
            """)
    logging.info("✅ API: site_slides/site_stats (step 38) ready")

    # ── Шаг 38б: site_stats.value → value_ru/value_uz (значение тоже разное RU/UZ, напр. "Бесплатно"/"Bepul") ──
    async with pool.acquire() as c:
        await c.execute("ALTER TABLE site_stats ADD COLUMN IF NOT EXISTS value_ru VARCHAR(50) DEFAULT ''")
        await c.execute("ALTER TABLE site_stats ADD COLUMN IF NOT EXISTS value_uz VARCHAR(50) DEFAULT ''")
        has_old_value = await c.fetchval("""
            SELECT EXISTS (SELECT 1 FROM information_schema.columns
                           WHERE table_name='site_stats' AND column_name='value')
        """)
        if has_old_value:
            await c.execute("UPDATE site_stats SET value_ru = value WHERE value_ru = '' AND value <> ''")
            await c.execute("UPDATE site_stats SET value_uz = value WHERE value_uz = '' AND value <> ''")
            await c.execute("UPDATE site_stats SET value_uz = 'Bepul' WHERE value = 'Бесплатно'")
            await c.execute("ALTER TABLE site_stats DROP COLUMN value")
    logging.info("✅ API: site_stats value_ru/value_uz (step 38б) ready")

    # ── Шаг 39: отзывы и FAQ на главной странице сайта ──
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS site_reviews (
            id          SERIAL PRIMARY KEY,
            company_id  INTEGER NOT NULL,
            author_name VARCHAR(100) DEFAULT '',
            rating      SMALLINT DEFAULT 5,
            text_ru     TEXT DEFAULT '',
            text_uz     TEXT DEFAULT '',
            city_ru     VARCHAR(100) DEFAULT '',
            city_uz     VARCHAR(100) DEFAULT '',
            sort_order  INT DEFAULT 0,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_site_reviews_company ON site_reviews(company_id, sort_order);

        CREATE TABLE IF NOT EXISTS site_faq (
            id           SERIAL PRIMARY KEY,
            company_id   INTEGER NOT NULL,
            question_ru  VARCHAR(300) DEFAULT '',
            question_uz  VARCHAR(300) DEFAULT '',
            answer_ru    TEXT DEFAULT '',
            answer_uz    TEXT DEFAULT '',
            sort_order   INT DEFAULT 0,
            created_at   TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_site_faq_company ON site_faq(company_id, sort_order);
        """)
    try:
      async with pool.acquire() as c:
        count_reviews = await c.fetchval("SELECT COUNT(*) FROM site_reviews WHERE company_id=1")
        if count_reviews == 0:
            await c.execute("""
                INSERT INTO site_reviews (company_id, author_name, rating, text_ru, text_uz, city_ru, city_uz, sort_order) VALUES
                (1, 'Малика Р.', 5,
                 'Заказывала чистку ковра — приехали вовремя, забрали и через 3 дня привезли обратно. Ковёр стал как новый, пятно от кофе полностью исчезло. Очень довольна!',
                 'Gilam tozalashni buyurtma qildim — o''z vaqtida kelishdi, olib ketishdi va 3 kundan so''ng qaytarib kelishdi. Gilam yangidek bo''ldi, qahva dog''i to''liq yo''qoldi. Juda mamnunman!',
                 'г. Навои', 'Navoiy', 0),
                (1, 'Бахром С.', 5,
                 'Чистили диван — работали аккуратно, не оставили грязи. Запах полностью ушёл. Вывоз и доставка бесплатно — это очень удобно. Рекомендую всем!',
                 'Divanni tozalashdi — ehtiyotkorlik bilan ishladilar, iflos qoldirmadilar. Hid to''liq yo''qoldi. Olib ketish va yetkazib berish bepul — bu juda qulay. Hammaga tavsiya qilaman!',
                 'г. Зарафшан', 'Zarafshon', 1),
                (1, 'Шахло Т.', 5,
                 'Обратилась по поводу штор — думала дорого выйдет, но цены оказались очень разумные. Шторы почистили быстро, вернули в идеальном состоянии. Спасибо!',
                 'Pardalar bo''yicha murojaat qildim — qimmat bo''ladi deb o''ylagandim, lekin narxlar juda maqbul bo''lib chiqdi. Pardalarni tez tozaladilar, mukammal holatda qaytardilar. Rahmat!',
                 'г. Навои', 'Navoiy', 2),
                (1, 'Акбар И.', 5,
                 'Заказал чистку матраса и двух ковров. Мастера приехали со своим оборудованием, работали чисто и быстро. Теперь обращаюсь регулярно, раз в полгода.',
                 'Matras va ikkita gilamni tozalashni buyurtma qildim. Ustalar o''z jihozlari bilan kelishdi, toza va tez ishladilar. Endi muntazam, olti oyda bir marta murojaat qilaman.',
                 'г. Зарафшан', 'Zarafshon', 3),
                (1, 'Нилуфар Х.', 5,
                 'Очень профессиональная команда. Всё по-честному: замерили площадь при мне, объяснили стоимость. Ковры вернули в срок и даже упаковали в плёнку. Отлично!',
                 'Juda professional jamoa. Hammasi halol: mening oldimda maydonni o''lchashdi, narxni tushuntirishdi. Gilamlarni o''z vaqtida qaytarishdi va hatto plyonkaga o''rashdi. Ajoyib!',
                 'г. Навои', 'Navoiy', 4)
                ON CONFLICT DO NOTHING;
            """)
        count_faq = await c.fetchval("SELECT COUNT(*) FROM site_faq WHERE company_id=1")
        if count_faq == 0:
            await c.execute("""
                INSERT INTO site_faq (company_id, question_ru, question_uz, answer_ru, answer_uz, sort_order) VALUES
                (1, 'Сколько времени занимает чистка?', 'Tozalash qancha vaqt oladi?',
                 'Стандартная чистка ковра занимает 2–3 рабочих дня с момента вывоза. Экспресс-режим — 1 день. Диваны и матрасы чистятся на месте за 1–3 часа.',
                 'Gilamni standart tozalash olib ketishdan 2–3 ish kunini oladi. Ekspress rejim — 1 kun. Divan va matraslar joyida 1–3 soat ichida tozalanadi.', 0),
                (1, 'Вывоз и доставка действительно бесплатны?', 'Olib ketish va yetkazib berish haqiqatan ham bepulmi?',
                 'Да, вывоз ковров и доставка обратно — полностью бесплатны. Вы платите только за саму чистку. Никаких скрытых платежей.',
                 'Ha, gilamlarni olib ketish va qaytarib yetkazish — mutlaqo bepul. Siz faqat tozalash uchun to''laysiz. Hech qanday yashirin to''lovlar yo''q.', 1),
                (1, 'Как оплатить? Принимаете безналично?', 'Qanday to''lash mumkin? Naqdsiz to''lovni qabul qilasizmi?',
                 'Принимаем наличные, банковский перевод и карту (Uzcard, Humo). Оплата — после получения готовой работы. Частичная предоплата тоже возможна по договорённости.',
                 'Naqd pul, bank o''tkazmasi va karta (Uzcard, Humo) qabul qilamiz. To''lov — tayyor ishni olgandan keyin. Kelishuv bo''yicha qisman oldindan to''lov ham mumkin.', 2),
                (1, 'Безопасна ли химия для детей и аллергиков?', 'Kimyoviy vositalar bolalar va allergiklarga xavfsizmi?',
                 'Используем профессиональные гипоаллергенные средства — безопасны для детей и домашних животных. После чистки ковёр полностью просушивается перед возвратом.',
                 'Professional gipoallergen vositalardan foydalanamiz — bolalar va uy hayvonlari uchun xavfsiz. Tozalashdan so''ng gilam qaytarilishidan oldin to''liq quritiladi.', 3),
                (1, 'Работаете в моём городе?', 'Mening shahrimda ishlaymisiz?',
                 'У нас два филиала: в Навои и Зарафшане. Также обслуживаем прилегающие районы — Учкудук, Тамди, Карманинский, Навбахорский и другие. Позвоните на 1221 — уточним.',
                 'Bizning ikkita filialimiz bor: Navoiy va Zarafshonda. Shuningdek qo''shni tumanlarga — Uchquduq, Tomdi, Karmana, Navbahor va boshqalarga ham xizmat ko''rsatamiz. 1221 ga qo''ng''iroq qiling — aniqlaymiz.', 4)
                ON CONFLICT DO NOTHING;
            """)
        await c.execute("""
            INSERT INTO config (key, value, company_id, updated_at) VALUES
            ('footer_about_ru', 'Химчистка ковров, мягкой мебели, матрасов и штор на дому в Навои и Зарафшане. Вывоз и доставка — бесплатно.', 1, NOW()),
            ('footer_about_uz', 'Navoiy va Zarafshonda uyga gilam, yumshoq mebel, matras va pardalarni quruq tozalash xizmati. Olib ketish va yetkazib berish — bepul.', 1, NOW())
            ON CONFLICT (company_id, key) DO NOTHING;
        """)
    except Exception as e:
      logging.warning(f"⚠️ API: сид отзывов/FAQ (step 39) не удался: {e}")
    logging.info("✅ API: site_reviews/site_faq (step 39) ready")

    # ── Шаг 40: заполнить шаблоны (company_id=0) слайдера/статистики/отзывов/FAQ
    #            реальными данными компании 1 — суперадминский каталог был пуст ──
    try:
      async with pool.acquire() as c:
        count_slides0 = await c.fetchval("SELECT COUNT(*) FROM site_slides WHERE company_id=0")
        if count_slides0 == 0:
            await c.execute("""
                INSERT INTO site_slides (company_id, image_url, eyebrow_ru, eyebrow_uz, title_ru, title_uz, text_ru, text_uz, sort_order)
                SELECT 0, image_url, eyebrow_ru, eyebrow_uz, title_ru, title_uz, text_ru, text_uz, sort_order
                FROM site_slides WHERE company_id=1
            """)
            logging.info("✅ API: шаблон site_slides заполнен из company_id=1")

        stats_filled0 = await c.fetchval(
            "SELECT COUNT(*) FROM site_stats WHERE company_id=0 AND (value_ru <> '' OR label_ru <> '')")
        if stats_filled0 == 0:
            await c.execute("DELETE FROM site_stats WHERE company_id=0")
            await c.execute("""
                INSERT INTO site_stats (company_id, value_ru, value_uz, label_ru, label_uz, sort_order)
                SELECT 0, value_ru, value_uz, label_ru, label_uz, sort_order
                FROM site_stats WHERE company_id=1
            """)
            logging.info("✅ API: шаблон site_stats заполнен из company_id=1")

        count_reviews0 = await c.fetchval("SELECT COUNT(*) FROM site_reviews WHERE company_id=0")
        if count_reviews0 == 0:
            await c.execute("""
                INSERT INTO site_reviews (company_id, author_name, rating, text_ru, text_uz, city_ru, city_uz, sort_order)
                SELECT 0, author_name, rating, text_ru, text_uz, city_ru, city_uz, sort_order
                FROM site_reviews WHERE company_id=1
            """)
            logging.info("✅ API: шаблон site_reviews заполнен из company_id=1")

        count_faq0 = await c.fetchval("SELECT COUNT(*) FROM site_faq WHERE company_id=0")
        if count_faq0 == 0:
            await c.execute("""
                INSERT INTO site_faq (company_id, question_ru, question_uz, answer_ru, answer_uz, sort_order)
                SELECT 0, question_ru, question_uz, answer_ru, answer_uz, sort_order
                FROM site_faq WHERE company_id=1
            """)
            logging.info("✅ API: шаблон site_faq заполнен из company_id=1")
    except Exception as e:
      logging.warning(f"⚠️ API: заполнение шаблонов слайдер/статистика/отзывы/FAQ (step 40) не удалось: {e}")
    logging.info("✅ API: templates from company_id=1 (step 40) ready")

    # ── Шаг 41: contact_email для company_id=1 (новое поле — иначе ARTEZ потеряет email на terms/privacy) ──
    try:
      async with pool.acquire() as c:
        await c.execute("""
            INSERT INTO config (key, value, company_id, updated_at)
            VALUES ('contact_email', 'info@artez.uz', 1, NOW())
            ON CONFLICT (company_id, key) DO NOTHING;
        """)
    except Exception as e:
      logging.warning(f"⚠️ API: сид contact_email (step 41) не удался: {e}")
    logging.info("✅ API: contact_email seed (step 41) ready")

    # ── Шаг 42: каталог шаблонов и палитр сайта (дизайн-система) ──────────
    try:
      async with pool.acquire() as c:
        await c.execute("""
            CREATE TABLE IF NOT EXISTS site_templates (
                id          SERIAL PRIMARY KEY,
                key         VARCHAR(50)  UNIQUE NOT NULL,
                name_ru     VARCHAR(100) NOT NULL,
                name_uz     VARCHAR(100) NOT NULL,
                preview_url TEXT         DEFAULT NULL,
                active      BOOLEAN      NOT NULL DEFAULT TRUE,
                sort_order  INT          NOT NULL DEFAULT 0,
                created_at  TIMESTAMPTZ  DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS site_palettes (
                id          SERIAL PRIMARY KEY,
                key         VARCHAR(50)  UNIQUE NOT NULL,
                name_ru     VARCHAR(100) NOT NULL,
                name_uz     VARCHAR(100) NOT NULL,
                colors      JSONB        NOT NULL,
                active      BOOLEAN      NOT NULL DEFAULT TRUE,
                sort_order  INT          NOT NULL DEFAULT 0,
                created_at  TIMESTAMPTZ  DEFAULT NOW()
            );
            ALTER TABLE companies ADD COLUMN IF NOT EXISTS site_template_key VARCHAR(50) NOT NULL DEFAULT 'template-01';
            ALTER TABLE companies ADD COLUMN IF NOT EXISTS site_palette_key  VARCHAR(50) NOT NULL DEFAULT 'teal-amber';
        """)

        count_tpl = await c.fetchval("SELECT COUNT(*) FROM site_templates")
        if count_tpl == 0:
            await c.executemany("""
                INSERT INTO site_templates (key, name_ru, name_uz, sort_order)
                VALUES ($1, $2, $3, $4)
            """, [
                ("template-01", "Классика",   "Klassik",     1),
                ("template-02", "Минимал",    "Minimal",     2),
                ("template-03", "Модерн",     "Zamonaviy",   3),
                ("template-04", "Карточки",   "Kartochkali", 4),
            ])

        count_pal = await c.fetchval("SELECT COUNT(*) FROM site_palettes")
        if count_pal == 0:
            await c.executemany("""
                INSERT INTO site_palettes (key, name_ru, name_uz, colors, sort_order)
                VALUES ($1, $2, $3, $4::jsonb, $5)
            """, [
                ("teal-amber",   "Тил и янтарь", "Tiniq va amber",
                 json.dumps({"teal-deep":"#0E2D2A","teal":"#1A5C54","teal-soft":"#2CC6B3",
                             "sand":"#F4EFE6","sand-card":"#FFFFFF","amber":"#E8A83C",
                             "amber-deep":"#C98C24","ink":"#141E1B","ink-soft":"#5A6D68","line":"#E0D9CC"}), 1),
                ("ocean-blue",   "Океан",        "Okean",
                 json.dumps({"teal-deep":"#0B2540","teal":"#155E9E","teal-soft":"#4FB6E8",
                             "sand":"#EEF3F8","sand-card":"#FFFFFF","amber":"#F2A93C",
                             "amber-deep":"#D48B1E","ink":"#131C26","ink-soft":"#5A6B7A","line":"#DCE4EC"}), 2),
                ("forest-green", "Лес",          "O'rmon",
                 json.dumps({"teal-deep":"#12331E","teal":"#25703F","teal-soft":"#5FBE7C",
                             "sand":"#F1F4EC","sand-card":"#FFFFFF","amber":"#E0A63A",
                             "amber-deep":"#BD8720","ink":"#161F18","ink-soft":"#59685C","line":"#DEE4D6"}), 3),
                ("burgundy-rose","Бургунди",     "Burgundiya",
                 json.dumps({"teal-deep":"#3A0F1B","teal":"#7A1F35","teal-soft":"#D9718A",
                             "sand":"#F7EEEF","sand-card":"#FFFFFF","amber":"#E8A83C",
                             "amber-deep":"#C98C24","ink":"#241318","ink-soft":"#6E5A5F","line":"#E9DBDD"}), 4),
                ("sunset-orange","Закат",        "Quyosh botishi",
                 json.dumps({"teal-deep":"#3D1E0B","teal":"#B5541F","teal-soft":"#F2905A",
                             "sand":"#FBF1E8","sand-card":"#FFFFFF","amber":"#F2C43C",
                             "amber-deep":"#D4A31E","ink":"#241A11","ink-soft":"#6E625A","line":"#EDE0D2"}), 5),
                ("violet-purple","Фиолет",       "Binafsha",
                 json.dumps({"teal-deep":"#221240","teal":"#5B3A9E","teal-soft":"#9C7FDB",
                             "sand":"#F2EEF9","sand-card":"#FFFFFF","amber":"#E8A83C",
                             "amber-deep":"#C98C24","ink":"#1C1730","ink-soft":"#635C74","line":"#E2DBEE"}), 6),
                ("slate-mono",   "Графит",       "Grafit",
                 json.dumps({"teal-deep":"#1C1F22","teal":"#3E454B","teal-soft":"#8A949C",
                             "sand":"#F2F2F0","sand-card":"#FFFFFF","amber":"#D9A441",
                             "amber-deep":"#B8862A","ink":"#17191B","ink-soft":"#5C6267","line":"#DFE0DD"}), 7),
                ("coral-teal",   "Коралл",       "Marjon",
                 json.dumps({"teal-deep":"#0E2D2A","teal":"#1A5C54","teal-soft":"#2CC6B3",
                             "sand":"#FBEEE9","sand-card":"#FFFFFF","amber":"#E86A3C",
                             "amber-deep":"#C94F24","ink":"#141E1B","ink-soft":"#5A6D68","line":"#EBDAD2"}), 8),
            ])
      logging.info("✅ API: site_templates/site_palettes каталог готов (step 42)")
    except Exception as e:
      logging.warning(f"⚠️ API: миграция каталога шаблонов/палитр (step 42) не удалась: {e}")

    # ── Шаг 43: contacts per-company (был global UNIQUE(phone)) ──
    # Тот же класс бага, что уже чинили для users.phone/site_contacts.branch/crm_clients.phone:
    # upsert_contact() делал ON CONFLICT (phone) без company_id — компания Б, сохраняя контакт
    # с уже существующим у компании А номером, молча ПЕРЕЗАПИСЫВАЛА данные контакта компании А
    # (не создавая свою запись), при этом получая в ответе чужую (уже не свою) строку.
    async with pool.acquire() as c:
        try:
            rows = await c.fetch("""
                SELECT conname FROM pg_constraint
                WHERE conrelid='contacts'::regclass AND contype='u'
                  AND array_length(conkey,1)=1
                  AND conkey[1]=(SELECT attnum FROM pg_attribute
                                 WHERE attrelid='contacts'::regclass AND attname='phone')
            """)
            for r in rows:
                try:
                    await c.execute(f'ALTER TABLE contacts DROP CONSTRAINT IF EXISTS "{r["conname"]}"')
                except Exception:
                    pass
        except Exception:
            pass
        try:
            await c.execute("""
                DO $$ BEGIN
                  ALTER TABLE contacts ADD CONSTRAINT contacts_co_phone UNIQUE (company_id, phone);
                EXCEPTION WHEN duplicate_object THEN NULL;
                END $$
            """)
        except Exception:
            pass
    logging.info("✅ API: contacts per-company constraint (step 43) ready")

    # ── Шаг 44: единая база "актуальных" контактов (реальные клиенты crm_clients +
    # дозвонившиеся из автодозвона) — источник для будущей массовой SMS-рассылки
    # и автодозвона. НЕ путать с crm_clients (карточка клиента) или contacts
    # (справочник для ручного импорта Excel-списков под автодозвон). company_id —
    # с самого начала (в отличие от прод-версии, где его добавляли миграцией задним
    # числом), т.к. таблица per-company с рождения.
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS active_contacts (
            id            SERIAL PRIMARY KEY,
            company_id    INTEGER NOT NULL,
            phone         VARCHAR(20) NOT NULL,
            first_name    VARCHAR(100) DEFAULT '',
            last_name     VARCHAR(100) DEFAULT '',
            source        VARCHAR(20) DEFAULT 'crm' CHECK (source IN ('crm','autodial','both')),
            crm_client_id INTEGER REFERENCES crm_clients(id) ON DELETE SET NULL,
            note          TEXT DEFAULT '',
            created_at    TIMESTAMPTZ DEFAULT NOW(),
            updated_at    TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (company_id, phone)
        );
        CREATE INDEX IF NOT EXISTS idx_active_contacts_company ON active_contacts(company_id);
        CREATE INDEX IF NOT EXISTS idx_active_contacts_phone   ON active_contacts(phone);
        """)
    logging.info("✅ API: active_contacts (step 44) ready")

    # ── Шаг 45: чёрный список (не участвуют в автодозвоне/SMS) — company_id с
    # рождения. Причина хранится только здесь; в contacts/active_contacts/users/
    # clients (таблица бота) — лёгкий флаг blacklisted, синхронизируемый отсюда.
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS blacklist (
            id         SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            phone      VARCHAR(20) NOT NULL,
            name       VARCHAR(150) DEFAULT '',
            note       TEXT DEFAULT '',
            added_by   VARCHAR(100) DEFAULT '',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (company_id, phone)
        );
        CREATE INDEX IF NOT EXISTS idx_blacklist_company ON blacklist(company_id);
        """)
        for _tbl in ("contacts", "active_contacts", "users", "clients", "crm_clients"):
            try:
                await c.execute(f"ALTER TABLE {_tbl} ADD COLUMN IF NOT EXISTS blacklisted BOOLEAN DEFAULT FALSE")
            except Exception:
                pass  # clients (таблица бота) может не существовать на этой БД
    logging.info("✅ API: blacklist (step 45) ready")


# ══════════════════════════════════════
#  ПОЛЬЗОВАТЕЛИ
# ══════════════════════════════════════
async def get_user_by_phone(phone: str, company_id: int):
    # company_id ОБЯЗАТЕЛЕН (без дефолта) — раньше дефолт =1 приводил к тому,
    # что при вызове без явного company_id код молча искал/находил пользователя
    # ЧУЖОЙ компании (IDOR, см. security-фикс 2026-07-29). Все вызовы теперь
    # обязаны резолвить свою компанию явно (через _resolve_client_company_id,
    # contextvar-cid или company_id лида/заказа), см. вызовы в main.py.
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE phone=$1 AND company_id=$2", phone, company_id)


async def get_user_by_id(user_id: int):
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE id=$1", user_id)

async def get_user_by_tg_id(tg_id, company_id: int):
    # company_id ОБЯЗАТЕЛЕН — без фильтрации по компании этот запрос мог найти
    # аккаунт ДРУГОЙ компании с тем же tg_id (users.tg_id ничем не ограничен
    # по компании) и по ошибке привязать/создать агента не в той компании
    # (см. security-фикс 2026-07-29, тот же класс что и phone spoofing в боте).
    if not pool: return None
    async with pool.acquire() as conn:
        # Пробуем как целое число, затем как строку
        try:
            return await conn.fetchrow(
                "SELECT * FROM users WHERE tg_id=$1 AND company_id=$2", int(tg_id), company_id)
        except Exception:
            try:
                return await conn.fetchrow(
                    "SELECT * FROM users WHERE tg_id::text=$1 AND company_id=$2", str(tg_id), company_id)
            except Exception:
                return None


async def create_user(phone: str, password_hash: str, first_name: str, company_id: int = 1):
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow("""
            INSERT INTO users (phone, password_hash, first_name, is_verified, company_id)
            VALUES ($1, $2, $3, FALSE, $4)
            ON CONFLICT (company_id, phone) DO UPDATE SET
                password_hash = EXCLUDED.password_hash,
                first_name    = EXCLUDED.first_name,
                updated_at    = NOW()
            RETURNING *
        """, phone, password_hash, first_name, company_id)


async def verify_user(phone: str, company_id: int = 1):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE users SET is_verified = TRUE, updated_at = NOW()
            WHERE phone = $1 AND company_id = $2
        """, phone, company_id)


async def link_user_tg_id(phone: str, tg_id: int, company_id: int):
    # company_id ОБЯЗАТЕЛЕН — без него UPDATE зацепил бы ВСЕ строки с этим
    # телефоном во ВСЕХ компаниях сразу (users.phone уникален только в паре
    # с company_id, один и тот же номер может быть зарегистрирован отдельно
    # в разных компаниях) и молча привязал бы чужой tg_id не в той компании.
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE users SET tg_id = $2, updated_at = NOW()
            WHERE phone = $1 AND company_id = $3
        """, phone, tg_id, company_id)

async def set_user_must_change_password(user_id: int, value: bool):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET must_change_password=$2 WHERE id=$1", user_id, value)

async def save_tg_phone_link(phone: str, tg_id: int):
    """Сохраняет связку телефон→tg_id от бота (до регистрации на сайте)."""
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO tg_phone_links (phone, tg_id, created_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (phone) DO UPDATE SET tg_id=$2, created_at=NOW()
        """, phone, tg_id)
        # Если пользователь уже зарегистрирован — сразу линкуем
        await conn.execute("""
            UPDATE users SET tg_id=$2, updated_at=NOW()
            WHERE phone=$1 AND tg_id IS NULL
        """, phone, tg_id)

async def get_tg_id_by_phone(phone: str):
    """Возвращает tg_id для телефона: сначала из users, потом из tg_phone_links."""
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT tg_id FROM users WHERE phone=$1 AND tg_id IS NOT NULL", phone)
        if row:
            return row["tg_id"]
        row = await conn.fetchrow(
            "SELECT tg_id FROM tg_phone_links WHERE phone=$1", phone)
        return row["tg_id"] if row else None


async def get_receipt_tg_id(order: dict) -> int | None:
    """Определяет tg_id клиента для отправки чека: сначала client_tg_id заказа, потом по телефону."""
    tg_id = order.get('client_tg_id')
    if tg_id:
        return tg_id
    phone = order.get('client_phone')
    if phone:
        return await get_tg_id_by_phone(phone)
    return None

async def log_receipt_send(order_id: int, staff_id: int, staff_name: str, note: str = "",
                            tg_chat_id: int | None = None, tg_message_id: int | None = None) -> None:
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO order_receipt_log (order_id, staff_id, staff_name, note, tg_chat_id, tg_message_id)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, order_id, staff_id, staff_name, note or "", tg_chat_id, tg_message_id)

async def get_last_receipt_send(order_id: int) -> dict | None:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT staff_name, note, created_at FROM order_receipt_log
            WHERE order_id=$1 ORDER BY created_at DESC LIMIT 1
        """, order_id)
        return dict(row) if row else None

async def get_prior_receipt_messages(order_id: int) -> list[dict]:
    """Ранее отправленные в Telegram чеки этого заказа (для удаления при повторной отправке)."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT tg_chat_id, tg_message_id FROM order_receipt_log
            WHERE order_id=$1 AND tg_message_id IS NOT NULL
        """, order_id)
        return [dict(r) for r in rows]


async def update_user_name(user_id: int, first_name: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE users SET first_name=$2, updated_at=NOW() WHERE id=$1
        """, user_id, first_name)


async def update_user_password(user_id: int, password_hash: str):
    # NB: без company_id-фильтра нарочно — вызывается и из self-service /api/me/password
    # (клиентский JWT НЕ содержит company_id, см. create_token(); _cid() там всегда даст
    # дефолт 1 и сломает смену пароля для всех остальных компаний). Для admin-эндпоинта
    # (admin_reset_site_user_password) владение user_id проверяется на вызывающей стороне.
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE users SET password_hash=$2, must_change_password=FALSE, updated_at=NOW() WHERE id=$1
        """, user_id, password_hash)

async def get_all_site_users(search: str = "", limit: int = 500):
    if not pool: return []
    cid = _cid()
    base = """
        SELECT u.id, u.phone, u.first_name, u.is_verified, u.tg_id,
               u.address, u.blacklisted,
               u.created_at, u.updated_at, u.last_login,
               EXISTS(SELECT 1 FROM staff s WHERE s.site_user_id = u.id AND s.active = TRUE) AS is_agent
        FROM users u
    """
    async with pool.acquire() as conn:
        if search:
            return await conn.fetch(
                base + "WHERE u.company_id=$1 AND (u.phone ILIKE $2 OR u.first_name ILIKE $2) ORDER BY u.created_at DESC LIMIT $3",
                cid, f"%{search}%", limit)
        return await conn.fetch(base + "WHERE u.company_id=$1 ORDER BY u.created_at DESC LIMIT $2", cid, limit)


async def get_site_user_in_company(user_id: int, company_id: int) -> bool:
    """Проверка владения перед admin-действием над users (сам update/delete не фильтрует
    company_id — см. комментарии в update_user_password/update_user_profile)."""
    if not pool: return False
    async with pool.acquire() as conn:
        return bool(await conn.fetchval(
            "SELECT 1 FROM users WHERE id=$1 AND company_id=$2", user_id, company_id))


async def update_user_last_login(user_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET last_login=NOW() WHERE id=$1", user_id)


async def update_user_profile(user_id: int, first_name: str, address: str = None):
    # NB: без company_id-фильтра — тоже вызывается из self-service /api/me (см. комментарий
    # в update_user_password). Admin-эндпоинт проверяет владение user_id до вызова.
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE users SET first_name=$2, address=$3, updated_at=NOW() WHERE id=$1
        """, user_id, first_name, address)


async def get_staff_notify_new_users():
    """Возвращает tg_id сотрудников с включённым notify_new_users."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT tg_id FROM staff WHERE notify_new_users=TRUE AND tg_id IS NOT NULL AND active=TRUE")
    return [r["tg_id"] for r in rows]


async def update_tg_client(tg_id: int, data: dict):
    if not pool: return
    cid = _cid()
    allowed = {"first_name", "last_name", "phone"}
    fields = {k: v for k, v in data.items() if k in allowed}
    if not fields: return
    sets = ", ".join(f"{k}=${i+2}" for i, k in enumerate(fields))
    cid_idx = len(fields) + 2
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE clients SET {sets}, updated_at=NOW() WHERE tg_id=$1 AND company_id=${cid_idx}",
            tg_id, *fields.values(), cid)

async def block_tg_client(tg_id: int, blocked: bool):
    if not pool: return
    cid = _cid()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE clients SET blocked=$1 WHERE tg_id=$2 AND company_id=$3", blocked, tg_id, cid)

async def delete_tg_client(tg_id: int):
    if not pool: return
    cid = _cid()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM clients WHERE tg_id=$1 AND company_id=$2", tg_id, cid)

async def get_all_bot_client_tg_ids() -> list:
    """Все tg_id клиентов бота своей компании (таблица clients, company_id пишет бот)."""
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT tg_id FROM clients WHERE tg_id IS NOT NULL AND company_id=$1", cid)
    return [r["tg_id"] for r in rows]

async def set_user_tg_id(phone: str, tg_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET tg_id=$2, updated_at=NOW() WHERE phone=$1", phone, tg_id)


async def delete_site_user(user_id: int) -> bool:
    """Admin-only (см. admin_delete_site_user) — company_id можно фильтровать напрямую."""
    if not pool: return False
    cid = _cid()
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM users WHERE id=$1 AND company_id=$2", user_id, cid)
    return result != "DELETE 0"


# ══════════════════════════════════════
#  SMS-КОДЫ
# ══════════════════════════════════════
async def save_sms_code(phone: str, code: str, purpose: str, expires_at):
    if not pool: return
    async with pool.acquire() as conn:
        # Деактивируем старые коды для этого номера и цели
        await conn.execute("""
            UPDATE sms_codes SET used = TRUE
            WHERE phone=$1 AND purpose=$2 AND used = FALSE
        """, phone, purpose)
        await conn.execute("""
            INSERT INTO sms_codes (phone, code, purpose, expires_at)
            VALUES ($1, $2, $3, $4)
        """, phone, code, purpose, expires_at)


async def check_sms_code(phone: str, code: str, purpose: str) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT id FROM sms_codes
            WHERE phone=$1 AND code=$2 AND purpose=$3
              AND used = FALSE AND expires_at > NOW()
            ORDER BY id DESC LIMIT 1
        """, phone, code, purpose)
        if not row:
            return False
        await conn.execute("UPDATE sms_codes SET used = TRUE WHERE id=$1", row["id"])
        return True


async def check_sms_rate_limit(phone: str, purpose: str) -> tuple[bool, str]:
    """Возвращает (ok, сообщение_об_ошибке). 60 сек между отправками, макс 5 за час."""
    if not pool: return True, ""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                EXTRACT(EPOCH FROM (NOW() - MAX(created_at)))::INT AS seconds_since_last,
                COUNT(*) AS count_hour
            FROM sms_codes
            WHERE phone=$1 AND purpose=$2 AND created_at > NOW() - INTERVAL '1 hour'
        """, phone, purpose)
        if row and row["count_hour"] and row["count_hour"] > 0:
            secs = row["seconds_since_last"]
            if secs is not None and secs < 60:
                wait = 60 - secs
                return False, f"Подождите {wait} сек. / {wait} soniya kuting"
            if row["count_hour"] >= 5:
                return False, "Превышен лимит. Попробуйте через час / Limit oshdi. 1 soatdan keyin urinib ko'ring"
    return True, ""


async def get_config(key: str) -> str | None:
    if not pool: return None
    cid = _cid()
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT value FROM config WHERE key=$1 AND company_id=$2", key, cid)


async def set_config(key: str, value: str):
    if not pool: return
    cid = _cid()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO config (key, value, company_id, updated_at) VALUES ($1, $2, $3, NOW())
            ON CONFLICT (company_id, key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """, key, value, cid)


async def get_config_for_company(key: str, company_id: int) -> str | None:
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT value FROM config WHERE key=$1 AND company_id=$2", key, company_id)


async def set_config_for_company(key: str, value: str, company_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO config (key, value, company_id, updated_at) VALUES ($1, $2, $3, NOW())
            ON CONFLICT (company_id, key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """, key, value, company_id)


async def get_company_id_by_order_bot_secret(secret: str) -> int | None:
    """Обратный поиск company_id по секрету вебхука бота заказов —
    для роутинга входящих Telegram-апдейтов на /api/order-bot/webhook/{secret}."""
    if not pool or not secret: return None
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT company_id FROM config WHERE key='order_bot_webhook_secret' AND value=$1", secret)


# ══════════════════════════════════════
#  ЗАКАЗЫ КЛИЕНТА (для личного кабинета)
# ══════════════════════════════════════
async def get_orders_by_phone(phone: str, company_id: int):
    if not pool: return []
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT order_num, service, branch, city, address, status,
                   pickup_date, pickup_time, total_price, created_at,
                   (SELECT COUNT(*) FROM order_items WHERE order_id=orders.id)::int AS item_count
            FROM orders
            WHERE client_phone = $1 AND company_id = $2
            ORDER BY created_at DESC
            LIMIT 50
        """, phone, company_id)


async def get_order_by_num_and_phone(order_num: str, phone: str, company_id: int) -> dict:
    """Заказ по номеру, только если принадлежит этому телефону в рамках своей компании —
    для проверки владения перед выдачей клиенту позиций/фото/видео заказа."""
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM orders WHERE order_num=$1 AND client_phone=$2 AND company_id=$3",
            order_num, phone, company_id)
        return dict(row) if row else {}


async def cancel_order_by_phone(order_num: str, phone: str, company_id: int):
    """Отменяет заказ со статусом 'new', принадлежащий этому номеру (в рамках своей компании).
    Возвращает dict с данными заказа или None если не найден/нельзя отменить."""
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE orders SET status='cancelled'
            WHERE order_num=$1 AND client_phone=$2 AND company_id=$3 AND status='new'
            RETURNING order_num, client_first_name, client_last_name, client_phone, service, branch
        """, order_num, phone, company_id)
        if not row:
            return None
        r = dict(row)
        r['client_name'] = f"{r.pop('client_first_name') or ''} {r.pop('client_last_name') or ''}".strip()
        return r


async def get_orders_by_tg_id(tg_id: int):
    if not pool: return []
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT order_num, service, branch, city, address, status,
                   pickup_date, pickup_time, created_at
            FROM orders
            WHERE client_tg_id = $1
            ORDER BY created_at DESC
            LIMIT 50
        """, tg_id)


# ══════════════════════════════════════
#  СОЗДАНИЕ ЗАЯВКИ С САЙТА
# ══════════════════════════════════════
async def get_next_order_num(prefix: str | None = None) -> str:
    """Возвращает следующий номер заказа на основе данных в БД (общий с ботом счётчик).
    Без явного prefix — берётся свой префикс для текущей компании (_cid()), чтобы
    номера заказов разных SaaS-компаний не смешивались под одним "ARTEZ-..." (баг
    найден 2026-08-21: раньше префикс был захардкожен одинаковым для всех тенантов).
    company_id=1 — это реальная компания ARTEZ (общая физ. БД с прод-сайтом
    artez.uz) — для неё префикс намеренно не трогаем, чтобы не рвать её живую
    последовательность номеров заказов независимо от значения slug."""
    if prefix is None:
        cid = _cid()
        if cid == 1:
            prefix = "ARTEZ"
        else:
            slug = None
            if pool:
                async with pool.acquire() as conn:
                    slug = await conn.fetchval("SELECT slug FROM companies WHERE id=$1", cid)
            prefix = re.sub(r'[^A-Z0-9]', '', (slug or '').upper())[:12] or f"C{cid}"
    if not pool:
        return f"{prefix}-1001"
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT order_num FROM orders
            WHERE order_num LIKE $1
            ORDER BY id DESC
            LIMIT 1
        """, f"{prefix}-%")
        if row and row["order_num"]:
            try:
                last_num = int(row["order_num"].split("-")[-1])
            except (ValueError, IndexError):
                last_num = 1000
        else:
            last_num = 1000
        return f"{prefix}-{last_num + 1}"


async def save_site_order(data: dict, source: str = "site") -> str:
    """Сохраняет заявку без обязательного Telegram ID. source: 'site' | 'staff'"""
    if not pool:
        return data.get("order_num", "")
    source_note = {"site": "Заявка создана через сайт", "staff": "Заявка создана сотрудником"}.get(source, "Заявка создана")
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO orders (
                order_num, source,
                client_tg_id, client_first_name, client_last_name, client_phone,
                branch, city, address, short_address, location, service, service_type, pickup_type, delivery_type, pickup_date, pickup_time, note,
                total_price, status, company_id
            ) VALUES (
                $1, $2,
                NULL, $3, $4, $5,
                $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                $18, 'new', $19
            )
            ON CONFLICT (order_num) DO NOTHING
        """,
            data.get("order_num"),
            source,
            data.get("first_name"),
            data.get("last_name", ""),
            data.get("phone"),
            data.get("branch"),
            data.get("city"),
            data.get("address"),
            data.get("short_address", ""),
            data.get("location"),
            data.get("service"),
            data.get("service_type") or "standard",
            data.get("pickup_type") or "courier",
            data.get("delivery_type") or "courier",
            data.get("pickup_date"),
            data.get("pickup_time"),
            data.get("note"),
            data.get("total_price"),
            _cid(),
        )
        await conn.execute("""
            INSERT INTO order_status_history (order_num, new_status, note)
            VALUES ($1, 'new', $2)
        """, data.get("order_num"), source_note)
    return data.get("order_num", "")


# ══════════════════════════════════════
#  ПРОМО-АКЦИИ
# ══════════════════════════════════════
async def _get_active_promotion(conn):
    """Текущая активная кампания (is_active=TRUE и сейчас между starts_at и ends_at)
    ТЕКУЩЕЙ компании — раньше не фильтровало по company_id вообще, поэтому любая
    компания видела активную акцию компании 1 (см. запрос пользователя 2026-08-08)."""
    return await conn.fetchrow("""
        SELECT * FROM promotions
        WHERE company_id = $1 AND is_active = TRUE AND NOW() BETWEEN starts_at AND ends_at
        ORDER BY id DESC LIMIT 1
    """, _cid())


def _promo_public_fields(promo, mode: str, expires_at) -> dict:
    return {
        "id":            promo["id"],
        "code":          promo["code"],
        "title_ru":      promo["title_ru"],
        "title_uz":      promo["title_uz"],
        "text_ru":       promo["text_ru"],
        "text_uz":       promo["text_uz"],
        "discount_pct":  float(promo["discount_pct"]) if promo["discount_pct"] is not None else 0,
        "sound_enabled": promo["sound_enabled"],
        "mode":          mode,
        "expires_at":    expires_at.isoformat() if expires_at else None,
    }


async def check_promo_eligibility(user_id: int, phone: str, channel: str) -> dict | None:
    """Единый источник правды по эквайру акции для сайта/бота (см. GET /api/promo/status).
    Возвращает None если акции нет/клиент не подходит, иначе dict с mode: full|silent|none."""
    if not pool:
        return None
    async with pool.acquire() as conn:
        promo = await _get_active_promotion(conn)
        if not promo:
            return None

        if promo["target_new_only"]:
            has_order = await conn.fetchval(
                "SELECT 1 FROM orders WHERE client_phone=$1 LIMIT 1", phone
            )
            if has_order:
                return None

        state = await conn.fetchrow(
            "SELECT * FROM promo_user_state WHERE promotion_id=$1 AND user_id=$2",
            promo["id"], user_id
        )
        if not state:
            state = await conn.fetchrow("""
                INSERT INTO promo_user_state (promotion_id, user_id, shown_at, expires_at, channel)
                VALUES ($1, $2, NOW(), NOW() + ($3 * INTERVAL '1 hour'), $4)
                ON CONFLICT (promotion_id, user_id) DO NOTHING
                RETURNING *
            """, promo["id"], user_id, promo["window_hours"], channel)
            if state:
                mode = "full"
            else:
                # Гонка: параллельный запрос уже вставил строку — перечитываем
                state = await conn.fetchrow(
                    "SELECT * FROM promo_user_state WHERE promotion_id=$1 AND user_id=$2",
                    promo["id"], user_id
                )
                if not state:
                    return None
                mode = "silent" if (state["used_order_id"] is None and state["expires_at"]
                                     and state["expires_at"] > datetime.now(timezone.utc)) else "none"
        else:
            if state["used_order_id"] is None and state["expires_at"] and state["expires_at"] > datetime.now(timezone.utc):
                mode = "silent"
            else:
                mode = "none"

        return _promo_public_fields(promo, mode, state["expires_at"])


async def get_active_promotion_public() -> dict | None:
    """Для незарегистрированных посетителей сайта/бота: общая информация об активной
    акции без персонального трекинга (нет user_id — нечего писать в promo_user_state).
    Личное окно 48ч и mode full/silent считаются только после регистрации, см.
    check_promo_eligibility()."""
    if not pool:
        return None
    async with pool.acquire() as conn:
        promo = await _get_active_promotion(conn)
        if not promo:
            return None
        return _promo_public_fields(promo, "public", promo["ends_at"])


async def apply_promo_to_order(order_num: str, user_id: int) -> int | None:
    """Если у пользователя есть живое (не истёкшее, не использованное) окно акции —
    привязывает заказ к акции (orders.promo_id) и закрывает окно (used_order_id).
    Возвращает id акции если применена, иначе None."""
    if not pool or not user_id:
        return None
    async with pool.acquire() as conn:
        async with conn.transaction():
            state = await conn.fetchrow("""
                SELECT pus.promotion_id
                FROM promo_user_state pus
                JOIN promotions p ON p.id = pus.promotion_id
                WHERE pus.user_id = $1 AND pus.used_order_id IS NULL
                  AND pus.expires_at > NOW() AND p.is_active = TRUE
                ORDER BY pus.created_at DESC LIMIT 1
            """, user_id)
            if not state:
                return None
            promo_id = state["promotion_id"]
            order_row = await conn.fetchrow(
                "UPDATE orders SET promo_id=$1 WHERE order_num=$2 RETURNING id",
                promo_id, order_num
            )
            if not order_row:
                return None
            await conn.execute("""
                UPDATE promo_user_state SET used_order_id=$1
                WHERE promotion_id=$2 AND user_id=$3 AND used_order_id IS NULL
            """, order_row["id"], promo_id, user_id)
            return promo_id


async def list_promotions() -> list:
    """Admin: все кампании (новые сверху) + is_currently_active и лёгкая статистика
    (shown_count/used_count) через LEFT JOIN на promo_user_state."""
    if not pool:
        return []
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT p.*,
                   (p.is_active AND NOW() BETWEEN p.starts_at AND p.ends_at) AS is_currently_active,
                   COUNT(pus.id)                AS shown_count,
                   COUNT(pus.used_order_id)     AS used_count
            FROM promotions p
            LEFT JOIN promo_user_state pus ON pus.promotion_id = p.id
            WHERE p.company_id=$1
            GROUP BY p.id
            ORDER BY p.id DESC
        """, cid)
        return [dict(r) for r in rows]


async def create_promotion(code: str, title_ru: str, title_uz: str, text_ru: str, text_uz: str,
                            discount_pct, ends_at, starts_at=None, window_hours: int = 48,
                            sound_enabled: bool = True, target_new_only: bool = False,
                            is_active: bool = True) -> dict:
    """Admin: создаёт новую промо-кампанию. Если is_active=True — деактивирует остальные
    кампании (правило "не более одной активной одновременно"), чтобы ORDER BY id DESC
    в _get_active_promotion() никогда не разрешал неоднозначность на практике."""
    cid = _cid()
    async with pool.acquire() as conn:
        async with conn.transaction():
            if is_active:
                await conn.execute("UPDATE promotions SET is_active = FALSE WHERE is_active = TRUE AND company_id=$1", cid)
            row = await conn.fetchrow("""
                INSERT INTO promotions (code, title_ru, title_uz, text_ru, text_uz, discount_pct,
                                         starts_at, ends_at, window_hours, sound_enabled,
                                         target_new_only, is_active, company_id)
                VALUES ($1,$2,$3,$4,$5,$6, COALESCE($7, NOW()), $8, $9, $10, $11, $12, $13)
                RETURNING *
            """, code, title_ru, title_uz, text_ru, text_uz, discount_pct,
                 starts_at, ends_at, window_hours, sound_enabled, target_new_only, is_active, cid)
            return dict(row)


async def update_promotion(promo_id: int, **kwargs) -> dict | None:
    """Admin: частичное обновление кампании. Если is_active=True среди полей — деактивирует
    остальные кампании перед апдейтом (та же гарантия "не более одной активной одновременно")."""
    if not pool:
        return None
    cid = _cid()
    allowed = {"code", "title_ru", "title_uz", "text_ru", "text_uz", "discount_pct",
               "starts_at", "ends_at", "window_hours", "sound_enabled",
               "target_new_only", "is_active"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM promotions WHERE id=$1 AND company_id=$2", promo_id, cid)
            return dict(row) if row else None
    sets = ", ".join(f"{k}=${i+2}" for i, k in enumerate(fields))
    vals = list(fields.values())
    cid_idx = len(fields) + 2
    async with pool.acquire() as conn:
        async with conn.transaction():
            if fields.get("is_active") is True:
                await conn.execute(
                    "UPDATE promotions SET is_active = FALSE WHERE is_active = TRUE AND id != $1 AND company_id=$2",
                    promo_id, cid
                )
            row = await conn.fetchrow(
                f"UPDATE promotions SET {sets} WHERE id=$1 AND company_id=${cid_idx} RETURNING *",
                promo_id, *vals, cid
            )
            return dict(row) if row else None


# ══════════════════════════════════════
#  ЦЕНЫ (общая таблица с ботом)
# ══════════════════════════════════════
async def get_all_prices() -> dict:
    """Возвращает все цены из таблицы prices: {service_key: {type_key: {price, unit_key, min_order, min_order_total}}}"""
    if not pool:
        return {}
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT service_key, type_key, price, unit_key, min_order, min_order_total FROM prices WHERE company_id=$1 ORDER BY service_key, type_key",
            cid
        )
    result = {}
    for r in rows:
        result.setdefault(r["service_key"], {})[r["type_key"]] = {
            "price": r["price"],
            "unit_key": r["unit_key"],
            "min_order": float(r["min_order"]) if r["min_order"] is not None else None,
            "min_order_total": float(r["min_order_total"]) if r["min_order_total"] is not None else None,
        }
    return result


async def set_price(service_key: str, type_key: str, price: int,
                     unit_key: str = None, min_order=None, min_order_total=None, company_id: int = None) -> bool:
    if not pool:
        return False
    cid = company_id if company_id is not None else _cid()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO prices (company_id, service_key, type_key, price, unit_key, min_order, min_order_total, updated_at)
            VALUES ($1, $2, $3, $4, COALESCE($5, 'm2'), $6, $7, NOW())
            ON CONFLICT (company_id, service_key, type_key) DO UPDATE SET
                price           = EXCLUDED.price,
                unit_key        = COALESCE($5, prices.unit_key),
                min_order       = $6,
                min_order_total = $7,
                updated_at      = NOW()
        """, cid, service_key, type_key, price, unit_key, min_order, min_order_total)
    return True


async def get_catalog_prices() -> dict:
    """Template prices (company_id=0)."""
    if not pool:
        return {}
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT service_key, type_key, price, unit_key, min_order, min_order_total FROM prices WHERE company_id=0 ORDER BY service_key, type_key"
        )
    result = {}
    for r in rows:
        result.setdefault(r["service_key"], {})[r["type_key"]] = {
            "price": r["price"],
            "unit_key": r["unit_key"],
            "min_order": float(r["min_order"]) if r["min_order"] is not None else None,
            "min_order_total": float(r["min_order_total"]) if r["min_order_total"] is not None else None,
        }
    return result


async def seed_company_prices(company_id: int, force: bool = False):
    """Seed company prices from template (company_id=0). If template empty, use hardcoded defaults."""
    if not pool:
        return
    async with pool.acquire() as conn:
        if not force:
            exists = await conn.fetchrow("SELECT id FROM prices WHERE company_id=$1 LIMIT 1", company_id)
            if exists:
                return
        if force:
            await conn.execute("DELETE FROM prices WHERE company_id=$1", company_id)
        template = await conn.fetch(
            "SELECT service_key, type_key, price, unit_key, min_order, min_order_total FROM prices WHERE company_id=0"
        )
    if template:
        for r in template:
            await set_price(r["service_key"], r["type_key"], r["price"], r["unit_key"], r["min_order"], r["min_order_total"], company_id=company_id)
    else:
        defaults = [
            ("carpet",      "standard",  5000, "m2",    None, None),
            ("carpet",      "express",   8000, "m2",    None, None),
            ("carpet_home", "standard",  6000, "m2",    None, None),
            ("carpet_home", "express",  10000, "m2",    None, None),
            ("sofa",        "standard", 150000, "piece", None, None),
            ("sofa",        "express",  200000, "piece", None, None),
            ("mattress",    "standard", 100000, "piece", None, None),
            ("mattress",    "express",  150000, "piece", None, None),
            ("curtains",    "standard", 10000, "m2",    None, None),
            ("curtains",    "express",  15000, "m2",    None, None),
        ]
        for skey, tkey, price, ukey, mord, mtot in defaults:
            await set_price(skey, tkey, price, ukey, mord, mtot, company_id=company_id)


# ══════════════════════════════════════
#  УСЛУГИ (названия RU/UZ)
# ══════════════════════════════════════
async def get_services():
    if not pool:
        return []
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM services WHERE company_id=$1 ORDER BY order_idx, key", cid
        )
        return [dict(r) for r in rows]

async def get_catalog_services() -> list:
    """Global service catalog (company_id=0) for superadmin."""
    if not pool:
        return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM services WHERE company_id=0 ORDER BY order_idx, key"
        )
        return [dict(r) for r in rows]

async def upsert_service(key: str, name_ru: str, name_uz: str, emoji: str = '', order_idx: int = 0, company_id: int = None):
    if not pool:
        return False
    cid = company_id if company_id is not None else _cid()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO services (company_id, key, name_ru, name_uz, emoji, order_idx)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (company_id, key) DO UPDATE SET
                name_ru   = EXCLUDED.name_ru,
                name_uz   = EXCLUDED.name_uz,
                emoji     = EXCLUDED.emoji,
                order_idx = EXCLUDED.order_idx
        """, cid, key, name_ru, name_uz, emoji, order_idx)
        return True

async def delete_service(key: str, company_id: int = None) -> bool:
    if not pool:
        return False
    cid = company_id if company_id is not None else _cid()
    async with pool.acquire() as conn:
        r = await conn.execute("DELETE FROM services WHERE company_id=$1 AND key=$2", cid, key)
        return r == "DELETE 1"

async def seed_company_services(company_id: int, force: bool = False):
    """Seed company services from catalog template (company_id=0). Falls back to hardcoded defaults."""
    if not pool:
        return
    async with pool.acquire() as conn:
        if not force:
            exists = await conn.fetchrow(
                "SELECT id FROM services WHERE company_id=$1 LIMIT 1", company_id
            )
            if exists:
                return
        if force:
            await conn.execute("DELETE FROM services WHERE company_id=$1", company_id)
        template = await conn.fetch(
            "SELECT key, name_ru, name_uz, emoji, order_idx FROM services WHERE company_id=0"
        )
    if template:
        for r in template:
            await upsert_service(r["key"], r["name_ru"], r["name_uz"], r["emoji"], r["order_idx"], company_id=company_id)
    else:
        defaults = [
            ("carpet",      "Чистка ковра",         "Gilam tozalash",      "🧺", 1),
            ("carpet_home", "Чистка ковра на дому",  "Uyda gilam tozalash", "🏠", 2),
            ("sofa",        "Диван, кресло",          "Divan, kreslo",       "🛋", 3),
            ("mattress",    "Матрас, одеяло",         "Matras, ko'rpa",      "🛏", 4),
            ("curtains",    "Шторы",                  "Pardalar",            "🪟", 5),
        ]
        for key, ru, uz, em, idx in defaults:
            await upsert_service(key, ru, uz, em, idx, company_id=company_id)

# ══════════════════════════════════════
#  ЕДИНИЦЫ ИЗМЕРЕНИЯ (общая таблица с ботом)
# ══════════════════════════════════════
async def get_all_units():
    if not pool:
        return []
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT * FROM units ORDER BY id")


async def add_unit(key: str, name_ru: str, name_uz: str, symbol_ru: str, symbol_uz: str) -> bool:
    if not pool:
        return False
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO units (key, name_ru, name_uz, symbol_ru, symbol_uz)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (key) DO UPDATE SET
                name_ru = EXCLUDED.name_ru,
                name_uz = EXCLUDED.name_uz,
                symbol_ru = EXCLUDED.symbol_ru,
                symbol_uz = EXCLUDED.symbol_uz
        """, key, name_ru, name_uz, symbol_ru, symbol_uz)
    return True


async def delete_unit(key: str) -> bool:
    if not pool:
        return False
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM units WHERE key=$1", key)
    return result != "DELETE 0"


_TYPE_NAMES_RU = {"standard": "Стандарт", "express": "Экспресс"}
_TYPE_NAMES_UZ = {"standard": "Standart", "express": "Ekspress"}

async def _price_match_maps() -> tuple[dict, dict]:
    """{service_ru_label: (svcKey, typeKey)} и то же для UZ — реконструирует те же
    подписи, что staff.html пишет в order_items.service_ru/service_uz при выборе
    услуги из справочника (см. sifFindPriceMatch в staff.html)."""
    prices   = await get_all_prices()
    services = await get_services()
    ru_map, uz_map = {}, {}
    for s in services:
        svc_key = s["key"]
        if svc_key not in prices:
            continue
        for type_key in prices[svc_key]:
            emoji = (s.get("emoji") or "").strip()
            ru_label = f"{emoji} {s['name_ru']}".strip() + f" — {_TYPE_NAMES_RU.get(type_key, type_key)}"
            uz_label = f"{emoji} {s['name_uz']}".strip() + f" — {_TYPE_NAMES_UZ.get(type_key, type_key)}"
            ru_map[ru_label] = (svc_key, type_key)
            uz_map[uz_label] = (svc_key, type_key)
    return ru_map, uz_map

async def get_orders_items_totals(order_ids: list[int]) -> dict[int, float]:
    """Order-level 'Итого' с учётом мин.по.позиции/мин.по.заказу — та же логика,
    что staffRenderItems() в staff.html (группировка по услуге, floor по каталогу).
    Используется, чтобы список заказов не расходился с карточкой заказа."""
    if not order_ids or not pool:
        return {}
    async with pool.acquire() as conn:
        items = await conn.fetch(
            "SELECT order_id, service, service_ru, service_uz, sqm, price_per_sqm, total_sum "
            "FROM order_items WHERE order_id = ANY($1::int[])", order_ids)
    prices = await get_all_prices()
    ru_map, uz_map = await _price_match_maps()

    def find_match(it):
        return (ru_map.get(it["service_ru"]) or uz_map.get(it["service_uz"])
                or ru_map.get(it["service"]) or uz_map.get(it["service"]))

    groups: dict[tuple, dict] = {}
    no_service_totals: dict[int, float] = {}
    for it in items:
        oid = it["order_id"]
        if not it["service"]:
            no_service_totals[oid] = no_service_totals.get(oid, 0.0) + float(it["total_sum"] or 0)
            continue
        m = find_match(it)
        gkey = it["service_ru"] or (f"{m[0]}::{m[1]}" if m else it["service"])
        g = groups.setdefault((oid, gkey), {"sqm": 0.0, "count": 0, "clamped": 0.0, "match": m})
        sqm   = float(it["sqm"] or 0)
        price = float(it["price_per_sqm"] or 0)
        total = float(it["total_sum"] or 0)
        g["sqm"]   += sqm
        g["count"] += 1
        catalog  = prices.get(m[0], {}).get(m[1]) if m else None
        pos_min  = catalog.get("min_order") if catalog else None
        g["clamped"] += (pos_min * price) if (pos_min and sqm and sqm < pos_min and price) else total

    totals: dict[int, float] = dict(no_service_totals)
    for (oid, _gkey), g in groups.items():
        m = g["match"]
        catalog   = prices.get(m[0], {}).get(m[1]) if m else None
        group_min = catalog.get("min_order_total") if catalog else None
        price     = catalog.get("price") if catalog else None
        qty       = g["sqm"] if g["sqm"] > 0 else g["count"]
        contribution = (group_min * price) if (group_min and qty < group_min and price) else g["clamped"]
        totals[oid] = totals.get(oid, 0.0) + contribution
    return totals


async def get_admin_orders(status: str = None, statuses: list = None, branch: str = None,
                            limit: int = 50, offset: int = 0):
    """Список заказов с постраничностью, всегда scoped на company_id. statuses (набор,
    из order_stages сотрудника) и status (выбранная вкладка) пересекаются, если заданы
    оба. Возвращает (orders, total_count)."""
    if not pool:
        return [], 0
    cid = _cid()
    async with pool.acquire() as conn:
        conditions = ["o.company_id = $1"]
        params = [cid]
        # statuses=[] (не None) значит «есть ограничение по этапам, но ни один не
        # распознан» — должно скрывать ВСЁ (fail-closed), а не пропускать
        # ограничение целиком, поэтому проверяем "is not None", а не truthiness.
        if statuses is not None:
            allowed = set(statuses) & {status} if status else set(statuses)
            conditions.append(f"o.status = ANY(${len(params)+1}::text[])")
            params.append(list(allowed) if allowed else ["__none__"])
        elif status:
            conditions.append(f"o.status = ${len(params)+1}")
            params.append(status)
        if branch:
            conditions.append(f"o.branch = ${len(params)+1}")
            params.append(branch)
        where = "WHERE " + " AND ".join(conditions)
        limit_idx, offset_idx = len(params) + 1, len(params) + 2
        q = f"""
            SELECT o.*,
                   COALESCE(i.cnt, 0)::int AS item_count,
                   COALESCE(i.corr, 0)::int AS corrected_count,
                   COALESCE((SELECT SUM(COALESCE(price_per_sqm,0)*COALESCE(sqm,0))
                              FROM order_items WHERE order_id=o.id), 0) AS items_total,
                   COALESCE((SELECT SUM(amount) FROM order_payments
                              WHERE order_id=o.id
                                AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL)), 0) AS paid_amount,
                   COUNT(*) OVER() AS total_count
            FROM orders o
            LEFT JOIN (
                SELECT order_id, COUNT(*) AS cnt,
                       COUNT(*) FILTER (WHERE measure_status='corrected') AS corr
                FROM order_items GROUP BY order_id
            ) i ON i.order_id = o.id
            {where}
            ORDER BY o.created_at DESC LIMIT ${limit_idx} OFFSET ${offset_idx}
        """
        params += [limit, offset]
        rows = await conn.fetch(q, *params)
        result = [dict(r) for r in rows]
        total = result[0]["total_count"] if result else 0
        for r in result:
            r.pop("total_count", None)
        totals = await get_orders_items_totals([r["id"] for r in result])
        for r in result:
            if r["id"] in totals:
                r["items_total"] = totals[r["id"]]
        return result, total


async def get_company_id_by_slug(slug: str) -> int | None:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id FROM companies WHERE slug=$1 AND active=TRUE", slug
        )
        return row["id"] if row else None


# ══════════════════════════════════════
#  СОТРУДНИКИ
# ══════════════════════════════════════
async def get_staff_by_login(login: str, company_id: int = 1):
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM staff WHERE login=$1 AND company_id=$2 AND active=TRUE",
            login, company_id
        )

async def get_staff_roles_by_logins(logins: list) -> dict:
    """Возвращает {login: role} для списка логинов (для проверки, что позицию замерил мойщик)."""
    if not pool or not logins: return {}
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT login, role FROM staff WHERE login = ANY($1)", list(logins))
        return {r["login"]: r["role"] for r in rows}

async def get_staff_by_site_user(site_user_id: int):
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM staff WHERE site_user_id=$1 AND active=TRUE", site_user_id
        )

async def link_staff_to_site_user(staff_id: int, site_user_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE staff SET site_user_id=$2 WHERE id=$1", staff_id, site_user_id
        )

async def get_staff_by_tg_id(tg_id):
    if not pool: return None
    async with pool.acquire() as conn:
        try:
            return await conn.fetchrow(
                "SELECT * FROM staff WHERE tg_id=$1 AND active=TRUE", int(tg_id))
        except Exception:
            try:
                return await conn.fetchrow(
                    "SELECT * FROM staff WHERE tg_id::text=$1 AND active=TRUE", str(tg_id))
            except Exception:
                return None

async def get_staff_by_tg_id_and_company(tg_id, company_id: int):
    """Как get_staff_by_tg_id, но со явным company_id — для бота заказов (вебхук общий
    на все компании, свою компанию узнаём из URL, не из contextvar)."""
    if not pool: return None
    async with pool.acquire() as conn:
        try:
            return await conn.fetchrow(
                "SELECT * FROM staff WHERE tg_id=$1 AND company_id=$2 AND active=TRUE", int(tg_id), company_id)
        except Exception:
            try:
                return await conn.fetchrow(
                    "SELECT * FROM staff WHERE tg_id::text=$1 AND company_id=$2 AND active=TRUE", str(tg_id), company_id)
            except Exception:
                return None

async def create_agent_from_user(user: dict, password_hash: str, branch: str = "") -> int:
    """Создаёт staff-аккаунт агента из пользователя сайта, авто-назначает зарплату 'leads'."""
    if not pool: return None
    # Получить глобальный процент комиссии за лид
    pct_str = await get_config("agent_commission_percent") or "0"
    try:
        lead_pct = float(pct_str)
    except ValueError:
        lead_pct = 0.0
    async with pool.acquire() as conn:
        staff_id = await conn.fetchval("""
            INSERT INTO staff (first_name, phone, login, password_hash, role,
                               tg_id, site_user_id, active, branch,
                               salary_type, salary_work_days, company_id)
            VALUES ($1,$2,$3,$4,'agent',$5,$6,TRUE,$7,'leads',26,$8)
            ON CONFLICT (company_id, login) DO NOTHING
            RETURNING id
        """, user["first_name"], user["phone"], user["phone"],
            password_hash, (str(user["tg_id"]) if user.get("tg_id") else None), user["id"], branch or None,
            user.get("company_id") or 1)
        if staff_id and lead_pct > 0:
            await conn.execute(
                "INSERT INTO staff_salary_percents (staff_id, role, percent)"
                " VALUES ($1,'lead',$2) ON CONFLICT (staff_id, role) DO NOTHING",
                staff_id, lead_pct)
        return staff_id

async def set_staff_temp_password(staff_id: int, temp_hash: str, expires_at):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE staff SET temp_password_hash=$2, temp_password_expires=$3,
                             must_change_password=TRUE
            WHERE id=$1
        """, staff_id, temp_hash, expires_at)

async def clear_staff_temp_password(staff_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE staff SET temp_password_hash=NULL, temp_password_expires=NULL,
                             must_change_password=FALSE
            WHERE id=$1
        """, staff_id)

async def get_staff_by_id(staff_id: int):
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM staff WHERE id=$1", staff_id)

async def get_first_admin_staff():
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM staff WHERE role='admin' AND active=TRUE ORDER BY id LIMIT 1")

async def get_all_staff():
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM staff WHERE company_id=$1 ORDER BY active DESC, first_name", cid
        )

async def get_staff_by_company(company_id: int):
    if not pool: return []
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT id, first_name, last_name, login, plain_password, role, active FROM staff WHERE company_id=$1 ORDER BY id",
            company_id
        )

async def count_staff_by_company(company_id: int) -> int:
    if not pool: return 0
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COUNT(*) AS cnt FROM staff WHERE company_id=$1 AND fired IS NOT TRUE",
            company_id
        )
        return row["cnt"] if row else 0

async def create_staff(data: dict, company_id: int | None = None) -> int:
    if not pool: return None
    cid = company_id if company_id is not None else _cid()
    async with pool.acquire() as conn:
        return await conn.fetchval("""
            INSERT INTO staff (first_name, last_name, middle_name, phone, login, password_hash,
                               plain_password, role, position, branch, tg_id, tg_username,
                               salary_type, salary_rate, hire_date, note, gender, birth_date,
                               company_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
            RETURNING id
        """, data["first_name"], data.get("last_name"), data.get("middle_name"),
            data.get("phone"), data["login"], data["password_hash"],
            data.get("plain_password"),
            data.get("role","callcenter"), data.get("position"), data.get("branch"),
            data.get("tg_id"), data.get("tg_username"),
            data.get("salary_type"), data.get("salary_rate"),
            data.get("hire_date"), data.get("note"), data.get("gender","M"),
            data.get("birth_date"), cid)

async def update_staff(staff_id: int, **kwargs):
    if not pool or not kwargs: return
    cid = _cid()
    allowed = {"first_name","last_name","middle_name","phone","login","role","position",
               "branch","tg_id","tg_username","salary_type","salary_rate","hire_date",
               "note","active","is_active","gender","birth_date"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if "is_active" in fields:
        fields["active"] = fields.pop("is_active")
    if not fields: return
    sets = ", ".join(f"{k}=${i+2}" for i, k in enumerate(fields))
    vals = list(fields.values())
    cid_param = len(fields) + 2
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE staff SET {sets}, updated_at=NOW() WHERE id=$1 AND company_id=${cid_param}",
            staff_id, *vals, cid
        )

async def get_staff_personal(staff_id: int) -> dict | None:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM staff_personal WHERE staff_id=$1", staff_id)
        return dict(row) if row else {}

async def upsert_staff_personal(staff_id: int, data: dict) -> None:
    if not pool: return
    fields = ["passport_series","passport_number","pinfl","home_address","extra_phone",
              "children_count","marital_status","spouse_name","spouse_birth_date",
              "spouse_phone","spouse_workplace","spouse_position"]
    filtered = {k: data.get(k) for k in fields}
    cols = ", ".join(filtered.keys())
    placeholders = ", ".join(f"${i+2}" for i in range(len(filtered)))
    updates = ", ".join(f"{k}=${i+2}" for i, k in enumerate(filtered))
    async with pool.acquire() as conn:
        await conn.execute(
            f"""INSERT INTO staff_personal (staff_id, {cols}, updated_at)
                VALUES ($1, {placeholders}, NOW())
                ON CONFLICT (staff_id) DO UPDATE SET {updates}, updated_at=NOW()""",
            staff_id, *list(filtered.values())
        )

async def get_staff_salary(staff_id: int) -> dict:
    if not pool: return {}
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT salary_type, salary_rate, salary_work_days, advance_percent FROM staff WHERE id=$1", staff_id)
        if not row:
            return {}
        result = {
            "salary_type":     row["salary_type"] or "fixed",
            "base_amount":     float(row["salary_rate"] or 0),
            "work_days":       row["salary_work_days"] or 26,
            "advance_percent": float(row["advance_percent"]) if row["advance_percent"] else None,
        }
        prows = await conn.fetch(
            "SELECT role, percent FROM staff_salary_percents WHERE staff_id=$1 AND company_id=$2 ORDER BY id", staff_id, cid)
        result["percents"] = [{"role": r["role"], "percent": float(r["percent"])} for r in prows]
        urows = await conn.fetch(
            "SELECT service_key, type_key, total_rate, unit_rate, rate_type "
            "FROM staff_salary_per_unit WHERE staff_id=$1 ORDER BY id", staff_id)
        result["per_unit"] = [{"service_key": r["service_key"], "type_key": r["type_key"],
                                "total_rate": float(r["total_rate"] or 0),
                                "unit_rate":  float(r["unit_rate"]  or 0),
                                "rate_type":  r["rate_type"] or "sum"} for r in urows]
        krows = await conn.fetch(
            "SELECT metric, target_value, bonus_type, bonus_value "
            "FROM staff_salary_kpi WHERE staff_id=$1 ORDER BY id", staff_id)
        result["kpi"] = [{"metric": r["metric"], "target_value": float(r["target_value"]),
                           "bonus_type": r["bonus_type"], "bonus_value": float(r["bonus_value"])} for r in krows]
        return result

async def save_staff_salary(staff_id: int, data: dict) -> None:
    if not pool: return
    cid = _cid()
    async with pool.acquire() as conn:
        adv_pct = data.get("advance_percent")
        await conn.execute(
            "UPDATE staff SET salary_type=$2, salary_rate=$3, salary_work_days=$4, advance_percent=$5, updated_at=NOW() WHERE id=$1",
            staff_id, data.get("salary_type", "fixed"),
            data.get("base_amount") or None, data.get("work_days", 26),
            float(adv_pct) if adv_pct is not None else None)
        await conn.execute("DELETE FROM staff_salary_percents WHERE staff_id=$1 AND company_id=$2", staff_id, cid)
        for p in (data.get("percents") or []):
            if p.get("role") and p.get("percent") is not None:
                await conn.execute(
                    "INSERT INTO staff_salary_percents (staff_id, role, percent, company_id) VALUES ($1,$2,$3,$4)"
                    " ON CONFLICT (staff_id, role) DO UPDATE SET percent=$3",
                    staff_id, p["role"], float(p["percent"]), cid)
        await conn.execute("DELETE FROM staff_salary_per_unit WHERE staff_id=$1", staff_id)
        for u in (data.get("per_unit") or []):
            if u.get("service_key") and u.get("type_key"):
                await conn.execute(
                    "INSERT INTO staff_salary_per_unit (staff_id, service_key, type_key, total_rate, unit_rate, rate_type)"
                    " VALUES ($1,$2,$3,$4,$5,$6)"
                    " ON CONFLICT (staff_id, service_key, type_key) DO UPDATE SET total_rate=$4, unit_rate=$5, rate_type=$6",
                    staff_id, u["service_key"], u["type_key"],
                    float(u.get("total_rate") or 0), float(u.get("unit_rate") or 0),
                    u.get("rate_type") or "sum")
        await conn.execute("DELETE FROM staff_salary_kpi WHERE staff_id=$1", staff_id)
        for k in (data.get("kpi") or []):
            if k.get("metric") and k.get("target_value") is not None:
                await conn.execute(
                    "INSERT INTO staff_salary_kpi (staff_id, metric, target_value, bonus_type, bonus_value)"
                    " VALUES ($1,$2,$3,$4,$5)",
                    staff_id, k["metric"], float(k["target_value"]),
                    k.get("bonus_type", "fixed"), float(k.get("bonus_value") or 0))

async def get_monthly_salary_calc(year: int, month: int, company_id: int = 1) -> list:
    """Расчёт зарплаты всех активных сотрудников за месяц."""
    if not pool: return []
    import calendar as _cal
    from datetime import date as _date
    start = _date(year, month, 1)
    end   = _date(year, month, _cal.monthrange(year, month)[1])

    async with pool.acquire() as conn:
        staff_rows = await conn.fetch("""
            SELECT s.id, s.first_name, s.last_name, s.role, s.branch, s.login,
                   s.salary_type, s.salary_rate, s.salary_work_days,
                   COALESCE(s.active, TRUE) AS active
            FROM staff s
            WHERE s.role <> 'agent'
              AND s.company_id = $3
              AND (
                s.active = TRUE OR s.active IS NULL
                OR EXISTS (
                  SELECT 1 FROM timesheet t
                  WHERE t.staff_id = s.id AND t.date >= $1 AND t.date <= $2
                )
              )
            ORDER BY s.branch NULLS LAST, s.last_name, s.first_name
        """, start, end, company_id)

        # Часы из табеля за период (все типы записей)
        ts_rows = await conn.fetch("""
            SELECT staff_id,
                   COALESCE(SUM(hours), 0)  AS total_hours,
                   COUNT(*)                  AS days_count
            FROM timesheet
            WHERE date >= $1 AND date <= $2
            GROUP BY staff_id
        """, start, end)
        ts_map = {r['staff_id']: r for r in ts_rows}

        # Забор/доставка водителей за период одним запросом
        route_rows = await conn.fetch("""
            SELECT r.driver_id,
                COUNT(CASE WHEN r.type = 'pickup' AND ro.stop_status = 'done' THEN 1 END) AS pickup_done,
                COUNT(CASE WHEN r.type IN ('delivery','mixed') AND ro.stop_status = 'done' THEN 1 END) AS delivery_done
            FROM routes r
            JOIN route_orders ro ON ro.route_id = r.id
            WHERE r.date >= $1 AND r.date <= $2 AND r.driver_id IS NOT NULL
            GROUP BY r.driver_id
        """, start, end)
        route_map = {r['driver_id']: r for r in route_rows}

        # Суммы заказов водителей за период (для percent pickup/delivery)
        route_sum_rows = await conn.fetch("""
            SELECT r.driver_id,
                COALESCE(SUM(CASE WHEN r.type = 'pickup'
                    THEN COALESCE((SELECT SUM(COALESCE(oi.actual_total_sum,
                                                       oi.price_per_sqm * COALESCE(oi.actual_sqm, oi.sqm), 0))
                                   FROM order_items oi WHERE oi.order_id = o.id), 0) END), 0) AS pickup_sum,
                COUNT(DISTINCT CASE WHEN r.type = 'pickup'
                    THEN ro.order_id END) AS pickup_count,
                COALESCE(SUM(CASE WHEN r.type IN ('delivery','mixed')
                    THEN COALESCE((SELECT SUM(COALESCE(oi.actual_total_sum,
                                                       oi.price_per_sqm * COALESCE(oi.actual_sqm, oi.sqm), 0))
                                   FROM order_items oi WHERE oi.order_id = o.id), 0) END), 0) AS delivery_sum,
                COUNT(DISTINCT CASE WHEN r.type IN ('delivery','mixed')
                    THEN ro.order_id END) AS delivery_count
            FROM routes r
            JOIN route_orders ro ON ro.route_id = r.id AND ro.stop_status = 'done'
            JOIN orders o ON o.id = ro.order_id
            WHERE r.date >= $1 AND r.date <= $2 AND r.driver_id IS NOT NULL
            GROUP BY r.driver_id
        """, start, end)
        route_sum_map = {r['driver_id']: dict(r) for r in route_sum_rows}

        # Ставки за точку
        pu_rows = await conn.fetch("""
            SELECT staff_id, service_key, unit_rate, rate_type
            FROM staff_salary_per_unit
            WHERE service_key IN ('__pickup__','__delivery__')
        """)
        pu_map = {}
        for r in pu_rows:
            pu_map.setdefault(r['staff_id'], {})[r['service_key']] = r

        # Процентные ставки сотрудников (тип percent / fixed_percent)
        sp_rows = await conn.fetch("SELECT staff_id, role, percent FROM staff_salary_percents")
        sp_map: dict = {}  # staff_id -> {role: percent}
        for r in sp_rows:
            sp_map.setdefault(r['staff_id'], {})[r['role']] = float(r['percent'])

        # Объём позиций по washer_login за период (для role='washing')
        washing_rows = await conn.fetch("""
            SELECT s.id AS staff_id,
                   COALESCE(SUM(
                       COALESCE(oi.actual_total_sum,
                                oi.price_per_sqm * COALESCE(oi.actual_sqm, oi.sqm), 0)
                   ), 0) AS total_sum,
                   COUNT(DISTINCT oi.id) AS item_count
            FROM staff s
            JOIN order_items oi ON oi.washer_login = s.login
            JOIN orders o ON o.id = oi.order_id
            WHERE s.login IS NOT NULL
              AND o.status != 'cancelled'
              AND COALESCE(o.washed_at, o.created_at) >= $1::timestamptz
              AND COALESCE(o.washed_at, o.created_at) <  $2::timestamptz
            GROUP BY s.id
        """, start, end)
        washing_map = {r['staff_id']: {'total': float(r['total_sum']), 'count': int(r['item_count'])}
                       for r in washing_rows}

        # Конвертированные лиды за период (для percent lead)
        lead_sum_rows = await conn.fetch("""
            SELECT l.assigned_to AS staff_id,
                COUNT(l.id) AS count,
                COALESCE(SUM(
                    COALESCE((SELECT SUM(COALESCE(oi.actual_total_sum,
                                                  oi.price_per_sqm * COALESCE(oi.actual_sqm, oi.sqm), 0))
                              FROM order_items oi WHERE oi.order_id = o.id), 0)
                ), 0) AS total
            FROM leads l
            LEFT JOIN orders o ON o.order_num = l.converted_order
            WHERE l.status = 'converted'
              AND l.updated_at >= $1::timestamptz
              AND l.updated_at <  $2::timestamptz
              AND l.assigned_to IS NOT NULL
            GROUP BY l.assigned_to
        """, start, end)
        lead_sum_map = {r['staff_id']: {'total': float(r['total']), 'count': int(r['count'])}
                        for r in lead_sum_rows}

        # Упакованные заказы за период (для percent packing)
        packing_rows = await conn.fetch("""
            SELECT s.id AS staff_id,
                COALESCE(SUM(
                    COALESCE((SELECT SUM(COALESCE(oi.actual_total_sum,
                                                  oi.price_per_sqm * COALESCE(oi.actual_sqm, oi.sqm), 0))
                              FROM order_items oi WHERE oi.order_id = o.id), 0)
                ), 0) AS total_sum,
                COUNT(DISTINCT o.id) AS order_count
            FROM staff s
            JOIN orders o ON o.packer_login = s.login
            WHERE s.login IS NOT NULL
              AND o.status NOT IN ('cancelled')
              AND COALESCE(o.packed_at, o.created_at) >= $1::timestamptz
              AND COALESCE(o.packed_at, o.created_at) <  $2::timestamptz
            GROUP BY s.id
        """, start, end)
        packing_map = {r['staff_id']: {'total': float(r['total_sum']), 'count': int(r['order_count'])}
                       for r in packing_rows}

        results = []
        for s in staff_rows:
            sid      = s['id']
            sal_type = s['salary_type'] or 'fixed'
            base     = float(s['salary_rate'] or 0)
            norm_days = int(s['salary_work_days'] or 26)
            entry = {
                'staff_id':    sid,
                'name':        f"{s['last_name'] or ''} {s['first_name'] or ''}".strip(),
                'role':        s['role'],
                'branch':      s['branch'],
                'active':      bool(s['active']),
                'salary_type': sal_type,
                'base_amount': base,
                'calc':        {},
                'total':       None,
            }

            # ── A: Фиксированная основа (fixed и fixed_percent) ──
            fixed_earn = 0.0
            if sal_type in ('fixed', 'fixed_percent'):
                ts = ts_map.get(sid)
                norm_hours = norm_days * 8.0
                if ts:
                    work_hours = float(ts['total_hours'])
                    work_days  = int(ts['days_count'])
                    if base > 0 and norm_hours > 0:
                        fixed_earn = round(base / norm_hours * work_hours, 2)
                    if sal_type == 'fixed':
                        entry['calc']  = {
                            'work_hours': work_hours, 'norm_hours': norm_hours,
                            'work_days':  work_days,  'norm_days':  norm_days,
                        }
                        entry['total'] = fixed_earn
                else:
                    entry['calc']  = {'no_timesheet': True, 'norm_days': norm_days}
                    entry['total'] = None
                    if sal_type == 'fixed':
                        results.append(entry)
                        continue

            # ── B: Зарплата за точку ──
            if sal_type == 'per_point':
                rd = route_map.get(sid, {})
                pickup_cnt   = int(rd.get('pickup_done')   or 0)
                delivery_cnt = int(rd.get('delivery_done') or 0)
                rates = pu_map.get(sid, {})
                p_row = rates.get('__pickup__')
                d_row = rates.get('__delivery__')
                p_rate = float(p_row['unit_rate'] if p_row else 0)
                d_rate = float(d_row['unit_rate'] if d_row else 0)
                entry['calc']  = {
                    'pickup_count':   pickup_cnt,  'pickup_rate':    p_rate,
                    'delivery_count': delivery_cnt, 'delivery_rate':  d_rate,
                }
                entry['total'] = pickup_cnt * p_rate + delivery_cnt * d_rate

            # ── C: Процентные составляющие (percent и fixed_percent) ──
            elif sal_type in ('percent', 'fixed_percent'):
                rates = sp_map.get(sid, {})
                total_pct  = 0.0
                calc_lines = {}

                washing_pct = rates.get('washing', 0)
                if washing_pct:
                    wd = washing_map.get(sid, {})
                    w_earn = round(wd.get('total', 0) * washing_pct / 100, 2)
                    total_pct += w_earn
                    calc_lines['washing'] = {
                        'pct': washing_pct, 'base': wd.get('total', 0),
                        'earn': w_earn, 'count': wd.get('count', 0),
                    }

                pickup_pct = rates.get('pickup', 0)
                if pickup_pct:
                    rd = route_sum_map.get(sid, {})
                    p_sum  = float(rd.get('pickup_sum',   0) or 0)
                    p_cnt  = int  (rd.get('pickup_count', 0) or 0)
                    p_earn = round(p_sum * pickup_pct / 100, 2)
                    total_pct += p_earn
                    calc_lines['pickup'] = {
                        'pct': pickup_pct, 'base': p_sum,
                        'earn': p_earn, 'count': p_cnt,
                    }

                delivery_pct = rates.get('delivery', 0)
                if delivery_pct:
                    rd = route_sum_map.get(sid, {})
                    d_sum  = float(rd.get('delivery_sum',   0) or 0)
                    d_cnt  = int  (rd.get('delivery_count', 0) or 0)
                    d_earn = round(d_sum * delivery_pct / 100, 2)
                    total_pct += d_earn
                    calc_lines['delivery'] = {
                        'pct': delivery_pct, 'base': d_sum,
                        'earn': d_earn, 'count': d_cnt,
                    }

                lead_pct = rates.get('lead', 0)
                if lead_pct:
                    ld = lead_sum_map.get(sid, {})
                    l_sum  = float(ld.get('total', 0) or 0)
                    l_cnt  = int  (ld.get('count', 0) or 0)
                    l_earn = round(l_sum * lead_pct / 100, 2)
                    total_pct += l_earn
                    calc_lines['lead'] = {
                        'pct': lead_pct, 'base': l_sum,
                        'earn': l_earn, 'count': l_cnt,
                    }

                packing_pct = rates.get('packing', 0)
                if packing_pct:
                    pd = packing_map.get(sid, {})
                    pk_sum  = float(pd.get('total', 0) or 0)
                    pk_cnt  = int  (pd.get('count', 0) or 0)
                    pk_earn = round(pk_sum * packing_pct / 100, 2)
                    total_pct += pk_earn
                    calc_lines['packing'] = {
                        'pct': packing_pct, 'base': pk_sum,
                        'earn': pk_earn, 'count': pk_cnt,
                    }

                if sal_type == 'fixed_percent' and fixed_earn:
                    calc_lines['fixed'] = {'rate': base, 'earn': fixed_earn}

                entry['calc']  = {'lines': calc_lines, 'norm_days': norm_days}
                entry['total'] = round(fixed_earn + total_pct, 2)

            # ── D: Прочие типы (не реализованы) ──
            elif sal_type not in ('fixed',):
                entry['total'] = None

            # ── E: Часы из табеля для не-фиксированных типов ──
            if sal_type not in ('fixed', 'fixed_percent'):
                ts = ts_map.get(sid)
                if ts:
                    norm_hours = norm_days * 8.0
                    entry['calc']['work_hours'] = float(ts['total_hours'])
                    entry['calc']['norm_hours'] = norm_hours
                    entry['calc']['work_days']  = int(ts['days_count'])
                    entry['calc']['norm_days']  = norm_days

            results.append(entry)

        return results


async def trigger_order_agent_commission(order_id: int, order_num: str, total_price: float) -> None:
    """При доставке заказа начисляет комиссию агенту, который создал лид."""
    if not pool or not total_price: return
    async with pool.acquire() as conn:
        # Уже начислено?
        exists = await conn.fetchval(
            "SELECT 1 FROM staff_commissions WHERE order_id=$1", order_id)
        if exists: return
        # Найти лид, из которого создан этот заказ
        lead = await conn.fetchrow(
            "SELECT id, created_by FROM leads WHERE converted_order=$1 AND created_by IS NOT NULL",
            order_num)
        if not lead: return
        agent_id = lead["created_by"]
        lead_id  = lead["id"]
        # Проверить что это агент
        role = await conn.fetchval("SELECT role FROM staff WHERE id=$1", agent_id)
        if role != "agent": return
        # Тип начисления из настроек
        comm_type = await conn.fetchval(
            "SELECT value FROM config WHERE key='agent_commission_type'") or "percent"
        # Индивидуальное значение агента (переопределяет глобальное)
        pct_row = await conn.fetchrow(
            "SELECT percent FROM staff_salary_percents WHERE staff_id=$1 AND role='lead'", agent_id)
        if comm_type == "fixed":
            if pct_row and float(pct_row["percent"]) > 0:
                amount = float(pct_row["percent"])
            else:
                fixed_str = await conn.fetchval(
                    "SELECT value FROM config WHERE key='agent_commission_fixed'") or "0"
                try: amount = float(fixed_str)
                except ValueError: amount = 0.0
            pct = 0  # не используется в fixed-режиме
            if amount <= 0: return
        else:
            if pct_row and float(pct_row["percent"]) > 0:
                pct = float(pct_row["percent"])
            else:
                pct_str = await conn.fetchval(
                    "SELECT value FROM config WHERE key='agent_commission_percent'") or "5.0"
                try: pct = float(pct_str)
                except ValueError: pct = 0.0
            if pct <= 0: return
            amount = round(total_price * pct / 100, 2)
        await conn.execute("""
            INSERT INTO staff_commissions
                (staff_id, order_id, order_num, lead_id, amount, percent, order_total)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
        """, agent_id, order_id, order_num, lead_id, amount, pct, total_price)

async def get_agent_commissions(staff_id: int, year: int = None, month: int = None) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        sql = """SELECT c.*, o.order_num as o_num
                 FROM staff_commissions c
                 LEFT JOIN orders o ON o.id = c.order_id
                 WHERE c.staff_id=$1"""
        args = [staff_id]
        if year:
            args.append(year); sql += f" AND EXTRACT(YEAR FROM c.created_at)=${len(args)}"
        if month:
            args.append(month); sql += f" AND EXTRACT(MONTH FROM c.created_at)=${len(args)}"
        sql += " ORDER BY c.created_at DESC LIMIT 200"
        rows = await conn.fetch(sql, *args)
        return [dict(r) for r in rows]

async def get_all_commissions(year: int = None, month: int = None) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        sql = """SELECT c.*, s.first_name, s.last_name
                 FROM staff_commissions c
                 LEFT JOIN staff s ON s.id = c.staff_id
                 WHERE 1=1"""
        args = []
        if year:
            args.append(year); sql += f" AND EXTRACT(YEAR FROM c.created_at)=${len(args)}"
        if month:
            args.append(month); sql += f" AND EXTRACT(MONTH FROM c.created_at)=${len(args)}"
        sql += " ORDER BY c.created_at DESC LIMIT 500"
        rows = await conn.fetch(sql, *args)
        return [dict(r) for r in rows]

async def update_staff_password(staff_id: int, password_hash: str, plain: str = None):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE staff SET password_hash=$2, plain_password=$3, updated_at=NOW() WHERE id=$1",
            staff_id, password_hash, plain
        )

async def get_company_admin_staff(company_id: int):
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM staff WHERE company_id=$1 AND role='admin' AND active=TRUE ORDER BY id LIMIT 1",
            company_id
        )

async def migrate_admin_logins():
    if not pool: return 0
    async with pool.acquire() as conn:
        res = await conn.execute("UPDATE staff SET login='admin' WHERE role='admin' AND login != 'admin'")
        return int(res.split()[-1])

# ══════════════════════════════════════
#  ОТДЕЛЫ И ДОЛЖНОСТИ
# ══════════════════════════════════════
async def get_departments(company_id: int):
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM departments WHERE company_id=$1 ORDER BY id", company_id)
        return [dict(r) for r in rows]

async def create_department(company_id: int, name: str, description: str = None, name_uz: str = '', description_uz: str = ''):
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "INSERT INTO departments (company_id, name, name_uz, description, description_uz) VALUES ($1,$2,$3,$4,$5) RETURNING *",
            company_id, name, name_uz or '', description, description_uz or ''
        )

async def update_department(dept_id: int, company_id: int, **fields):
    if not pool: return None
    allowed = {"name", "name_uz", "description", "description_uz"}
    sets, vals = [], [dept_id, company_id]
    for k, v in fields.items():
        if k in allowed:
            vals.append(v); sets.append(f"{k}=${len(vals)}")
    if not sets: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            f"UPDATE departments SET {', '.join(sets)} WHERE id=$1 AND company_id=$2 RETURNING *",
            *vals
        )

async def delete_department(dept_id: int, company_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM departments WHERE id=$1 AND company_id=$2", dept_id, company_id)

async def get_positions(company_id: int):
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM positions WHERE company_id=$1 ORDER BY sort_order, id",
            company_id
        )
        return [dict(r) for r in rows]

async def create_position(company_id: int, data: dict):
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """INSERT INTO positions (company_id, dept_id, name, name_uz, role, salary_type, salary_rate, description, description_uz, sort_order)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *""",
            company_id, data.get("dept_id"), data["name"], data.get("name_uz") or '',
            data.get("role"), data.get("salary_type"), data.get("salary_rate"),
            data.get("description"), data.get("description_uz") or '', data.get("sort_order", 0)
        )

async def update_position(pos_id: int, company_id: int, **fields):
    if not pool: return None
    allowed = {"dept_id", "name", "name_uz", "role", "salary_type", "salary_rate", "description", "description_uz"}
    sets, vals = [], [pos_id, company_id]
    for k, v in fields.items():
        if k in allowed:
            vals.append(v); sets.append(f"{k}=${len(vals)}")
    if not sets: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            f"UPDATE positions SET {', '.join(sets)} WHERE id=$1 AND company_id=$2 RETURNING *",
            *vals
        )

async def delete_position(pos_id: int, company_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM positions WHERE id=$1 AND company_id=$2", pos_id, company_id)

async def seed_departments_positions(company_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT id FROM departments WHERE company_id=$1 LIMIT 1", company_id)
    if existing:
        return
    depts = [
        # name, description, name_uz, description_uz
        ("Работа с клиентами", "Продажи, консультации, приём заказов",          "Mijozlar bilan ishlash",  "Sotish, maslahat, buyurtmalarni qabul qilish"),
        ("Логистика",          "Забор, доставка, маршруты",                      "Logistika",               "Olib kelish, yetkazib berish, marshrutlar"),
        ("Производство",       "Чистка, сушка, упаковка, контроль качества",     "Ishlab chiqarish",        "Tozalash, quritish, qadoqlash, sifat nazorati"),
        ("Склад",              "Учёт и хранение изделий",                        "Ombor",                   "Buyumlarni hisobga olish va saqlash"),
        ("Технический отдел",  "Обслуживание оборудования",                      "Texnik bo'lim",           "Uskunalarga texnik xizmat ko'rsatish"),
        ("Администрация",      "Управление, бухгалтерия, кадры",                 "Ma'muriyat",              "Boshqaruv, buxgalteriya, kadrlar"),
    ]
    dept_ids = []
    for name, desc, name_uz, desc_uz in depts:
        row = await create_department(company_id, name, desc, name_uz, desc_uz)
        dept_ids.append(row["id"])
    dClient, dLogistic, dProd, dWarehouse, dTech, dAdmin = dept_ids
    pos_list = [
        # dept_id, name, name_uz, role, salary_type, salary_rate, desc, desc_uz
        # Работа с клиентами
        (dClient,    "Менеджер по продажам",  "Savdo bo'yicha menejer",        "manager",    "mixed",   2000000, "Консультирует клиентов, оформляет заказы, работает с входящими обращениями",  "Mijozlarga maslahat beradi, buyurtmalarni rasmiylashtiradi, kiruvchi murojaatlar bilan ishlaydi"),
        (dClient,    "Оператор колл-центра",  "Qo'ng'iroq markazi operatori",  "callcenter", "fixed",   1500000, "Обрабатывает входящие звонки, мессенджеры и Telegram-бот",                    "Kiruvchi qo'ng'iroqlar, messenjerlar va Telegram-botni qayta ishlaydi"),
        (dClient,    "Приёмщик",              "Qabul qiluvchi",                "receiver",   "fixed",   1500000, "Встречает клиентов в точке приёма, оформляет документы",                      "Qabul punktida mijozlarni kutib oladi, hujjatlarni rasmiylashtiradi"),
        # Логистика
        (dLogistic,  "Водитель-курьер",       "Haydovchi-kuryer",              "driver",     "mixed",   1800000, "Забор и доставка изделий клиентам",                                           "Buyumlarni mijozlardan olib keladi va yetkazib beradi"),
        (dLogistic,  "Оператор логистики",    "Logistika operatori",           "logistics",  "fixed",   1600000, "Планирует маршруты, контролирует забор и доставку",                           "Marshrutlarni rejalashtiradi, olib kelish va yetkazib berishni nazorat qiladi"),
        (dLogistic,  "Экспедитор",            "Ekspeditor",                    "driver",     "fixed",   1400000, "Сопровождение груза, оформление документов при передаче",                     "Yukni hamroh qiladi, topshirish vaqtida hujjatlarni rasmiylashtiradi"),
        # Производство
        (dProd,      "Чистильщик / Мойщик",  "Tozalovchi / Yuvuvchi",        "washer",     "mixed",   1800000, "Химчистка ковров, мягкой мебели, матрасов и штор",                            "Gilam, yumshoq mebel, matras va pardalarni kimyoviy tozalaydi"),
        (dProd,      "Оператор сушки",        "Quritish operatori",            "washer",     "fixed",   1500000, "Контроль сушильного оборудования, температурного режима",                     "Quritish uskunasi va harorat rejimini nazorat qiladi"),
        (dProd,      "Оператор упаковки",     "Qadoqlash operatori",           "packer",     "fixed",   1400000, "Упаковка готовых изделий, маркировка, подготовка к выдаче",                   "Tayyor buyumlarni qadoqlaydi, belgilaydi, berish uchun tayyorlaydi"),
        (dProd,      "Контролёр качества",    "Sifat nazoratchisi",            "washer",     "fixed",   1700000, "Проверка качества до и после чистки, приёмка от мойщика",                     "Tozalashdan oldin va keyin sifatni tekshiradi, yuvuvchidan qabul qiladi"),
        # Склад
        (dWarehouse, "Кладовщик",             "Omborchi",                      "storekeeper","fixed",   1500000, "Учёт поступивших и выданных изделий, ведение складского журнала",             "Kelgan va berilgan buyumlarni hisobga oladi, ombor jurnalini yuritadi"),
        (dWarehouse, "Сортировщик",           "Saralovchi",                    "sorter",     "fixed",   1300000, "Распределение изделий по типу, срочности и клиенту",                          "Buyumlarni tur, shoshilinchlik va mijoz bo'yicha ajratadi"),
        # Технический отдел
        (dTech,      "Техник / Механик",      "Texnik / Mexanik",              "technician", "fixed",   2000000, "Обслуживание и ремонт чистящего и сушильного оборудования",                   "Tozalash va quritish uskunalarini xizmat ko'rsatadi va ta'mirlaydi"),
        # Администрация
        (dAdmin,     "Директор",              "Direktor",                      "admin",      "fixed",   5000000, "Общее руководство компанией",                                                 "Kompaniyani umumiy boshqarish"),
        (dAdmin,     "Бухгалтер",             "Buxgalter",                     "admin",      "fixed",   2500000, "Финансовый учёт, зарплата, налоги",                                           "Moliyaviy hisobot, ish haqi, soliqlar"),
        (dAdmin,     "HR-менеджер",           "HR-menejer",                    "admin",      "fixed",   2000000, "Кадры, табель, подбор персонала",                                             "Kadrlar, tabel, xodimlarni tanlash"),
        (dAdmin,     "IT-администратор",      "IT-administrator",              "admin",      "fixed",   2200000, "Поддержка сайта, бота, оборудования",                                        "Sayt, bot va uskunalarni qo'llab-quvvatlash"),
        (dAdmin,     "Агент",                 "Agent",                         "agent",      "percent", 0,       "Привлечение клиентов",                                                        "Mijozlarni jalb qilish"),
    ]
    for i, (dept_id, name, name_uz, role, salary_type, salary_rate, desc, desc_uz) in enumerate(pos_list):
        await create_position(company_id, {
            "dept_id": dept_id, "name": name, "name_uz": name_uz, "role": role,
            "salary_type": salary_type, "salary_rate": salary_rate,
            "description": desc, "description_uz": desc_uz, "sort_order": i + 1
        })


# ── ШАБЛОН ОТДЕЛОВ И ДОЛЖНОСТЕЙ (company_id=0) ──────────────────────────────
TEMPLATE_CID = 0

async def get_template_departments():
    return await get_departments(TEMPLATE_CID)

async def create_template_department(name: str, description: str = None, name_uz: str = '', description_uz: str = ''):
    return await create_department(TEMPLATE_CID, name, description, name_uz, description_uz)

async def update_template_department(dept_id: int, **fields):
    return await update_department(dept_id, TEMPLATE_CID, **fields)

async def delete_template_department(dept_id: int):
    await delete_department(dept_id, TEMPLATE_CID)

async def get_template_positions():
    return await get_positions(TEMPLATE_CID)

async def create_template_position(data: dict):
    return await create_position(TEMPLATE_CID, data)

async def update_template_position(pos_id: int, **fields):
    return await update_position(pos_id, TEMPLATE_CID, **fields)

async def delete_template_position(pos_id: int):
    await delete_position(pos_id, TEMPLATE_CID)

async def import_template_from_company(company_id: int):
    """Copy departments+positions from company_id into template (company_id=0). Clears existing template first."""
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM positions WHERE company_id=$1", TEMPLATE_CID)
        await conn.execute("DELETE FROM departments WHERE company_id=$1", TEMPLATE_CID)
    # Copy departments, build mapping old_id -> new_id
    old_depts = await get_departments(company_id)
    id_map = {}
    for d in old_depts:
        new_d = await create_template_department(d["name"], d.get("description"), d.get("name_uz", ""), d.get("description_uz", ""))
        id_map[d["id"]] = new_d["id"]
    # Copy positions with remapped dept_id
    old_pos = await get_positions(company_id)
    for p in old_pos:
        await create_template_position({
            "dept_id":        id_map.get(p["dept_id"]) if p["dept_id"] else None,
            "name":           p["name"],
            "name_uz":        p.get("name_uz", ""),
            "role":           p.get("role"),
            "salary_type":    p.get("salary_type"),
            "salary_rate":    p.get("salary_rate"),
            "description":    p.get("description"),
            "description_uz": p.get("description_uz", ""),
        })

async def seed_from_template(company_id: int):
    """Seed company from template (company_id=0). If template empty, use hardcoded defaults."""
    if not pool: return
    async with pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT id FROM departments WHERE company_id=$1 LIMIT 1", company_id)
    if existing:
        return
    template_depts = await get_template_departments()
    if template_depts:
        id_map = {}
        for d in template_depts:
            new_d = await create_department(company_id, d["name"], d.get("description"), d.get("name_uz", ""), d.get("description_uz", ""))
            id_map[d["id"]] = new_d["id"]
        template_pos = await get_template_positions()
        for p in template_pos:
            await create_position(company_id, {
                "dept_id":        id_map.get(p["dept_id"]) if p["dept_id"] else None,
                "name":           p["name"],
                "name_uz":        p.get("name_uz", ""),
                "role":           p.get("role"),
                "salary_type":    p.get("salary_type"),
                "salary_rate":    p.get("salary_rate"),
                "description":    p.get("description"),
                "description_uz": p.get("description_uz", ""),
            })
    else:
        await seed_departments_positions(company_id)
    await seed_company_services(company_id)
    await seed_company_prices(company_id)


async def import_departments_from_template(company_id: int):
    """Delete company's departments and copy from template (company_id=0)."""
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM positions WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM departments WHERE company_id=$1", company_id)
    template_depts = await get_template_departments()
    id_map = {}
    for d in template_depts:
        new_d = await create_department(company_id, d["name"], d.get("description"), d.get("name_uz", ""), d.get("description_uz", ""))
        id_map[d["id"]] = new_d["id"]
    return id_map

async def import_positions_from_template(company_id: int):
    """Delete company's positions and copy from template (company_id=0), preserving sort_order."""
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM positions WHERE company_id=$1", company_id)
    company_depts = await get_departments(company_id)
    company_dept_map = {d["name"]: d["id"] for d in company_depts}
    template_depts = await get_template_departments()
    template_dept_map = {d["id"]: d["name"] for d in template_depts}
    template_pos = await get_template_positions()  # ordered by sort_order, id
    for i, p in enumerate(template_pos):
        dept_name = template_dept_map.get(p["dept_id"]) if p["dept_id"] else None
        dept_id = company_dept_map.get(dept_name) if dept_name else None
        await create_position(company_id, {
            "dept_id":        dept_id,
            "name":           p["name"],
            "name_uz":        p.get("name_uz", ""),
            "role":           p.get("role"),
            "salary_type":    p.get("salary_type"),
            "salary_rate":    p.get("salary_rate"),
            "description":    p.get("description"),
            "description_uz": p.get("description_uz", ""),
            "sort_order":     p.get("sort_order") or (i + 1),
        })

async def migrate_name_uz():
    """Fill name_uz / description_uz for existing departments and positions where still empty."""
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE departments SET
                name_uz = CASE name
                    WHEN 'Работа с клиентами' THEN 'Mijozlar bilan ishlash'
                    WHEN 'Логистика'          THEN 'Logistika'
                    WHEN 'Производство'       THEN 'Ishlab chiqarish'
                    WHEN 'Склад'              THEN 'Ombor'
                    WHEN 'Технический отдел'  THEN 'Texnik bo''lim'
                    WHEN 'Администрация'      THEN 'Ma''muriyat'
                    ELSE name_uz END,
                description_uz = CASE name
                    WHEN 'Работа с клиентами' THEN 'Sotish, maslahat, buyurtmalarni qabul qilish'
                    WHEN 'Логистика'          THEN 'Olib kelish, yetkazib berish, marshrutlar'
                    WHEN 'Производство'       THEN 'Tozalash, quritish, qadoqlash, sifat nazorati'
                    WHEN 'Склад'              THEN 'Buyumlarni hisobga olish va saqlash'
                    WHEN 'Технический отдел'  THEN 'Uskunalarga texnik xizmat ko''rsatish'
                    WHEN 'Администрация'      THEN 'Boshqaruv, buxgalteriya, kadrlar'
                    ELSE description_uz END
            WHERE name_uz = '' OR description_uz = ''
        """)
        await conn.execute("""
            UPDATE positions SET
                name_uz = CASE name
                    WHEN 'Менеджер по продажам'  THEN 'Savdo bo''yicha menejer'
                    WHEN 'Оператор колл-центра'  THEN 'Qo''ng''iroq markazi operatori'
                    WHEN 'Приёмщик'              THEN 'Qabul qiluvchi'
                    WHEN 'Водитель-курьер'        THEN 'Haydovchi-kuryer'
                    WHEN 'Оператор логистики'    THEN 'Logistika operatori'
                    WHEN 'Экспедитор'            THEN 'Ekspeditor'
                    WHEN 'Чистильщик / Мойщик'  THEN 'Tozalovchi / Yuvuvchi'
                    WHEN 'Оператор сушки'        THEN 'Quritish operatori'
                    WHEN 'Оператор упаковки'     THEN 'Qadoqlash operatori'
                    WHEN 'Контролёр качества'    THEN 'Sifat nazoratchisi'
                    WHEN 'Кладовщик'             THEN 'Omborchi'
                    WHEN 'Сортировщик'           THEN 'Saralovchi'
                    WHEN 'Техник / Механик'      THEN 'Texnik / Mexanik'
                    WHEN 'Директор'              THEN 'Direktor'
                    WHEN 'Бухгалтер'             THEN 'Buxgalter'
                    WHEN 'HR-менеджер'           THEN 'HR-menejer'
                    WHEN 'IT-администратор'      THEN 'IT-administrator'
                    WHEN 'Агент'                 THEN 'Agent'
                    ELSE name_uz END,
                description_uz = CASE name
                    WHEN 'Менеджер по продажам'  THEN 'Mijozlarga maslahat beradi, buyurtmalarni rasmiylashtiradi, kiruvchi murojaatlar bilan ishlaydi'
                    WHEN 'Оператор колл-центра'  THEN 'Kiruvchi qo''ng''iroqlar, messenjerlar va Telegram-botni qayta ishlaydi'
                    WHEN 'Приёмщик'              THEN 'Qabul punktida mijozlarni kutib oladi, hujjatlarni rasmiylashtiradi'
                    WHEN 'Водитель-курьер'        THEN 'Buyumlarni mijozlardan olib keladi va yetkazib beradi'
                    WHEN 'Оператор логистики'    THEN 'Marshrutlarni rejalashtiradi, olib kelish va yetkazib berishni nazorat qiladi'
                    WHEN 'Экспедитор'            THEN 'Yukni hamroh qiladi, topshirish vaqtida hujjatlarni rasmiylashtiradi'
                    WHEN 'Чистильщик / Мойщик'  THEN 'Gilam, yumshoq mebel, matras va pardalarni kimyoviy tozalaydi'
                    WHEN 'Оператор сушки'        THEN 'Quritish uskunasi va harorat rejimini nazorat qiladi'
                    WHEN 'Оператор упаковки'     THEN 'Tayyor buyumlarni qadoqlaydi, belgilaydi, berish uchun tayyorlaydi'
                    WHEN 'Контролёр качества'    THEN 'Tozalashdan oldin va keyin sifatni tekshiradi, yuvuvchidan qabul qiladi'
                    WHEN 'Кладовщик'             THEN 'Kelgan va berilgan buyumlarni hisobga oladi, ombor jurnalini yuritadi'
                    WHEN 'Сортировщик'           THEN 'Buyumlarni tur, shoshilinchlik va mijoz bo''yicha ajratadi'
                    WHEN 'Техник / Механик'      THEN 'Tozalash va quritish uskunalarini xizmat ko''rsatadi va ta''mirlaydi'
                    WHEN 'Директор'              THEN 'Kompaniyani umumiy boshqarish'
                    WHEN 'Бухгалтер'             THEN 'Moliyaviy hisobot, ish haqi, soliqlar'
                    WHEN 'HR-менеджер'           THEN 'Kadrlar, tabel, xodimlarni tanlash'
                    WHEN 'IT-администратор'      THEN 'Sayt, bot va uskunalarni qo''llab-quvvatlash'
                    WHEN 'Агент'                 THEN 'Mijozlarni jalb qilish'
                    ELSE description_uz END
            WHERE name_uz = '' OR description_uz = ''
        """)

async def migrate_company1_positions():
    """One-time: add 6 missing positions to company_id=1 (seeded before 18-pos update)."""
    if not pool: return
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM positions WHERE company_id=1")
        if count >= 18:
            return
        depts = await conn.fetch("SELECT id, name FROM departments WHERE company_id=1")
        if not depts:
            return
        dept_map = {d["name"]: d["id"] for d in depts}
        missing = [
            (dept_map.get("Логистика"),    "Экспедитор",         "driver", "fixed",   1400000, "Сопровождение груза, оформление документов при передаче"),
            (dept_map.get("Производство"), "Оператор сушки",     "washer", "fixed",   1500000, "Контроль сушильного оборудования, температурного режима"),
            (dept_map.get("Производство"), "Контролёр качества", "washer", "fixed",   1700000, "Проверка качества до и после чистки, приёмка от мойщика"),
            (dept_map.get("Администрация"),"Бухгалтер",          "admin",  "fixed",   2500000, "Финансовый учёт, зарплата, налоги"),
            (dept_map.get("Администрация"),"HR-менеджер",        "admin",  "fixed",   2000000, "Кадры, табель, подбор персонала"),
            (dept_map.get("Администрация"),"IT-администратор",   "admin",  "fixed",   2200000, "Поддержка сайта, бота, оборудования"),
        ]
        for dept_id, name, role, salary_type, salary_rate, desc in missing:
            if not dept_id:
                continue
            exists = await conn.fetchval(
                "SELECT id FROM positions WHERE company_id=1 AND name=$1", name
            )
            if not exists:
                await conn.execute(
                    """INSERT INTO positions (company_id, dept_id, name, role, salary_type, salary_rate, description)
                       VALUES (1, $1, $2, $3, $4, $5, $6)""",
                    dept_id, name, role, salary_type, salary_rate, desc
                )


# ══════════════════════════════════════
#  ЛИДЫ
# ══════════════════════════════════════
async def get_timesheet(year: int, month: int, staff_id: int = None) -> list:
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        sql = """
            SELECT t.id, t.staff_id, t.date::text, t.hours, t.type, t.note,
                   s.first_name, s.last_name,
                   (s.first_name || ' ' || COALESCE(s.last_name,'')) AS staff_name
            FROM timesheet t
            JOIN staff s ON s.id = t.staff_id
            WHERE EXTRACT(YEAR FROM t.date)=$1 AND EXTRACT(MONTH FROM t.date)=$2
              AND t.company_id=$3
        """
        args = [year, month, cid]
        if staff_id:
            args.append(staff_id); sql += f" AND t.staff_id=${len(args)}"
        sql += " ORDER BY t.date DESC, s.last_name, s.first_name"
        rows = await conn.fetch(sql, *args)
        return [dict(r) for r in rows]

async def save_timesheet(data: dict) -> dict:
    if not pool: return {}
    cid = _cid()
    from datetime import date as _date
    d = data["date"]
    if isinstance(d, str): d = _date.fromisoformat(d)
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO timesheet (staff_id, date, hours, type, note, company_id)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (staff_id, date)
            DO UPDATE SET hours=$3, type=$4, note=$5
            RETURNING id, staff_id, date::text, hours, type, note
        """, int(data["staff_id"]), d,
            float(data.get("hours") or 8),
            data.get("type", "work"),
            data.get("note", ""),
            cid)
        return dict(row) if row else {}

async def update_timesheet(ts_id: int, data: dict) -> dict:
    if not pool: return {}
    from datetime import date as _date
    d = data["date"]
    if isinstance(d, str): d = _date.fromisoformat(d)
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE timesheet SET staff_id=$2, date=$3, hours=$4, type=$5, note=$6
            WHERE id=$1
            RETURNING id, staff_id, date::text, hours, type, note
        """, ts_id, int(data["staff_id"]), d,
            float(data.get("hours") or 8),
            data.get("type", "work"),
            data.get("note", ""))
        return dict(row) if row else {}

async def delete_timesheet(ts_id: int) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM timesheet WHERE id=$1", ts_id)
        return result == "DELETE 1"

async def init_timesheet_month(year: int, month: int, until_today: bool = False) -> dict:
    """Создать записи 'work' для всех активных не-агентов на каждый будний день месяца.
    Пропускает сотрудников с salary_type='percent'.
    Учитывает hire_date — не создаёт записи раньше даты приёма.
    ON CONFLICT DO NOTHING — уже существующие записи не трогает."""
    if not pool: return {"created": 0}
    import calendar as _cal
    from datetime import date
    _, last_day = _cal.monthrange(year, month)
    today = date.today()
    if until_today and date(year, month, 1) <= today:
        last_day = min(last_day, today.day if (today.year == year and today.month == month) else last_day)
    month_start = date(year, month, 1)
    async with pool.acquire() as conn:
        staff_rows = await conn.fetch("""
            SELECT id, salary_type, hire_date FROM staff
            WHERE (active IS NULL OR active = TRUE)
              AND COALESCE(role,'') != 'agent'
              AND COALESCE(salary_type,'') NOT IN ('percent','per_unit','per_point')
        """)
        count = 0
        skipped = 0
        for s in staff_rows:
            # Дата приёма: если нанят позже начала месяца — начинаем с его даты
            hire = s["hire_date"]
            if hire and hire > date(year, month, last_day):
                skipped += 1
                continue  # ещё не работал в этом месяце
            start_day = max(1, hire.day if (hire and hire.year == year and hire.month == month) else 1)
            for day in range(start_day, last_day + 1):
                d = date(year, month, day)
                if d.weekday() == 6:  # воскресенье — пропуск
                    continue
                r = await conn.execute("""
                    INSERT INTO timesheet (staff_id, date, hours, type)
                    VALUES ($1, $2, 8, 'work')
                    ON CONFLICT (staff_id, date) DO NOTHING
                """, s["id"], d)
                if r.endswith("0 1"):
                    count += 1
    return {"created": count, "skipped": skipped}

async def reset_timesheet_month(year: int, month: int) -> dict:
    if not pool: return {"deleted": 0}
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM timesheet WHERE EXTRACT(YEAR FROM date)=$1 AND EXTRACT(MONTH FROM date)=$2",
            year, month)
        deleted = int(result.split()[-1]) if result else 0
    return {"deleted": deleted}

# ══════════════════════════════════════
#  ОТМЕТКИ ПРИХОДА/УХОДА (staff_attendance)
# ══════════════════════════════════════

async def attendance_check_in(staff_id: int) -> dict:
    if not pool: return {}
    cid = _cid()
    today = datetime.now(_TASHKENT).date()
    async with pool.acquire() as conn:
        events = await conn.fetch("""
            SELECT id, staff_id, event_type, created_at
            FROM staff_attendance_events
            WHERE staff_id=$1 AND (created_at AT TIME ZONE 'Asia/Tashkent')::date = $2
            ORDER BY created_at
        """, staff_id, today)
        if events and events[-1]["event_type"] == "in":
            return {"error": "already_in"}
        row = await conn.fetchrow("""
            INSERT INTO staff_attendance_events (staff_id, event_type, company_id)
            VALUES ($1, 'in', $2)
            RETURNING id, staff_id, event_type, created_at
        """, staff_id, cid)
        return dict(row) if row else {}

async def attendance_check_out(staff_id: int) -> dict:
    if not pool: return {}
    cid = _cid()
    today = datetime.now(_TASHKENT).date()
    async with pool.acquire() as conn:
        events = await conn.fetch("""
            SELECT id, staff_id, event_type, created_at
            FROM staff_attendance_events
            WHERE staff_id=$1 AND (created_at AT TIME ZONE 'Asia/Tashkent')::date = $2
            ORDER BY created_at
        """, staff_id, today)
        if not events or events[-1]["event_type"] == "out":
            return {"error": "not_checked_in"}
        row = await conn.fetchrow("""
            INSERT INTO staff_attendance_events (staff_id, event_type, company_id)
            VALUES ($1, 'out', $2)
            RETURNING id, staff_id, event_type, created_at
        """, staff_id, cid)
        return dict(row) if row else {}

def _pair_attendance_sessions(events: list) -> list:
    """Pairs consecutive in/out events (sorted ascending by created_at) into sessions."""
    sessions = []
    open_session = None
    for ev in events:
        if ev["event_type"] == "in":
            open_session = {"check_in_at": ev["created_at"], "check_out_at": None}
            sessions.append(open_session)
        elif ev["event_type"] == "out":
            if open_session is not None and open_session["check_out_at"] is None:
                open_session["check_out_at"] = ev["created_at"]
                open_session = None
            # else: stray "out" with no open session — defensively skip it
    return sessions

def _sessions_total_hours(sessions: list) -> float:
    total = timedelta()
    for s in sessions:
        if s["check_out_at"] is not None:
            total += (s["check_out_at"] - s["check_in_at"])
    return round(total.total_seconds() / 3600, 1)

async def get_attendance_today(staff_id: int) -> dict:
    if not pool: return {"current_state": "out", "sessions": [], "total_hours": 0}
    today = datetime.now(_TASHKENT).date()
    async with pool.acquire() as conn:
        events = await conn.fetch("""
            SELECT id, staff_id, event_type, created_at
            FROM staff_attendance_events
            WHERE staff_id=$1 AND (created_at AT TIME ZONE 'Asia/Tashkent')::date = $2
            ORDER BY created_at
        """, staff_id, today)
        events = [dict(e) for e in events]
        sessions = _pair_attendance_sessions(events)
        current_state = "in" if events and events[-1]["event_type"] == "in" else "out"
        total_hours = _sessions_total_hours(sessions)
        return {
            "current_state": current_state,
            "sessions": [
                {
                    "check_in_at": s["check_in_at"].isoformat() if s["check_in_at"] else None,
                    "check_out_at": s["check_out_at"].isoformat() if s["check_out_at"] else None,
                }
                for s in sessions
            ],
            "total_hours": total_hours,
        }

async def get_admin_attendance(year: int, month: int, staff_id: int = None) -> list:
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        sql = """
            SELECT a.staff_id, a.event_type, a.created_at,
                   s.first_name, s.last_name, s.role, s.branch, s.salary_type
            FROM staff_attendance_events a
            JOIN staff s ON s.id = a.staff_id
            WHERE EXTRACT(YEAR FROM (a.created_at AT TIME ZONE 'Asia/Tashkent'))=$1
              AND EXTRACT(MONTH FROM (a.created_at AT TIME ZONE 'Asia/Tashkent'))=$2
              AND a.company_id=$3
        """
        args = [year, month, cid]
        if staff_id:
            args.append(staff_id); sql += f" AND a.staff_id=${len(args)}"
        sql += " ORDER BY a.staff_id, a.created_at"
        rows = await conn.fetch(sql, *args)

        groups = {}
        meta = {}
        for r in rows:
            local_date = r["created_at"].astimezone(_TASHKENT).date()
            key = (r["staff_id"], local_date)
            groups.setdefault(key, []).append({
                "event_type": r["event_type"],
                "created_at": r["created_at"],
            })
            if key not in meta:
                meta[key] = {
                    "staff_id": r["staff_id"],
                    "name": f"{r['last_name'] or ''} {r['first_name'] or ''}".strip(),
                    "role": r["role"],
                    "branch": r["branch"],
                }

        result = []
        for key, events in groups.items():
            sessions = _pair_attendance_sessions(events)
            result.append({
                "staff_id": meta[key]["staff_id"],
                "name": meta[key]["name"],
                "role": meta[key]["role"],
                "branch": meta[key]["branch"],
                "date": key[1].strftime("%Y-%m-%d"),
                "sessions": [
                    {
                        "check_in_at": s["check_in_at"].isoformat() if s["check_in_at"] else None,
                        "check_out_at": s["check_out_at"].isoformat() if s["check_out_at"] else None,
                    }
                    for s in sessions
                ],
                "total_hours": _sessions_total_hours(sessions),
            })
        result.sort(key=lambda x: x["staff_id"])
        result.sort(key=lambda x: x["date"], reverse=True)
        return result

async def create_lead(data: dict, company_id: int | None = None) -> dict:
    """company_id: явный параметр для вызовов вне обычного request-контекста (например,
    из order_bot_handlers.py — общий вебхук на все компании, contextvar _cid() там не
    настроен per-request). Если не передан — берём из contextvar, как раньше."""
    if not pool: return None
    cid = company_id if company_id is not None else _cid()
    # Тег акции (только видимость для сотрудников, не расходует окно) — только для
    # лидов с сайта/бота, привязанных к зарегистрированному пользователю с живым окном
    promo_id = None
    source = data.get("source", "staff")
    if source in ("site", "bot") and data.get("client_phone"):
        try:
            async with pool.acquire() as pconn:
                user = await pconn.fetchrow("SELECT id FROM users WHERE phone=$1", data["client_phone"])
                if user:
                    promo_row = await pconn.fetchrow("""
                        SELECT pus.promotion_id
                        FROM promo_user_state pus
                        JOIN promotions p ON p.id = pus.promotion_id
                        WHERE pus.user_id=$1 AND pus.used_order_id IS NULL
                          AND pus.expires_at > NOW() AND p.is_active = TRUE
                        ORDER BY pus.created_at DESC LIMIT 1
                    """, user["id"])
                    if promo_row:
                        promo_id = promo_row["promotion_id"]
        except Exception as e:
            logging.error(f"create_lead: promo tag failed: {e}")
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO leads (client_name, client_phone, service, branch,
                               city, address, short_address, note, status, assigned_to,
                               created_by, volunteer_id, location, location_address,
                               source, client_tg_id, pickup_date, pickup_time, promo_id,
                               company_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
            RETURNING *
        """, data.get("client_name"), data["client_phone"],
            data.get("service"), data.get("branch"), data.get("city"),
            data.get("address"), data.get("short_address", ""), data.get("note"),
            data.get("status","new"), data.get("assigned_to"), data.get("created_by"),
            data.get("volunteer_id"), data.get("location"), data.get("location_address"),
            source, data.get("client_tg_id"),
            data.get("pickup_date", ""), data.get("pickup_time", ""), promo_id, cid)
        rid      = row["id"]
        lead_num = f"LEAD-{rid:04d}"
        lead_code = f"L-{rid:04d}"
        await conn.execute(
            "UPDATE leads SET lead_num=$1, lead_code=$2 WHERE id=$3",
            lead_num, lead_code, rid
        )
        return dict(row) | {"lead_num": lead_num, "lead_code": lead_code}

async def get_leads(status: str = None, branch: str = None,
                    assigned_to: int = None, limit: int = 100):
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        filters, args = [f"l.company_id=$1"], [cid]
        if status:
            args.append(status);   filters.append(f"l.status=${len(args)}")
        if branch:
            args.append(branch);   filters.append(f"l.branch=${len(args)}")
        if assigned_to:
            args.append(assigned_to); filters.append(f"l.assigned_to=${len(args)}")
        args.append(limit)
        return await conn.fetch(
            f"""SELECT l.*,
                       s.first_name   AS creator_first_name,
                       s.last_name    AS creator_last_name,
                       s.position     AS creator_position,
                       s.login        AS creator_login,
                       s.phone        AS creator_phone,
                       vol.first_name AS volunteer_first_name,
                       vol.last_name  AS volunteer_last_name,
                       vol.phone      AS volunteer_phone,
                       asgn.first_name AS assigned_first_name,
                       asgn.last_name  AS assigned_last_name,
                       asgn.phone      AS assigned_phone
                FROM leads l
                LEFT JOIN staff s    ON s.id    = l.created_by
                LEFT JOIN staff vol  ON vol.id  = l.volunteer_id
                LEFT JOIN staff asgn ON asgn.id = l.assigned_to
                WHERE {' AND '.join(filters)}
                ORDER BY l.created_at DESC LIMIT ${len(args)}""", *args
        )

async def update_lead_status(lead_id: int, status: str, scheduled_at=None):
    if not pool: return
    cid = _cid()
    async with pool.acquire() as conn:
        if status == "callback" and scheduled_at:
            await conn.execute(
                "UPDATE leads SET status=$2, callback_at=$3, updated_at=NOW() WHERE id=$1 AND company_id=$4",
                lead_id, status, scheduled_at, cid
            )
        else:
            await conn.execute(
                "UPDATE leads SET status=$2, callback_at=NULL, updated_at=NOW() WHERE id=$1 AND company_id=$3",
                lead_id, status, cid
            )

async def update_lead(lead_id: int, **kwargs) -> dict | None:
    if not pool: return None
    cid = _cid()
    allowed = {"client_name","client_phone","service","branch","city","address","short_address","note","status","location","location_address","volunteer_id","pickup_type","delivery_type","pickup_date","pickup_time"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields: return None
    set_parts = ", ".join(f"{k}=${i+2}" for i, k in enumerate(fields))
    cid_param = len(fields) + 2
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE leads SET {set_parts}, updated_at=NOW() WHERE id=$1 AND company_id=${cid_param} RETURNING *",
            lead_id, *list(fields.values()), cid)
        return dict(row) if row else None

async def delete_lead(lead_id: int) -> bool:
    if not pool: return False
    cid = _cid()
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM leads WHERE id=$1 AND company_id=$2", lead_id, cid)
        if res != "DELETE 1":
            return False
        await conn.execute("DELETE FROM agent_notifications WHERE lead_id=$1", lead_id)
        await conn.execute("DELETE FROM lead_reminders       WHERE lead_id=$1", lead_id)
        await conn.execute("DELETE FROM lead_calls           WHERE lead_id=$1", lead_id)
        return True

async def get_leads_by_agent(agent_id: int, status: str = None):
    if not pool: return []
    async with pool.acquire() as conn:
        filters = ["(l.created_by=$1 OR l.volunteer_id=$1)"]
        args = [agent_id]
        if status:
            args.append(status); filters.append(f"l.status=${len(args)}")
        return await conn.fetch(
            f"""SELECT l.*,
                       s.first_name   AS creator_first_name,
                       s.last_name    AS creator_last_name,
                       s.position     AS creator_position,
                       s.login        AS creator_login,
                       s.phone        AS creator_phone,
                       vol.first_name AS volunteer_first_name,
                       vol.last_name  AS volunteer_last_name,
                       vol.phone      AS volunteer_phone,
                       asgn.first_name AS assigned_first_name,
                       asgn.last_name  AS assigned_last_name,
                       asgn.phone      AS assigned_phone
                FROM leads l
                LEFT JOIN staff s    ON s.id    = l.created_by
                LEFT JOIN staff vol  ON vol.id  = l.volunteer_id
                LEFT JOIN staff asgn ON asgn.id = l.assigned_to
                WHERE {' AND '.join(filters)} ORDER BY l.created_at DESC LIMIT 200""", *args)

async def generate_lead_code() -> str:
    if not pool: return "L-0001"
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM leads") or 0
        return f"L-{count+1:04d}"

async def set_lead_code(lead_id: int, code: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("UPDATE leads SET lead_code=$2 WHERE id=$1 AND lead_code IS NULL", lead_id, code)

async def get_lead_by_id(lead_id: int):
    if not pool: return None
    cid = _cid()
    async with pool.acquire() as conn:
        return await conn.fetchrow("""
            SELECT l.*,
                   s.first_name  AS creator_first_name,
                   s.last_name   AS creator_last_name,
                   s.position    AS creator_position,
                   s.login       AS creator_login,
                   s.phone       AS creator_phone,
                   vol.first_name AS volunteer_first_name,
                   vol.last_name  AS volunteer_last_name,
                   vol.phone      AS volunteer_phone,
                   conv.first_name AS converted_first_name,
                   conv.last_name  AS converted_last_name,
                   asgn.first_name AS assigned_first_name,
                   asgn.last_name  AS assigned_last_name,
                   asgn.phone      AS assigned_phone
            FROM leads l
            LEFT JOIN staff s    ON s.id    = l.created_by
            LEFT JOIN staff vol  ON vol.id  = l.volunteer_id
            LEFT JOIN staff conv ON conv.id = l.converted_by
            LEFT JOIN staff asgn ON asgn.id = l.assigned_to
            WHERE l.id = $1 AND l.company_id = $2
        """, lead_id, cid)

# ── lead_calls (журнал звонков) ───────────────────────────────────────

async def add_lead_call(lead_id: int, operator_id: int, action: str,
                         note: str = None, scheduled_at=None):
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO lead_calls (lead_id, operator_id, action, note, scheduled_at)
            VALUES ($1,$2,$3,$4,$5) RETURNING *
        """, lead_id, operator_id, action, note, scheduled_at)
        return dict(row) if row else None

async def cancel_pending_lead_reminders(lead_id: int):
    """Отменяет ещё не сработавшие напоминания по лиду и удаляет уже созданные
    уведомления «Пора перезвонить» — вызывается при конвертации в заказ /
    закрытии как потерянный, чтобы не слать и не показывать в списке
    Уведомлений «Пора перезвонить» по уже закрытому лиду."""
    if not pool: return
    cid = _cid()
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM lead_reminders WHERE lead_id=$1 AND sent_tg=FALSE AND sent_browser=FALSE "
            "AND lead_id IN (SELECT id FROM leads WHERE company_id=$2)",
            lead_id, cid
        )
        await conn.execute(
            "DELETE FROM agent_notifications WHERE lead_id=$1 AND action='callback' "
            "AND lead_id IN (SELECT id FROM leads WHERE company_id=$2)",
            lead_id, cid
        )

async def get_lead_calls(lead_id: int):
    if not pool: return []
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT lc.*, s.first_name, s.last_name, s.position
            FROM lead_calls lc
            LEFT JOIN staff s ON s.id = lc.operator_id
            WHERE lc.lead_id = $1
            ORDER BY lc.created_at DESC
        """, lead_id)

# ── lead_reminders ────────────────────────────────────────────────────

async def add_lead_reminder(lead_id: int, staff_id: int, remind_at, message: str = None):
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO lead_reminders (lead_id, staff_id, remind_at, message)
            VALUES ($1,$2,$3,$4) RETURNING *
        """, lead_id, staff_id, remind_at, message)
        return dict(row) if row else None

async def get_due_reminders(staff_id: int):
    """Возвращает напоминания которые уже наступили, ещё не отправлены в браузер."""
    if not pool: return []
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT r.*, l.client_name, l.client_phone, l.lead_code
            FROM lead_reminders r
            JOIN leads l ON l.id = r.lead_id
            WHERE r.staff_id = $1 AND r.remind_at <= NOW() AND r.sent_browser = FALSE
        """, staff_id)

async def mark_reminder_sent(reminder_id: int, channel: str = "browser"):
    if not pool: return
    async with pool.acquire() as conn:
        col = "sent_tg" if channel == "tg" else "sent_browser"
        await conn.execute(f"UPDATE lead_reminders SET {col}=TRUE WHERE id=$1", reminder_id)

async def get_pending_tg_reminders():
    """Для фонового воркера — все напоминания для отправки в Telegram.
    Получатель = assigned_to (кто взял лид), если взят; иначе staff_id (кто поставил напоминание)."""
    if not pool: return []
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT r.*, l.client_name, l.client_phone, l.lead_code,
                   -- целевой получатель: assigned_to имеет приоритет
                   COALESCE(l.assigned_to, r.staff_id) AS target_staff_id,
                   tgt.tg_id AS staff_tg_id,
                   tgt.first_name AS staff_first_name,
                   tgt.last_name  AS staff_last_name
            FROM lead_reminders r
            JOIN leads l ON l.id = r.lead_id
            -- joined на фактического получателя
            JOIN staff tgt ON tgt.id = COALESCE(l.assigned_to, r.staff_id)
            WHERE r.remind_at <= NOW() AND r.sent_tg = FALSE
        """)

# ── agent_notifications ───────────────────────────────────────────────

async def ensure_agent_notifications_table():
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS agent_notifications (
                id         SERIAL PRIMARY KEY,
                agent_id   INTEGER NOT NULL REFERENCES staff(id) ON DELETE CASCADE,
                lead_id    INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
                action     VARCHAR(50) NOT NULL,
                message    TEXT,
                is_read    BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_agent_notif_agent ON agent_notifications(agent_id, is_read);
        """)

async def create_agent_notification(agent_id: int, lead_id: int, action: str, message: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO agent_notifications (agent_id, lead_id, action, message)
            VALUES ($1,$2,$3,$4)
        """, agent_id, lead_id, action, message)

async def get_agent_notifications(agent_id: int, limit: int = 50):
    if not pool: return []
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT n.*, l.client_name, l.client_phone, l.lead_code
            FROM agent_notifications n
            JOIN leads l ON l.id = n.lead_id
            WHERE n.agent_id = $1
            ORDER BY n.created_at DESC LIMIT $2
        """, agent_id, limit)

async def count_unread_agent_notifications(agent_id: int) -> int:
    if not pool: return 0
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT COUNT(*) FROM agent_notifications WHERE agent_id=$1 AND is_read=FALSE",
            agent_id) or 0

async def mark_agent_notifications_read(agent_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE agent_notifications SET is_read=TRUE WHERE agent_id=$1 AND is_read=FALSE",
            agent_id)

async def mark_agent_notification_read_by_id(notif_id: int, agent_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE agent_notifications SET is_read=TRUE WHERE id=$1 AND agent_id=$2",
            notif_id, agent_id)


# ── washer_notifications ─────────────────────────────────────────────────────

async def ensure_washer_notifications_table():
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS washer_notifications (
                id         SERIAL PRIMARY KEY,
                staff_id   INTEGER NOT NULL REFERENCES staff(id) ON DELETE CASCADE,
                order_id   INTEGER NOT NULL,
                order_num  VARCHAR(50) DEFAULT '',
                message    TEXT DEFAULT '',
                is_read    BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_washer_notif_staff ON washer_notifications(staff_id, is_read);
        """)
        # Idempotent columns added after initial release
        await conn.execute("ALTER TABLE washer_notifications ADD COLUMN IF NOT EXISTS item_id INTEGER")
        await conn.execute("ALTER TABLE washer_notifications ADD COLUMN IF NOT EXISTS notification_type VARCHAR(30) DEFAULT 'order_item'")

async def create_washer_notification(staff_id: int, order_id: int, order_num: str, message: str,
                                      item_id: int = None, notification_type: str = 'order_item'):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO washer_notifications (staff_id, order_id, order_num, message, item_id, notification_type) "
            "VALUES ($1,$2,$3,$4,$5,$6)",
            staff_id, order_id, order_num, message, item_id, notification_type)

async def get_washer_notifications(staff_id: int, limit: int = 50) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM washer_notifications WHERE staff_id=$1 ORDER BY created_at DESC LIMIT $2",
            staff_id, limit)
        return [dict(r) for r in rows]

async def count_unread_washer_notifications(staff_id: int) -> int:
    if not pool: return 0
    async with pool.acquire() as conn:
        return int(await conn.fetchval(
            "SELECT COUNT(*) FROM washer_notifications WHERE staff_id=$1 AND is_read=FALSE", staff_id))

async def mark_washer_notifications_read(staff_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE washer_notifications SET is_read=TRUE WHERE staff_id=$1 AND is_read=FALSE", staff_id)

async def mark_washer_notification_read(notif_id: int, staff_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE washer_notifications SET is_read=TRUE WHERE id=$1 AND staff_id=$2", notif_id, staff_id)


async def convert_lead_to_order(lead_id: int, order_num: str, converted_by: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE leads SET status='converted', converted_by=$2,
                             converted_order=$3, updated_at=NOW()
            WHERE id=$1
        """, lead_id, converted_by, order_num)


# ══════════════════════════════════════
#  CRM КЛИЕНТЫ
# ══════════════════════════════════════
async def upsert_crm_client(phone: str, first_name: str = "", last_name: str = "",
                             tg_id: int = None, tg_username: str = None,
                             source: str = "unknown", address: str = "",
                             short_address: str = "") -> dict:
    """Создаёт или обновляет запись клиента своей компании. Статус не понижается."""
    if not pool or not phone:
        return {}
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO crm_clients (phone, first_name, last_name, tg_id, tg_username, source, address, short_address, company_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (company_id, phone) DO UPDATE SET
                first_name    = CASE WHEN $2 != '' THEN $2 ELSE crm_clients.first_name END,
                last_name     = CASE WHEN $3 != '' THEN $3 ELSE crm_clients.last_name END,
                tg_id         = COALESCE($4, crm_clients.tg_id),
                tg_username   = CASE WHEN $5 IS NOT NULL AND $5 != ''
                                     THEN $5 ELSE crm_clients.tg_username END,
                address       = CASE WHEN $7 != '' THEN $7 ELSE crm_clients.address END,
                short_address = CASE WHEN $8 != '' THEN $8 ELSE crm_clients.short_address END,
                updated_at    = NOW()
            RETURNING *
        """, phone, first_name or "", last_name or "", tg_id, tg_username, source,
             address or "", short_address or "", cid)
        result = dict(row) if row else {}
        if result:
            # Реальный клиент — автоматически зеркалим в active_contacts (см. Шаг 44).
            try:
                await upsert_active_contact(phone, first_name=result.get("first_name") or "",
                                             last_name=result.get("last_name") or "",
                                             source="crm", crm_client_id=result.get("id"), cid=cid)
            except Exception:
                logging.warning("upsert_active_contact failed for %s (company %s)", phone, cid, exc_info=True)
        return result


async def refresh_crm_client_stats(phone: str):
    """Пересчитывает orders_count и last_order_at из таблицы orders."""
    if not pool or not phone:
        return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE crm_clients SET
                orders_count  = (SELECT COUNT(*) FROM orders WHERE client_phone = $1),
                last_order_at = (SELECT MAX(created_at) FROM orders WHERE client_phone = $1),
                status = CASE
                    WHEN status NOT IN ('vip','inactive')
                         AND (SELECT COUNT(*) FROM orders
                              WHERE client_phone = $1 AND status = 'done') > 0
                    THEN 'active'
                    ELSE status
                END,
                updated_at = NOW()
            WHERE phone = $1
        """, phone)


async def get_crm_client_by_phone(phone: str) -> dict | None:
    if not pool:
        return None
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM crm_clients WHERE phone = $1 AND company_id=$2", phone, cid)
        return dict(row) if row else None


async def get_crm_client_by_id(client_id: int) -> dict | None:
    if not pool:
        return None
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM crm_clients WHERE id = $1 AND company_id=$2", client_id, cid)
        return dict(row) if row else None


async def get_crm_clients_list(search: str = "", limit: int = 50, offset: int = 0) -> list[dict]:
    if not pool:
        return []
    cid = _cid()
    async with pool.acquire() as conn:
        if search:
            rows = await conn.fetch("""
                SELECT * FROM crm_clients
                WHERE (phone ILIKE $1 OR first_name ILIKE $1 OR last_name ILIKE $1
                   OR short_address ILIKE $1 OR address ILIKE $1) AND company_id=$4
                ORDER BY updated_at DESC LIMIT $2 OFFSET $3
            """, f"%{search}%", limit, offset, cid)
        else:
            rows = await conn.fetch("""
                SELECT * FROM crm_clients
                WHERE company_id=$3
                ORDER BY updated_at DESC LIMIT $1 OFFSET $2
            """, limit, offset, cid)
        return [dict(r) for r in rows]


async def update_crm_client(client_id: int, **kwargs) -> dict | None:
    if not pool or not kwargs:
        return None
    allowed = {"first_name", "last_name", "phone2", "status", "note", "address", "short_address"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        return None
    cid = _cid()
    set_parts = ", ".join(f"{k} = ${i+3}" for i, k in enumerate(fields))
    vals = [client_id, cid] + list(fields.values())
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE crm_clients SET {set_parts}, updated_at=NOW() WHERE id=$1 AND company_id=$2 RETURNING *",
            *vals
        )
        return dict(row) if row else None


async def set_crm_client_discount_category(client_id: int, category: str | None,
                                            pct: float | None, staff_id: int | None) -> dict | None:
    """Устанавливает/снимает постоянную категорию скидки клиента (пенсионер/инвалид).
    При установке фиксирует, кто и когда подтвердил документ. При снятии — очищает и фото."""
    if not pool:
        return None
    cid = _cid()
    async with pool.acquire() as conn:
        if category:
            row = await conn.fetchrow("""
                UPDATE crm_clients SET
                    discount_category             = $2,
                    discount_category_pct         = $3,
                    discount_category_verified_by = $4,
                    discount_category_verified_at = NOW(),
                    updated_at                    = NOW()
                WHERE id = $1 AND company_id=$5
                RETURNING *
            """, client_id, category, pct, staff_id, cid)
        else:
            row = await conn.fetchrow("""
                UPDATE crm_clients SET
                    discount_category               = NULL,
                    discount_category_pct           = NULL,
                    discount_category_photo_file_id = NULL,
                    discount_category_verified_by   = NULL,
                    discount_category_verified_at   = NULL,
                    updated_at                       = NOW()
                WHERE id = $1 AND company_id=$2
                RETURNING *
            """, client_id, cid)
        return dict(row) if row else None


async def save_crm_client_discount_photo(client_id: int, file_id: str) -> dict | None:
    """Сохраняет file_id фото подтверждающего документа (пенсионное/инвалидное удостоверение)."""
    if not pool:
        return None
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE crm_clients SET discount_category_photo_file_id=$2, updated_at=NOW() WHERE id=$1 AND company_id=$3 RETURNING *",
            client_id, file_id, cid
        )
        return dict(row) if row else None


async def get_crm_client_orders(phone: str, limit: int = 20) -> list[dict]:
    if not pool:
        return []
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT order_num, source, service, status, created_at, total_price, branch, address
            FROM orders WHERE client_phone = $1 AND company_id=$3
            ORDER BY created_at DESC LIMIT $2
        """, phone, limit, cid)
        return [dict(r) for r in rows]


async def get_crm_clients_count() -> dict:
    """Возвращает кол-во клиентов по статусам (своей компании)."""
    if not pool:
        return {}
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT status, COUNT(*) as cnt FROM crm_clients WHERE company_id=$1 GROUP BY status", cid
        )
        return {r["status"]: r["cnt"] for r in rows}


# ══════════════════════════════════════════════════════════════════════════════
# ACTIVE CONTACTS (единая база "актуальных" номеров — crm_clients + дозвонившиеся
# автодозвона; источник для будущей массовой SMS-рассылки/автодозвона)
# ══════════════════════════════════════════════════════════════════════════════

def _autodial_phone_to_e164(phone: str) -> str:
    """Дозвонщик (autodial_agent.py) хранит номер в AMI-формате (9 цифр, без 998).
    Приводим к каноническому +998XXXXXXXXX, как везде в остальной базе."""
    if not phone:
        return ""
    digits = "".join(ch for ch in phone if ch.isdigit())
    if len(digits) == 9:
        return f"+998{digits}"
    if len(digits) == 12 and digits.startswith("998"):
        return f"+{digits}"
    if phone.startswith("+"):
        return phone
    return ""


def _normalize_phone_local(phone: str) -> str:
    """Локальная копия normalize_phone() из main.py — без кросс-модульного импорта."""
    if not phone:
        return ""
    p = phone.strip().replace(" ", "").replace("-", "")
    if not p.startswith("+"):
        p = "+" + p
    return p


async def upsert_active_contact(phone: str, first_name: str = "", last_name: str = "",
                                 source: str = "crm", crm_client_id: int = None,
                                 note: str = "", cid: int = None) -> dict:
    """Добавляет/обновляет номер в active_contacts своей компании. Дубликаты по
    (company_id, phone) схлопываются; source мержится в 'both', если пришёл из
    другого канала. cid можно передать явно — для вызовов вне request-контекста
    (напр. /api/verify, где company_id резолвится по slug, а не из JWT)."""
    if not pool or not phone:
        return {}
    company_id = cid if cid is not None else _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO active_contacts (company_id, phone, first_name, last_name, source, crm_client_id, note)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (company_id, phone) DO UPDATE SET
                first_name    = CASE WHEN $3 != '' THEN $3 ELSE active_contacts.first_name END,
                last_name     = CASE WHEN $4 != '' THEN $4 ELSE active_contacts.last_name END,
                source        = CASE WHEN active_contacts.source = $5 THEN active_contacts.source ELSE 'both' END,
                crm_client_id = COALESCE($6, active_contacts.crm_client_id),
                updated_at    = NOW()
            RETURNING *
        """, company_id, phone, first_name or "", last_name or "", source, crm_client_id, note or "")
        return dict(row) if row else {}


async def import_answered_autodial_calls(campaign_id: int) -> int:
    """Импортирует дозвонившихся из кампании автодозвона в active_contacts своей
    компании. ВАЖНО: agent (autodial_agent.py) пишет результат звонка в
    status='answered', поле answered_at в autodial_calls никогда не заполняется —
    фильтровать надо по status, не по answered_at (баг найден и исправлен в проде
    04.08 по логам — здесь сразу делаем правильно). Возвращает кол-во импортированных."""
    if not pool:
        return 0
    cid = _cid()
    async with pool.acquire() as conn:
        owner = await conn.fetchval("SELECT company_id FROM autodial_campaigns WHERE id=$1", campaign_id)
        if owner != cid:
            return 0
        rows = await conn.fetch(
            "SELECT DISTINCT ON (phone) phone, name FROM autodial_calls "
            "WHERE campaign_id=$1 AND status='answered'",
            campaign_id
        )
        count = 0
        for r in rows:
            phone = _autodial_phone_to_e164(r["phone"])
            if not phone:
                continue
            await upsert_active_contact(phone, first_name=r["name"] or "", source="autodial", cid=cid)
            count += 1
        return count


async def backfill_active_contacts() -> dict:
    """Разовый импорт исторических контактов (crm_clients + лиды + подтверждённые
    пользователи сайта) своей компании в active_contacts. НЕ трогает contacts
    (справочник) — по замыслу это отдельная, третья база (см. переписку с ARTEZ)."""
    if not pool:
        return {"crm_clients": 0, "leads": 0, "site_users": 0}
    cid = _cid()
    stats = {"crm_clients": 0, "leads": 0, "site_users": 0}
    async with pool.acquire() as conn:
        clients = await conn.fetch("SELECT * FROM crm_clients WHERE company_id=$1", cid)
        for c in clients:
            await upsert_active_contact(c["phone"], first_name=c["first_name"] or "",
                                         last_name=c["last_name"] or "", source="crm",
                                         crm_client_id=c["id"], cid=cid)
            stats["crm_clients"] += 1

        leads = await conn.fetch(
            "SELECT DISTINCT ON (client_phone) client_phone, client_name FROM leads "
            "WHERE company_id=$1 AND client_phone IS NOT NULL AND client_phone != ''", cid)
        for l in leads:
            phone = _normalize_phone_local(l["client_phone"])
            if not phone:
                continue
            await upsert_crm_client(phone, first_name=l["client_name"] or "", source="lead_backfill")
            stats["leads"] += 1

        users = await conn.fetch(
            "SELECT phone, first_name FROM users WHERE company_id=$1 AND is_verified=true", cid)
        for u in users:
            phone = _normalize_phone_local(u["phone"])
            if not phone:
                continue
            await upsert_crm_client(phone, first_name=u["first_name"] or "", source="site_backfill")
            stats["site_users"] += 1
    return stats


async def get_active_contacts_list(search: str = "", limit: int = 50, offset: int = 0) -> list[dict]:
    if not pool:
        return []
    cid = _cid()
    async with pool.acquire() as conn:
        if search:
            rows = await conn.fetch("""
                SELECT ac.*, cc.address AS address FROM active_contacts ac
                LEFT JOIN crm_clients cc ON cc.id = ac.crm_client_id
                WHERE ac.company_id=$1 AND (ac.phone ILIKE $2 OR ac.first_name ILIKE $2 OR ac.last_name ILIKE $2)
                ORDER BY ac.updated_at DESC
                LIMIT $3 OFFSET $4
            """, cid, f"%{search}%", limit, offset)
        else:
            rows = await conn.fetch("""
                SELECT ac.*, cc.address AS address FROM active_contacts ac
                LEFT JOIN crm_clients cc ON cc.id = ac.crm_client_id
                WHERE ac.company_id=$1
                ORDER BY ac.updated_at DESC
                LIMIT $2 OFFSET $3
            """, cid, limit, offset)
        return [dict(r) for r in rows]


async def get_active_contacts_count(search: str = "") -> int:
    if not pool:
        return 0
    cid = _cid()
    async with pool.acquire() as conn:
        if search:
            return await conn.fetchval(
                "SELECT COUNT(*) FROM active_contacts WHERE company_id=$1 AND (phone ILIKE $2 OR first_name ILIKE $2 OR last_name ILIKE $2)",
                cid, f"%{search}%")
        return await conn.fetchval("SELECT COUNT(*) FROM active_contacts WHERE company_id=$1", cid)


# ══════════════════════════════════════════════════════════════════════════════
# ЧЁРНЫЙ СПИСОК (не участвуют в автодозвоне/SMS) — company-scoped, причина
# хранится только здесь; в contacts/active_contacts/users/clients — флаг.
# ══════════════════════════════════════════════════════════════════════════════

_BLACKLIST_SOURCE_TABLES = ("contacts", "active_contacts", "users", "clients", "crm_clients")

async def _mark_blacklisted_everywhere(conn, cid: int, phone: str, value: bool):
    for table in _BLACKLIST_SOURCE_TABLES:
        try:
            await conn.execute(f"UPDATE {table} SET blacklisted=$1 WHERE phone=$2 AND company_id=$3", value, phone, cid)
        except Exception:
            pass  # clients (таблица бота) может не существовать на этой БД

async def upsert_blacklist_entry(phone: str, note: str = "", added_by: str = "", name: str = "") -> dict:
    if not pool or not phone:
        return {}
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO blacklist (company_id, phone, note, added_by, name)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (company_id, phone) DO UPDATE SET
                note = CASE WHEN $3 != '' THEN $3 ELSE blacklist.note END,
                name = CASE WHEN $5 != '' THEN $5 ELSE blacklist.name END,
                updated_at = NOW()
            RETURNING *
        """, cid, phone, note or "", added_by or "", name or "")
        await _mark_blacklisted_everywhere(conn, cid, phone, True)
        return dict(row) if row else {}

async def remove_from_blacklist(phone: str) -> bool:
    if not pool or not phone:
        return False
    cid = _cid()
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM blacklist WHERE phone=$1 AND company_id=$2", phone, cid)
        await _mark_blacklisted_everywhere(conn, cid, phone, False)
        return res == "DELETE 1"

async def get_blacklist_list(search: str = "", limit: int = 50, offset: int = 0) -> list[dict]:
    if not pool:
        return []
    cid = _cid()
    async with pool.acquire() as conn:
        if search:
            rows = await conn.fetch(
                "SELECT * FROM blacklist WHERE company_id=$1 AND (phone ILIKE $2 OR note ILIKE $2) "
                "ORDER BY created_at DESC LIMIT $3 OFFSET $4",
                cid, f"%{search}%", limit, offset)
        else:
            rows = await conn.fetch(
                "SELECT * FROM blacklist WHERE company_id=$1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
                cid, limit, offset)
        return [dict(r) for r in rows]

async def get_blacklist_count(search: str = "") -> int:
    if not pool:
        return 0
    cid = _cid()
    async with pool.acquire() as conn:
        if search:
            return await conn.fetchval(
                "SELECT COUNT(*) FROM blacklist WHERE company_id=$1 AND (phone ILIKE $2 OR note ILIKE $2)",
                cid, f"%{search}%")
        return await conn.fetchval("SELECT COUNT(*) FROM blacklist WHERE company_id=$1", cid)

async def sync_blacklist_flags() -> dict:
    """Кнопка «Обновить» — досвежа проставляет флаг blacklisted по текущему
    списку blacklist своей компании и снимает флаг там, где номера в ЧС больше нет."""
    if not pool:
        return {}
    cid = _cid()
    stats = {}
    async with pool.acquire() as conn:
        for table in _BLACKLIST_SOURCE_TABLES:
            try:
                r1 = await conn.execute(f"""
                    UPDATE {table} SET blacklisted=TRUE
                    WHERE company_id=$1 AND phone IN (SELECT phone FROM blacklist WHERE company_id=$1)
                          AND blacklisted IS DISTINCT FROM TRUE
                """, cid)
                r2 = await conn.execute(f"""
                    UPDATE {table} SET blacklisted=FALSE
                    WHERE company_id=$1 AND phone NOT IN (SELECT phone FROM blacklist WHERE company_id=$1)
                          AND blacklisted=TRUE
                """, cid)
                stats[table] = {"marked": int(r1.split()[-1]), "cleared": int(r2.split()[-1])}
            except Exception:
                stats[table] = {"marked": 0, "cleared": 0}
    return stats


# ══════════════════════════════════════════════════════════════════════════════
# CONTACTS (справочник)
# ══════════════════════════════════════════════════════════════════════════════

async def search_contacts(q: str, limit: int = 10) -> list[dict]:
    """Быстрый поиск по телефону или имени для автодополнения."""
    if not pool or not q:
        return []
    cid = _cid()
    q = q.strip()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT c.id, c.first_name, c.last_name, c.middle_name, c.phone, c.phone2,
                   c.address, c.short_address, c.source,
                   cc.discount_category, cc.discount_category_pct
            FROM contacts c
            LEFT JOIN crm_clients cc ON cc.phone = c.phone
            WHERE c.company_id=$4
              AND (c.phone ILIKE $1
               OR c.phone2 ILIKE $1
               OR c.first_name ILIKE $1
               OR c.last_name ILIKE $1
               OR (c.first_name || ' ' || c.last_name) ILIKE $1
               OR (c.last_name || ' ' || c.first_name) ILIKE $1
               OR c.short_address ILIKE $1)
            ORDER BY
                CASE WHEN c.phone ILIKE $2 THEN 0
                     WHEN c.phone2 ILIKE $2 THEN 1
                     ELSE 2 END,
                c.last_name, c.first_name
            LIMIT $3
        """, f"%{q}%", f"{q}%", limit, cid)
        return [dict(r) for r in rows]


async def upsert_contact(phone: str, first_name: str = "", last_name: str = "",
                         middle_name: str = "", phone2: str = "",
                         address: str = "", short_address: str = "",
                         source: str = "ARTEZ") -> dict | None:
    """Добавить или обновить контакт (ON CONFLICT по company_id+phone — раньше был
    глобальный UNIQUE(phone), из-за чего компания Б, сохраняя контакт с уже существующим
    у компании А номером, молча перезаписывала данные контакта компании А)."""
    if not pool or not phone:
        return None
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO contacts (phone, first_name, last_name, middle_name, phone2, address, short_address, source, company_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (company_id, phone) DO UPDATE SET
                first_name    = CASE WHEN $2 != '' THEN $2 ELSE contacts.first_name END,
                last_name     = CASE WHEN $3 != '' THEN $3 ELSE contacts.last_name END,
                middle_name   = CASE WHEN $4 != '' THEN $4 ELSE contacts.middle_name END,
                phone2        = CASE WHEN $5 != '' THEN $5 ELSE contacts.phone2 END,
                address       = CASE WHEN $6 != '' THEN $6 ELSE contacts.address END,
                short_address = CASE WHEN $7 != '' THEN $7 ELSE contacts.short_address END,
                updated_at    = NOW()
            RETURNING *
        """, phone, first_name, last_name, middle_name, phone2, address, short_address, source, cid)
        return dict(row) if row else None


async def bulk_insert_contacts(rows: list[dict]) -> dict:
    """Массовая вставка контактов своей компании. Возвращает {ok, dup, err, blacklisted}."""
    if not pool:
        return {"ok": 0, "dup": 0, "err": len(rows), "blacklisted": 0}
    cid = _cid()
    ok = dup = err = blacklisted = 0
    async with pool.acquire() as conn:
        bl_phones = {r["phone"] for r in await conn.fetch(
            "SELECT phone FROM blacklist WHERE company_id=$1", cid)}
        for r in rows:
            phone = str(r.get("phone", "")).strip()
            if not phone:
                err += 1
                continue
            if phone in bl_phones:
                blacklisted += 1
                continue
            try:
                res = await conn.fetchval("""
                    INSERT INTO contacts
                        (phone, first_name, last_name, middle_name, phone2, address, short_address, source, company_id)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                    ON CONFLICT (company_id, phone) DO NOTHING
                    RETURNING id
                """,
                    phone,
                    str(r.get("first_name",    "") or "").strip(),
                    str(r.get("last_name",     "") or "").strip(),
                    str(r.get("middle_name",   "") or "").strip(),
                    str(r.get("phone2",        "") or "").strip(),
                    str(r.get("address",       "") or "").strip(),
                    str(r.get("short_address", "") or "").strip(),
                    str(r.get("source", "Старая база")),
                    cid,
                )
                if res:
                    ok += 1
                else:
                    dup += 1
            except Exception:
                err += 1
    return {"ok": ok, "dup": dup, "err": err, "blacklisted": blacklisted}


async def get_contacts_list(search: str = "", limit: int = 50, offset: int = 0) -> list[dict]:
    if not pool:
        return []
    cid = _cid()
    async with pool.acquire() as conn:
        if search:
            rows = await conn.fetch("""
                SELECT * FROM contacts
                WHERE company_id=$4
                  AND (phone ILIKE $1 OR phone2 ILIKE $1
                   OR first_name ILIKE $1 OR last_name ILIKE $1
                   OR middle_name ILIKE $1 OR address ILIKE $1
                   OR short_address ILIKE $1)
                ORDER BY id DESC LIMIT $2 OFFSET $3
            """, f"%{search}%", limit, offset, cid)
        else:
            rows = await conn.fetch(
                "SELECT * FROM contacts WHERE company_id=$3 ORDER BY id DESC LIMIT $1 OFFSET $2",
                limit, offset, cid)
        return [dict(r) for r in rows]


async def get_contacts_total(search: str = "") -> int:
    if not pool:
        return 0
    cid = _cid()
    async with pool.acquire() as conn:
        if search:
            return await conn.fetchval("""
                SELECT COUNT(*) FROM contacts
                WHERE company_id=$2
                  AND (phone ILIKE $1 OR phone2 ILIKE $1
                   OR first_name ILIKE $1 OR last_name ILIKE $1
                   OR short_address ILIKE $1)
            """, f"%{search}%", cid)
        return await conn.fetchval("SELECT COUNT(*) FROM contacts WHERE company_id=$1", cid)


async def get_contacts_source_counts() -> dict:
    if not pool:
        return {}
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT source, COUNT(*) cnt FROM contacts WHERE company_id=$1 GROUP BY source", cid)
        return {r["source"]: r["cnt"] for r in rows}


async def update_contact(contact_id: int, **kwargs) -> dict | None:
    if not pool:
        return None
    allowed = {"phone","first_name","last_name","middle_name","phone2","address","short_address","source"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        return None
    cid = _cid()
    set_parts = ", ".join(f"{k}=${i+2}" for i, k in enumerate(fields))
    vals = list(fields.values())
    cid_param = len(fields) + 2
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE contacts SET {set_parts}, updated_at=NOW() WHERE id=$1 AND company_id=${cid_param} RETURNING *",
            contact_id, *vals, cid)
        return dict(row) if row else None


async def delete_crm_client(client_id: int) -> bool:
    if not pool:
        return False
    cid = _cid()
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM crm_clients WHERE id=$1 AND company_id=$2", client_id, cid)
        return res == "DELETE 1"


async def delete_contact(contact_id: int) -> bool:
    if not pool:
        return False
    cid = _cid()
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM contacts WHERE id=$1 AND company_id=$2", contact_id, cid)
        return res == "DELETE 1"


async def delete_all_contacts() -> int:
    """Удалить все записи из contacts своей компании. Возвращает количество удалённых."""
    if not pool:
        return 0
    cid = _cid()
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM contacts WHERE company_id=$1", cid)
        # res == "DELETE 41234"
        try:
            return int(res.split()[-1])
        except Exception:
            return 0


# ══════════════════════════════════════
#  ПОЗИЦИИ УСЛУГ В ЗАКАЗАХ
# ══════════════════════════════════════
async def update_order_status(order_id: int, status: str, note: str = "") -> dict:
    if not pool: return {}
    cid = _cid()
    async with pool.acquire() as conn:
        if status == 'washing':
            sql = "UPDATE orders SET status=$2, washed_at=NOW() WHERE id=$1 AND company_id=$3 RETURNING *"
        elif status == 'packing':
            sql = "UPDATE orders SET status=$2, packed_at=NOW() WHERE id=$1 AND company_id=$3 RETURNING *"
        elif status != 'ready':
            # Уходим из "Готов" — отсрочка доставки (если была) больше не актуальна.
            sql = """UPDATE orders SET status=$2, postponed_until=NULL,
                     postpone_reason=NULL, postponed_by=NULL WHERE id=$1 AND company_id=$3 RETURNING *"""
        else:
            sql = "UPDATE orders SET status=$2 WHERE id=$1 AND company_id=$3 RETURNING *"
        row = await conn.fetchrow(sql, order_id, status, cid)
        if row:
            await conn.execute("""
                INSERT INTO order_status_history (order_num, new_status, note)
                VALUES ($1, $2, $3)
            """, dict(row).get("order_num", ""), status, note)
        return dict(row) if row else {}

async def set_order_postpone(order_id: int, until_date, reason: str, staff_id: int) -> dict:
    """Отложить доставку заказа в статусе "Готов" — клиент попросил подождать."""
    if not pool: return {}
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE orders SET postponed_until=$2, postpone_reason=$3, postponed_by=$4
            WHERE id=$1 AND company_id=$5 AND status='ready' RETURNING *
        """, order_id, until_date, reason, staff_id, cid)
        return dict(row) if row else {}

async def get_orders_for_postpone_reminder() -> list:
    """Заказы всех компаний, у которых отсрочка доставки истекает завтра — для
    ежедневного напоминания администраторам/менеджерам созвониться с клиентом."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT o.*, TRIM(COALESCE(s.last_name,'') || ' ' || COALESCE(s.first_name,'')) AS postponed_by_name
            FROM orders o
            LEFT JOIN staff s ON s.id = o.postponed_by
            WHERE o.status='ready' AND o.postponed_until = (CURRENT_DATE + INTERVAL '1 day')::date
        """)
        return [dict(r) for r in rows]

async def get_order_managers(company_id: int, branch: str | None) -> list:
    """admin (видят все филиалы своей компании) + manager этого филиала, с TG."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, first_name, last_name, tg_id FROM staff
            WHERE company_id=$1 AND active=TRUE AND tg_id IS NOT NULL
              AND (role='admin' OR (role='manager' AND (branch=$2 OR branch IS NULL)))
        """, company_id, branch)
        return [dict(r) for r in rows]

async def get_order_items(order_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM order_items WHERE order_id=$1 ORDER BY id", order_id)
        return [dict(r) for r in rows]

async def create_order_item(order_id: int, service: str, sqm: float,
                             price_per_sqm: float, width_cm: float = None,
                             length_cm: float = None) -> dict:
    if not pool: return {}
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO order_items (order_id, service, width_cm, length_cm, sqm, price_per_sqm, company_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *
        """, order_id, service, width_cm, length_cm, sqm, price_per_sqm, cid)
        return dict(row) if row else {}

async def create_empty_items(order_id: int, count: int) -> list:
    if not pool: return []
    cid = _cid()
    result = []
    async with pool.acquire() as conn:
        for _ in range(count):
            row = await conn.fetchrow("""
                INSERT INTO order_items (order_id, service, sqm, price_per_sqm, company_id)
                VALUES ($1, '', 0, 0, $2) RETURNING *
            """, order_id, cid)
            if row:
                result.append(dict(row))
    return result

async def create_empty_items_by_service(order_id: int, service_counts: dict) -> list:
    """Как create_empty_items, но с разбивкой по услугам (по N позиций на каждую)."""
    if not pool: return []
    cid = _cid()
    result = []
    async with pool.acquire() as conn:
        for service, count in service_counts.items():
            for _ in range(int(count or 0)):
                row = await conn.fetchrow("""
                    INSERT INTO order_items (order_id, service, sqm, price_per_sqm, company_id)
                    VALUES ($1, $2, 0, 0, $3) RETURNING *
                """, order_id, service, cid)
                if row:
                    result.append(dict(row))
    return result

async def get_order_by_phone_pending(phone: str) -> dict | None:
    """Ищет заказ клиента по телефону в статусе 'new'/'confirmed' (ещё не забран водителем),
    своей компании (_cid()) — для флоу «Забор»: определить, создан ли уже заказ менеджером."""
    if not pool: return None
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT * FROM orders
            WHERE client_phone=$1 AND company_id=$2 AND status IN ('new','confirmed')
            ORDER BY created_at DESC LIMIT 1
        """, phone, cid)
        return dict(row) if row else None

async def update_order_item(item_id: int, **kwargs) -> dict:
    if not pool: return {}
    allowed = {"service", "service_ru", "service_uz", "width_cm", "length_cm", "sqm", "price_per_sqm"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields: return {}
    set_parts = ", ".join(f"{k}=${i+2}" for i, k in enumerate(fields))
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE order_items SET {set_parts} WHERE id=$1 RETURNING *",
            item_id, *list(fields.values()))
        return dict(row) if row else {}

async def delete_order_item(item_id: int) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM order_items WHERE id=$1", item_id)
        return res == "DELETE 1"

async def delete_order(order_id: int) -> bool:
    if not pool: return False
    cid = _cid()
    async with pool.acquire() as conn:
        # Блокируем удаление если есть платежи — деньги должны быть сняты вручную
        payment_count = await conn.fetchval(
            "SELECT COUNT(*) FROM order_payments WHERE order_id=$1", order_id)
        if payment_count:
            raise ValueError(f"has_payments:{payment_count}")
        # Получаем order_num до удаления (нужен для history)
        row = await conn.fetchrow("SELECT order_num FROM orders WHERE id=$1 AND company_id=$2", order_id, cid)
        order_num = dict(row).get("order_num") if row else None
        # Удаляем медиафайлы позиций
        await conn.execute("DELETE FROM order_item_media WHERE order_id=$1", order_id)
        # Удаляем позиции
        await conn.execute("DELETE FROM order_items WHERE order_id=$1", order_id)
        # Удаляем фото заказа
        await conn.execute("DELETE FROM order_photos WHERE order_id=$1", order_id)
        # Удаляем из маршрутов
        await conn.execute("DELETE FROM route_orders WHERE order_id=$1", order_id)
        # Удаляем историю статусов
        if order_num:
            await conn.execute("DELETE FROM order_status_history WHERE order_num=$1", order_num)
        # Удаляем сам заказ
        res = await conn.execute("DELETE FROM orders WHERE id=$1 AND company_id=$2", order_id, cid)
        return res == "DELETE 1"

async def get_order_by_id(order_id: int) -> dict:
    if not pool: return {}
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM orders WHERE id=$1 AND company_id=$2", order_id, cid)
        return dict(row) if row else {}

async def get_order_client_phone(order_id: int, company_id: int) -> str | None:
    """Телефон клиента заказа — для проверки владения в file-proxy эндпоинтах
    (/api/media, /api/item-media), где company_id берётся напрямую из JWT,
    а не из request-контекста _cid() (там ручной разбор токена, не middleware)."""
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT client_phone FROM orders WHERE id=$1 AND company_id=$2", order_id, company_id)
        return row["client_phone"] if row else None

async def update_order(order_id: int, **kwargs) -> dict:
    if not pool: return {}
    cid = _cid()
    allowed = {"client_first_name", "client_last_name", "client_phone",
               "branch", "address", "short_address", "location", "location_address", "note", "deadline",
               "service_type", "pickup_type", "self_pickup_discount",
               "discount_sum", "manual_discount",
               "delivery_type", "delivery_discount", "delivery_discount_pct"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields: return {}
    set_parts = ", ".join(f"{k}=${i+2}" for i, k in enumerate(fields))
    cid_param = len(fields) + 2
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE orders SET {set_parts} WHERE id=$1 AND company_id=${cid_param} RETURNING *",
            order_id, *list(fields.values()), cid)
        return dict(row) if row else {}

async def update_order_discount(order_id: int, discount_sum: float) -> dict:
    if not pool: return {}
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE orders SET discount_sum=$2 WHERE id=$1 AND company_id=$3 RETURNING *", order_id, discount_sum, cid)
        return dict(row) if row else {}

async def confirm_item_measure(item_id: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE order_items SET measure_status='confirmed' WHERE id=$1 RETURNING *", item_id)
        return dict(row) if row else {}

async def save_measure_dims(item_id: int, width_cm: float, length_cm: float) -> dict:
    if not pool: return {}
    sqm = round(width_cm * length_cm / 10000, 3)
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE order_items
            SET width_cm=$2, length_cm=$3, sqm=$4
            WHERE id=$1 AND measure_status != 'approved'
            RETURNING *
        """, item_id, width_cm, length_cm, sqm)
        return dict(row) if row else {}

async def save_measure_qty(item_id: int, quantity: float) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE order_items
            SET width_cm=NULL, length_cm=NULL, sqm=$2
            WHERE id=$1 AND measure_status != 'approved'
            RETURNING *
        """, item_id, round(quantity, 3))
        return dict(row) if row else {}

async def update_item_washer(item_id: int, washer_login: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE order_items SET washer_login=$2 WHERE id=$1 RETURNING *", item_id, washer_login or None)
        return dict(row) if row else {}


# ══════════════════════════════════════
#  ИСТОРИЯ СТАТУСОВ ЗАКАЗА
# ══════════════════════════════════════
async def get_order_status_history(order_num: str) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM order_status_history WHERE order_num=$1 ORDER BY created_at",
            order_num)
        return [dict(r) for r in rows]

async def get_order_debt_history(order_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT h.new_status, h.note, h.created_at
              FROM order_status_history h
              JOIN orders o ON o.order_num = h.order_num
             WHERE o.id = $1
               AND h.new_status IN ('delivered','debt_extended','debt_paid')
             ORDER BY h.created_at
        """, order_id)
        return [dict(r) for r in rows]

# ══════════════════════════════════════
#  ДУБЛИКАТЫ ТЕЛЕФОНА
# ══════════════════════════════════════
async def check_phone_duplicate(phone: str) -> dict:
    if not pool: return {}
    clean = ''.join(c for c in phone if c.isdigit() or c == '+')
    if not clean: return {}
    async with pool.acquire() as conn:
        order = await conn.fetchrow(
            "SELECT id, order_num, client_first_name, client_last_name, status FROM orders "
            "WHERE REGEXP_REPLACE(client_phone,'[^0-9]','','g') = REGEXP_REPLACE($1,'[^0-9]','','g') "
            "ORDER BY created_at DESC LIMIT 1", clean)
        lead = await conn.fetchrow(
            "SELECT id, name, status FROM leads "
            "WHERE REGEXP_REPLACE(phone,'[^0-9]','','g') = REGEXP_REPLACE($1,'[^0-9]','','g') "
            "ORDER BY created_at DESC LIMIT 1", clean)
        return {
            "order": dict(order) if order else None,
            "lead":  dict(lead)  if lead  else None,
        }

# ══════════════════════════════════════
#  TELEGRAM — ШАБЛОНЫ УВЕДОМЛЕНИЙ
# ══════════════════════════════════════

async def get_tg_status_messages() -> list[dict]:
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM tg_status_messages WHERE company_id=$1 ORDER BY status", cid)
        return [dict(r) for r in rows]

async def get_tg_status_message(status: str) -> dict | None:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM tg_status_messages WHERE status=$1", status)
        return dict(row) if row else None

async def upsert_tg_status_message(status: str, enabled: bool, message_ru: str, message_uz: str) -> dict:
    if not pool: return {}
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO tg_status_messages (status, enabled, message_ru, message_uz, company_id)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (status, company_id) DO UPDATE
              SET enabled=$2, message_ru=$3, message_uz=$4
            RETURNING *
        """, status, enabled, message_ru, message_uz, cid)
        return dict(row) if row else {}

async def get_tg_template_messages() -> list[dict]:
    """Шаблоны TG-уведомлений суперадмина (company_id=0)."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM tg_status_messages WHERE company_id=0 ORDER BY status", )
        return [dict(r) for r in rows]

async def upsert_tg_template_message(status: str, enabled: bool, message_ru: str, message_uz: str) -> dict:
    """Редактировать шаблон TG-уведомления суперадмина (company_id=0)."""
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO tg_status_messages (status, enabled, message_ru, message_uz, company_id)
            VALUES ($1, $2, $3, $4, 0)
            ON CONFLICT (status, company_id) DO UPDATE
              SET enabled=$2, message_ru=$3, message_uz=$4
            RETURNING *
        """, status, enabled, message_ru, message_uz)
        return dict(row) if row else {}

async def seed_company_tg_messages(company_id: int, force: bool = False):
    """Копирует TG-шаблоны из company_id=0 в company_id. Если шаблон пустой — использует хардкодные дефолты."""
    if not pool: return
    async with pool.acquire() as conn:
        if not force:
            existing = await conn.fetchval(
                "SELECT COUNT(*) FROM tg_status_messages WHERE company_id=$1", company_id)
            if existing > 0:
                return
        template = await conn.fetch(
            "SELECT status, enabled, message_ru, message_uz FROM tg_status_messages WHERE company_id=0")
        source = [(r["status"], r["enabled"], r["message_ru"], r["message_uz"]) for r in template] \
                 if template else _TG_STATUS_DEFAULTS
        await conn.executemany("""
            INSERT INTO tg_status_messages (status, enabled, message_ru, message_uz, company_id)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (status, company_id) DO NOTHING
        """, [(s, e, ru, uz, company_id) for s, e, ru, uz in source])


_SMS_TEMPLATE_DEFAULTS = {
    "sms_text_register":       "Kod podtverzhdeniya: {code}",
    "sms_text_login":          "Kod podtverzhdeniya dlya vhoda: {code}",
    "sms_text_reset":          "Kod vosstanovleniya parolya: {code}",
    "sms_pickup_enabled":      "false",
    "sms_pickup_template_ru":  "Zakaz {order_num} prinyat kurerom ({count} poz.). Voprosy: {phones} bot {bot_link} Status na sayte {site_link}",
    "sms_pickup_template_uz":  "{order_num} buyurtma kuryer tomonidan qabul qilindi ({count} dona). Savollar: {phones} bot {bot_link} Holat: {site_link}",
    "sms_ready_enabled":       "false",
    "sms_ready_template_ru":   "Zakaz {order_num} gotov. Voprosy: {phones} bot {bot_link} Status na sayte {site_link}",
    "sms_ready_template_uz":   "{order_num} buyurtma tayyor. Savollar: {phones} bot {bot_link} Holat: {site_link}",
}

async def seed_company_sms_templates(company_id: int, force: bool = False):
    """Копирует SMS-шаблоны из company_id=0 (заполняет суперадмин) в company_id —
    по ключам (ON CONFLICT DO NOTHING), а не всё-или-ничего: у новой компании заполнит
    все 6 ключей, у уже существующей — только те, что ещё не заданы (например, новый
    ключ sms_pickup_* добавили позже — старые компании получат только его).
    force=True — сначала стирает текущие значения компании и переписывает из шаблона."""
    if not pool: return
    keys = list(_SMS_TEMPLATE_DEFAULTS.keys())
    async with pool.acquire() as conn:
        if force:
            await conn.execute(
                "DELETE FROM config WHERE company_id=$1 AND key = ANY($2::text[])",
                company_id, keys)
        template = {r["key"]: r["value"] for r in await conn.fetch(
            "SELECT key, value FROM config WHERE company_id=0 AND key = ANY($1::text[])", keys)}
        for key, default_val in _SMS_TEMPLATE_DEFAULTS.items():
            val = template.get(key) or default_val
            await conn.execute("""
                INSERT INTO config (key, value, company_id, updated_at) VALUES ($1, $2, $3, NOW())
                ON CONFLICT (company_id, key) DO NOTHING
            """, key, val, company_id)


# Правила приёма и выдачи заказов — дефолт для новой компании (пока суперадмин
# не заполнил свой шаблон в company_id=0). Тот же формат, что и в проде ARTEZ.
_DEFAULT_ORDER_RULES_JSON = json.dumps([
    {
        "emoji": "💰",
        "title_ru": "ИНФОРМАЦИЯ О СТОИМОСТИ И РАЗМЕРАХ",
        "text_ru": "Точные замеры изделий (в квадратных метрах), финальная стоимость услуг, действующие скидки и текущий статус выполнения заказа отображаются в Личном кабинете клиента на сайте компании, а также дублируются в Telegram-боте после автоматической регистрации по номеру телефона заказчика.",
        "title_uz": "NARX VA O'LCHAMLAR HAQIDA MA'LUMOT",
        "text_uz": "Mahsulotlarning aniq o'lchamlari (kvadrat metrda), xizmatlarning yakuniy qiymati, amaldagi chegirmalar va buyurtmaning bajarilish holati mijozning telefon raqami orqali avtomatik ro'yxatdan o'tgandan so'ng kompaniya saytidagi shaxsiy kabinetida hamda Telegram-botida ko'rsatiladi."
    },
    {
        "emoji": "📐",
        "title_ru": "УСЛОВИЯ МИНИМАЛЬНОГО ЗАКАЗА",
        "text_ru": "Компания принимает заказы на профессиональную стирку ковровых покрытий объемом не менее 10 м². Если суммарный объем (общая площадь) всех ковров в заказе составляет менее 10 м², оплата все равно взимается клиентом в полном объеме как за 10 м². (Например: при тарифе 13 000 сум за 1 м² минимальная стоимость любого заказа составит 130 000 сум, даже если фактическая площадь изделия меньше). Если площадь отдельного ковра составляет менее 1 кв.м, при расчете стоимости она округляется и учитывается как 1 кв.м.",
        "title_uz": "MINIMAL BUYURTMA SHARTLARI",
        "text_uz": "Kompaniya gilamlarni professional yuvish uchun kamida 10 m² hajmdagi buyurtmalarni qabul qiladi. Agar buyurtmadagi barcha gilamlarning umumiy maydoni 10 m² dan kam bo'lsa, to'lov baribir 10 m² uchun to'liq miqdorda undiriladi."
    },
    {
        "emoji": "🩹",
        "title_ru": "ОГРАНИЧЕНИЕ ГАРАНТИИ НА ВЫВЕДЕНИЕ ПЯТЕН",
        "text_ru": "Исполнитель делает все возможное для удаления загрязнений, однако ПРИНИМАЮТСЯ БЕЗ ГАРАНТИИ полного выведения следующие виды пятен: застарелые пятна (находящиеся на изделии длительное время), а также пятна неизвестного происхождения; следы крови, мазута, автомобильных масел, зеленки, йода, парафина, воска, пластилина, жира; следы жевательной резинки, детских слаймов, рвотных масс, красного вина, ягод и натуральных соков; следы мочи и меток животных (пятна могут не уйти полностью, на светлых тканях возможны желтые разводы и остаточный запах); пятна ржавчины и плесени (после их удаления возможно истончение ткани или появление дыр); следы бытового или строительного клея, лакокрасочных материалов, маркеров и краски для волос; любые пятна на изделиях, которые заказчик пытался вывести самостоятельно с помощью бытовой химии.",
        "title_uz": "DOG'LARNI KETKAZISHGA KAFOLAT CHEKLANISHI",
        "text_uz": "Ijrochi ifloslanishlarni olib tashlash uchun barcha choralarni ko'radi, biroq quyidagi turdagi dog'lar to'liq ketishiga KAFOLATSIZ qabul qilinadi: eski dog'lar, qon, mazut, avtomobil moylari, zelenka, yod, parafin, mum, plastilin, yog' izlari; saqich, bolalar slaymlari, qusuq moddalari, qizil vino, rezavor mevalar va tabiiy sharbatlar izlari; hayvonlarning siydigi; zang va mog'or dog'lari; maishiy yoki qurilish yelimlarining izlari, lak-bo'yoq materiallari, markerlar va soch bo'yoqlari; shuningdek mijoz tomonidan mustaqil ravishda ketkazishga urinib ko'rilgan har qanday dog'lar."
    },
    {
        "emoji": "⚠️",
        "title_ru": "ОТКАЗ ОТ ОТВЕТСТВЕННОСТИ ЗА ДЕФЕКТЫ И ЕСТЕСТВЕННЫЙ ИЗНОС",
        "text_ru": "Химчистка полностью снимает с себя ответственность за результат оказания услуг и проявление негативных последствий в следующих случаях: отсутствие фабричной маркировки производителя, неправильная или недостаточная информация по разрешенной технологии чистки на ярлыке изделия; деформация, поломка или разрушение несъемной фурнитуры, декоративных элементов или клеевой основы; проявление скрытых дефектов, которые невозможно было обнаружить при визуальном осмотре в момент приема (например, ветхость нитей, гниение основы от сырости, заводской брак); проявление дефектов, вызванных естественным эксплуатационным износом изделия (потертости ворса, выцветание от солнца, расхождение старых, ветхих или ослабленных швов).",
        "title_uz": "NUQSONLAR VA TABIIY ESKIRISH UCHUN JAVOBGARLIKDAN VOZ KECHISH",
        "text_uz": "Kimyoviy tozalash quyidagi hollarda javobgarlikni to'liq o'zidan soqit qiladi: ishlab chiqaruvchining zavod yorlig'i bo'lmaganda; yechib olinmaydigan furnitura, dekorativ elementlar yoki yelim asosining deformatsiyasi, sinishi yoki parchalanishida; qabul qilish paytida aniqlash imkoni bo'lmagan yashirin nuqsonlar namoyon bo'lganda; mahsulotning tabiiy eskirishi natijasida yuzaga kelgan nuqsonlar namoyon bo'lganda (patlarning siyraklashishi, quyoshda rangning o'chishi, choklarning so'kilishi)."
    },
    {
        "emoji": "⏱️",
        "title_ru": "СРОКИ ВЫПОЛНЕНИЯ И ОТВЕТСТВЕННОЕ ХРАНЕНИЕ",
        "text_ru": "Все заказы выполняются в стандартной форме в порядке общей очереди. Срочные заказы выполняются вне очереди в срок от 2 до 3 дней, при этом применяется наценка в размере 30% от базовой стоимости заказа. Срок бесплатного ответственного хранения готового изделия составляет 15 календарных дней с момента отправки клиенту уведомления о выполнении заказа (SMS, сайт или Telegram-бот).",
        "title_uz": "BAJARISH MUDDATLARI VA JAVOBGARLIKKA SAQLASH",
        "text_uz": "Barcha buyurtmalar standart shaklda, umumiy navbat tartibida bajariladi. Shoshilinch buyurtmalar navbatsiz 2 kundan 3 kungacha bo'lgan muddatda bajariladi, bunda buyurtmaning asosiy qiymatiga 30% miqdorida ustama narx qo'llaniladi. Tayyor mahsulotni bepul saqlash muddati bildirishnoma yuborilgan vaqtdan boshlab 15 kalendar kunini tashkil etadi."
    },
    {
        "emoji": "📋",
        "title_ru": "ПОРЯДОК ПРЕДЪЯВЛЕНИЯ ПРЕТЕНЗИЙ",
        "text_ru": "Претензии по качеству оказанных услуг (выполненных работ) должны быть предъявлены заказчиком в момент приема-передачи готового заказа. Если по объективной причине заказчик не смог сразу осмотреть принимаемое готовое изделие, он имеет право предъявить мотивированные претензии к качеству в течение 10 календарных дней. По пошествии 3 дней с момента выдачи химчистка полностью снимает с себя всякую ответственность, и заказ считается выполненным качественно.",
        "title_uz": "E'TIROZLARNI BILDIRISH TARTIBI",
        "text_uz": "Ko'rsatilgan xizmatlar sifati bo'yicha e'tirozlar mijoz tomonidan tayyor buyurtmani qabul qilib olish vaqtida bildirilishi kerak. Agar obyektiv sabablarga ko'ra mijoz tayyor mahsulotni darhol ko'zdan kechira olmagan bo'lsa, u 10 kalendar kuni ichida sifat bo'yicha asoslantirilgan e'tirozlarni bildirish huquqiga ega. Mahsulot topshirilgan vaqtdan boshlab 10 kun o'tgach, kimyoviy tozalash har qanday javobgarlikni to'liq o'zidan soqit qiladi."
    },
], ensure_ascii=False)

async def seed_company_order_rules(company_id: int, force: bool = False):
    """Копирует правила приёма/выдачи из company_id=0 (шаблон суперадмина) в новую
    компанию; если суперадмин ещё не заполнил свой шаблон — берёт дефолт ARTEZ.
    force=True — переписывает поверх текущего значения компании."""
    if not pool: return
    async with pool.acquire() as conn:
        if force:
            await conn.execute(
                "DELETE FROM config WHERE company_id=$1 AND key='order_rules'", company_id)
        template = await conn.fetchval(
            "SELECT value FROM config WHERE company_id=0 AND key='order_rules'")
        val = template or _DEFAULT_ORDER_RULES_JSON
        await conn.execute("""
            INSERT INTO config (key, value, company_id, updated_at) VALUES ('order_rules', $1, $2, NOW())
            ON CONFLICT (company_id, key) DO NOTHING
        """, val, company_id)


async def get_client_by_tg_phone(tg_phone: str, company_id: int = 1) -> dict | None:
    """Ищет клиента бота по tg_phone (верифицированный) или phone (запасной), своей компании."""
    if not pool: return None
    alt = tg_phone[1:] if tg_phone.startswith("+") else "+" + tg_phone
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                """SELECT tg_id, tg_phone, phone, first_name FROM clients
                   WHERE (tg_phone=$1 OR tg_phone=$2
                      OR phone=$1    OR phone=$2)
                     AND company_id=$3
                   LIMIT 1""",
                tg_phone, alt, company_id)
            return dict(row) if row else None
        except Exception:
            return None


async def get_tg_clients(search: str = "", limit: int = 200) -> list[dict]:
    """Клиенты из таблицы бота (clients) — все кто писал в Telegram, своей компании.
    company_id уже проставляется ботом (см. artez_bot COMPANY_ID) — здесь просто фильтруем."""
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        # Проверяем что таблица clients существует (создаётся ботом)
        exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='clients')"
        )
        if not exists:
            return []
        blocked_col = "blocked" if await conn.fetchval(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_name='clients' AND column_name='blocked'"
        ) else "FALSE::boolean AS blocked"
        bl_col = "blacklisted" if await conn.fetchval(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_name='clients' AND column_name='blacklisted'"
        ) else "FALSE::boolean AS blacklisted"
        if search:
            rows = await conn.fetch(f"""
                SELECT id, tg_id, tg_username, first_name, last_name, phone,
                       lang, total_orders AS orders_count,
                       NULL::numeric AS total_spent,
                       NULL::timestamptz AS last_order_at,
                       created_at, {blocked_col}, {bl_col}
                FROM clients
                WHERE company_id=$1
                  AND (phone ILIKE $2 OR first_name ILIKE $2
                   OR last_name ILIKE $2 OR tg_username ILIKE $2)
                ORDER BY created_at DESC
                LIMIT $3
            """, cid, f"%{search}%", limit)
        else:
            rows = await conn.fetch(f"""
                SELECT id, tg_id, tg_username, first_name, last_name, phone,
                       lang, total_orders AS orders_count,
                       NULL::numeric AS total_spent,
                       NULL::timestamptz AS last_order_at,
                       created_at, {blocked_col}, {bl_col}
                FROM clients
                WHERE company_id=$1
                ORDER BY created_at DESC
                LIMIT $2
            """, cid, limit)
        return [dict(r) for r in rows]

async def upsert_push_subscription(staff_id: int, endpoint: str, p256dh: str, auth: str):
    # NB: конфликт по (staff_id, endpoint), а не по endpoint одному — иначе один
    # физический браузер, использованный для входа под разными staff_id (включая
    # разные компании в SaaS), "крал" бы подписку друг у друга при переподписке
    # (см. комментарий у CREATE UNIQUE INDEX idx_push_staff_endpoint).
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO push_subscriptions (staff_id, endpoint, p256dh, auth)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (staff_id, endpoint) DO UPDATE SET p256dh=$3, auth=$4
        """, staff_id, endpoint, p256dh, auth)

async def get_push_subscriptions(staff_id: int):
    if not pool: return []
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM push_subscriptions WHERE staff_id=$1", staff_id)

async def delete_push_subscription(endpoint: str, staff_id: int | None = None):
    """endpoint (один физический браузер) теперь может принадлежать НЕСКОЛЬКИМ
    staff_id одновременно (см. UNIQUE INDEX idx_push_staff_endpoint) — без явного
    staff_id удалит подписки ВСЕХ сотрудников с этого браузера, что почти всегда
    не то, что нужно. Передавайте staff_id при self-service отписке; endpoint-only
    оставлен только для обратной совместимости, для точечной чистки одной мёртвой
    подписки используйте delete_push_subscription_by_id(sub["id"])."""
    if not pool: return
    async with pool.acquire() as conn:
        if staff_id is not None:
            await conn.execute("DELETE FROM push_subscriptions WHERE endpoint=$1 AND staff_id=$2", endpoint, staff_id)
        else:
            await conn.execute("DELETE FROM push_subscriptions WHERE endpoint=$1", endpoint)

async def delete_push_subscription_by_id(sub_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM push_subscriptions WHERE id=$1", sub_id)

# ── order_photos ──────────────────────────────────────────────────────────────

async def get_order_photos(order_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM order_photos WHERE order_id=$1 ORDER BY created_at", order_id)
        return [dict(r) for r in rows]

async def save_order_photo(order_id: int, tg_file_id: str, tg_file_type: str,
                           photo_type: str, note: str, uploaded_by: str) -> dict:
    if not pool: return {}
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO order_photos (order_id, tg_file_id, tg_file_type, photo_type, note, uploaded_by, company_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *
        """, order_id, tg_file_id, tg_file_type, photo_type, note, uploaded_by, cid)
        return dict(row) if row else {}

async def delete_order_photo(photo_id: int, company_id: int) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        res = await conn.execute("""
            DELETE FROM order_photos p
            USING orders o
            WHERE p.id=$1 AND o.id = p.order_id AND o.company_id=$2
        """, photo_id, company_id)
        return res != "DELETE 0"

async def get_photo_by_id(photo_id: int, company_id: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT p.* FROM order_photos p
            JOIN orders o ON o.id = p.order_id
            WHERE p.id=$1 AND o.company_id=$2
        """, photo_id, company_id)
        return dict(row) if row else {}

# ── Оплата заказов ────────────────────────────────────────────────────────────

async def update_order_payment(order_id: int, payment_method: str, payment_status: str,
                                prepaid_amount: float) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        paid_at = "NOW()" if payment_status == "paid" else "NULL"
        row = await conn.fetchrow(f"""
            UPDATE orders SET
                payment_method = $1,
                payment_status = $2,
                prepaid_amount = $3,
                paid_at        = {paid_at}
            WHERE id = $4 RETURNING *
        """, payment_method, payment_status, prepaid_amount, order_id)
        return dict(row) if row else {}

# ── Касса ─────────────────────────────────────────────────────────────────────

async def get_cash_summary(date_from: str, date_to: str) -> dict:
    if not pool: return {}
    ts_from, ts_to = _tz_range(date_from, date_to)
    async with pool.acquire() as conn:
        # Суммы по методу оплаты (из order_payments)
        rows = await conn.fetch("""
            SELECT
                method AS payment_method,
                COALESCE(SUM(amount), 0) AS amount,
                COUNT(DISTINCT order_id) AS cnt
            FROM order_payments
            WHERE created_at >= $1 AND created_at < $2
              AND NOT (confirmed = FALSE AND confirmed_at IS NOT NULL)
            GROUP BY method
        """, ts_from, ts_to)
        # Заказы с оплатами за период
        orders = await conn.fetch("""
            SELECT
                o.id, o.order_num, o.created_at, o.total_price, o.discount_sum,
                o.payment_method, o.payment_status, o.prepaid_amount, o.paid_at,
                sub.paid_total,
                sub.last_payment_at AS payment_at
            FROM orders o
            JOIN (
                SELECT
                    order_id,
                    SUM(amount) AS paid_total,
                    MAX(created_at) AS last_payment_at
                FROM order_payments
                WHERE created_at >= $1 AND created_at < $2
                  AND NOT (confirmed = FALSE AND confirmed_at IS NOT NULL)
                GROUP BY order_id
            ) sub ON sub.order_id = o.id
            ORDER BY sub.last_payment_at DESC
        """, ts_from, ts_to)
        return {
            "summary": [dict(r) for r in rows],
            "orders": [dict(r) for r in orders],
        }

async def get_payments_log(date_from: str, date_to: str) -> list:
    if not pool: return []
    ts_from, ts_to = _tz_range(date_from, date_to)
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                op.id,
                op.order_id,
                o.order_num,
                op.amount,
                op.method,
                op.purpose,
                op.note,
                op.confirmed,
                op.confirmed_at,
                op.reject_note,
                op.created_at,
                op.created_by,
                TRIM(COALESCE(cs.last_name,'') || ' ' || COALESCE(cs.first_name,'')) AS created_by_name,
                TRIM(COALESCE(cv.last_name,'') || ' ' || COALESCE(cv.first_name,'')) AS confirmed_by_name,
                op.driver_tg_id
            FROM order_payments op
            LEFT JOIN orders o ON o.id = op.order_id
            LEFT JOIN staff cs ON cs.id = op.created_by_staff_id
            LEFT JOIN staff cv ON cv.id = op.confirmed_by
            WHERE op.created_at >= $1 AND op.created_at < $2
            ORDER BY op.created_at DESC
        """, ts_from, ts_to)
        return [dict(r) for r in rows]

async def close_cash_shift(shift_date, closed_by: str, note: str) -> dict:
    if not pool: return {}
    cid = _cid()
    from datetime import date as _date
    if isinstance(shift_date, str):
        shift_date = _date.fromisoformat(shift_date)
    async with pool.acquire() as conn:
        totals = await conn.fetchrow("""
            SELECT
                COALESCE(SUM(CASE WHEN payment_method='cash' THEN
                    CASE WHEN payment_status='paid' THEN COALESCE(total_price,0)-COALESCE(discount_sum,0)
                         WHEN payment_status='partial' THEN COALESCE(prepaid_amount,0)
                         ELSE 0 END ELSE 0 END),0) AS cash_total,
                COALESCE(SUM(CASE WHEN payment_method='card' THEN
                    CASE WHEN payment_status='paid' THEN COALESCE(total_price,0)-COALESCE(discount_sum,0)
                         WHEN payment_status='partial' THEN COALESCE(prepaid_amount,0)
                         ELSE 0 END ELSE 0 END),0) AS card_total,
                COALESCE(SUM(CASE WHEN payment_method='transfer' THEN
                    CASE WHEN payment_status='paid' THEN COALESCE(total_price,0)-COALESCE(discount_sum,0)
                         WHEN payment_status='partial' THEN COALESCE(prepaid_amount,0)
                         ELSE 0 END ELSE 0 END),0) AS transfer_total,
                COUNT(*) FILTER (WHERE payment_status IN ('paid','partial')) AS orders_count
            FROM orders WHERE created_at::date = $1 AND company_id=$2
        """, shift_date, cid)
        ct = float(totals['cash_total']); kt = float(totals['card_total'])
        tt = float(totals['transfer_total']); oc = int(totals['orders_count'])
        open_shift = await conn.fetchrow(
            "SELECT id FROM cash_shifts WHERE status='open' AND company_id=$1 ORDER BY opened_at DESC LIMIT 1", cid)
        if open_shift:
            row = await conn.fetchrow("""
                UPDATE cash_shifts SET
                    shift_date=$1, closed_by=$2, note=$3, status='closed', closed_at=NOW(),
                    cash_total=$4, card_total=$5, transfer_total=$6, grand_total=$7, orders_count=$8
                WHERE id=$9 AND company_id=$10 RETURNING *
            """, shift_date, closed_by, note, ct, kt, tt, ct+kt+tt, oc, open_shift['id'], cid)
        else:
            row = await conn.fetchrow("""
                INSERT INTO cash_shifts (shift_date, closed_by, cash_total, card_total, transfer_total, grand_total, orders_count, note, status, closed_at, company_id)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'closed',NOW(),$9) RETURNING *
            """, shift_date, closed_by, ct, kt, tt, ct+kt+tt, oc, note, cid)
        return dict(row) if row else {}

async def open_cash_shift(opened_by_id: int) -> dict | None:
    """None — если уже есть открытая смена."""
    if not pool: return {}
    cid = _cid()
    async with pool.acquire() as conn:
        existing = await conn.fetchval(
            "SELECT id FROM cash_shifts WHERE status='open' AND company_id=$1 LIMIT 1", cid)
        if existing:
            return None
        row = await conn.fetchrow("""
            INSERT INTO cash_shifts (opened_at, opened_by, status, shift_date, company_id)
            VALUES (NOW(), $1, 'open', NOW()::date, $2) RETURNING *
        """, opened_by_id, cid)
        return dict(row) if row else {}

async def delete_cash_shift(shift_id: int) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM cash_shifts WHERE id=$1", shift_id)
        return res == "DELETE 1"

async def get_current_shift() -> dict:
    if not pool: return {}
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT cs.*,
                   TRIM(COALESCE(s.last_name,'') || ' ' || COALESCE(s.first_name,'')) AS opener_name
            FROM cash_shifts cs
            LEFT JOIN staff s ON s.id = cs.opened_by
            WHERE cs.status = 'open' AND cs.company_id=$1
            ORDER BY cs.opened_at DESC LIMIT 1
        """, cid)
        return dict(row) if row else {}

async def _get_current_shift_id(conn) -> int:
    row = await conn.fetchrow(
        "SELECT id FROM cash_shifts WHERE status='open' ORDER BY opened_at DESC LIMIT 1")
    return row['id'] if row else None

async def get_cash_shifts(limit: int = 50) -> list:
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT cs.*,
                   TRIM(COALESCE(s.last_name,'') || ' ' || COALESCE(s.first_name,'')) AS opener_name
            FROM cash_shifts cs
            LEFT JOIN staff s ON s.id = cs.opened_by
            WHERE cs.company_id=$1
            ORDER BY COALESCE(cs.opened_at, cs.closed_at) DESC LIMIT $2
        """, cid, limit)
        return [dict(r) for r in rows]

async def get_cash_daily_total(date_str: str) -> dict:
    if not pool: return {}
    ts_from, ts_to = _tz_range(date_str, date_str)
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                COALESCE(SUM(CASE WHEN method='cash'     THEN amount ELSE 0 END), 0) AS cash,
                COALESCE(SUM(CASE WHEN method='card'     THEN amount ELSE 0 END), 0) AS card,
                COALESCE(SUM(CASE WHEN method='transfer' THEN amount ELSE 0 END), 0) AS transfer,
                COALESCE(SUM(amount), 0) AS total
            FROM order_payments
            WHERE created_at >= $1 AND created_at < $2
              AND NOT (confirmed = FALSE AND confirmed_at IS NOT NULL)
        """, ts_from, ts_to)
        return dict(row) if row else {}

async def get_cash_payment_history(year: int, month: int, branch: str = '', day: int = 0) -> list:
    if not pool: return []
    from datetime import date as _date
    import calendar
    if day > 0:
        target = _date(year, month, day)
        ts_from, ts_to = _tz_range(str(target), str(target))
    else:
        first_day = _date(year, month, 1)
        last_day  = _date(year, month, calendar.monthrange(year, month)[1])
        ts_from, ts_to = _tz_range(str(first_day), str(last_day))
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                (op.created_at AT TIME ZONE 'Asia/Tashkent')::date AS pay_date,
                s.id AS staff_id,
                TRIM(COALESCE(s.last_name,'') || ' ' || COALESCE(s.first_name,'')) AS staff_name,
                s.branch,
                COALESCE(SUM(CASE WHEN op.method='cash'     THEN op.amount ELSE 0 END), 0) AS cash,
                COALESCE(SUM(CASE WHEN op.method='card'     THEN op.amount ELSE 0 END), 0) AS card,
                COALESCE(SUM(CASE WHEN op.method='transfer' THEN op.amount ELSE 0 END), 0) AS transfer,
                COALESCE(SUM(op.amount), 0) AS total
            FROM order_payments op
            JOIN staff s ON s.id = op.created_by_staff_id
            WHERE op.created_at >= $1 AND op.created_at < $2
              AND NOT (op.confirmed = FALSE AND op.confirmed_at IS NOT NULL)
              AND ($3 = '' OR s.branch = $3)
            GROUP BY pay_date, s.id, s.last_name, s.first_name, s.branch
            ORDER BY pay_date DESC, staff_name
        """, ts_from, ts_to, branch)
        return [dict(r) for r in rows]

# ── order_payments ────────────────────────────────────────────────────────────

async def get_order_payments(order_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT p.*,
                   o.client_first_name, o.client_last_name, o.short_address,
                   TRIM(COALESCE(s.last_name,'') || ' ' || COALESCE(s.first_name,'')) AS staff_full_name,
                   s.phone AS staff_phone
            FROM order_payments p
            LEFT JOIN orders o ON o.id = p.order_id
            LEFT JOIN staff s ON s.id = p.created_by_staff_id
            WHERE p.order_id=$1
            ORDER BY p.created_at
        """, order_id)
        return [dict(r) for r in rows]

async def add_order_payment(order_id: int, amount: float, method: str, purpose: str, note: str, created_by: str, handed_to_staff_id: int = None, created_by_staff_id: int = None) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        # Долг ДО этого платежа — та же формула, что в get_orders_with_debt (items_total
        # минус все скидки минус уже подтверждённые платежи). Нужно ДО INSERT, чтобы
        # понять, именно ЭТОТ платёж закрыл долг (для записи в историю ниже).
        debt_before_row = await conn.fetchrow("""
            SELECT o.debt_responsible_id, GREATEST(0,
                COALESCE(NULLIF((SELECT SUM(COALESCE(price_per_sqm,0)*COALESCE(sqm,0))
                                  FROM order_items WHERE order_id=o.id), 0),
                         COALESCE(o.total_price,0), 0)
                - COALESCE(o.discount_sum,0) - COALESCE(o.delivery_discount,0) - COALESCE(o.manual_discount,0)
                - COALESCE((SELECT SUM(amount) FROM order_payments
                             WHERE order_id=o.id AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL)),0)
            ) AS debt_before
            FROM orders o WHERE o.id=$1
        """, order_id)
        row = await conn.fetchrow("""
            INSERT INTO order_payments (order_id, amount, method, purpose, note, created_by, handed_to_staff_id, created_by_staff_id, company_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *
        """, order_id, amount, method, purpose, note, created_by, handed_to_staff_id, created_by_staff_id, _cid())
        # Пересчитать payment_status на orders (не считаем отклонённые)
        total_row = await conn.fetchrow(
            "SELECT COALESCE(SUM(amount),0) AS paid FROM order_payments WHERE order_id=$1 AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL)", order_id)
        paid = float(total_row['paid'])
        order = await conn.fetchrow("SELECT total_price, discount_sum FROM orders WHERE id=$1", order_id)
        if order:
            net = float(order['total_price'] or 0) - float(order['discount_sum'] or 0)
            status = 'paid' if paid >= net and net > 0 else ('partial' if paid > 0 else 'unpaid')
            await conn.execute(
                "UPDATE orders SET payment_status=$1 WHERE id=$2", status, order_id)
        # Долг закрыт ИМЕННО этим платежом — фиксируем в истории, иначе «Долг погашен»
        # в карточке долга никогда не появлялся (см. фикс 2026-07-30). Новый платёж
        # сразу считается в сумме (та же логика, что в get_orders_with_debt — карта/
        # перевод исключаются из суммы только когда явно ОТКЛОНЕНЫ, не по умолчанию).
        if debt_before_row and debt_before_row["debt_responsible_id"] and float(debt_before_row["debt_before"]) > 0:
            if float(debt_before_row["debt_before"]) - amount <= 0:
                await conn.execute(
                    "INSERT INTO order_status_history(order_num, new_status, note) "
                    "SELECT order_num, 'debt_paid', $2 FROM orders WHERE id=$1",
                    order_id, f"Долг погашен: {created_by}")
        return dict(row) if row else {}

async def _recalc_payment_status(conn, order_id: int):
    total_row = await conn.fetchrow(
        "SELECT COALESCE(SUM(amount),0) AS paid FROM order_payments WHERE order_id=$1 AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL)", order_id)
    paid = float(total_row['paid'])
    order = await conn.fetchrow("""
        SELECT total_price, discount_sum, delivery_discount, manual_discount,
               COALESCE((SELECT SUM(COALESCE(price_per_sqm,0)*COALESCE(sqm,0))
                         FROM order_items WHERE order_id=$1), 0) AS items_total
        FROM orders WHERE id=$1
    """, order_id)
    if order:
        base = float(order['total_price'] or 0) or float(order['items_total'] or 0)
        net = base - float(order['discount_sum'] or 0) - float(order['delivery_discount'] or 0) - float(order['manual_discount'] or 0)
        status = 'paid' if paid >= net and net > 0 else ('partial' if paid > 0 else 'unpaid')
        await conn.execute("UPDATE orders SET payment_status=$1 WHERE id=$2", status, order_id)

async def add_order_activity(order_id: int, staff_id: int, staff_name: str, action: str, details: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO order_activity (order_id, staff_id, staff_name, action, details) VALUES ($1,$2,$3,$4,$5)",
            order_id, staff_id or None, staff_name, action, details)

async def get_order_activity(order_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM order_activity WHERE order_id=$1 ORDER BY created_at DESC LIMIT 100", order_id)
        return [dict(r) for r in rows]

async def delete_order_payment(payment_id: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM order_payments WHERE id=$1", payment_id)
        if not row: return {}
        order_id = row['order_id']
        await conn.execute("DELETE FROM order_payments WHERE id=$1", payment_id)
        await _recalc_payment_status(conn, order_id)
        return dict(row)

async def edit_order_payment(payment_id: int, amount: float, method: str, purpose: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE order_payments SET amount=$2, method=$3, purpose=$4 WHERE id=$1 RETURNING *",
            payment_id, amount, method, purpose)
        if row:
            await _recalc_payment_status(conn, row['order_id'])
        return dict(row) if row else {}

# ── order_item_media (замеры) ─────────────────────────────────────────────────

async def get_item_media(item_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM order_item_media WHERE item_id=$1 ORDER BY created_at", item_id)
        return [dict(r) for r in rows]

async def add_item_media(item_id: int, order_id: int, tg_file_id: str, tg_file_type: str, created_by: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO order_item_media (item_id, order_id, tg_file_id, tg_file_type, created_by, company_id)
            VALUES ($1,$2,$3,$4,$5,$6) RETURNING *
        """, item_id, order_id, tg_file_id, tg_file_type, created_by, _cid())
        return dict(row) if row else {}

async def delete_item_media(media_id: int, company_id: int) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        result = await conn.execute("""
            DELETE FROM order_item_media m
            USING orders o
            WHERE m.id=$1 AND o.id = m.order_id AND o.company_id=$2
        """, media_id, company_id)
        return result != "DELETE 0"

async def claim_measure_review(item_id: int, staff_id: int) -> dict:
    """Пометить замер как «принят на проверку» конкретным сотрудником."""
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE order_items
            SET review_claimed_by=$2, review_claimed_at=NOW()
            WHERE id=$1 AND measure_status='submitted'
            RETURNING *
        """, item_id, staff_id)
        return dict(row) if row else {}

async def get_pending_measure_reviews() -> list:
    """Все замеры со статусом 'submitted' с информацией о заказе и кто принял."""
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT oi.id AS item_id, oi.order_id, oi.service, oi.review_claimed_by,
                   oi.review_claimed_at,
                   o.order_num, o.status AS order_status,
                   o.client_first_name, o.client_last_name, o.client_phone,
                   s.first_name AS claimer_first, s.last_name AS claimer_last
            FROM order_items oi
            JOIN orders o ON o.id = oi.order_id
            LEFT JOIN staff s ON s.id = oi.review_claimed_by
            WHERE oi.measure_status = 'submitted' AND o.company_id = $1
            ORDER BY oi.id ASC
        """, cid)
        return [dict(r) for r in rows]

async def get_all_approvers() -> list:
    """Все сотрудники у которых can_approve_measure = true."""
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id FROM staff WHERE can_approve_measure=TRUE AND active=TRUE AND company_id=$1",
            cid
        )
        return [dict(r) for r in rows]

async def get_all_cashiers_for_push() -> list:
    """Все сотрудники у которых can_manage_cash = true."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id FROM staff WHERE can_manage_cash=TRUE AND active=TRUE"
        )
        return [dict(r) for r in rows]

async def reject_payment(payment_id: int, rejected_by: int, note: str = "") -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE order_payments
               SET confirmed=FALSE, confirmed_by=$2, confirmed_at=NOW(), reject_note=$3
             WHERE id=$1
             RETURNING *
        """, payment_id, rejected_by or None, note or None)
        if row:
            await _recalc_payment_status(conn, row["order_id"])
        return dict(row) if row else {}

async def submit_item_measure(item_id: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE order_items SET measure_status='submitted', reject_note=NULL
            WHERE id=$1 AND sqm IS NOT NULL
            RETURNING *
        """, item_id)
        return dict(row) if row else {}

async def approve_item_measure(item_id: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE order_items SET measure_status='approved', reject_note=NULL
            WHERE id=$1 RETURNING *
        """, item_id)
        return dict(row) if row else {}

async def direct_approve_measure(item_id: int, width_cm: float, length_cm: float) -> dict:
    if not pool: return {}
    sqm = round(width_cm * length_cm / 10000, 3)
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE order_items
               SET width_cm=$2, length_cm=$3, sqm=$4,
                   measure_status='approved', reject_note=NULL
             WHERE id=$1 RETURNING *
        """, item_id, width_cm, length_cm, sqm)
        return dict(row) if row else {}

async def direct_approve_measure_qty(item_id: int, quantity: float) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE order_items
               SET width_cm=NULL, length_cm=NULL, sqm=$2,
                   measure_status='approved', reject_note=NULL
             WHERE id=$1 RETURNING *
        """, item_id, round(quantity, 3))
        return dict(row) if row else {}

async def reject_item_measure(item_id: int, note: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE order_items SET measure_status='rejected', reject_note=$2
            WHERE id=$1 RETURNING *
        """, item_id, note or '')
        return dict(row) if row else {}

async def get_item_media_by_id(media_id: int, company_id: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT m.* FROM order_item_media m
            JOIN orders o ON o.id = m.order_id
            WHERE m.id=$1 AND o.company_id=$2
        """, media_id, company_id)
        return dict(row) if row else {}

# ── МАРШРУТЫ (routes) ────────────────────────────────────────────────────────

async def get_routes(date: str | None = None, driver_id: int | None = None,
                     branch: str | None = None, status: str | None = None) -> list:
    if not pool: return []
    cid = _cid()
    from datetime import date as _date
    if isinstance(date, str): date = _date.fromisoformat(date)
    filters, vals, i = [f"r.company_id=${1}"], [cid], 2
    if date:      filters.append(f"r.date=${i}::date"); vals.append(date); i+=1
    if driver_id: filters.append(f"r.driver_id=${i}"); vals.append(driver_id); i+=1
    if branch:    filters.append(f"r.branch=${i}"); vals.append(branch); i+=1
    if status:    filters.append(f"r.status=${i}"); vals.append(status); i+=1
    where = "WHERE " + " AND ".join(filters)
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT r.*,
                   s.first_name || ' ' || s.last_name AS driver_name,
                   COUNT(ro.id) AS order_count
            FROM routes r
            LEFT JOIN staff s ON s.id = r.driver_id
            LEFT JOIN route_orders ro ON ro.route_id = r.id
            {where}
            GROUP BY r.id, s.first_name, s.last_name
            ORDER BY r.date DESC, r.id DESC
        """, *vals)
        return [dict(r) for r in rows]

async def roll_forward_stale_routes() -> int:
    """Переносит planned/active маршруты с прошедшей датой на сегодня (Asia/Tashkent)."""
    if not pool: return 0
    today = datetime.now(_TASHKENT).date()
    async with pool.acquire() as conn:
        result = await conn.execute("""
            UPDATE routes SET date = $1, updated_at = NOW()
            WHERE status IN ('planned','active') AND date < $1
        """, today)
        return int(result.split()[-1]) if result else 0

async def create_route(data: dict) -> dict:
    if not pool: return {}
    cid = _cid()
    from datetime import date as _date
    d = _date.fromisoformat(data["date"]) if isinstance(data.get("date"), str) else data.get("date")
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO routes (name, date, driver_id, branch, type, status, note, company_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *
        """, data.get("name",""), d, data.get("driver_id"),
             data.get("branch"), data.get("type","mixed"),
             data.get("status","planned"), data.get("note"), cid)
        return dict(row) if row else {}

async def get_active_route_orders() -> list:
    """Все заказы в активных маршрутах (не done/cancelled) → [{order_id, route_id, route_name, route_date, route_type}]"""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT ro.order_id, r.id AS route_id, r.name AS route_name,
                   r.date AS route_date, r.type AS route_type
            FROM route_orders ro
            JOIN routes r ON r.id = ro.route_id
            WHERE r.status NOT IN ('done','cancelled')
        """)
        return [dict(r) for r in rows]

async def get_route(route_id: int) -> dict | None:
    if not pool: return None
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT r.*, s.first_name || ' ' || s.last_name AS driver_name
            FROM routes r LEFT JOIN staff s ON s.id=r.driver_id WHERE r.id=$1 AND r.company_id=$2
        """, route_id, cid)
        if not row: return None
        route = dict(row)
        stops = await conn.fetch("""
            SELECT ro.*, o.order_num, o.client_first_name, o.client_last_name,
                   o.client_phone, o.address, o.short_address,
                   o.location, o.location_address, o.status AS order_status,
                   ro.driver_confirmed,
                   o.service, o.branch,
                   o.pickup_date, o.deadline,
                   o.total_price, o.prepaid_amount, o.payment_status,
                   o.discount_sum, o.delivery_discount, o.manual_discount,
                   COALESCE((SELECT SUM(COALESCE(price_per_sqm,0)*COALESCE(sqm,0))
                              FROM order_items WHERE order_id=o.id), 0) AS items_total,
                   COALESCE((SELECT COUNT(*) FROM order_items WHERE order_id=o.id), 0)::int AS item_count,
                   COALESCE((SELECT SUM(amount) FROM order_payments
                              WHERE order_id=o.id
                                AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL)), 0) AS paid_amount
            FROM route_orders ro
            JOIN orders o ON o.id=ro.order_id
            WHERE ro.route_id=$1
            ORDER BY ro.sort_order, ro.id
        """, route_id)
        route["stops"] = [dict(s) for s in stops]
        return route

async def update_route(route_id: int, data: dict) -> dict:
    if not pool: return {}
    cid = _cid()
    allowed = {"name","date","driver_id","branch","type","status","note"}
    fields = {k: v for k, v in data.items() if k in allowed and v is not None}
    if "date" in fields and isinstance(fields["date"], str):
        from datetime import date as _date
        fields["date"] = _date.fromisoformat(fields["date"])
    if not fields: return {}
    sets = ", ".join(f"{k}=${i+2}" for i, k in enumerate(fields))
    cid_idx = len(fields) + 2
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE routes SET {sets}, updated_at=NOW() WHERE id=$1 AND company_id=${cid_idx} RETURNING *",
            route_id, *fields.values(), cid)
        return dict(row) if row else {}

async def delete_route(route_id: int) -> bool:
    if not pool: return False
    cid = _cid()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM routes WHERE id=$1 AND company_id=$2", route_id, cid)
        return True

async def add_orders_to_route(route_id: int, order_ids: list[int]) -> int:
    if not pool: return 0
    async with pool.acquire() as conn:
        cur_max = await conn.fetchval(
            "SELECT COALESCE(MAX(sort_order),0) FROM route_orders WHERE route_id=$1", route_id)
        count = 0
        for i, oid in enumerate(order_ids):
            try:
                await conn.execute("""
                    INSERT INTO route_orders (route_id, order_id, sort_order)
                    VALUES ($1,$2,$3) ON CONFLICT DO NOTHING
                """, route_id, oid, (cur_max or 0) + i + 1)
                count += 1
            except Exception:
                pass
        return count

async def remove_order_from_route(route_id: int, order_id: int) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM route_orders WHERE route_id=$1 AND order_id=$2", route_id, order_id)
        return True

async def update_route_stop(route_id: int, order_id: int, data: dict) -> bool:
    if not pool: return False
    allowed = {"sort_order","stop_status","note"}
    fields = {k: v for k, v in data.items() if k in allowed}
    if not fields: return False
    sets = ", ".join(f"{k}=${i+3}" for i, k in enumerate(fields))
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE route_orders SET {sets} WHERE route_id=$1 AND order_id=$2",
            route_id, order_id, *fields.values())
        return True

async def get_channel_msg_for_order(order_id: int):
    """Возвращает (branch, channel_msg_id) для обновления кнопок в канале."""
    if not pool: return None, None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT r.branch, r.tg_delivery_msg_ids
            FROM route_orders ro
            JOIN routes r ON r.id = ro.route_id
            WHERE ro.order_id = $1
            ORDER BY ro.id DESC LIMIT 1
        """, order_id)
    if not row: return None, None
    raw = row["tg_delivery_msg_ids"]
    if not raw: return row["branch"], None
    import json as _j
    try:
        msg_ids = _j.loads(raw) if isinstance(raw, str) else raw
        return row["branch"], msg_ids.get(str(order_id))
    except Exception:
        return row["branch"], None


async def get_channel_stop_full(order_id: int) -> dict | None:
    """Полные данные стопа для перестройки текста сообщения в канале после изменения оплаты."""
    if not pool: return None
    import json as _j
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT r.branch, r.tg_delivery_msg_ids, ro.sort_order,
                   o.id AS order_id, o.order_num, o.status,
                   o.client_first_name, o.client_last_name, o.client_phone,
                   o.address, o.short_address, o.location, o.location_address,
                   COALESCE(o.total_price, 0) AS total_price,
                   COALESCE(o.discount_sum, 0) AS discount_sum,
                   COALESCE(o.delivery_discount, 0) AS delivery_discount,
                   COALESCE(o.manual_discount, 0) AS manual_discount,
                   COALESCE((SELECT SUM(COALESCE(price_per_sqm,0)*COALESCE(sqm,0))
                              FROM order_items WHERE order_id=o.id), 0) AS items_total,
                   COALESCE((SELECT SUM(amount) FROM order_payments
                              WHERE order_id=o.id
                                AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL)), 0) AS paid_amount
            FROM route_orders ro
            JOIN routes r ON r.id = ro.route_id
            JOIN orders o ON o.id = ro.order_id
            WHERE ro.order_id = $1
            ORDER BY ro.id DESC LIMIT 1
        """, order_id)
    if not row: return None
    d = dict(row)
    raw = d.get("tg_delivery_msg_ids") or "{}"
    try: msg_ids = _j.loads(raw) if isinstance(raw, str) else (raw or {})
    except: msg_ids = {}
    d["msg_id"] = msg_ids.get(str(order_id))
    stored_ch = msg_ids.get("__channel__")
    d["channel_id"] = int(stored_ch) if stored_ch else None
    return d


# ── Касса / наличные ──────────────────────────────────────────────────────────

async def get_cashiers() -> list:
    """Ответственные за кассу (can_manage_cash). Admins первыми."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT id, first_name, last_name, role, position
               FROM staff
               WHERE can_manage_cash=TRUE AND active=TRUE
               ORDER BY (role='admin') DESC, last_name, first_name""")
        return [dict(r) for r in rows]

async def get_my_cash_balance(staff_id: int) -> dict:
    """Баланс конкретного сотрудника: принял / сдал / на руках."""
    if not pool: return {}
    async with pool.acquire() as conn:
        # Принял от клиентов (только по staff_id)
        r1 = await conn.fetchval(
            """SELECT COALESCE(SUM(amount),0) FROM order_payments
               WHERE method='cash' AND created_by_staff_id=$1""",
            staff_id)
        # Сдал сразу при записи (handed_to != me)
        r2 = await conn.fetchval(
            """SELECT COALESCE(SUM(amount),0) FROM order_payments
               WHERE method='cash' AND handed_to_staff_id IS NOT NULL AND handed_to_staff_id!=$1
               AND created_by_staff_id=$1""",
            staff_id)
        # Получил от других сотрудников через платёж (они сдали мне)
        r3 = await conn.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM order_payments WHERE handed_to_staff_id=$1 AND method='cash' AND (created_by_staff_id IS NULL OR created_by_staff_id<>$1)",
            staff_id)
        # Получил через ручную передачу (cash_handovers to me, только подтверждённые)
        r4 = await conn.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM cash_handovers WHERE to_staff_id=$1 AND status='confirmed'", staff_id)
        # Сдал через ручную передачу (cash_handovers from me, только подтверждённые)
        r5 = await conn.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM cash_handovers WHERE from_staff_id=$1 AND status='confirmed'", staff_id)
        # Ожидают подтверждения (cash_handovers from me, status='pending')
        r6 = await conn.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM cash_handovers WHERE from_staff_id=$1 AND status='pending'", staff_id)
        # Расходы утверждённые — вычитаются из наличных на руках
        r7 = await conn.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM expenses WHERE created_by_staff_id=$1 AND status IN ('approved','paid')",
            staff_id)
        collected         = float(r1)
        given_imm         = float(r2)
        recv_others       = float(r3)
        recv_hand         = float(r4)
        given_hand        = float(r5)
        pending_sent      = float(r6)
        expenses_approved = float(r7)
        on_hand = collected - given_imm + recv_others + recv_hand - given_hand - expenses_approved
        return {
            "collected":            collected,
            "given_immediately":    given_imm,
            "received_from_others": recv_others + recv_hand,
            "handed_over":          given_imm + given_hand,
            "pending_sent":         pending_sent,
            "expenses_approved":    expenses_approved,
            "on_hand":              on_hand,
        }

async def get_cash_balance() -> list:
    """Баланс наличных по всем сотрудникам (два уровня: исполнители + ответственные)."""
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, first_name, last_name, can_manage_cash, role FROM staff "
            "WHERE active=TRUE AND company_id=$1 ORDER BY (role='admin') DESC, can_manage_cash DESC, last_name, first_name",
            cid)
        result = []
        for s in rows:
            bal = await get_my_cash_balance(s['id'])
            is_admin = s['role'] == 'admin'
            if not is_admin and not s['can_manage_cash'] and bal['collected'] == 0 and bal['received_from_others'] == 0:
                continue
            result.append({
                "id": s['id'],
                "first_name": s['first_name'],
                "last_name":  s['last_name'],
                "can_manage_cash": s['can_manage_cash'],
                "role": s['role'],
                **bal,
            })
        return result

async def add_cash_handover(from_staff_id: int, to_staff_id: int, amount: float, note: str) -> dict:
    if not pool: return {}
    cid = _cid()
    async with pool.acquire() as conn:
        shift_id = await _get_current_shift_id(conn)
        row = await conn.fetchrow("""
            INSERT INTO cash_handovers (from_staff_id, to_staff_id, amount, note, shift_id, company_id)
            VALUES ($1,$2,$3,$4,$5,$6) RETURNING *
        """, from_staff_id, to_staff_id, amount, note, shift_id, cid)
        return dict(row) if row else {}

async def get_cash_handovers(limit: int = 50) -> list:
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT ch.*,
                   TRIM(COALESCE(sf.last_name,'') || ' ' || COALESCE(sf.first_name,'')) AS from_name,
                   TRIM(COALESCE(st.last_name,'') || ' ' || COALESCE(st.first_name,'')) AS to_name
            FROM cash_handovers ch
            LEFT JOIN staff sf ON sf.id = ch.from_staff_id
            LEFT JOIN staff st ON st.id = ch.to_staff_id
            WHERE (ch.to_type='staff' OR ch.to_type IS NULL) AND ch.company_id=$2
            ORDER BY ch.created_at DESC LIMIT $1
        """, limit, cid)
        return [dict(r) for r in rows]

async def confirm_cash_handover(handover_id: int, confirmed_by: int) -> dict:
    if not pool: return {}
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE cash_handovers SET status='confirmed', confirmed_at=NOW(), confirmed_by=$2
            WHERE id=$1 AND company_id=$3 RETURNING *
        """, handover_id, confirmed_by, cid)
        return dict(row) if row else {}

async def reject_cash_handover(handover_id: int, rejected_by: int) -> dict:
    if not pool: return {}
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE cash_handovers SET status='rejected', confirmed_at=NOW(), confirmed_by=$2
            WHERE id=$1 AND status='pending' AND company_id=$3 RETURNING *
        """, handover_id, rejected_by, cid)
        return dict(row) if row else {}

async def cancel_cash_handover(handover_id: int, cancelled_by: int, is_admin: bool = False) -> dict:
    if not pool: return {}
    cid = _cid()
    async with pool.acquire() as conn:
        # Admin can cancel any, staff only pending own
        if is_admin:
            row = await conn.fetchrow(
                "UPDATE cash_handovers SET status='cancelled', confirmed_at=NOW(), confirmed_by=$2 WHERE id=$1 AND company_id=$3 RETURNING *",
                handover_id, cancelled_by, cid)
        else:
            row = await conn.fetchrow(
                "UPDATE cash_handovers SET status='cancelled', confirmed_at=NOW(), confirmed_by=$2 WHERE id=$1 AND from_staff_id=$2 AND status='pending' AND company_id=$3 RETURNING *",
                handover_id, cancelled_by, cid)
        return dict(row) if row else {}

async def update_handover_tg_msg(handover_id: int, tg_chat_id: int, tg_msg_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE cash_handovers SET tg_chat_id=$2, tg_msg_id=$3 WHERE id=$1",
            handover_id, tg_chat_id, tg_msg_id)

async def mark_expense_paid(expense_id: int, paid_by: int, paid_from: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE expenses SET status='paid', paid_by=$2, paid_from=$3, paid_at=NOW()
            WHERE id=$1 AND status='approved' RETURNING *
        """, expense_id, paid_by, paid_from)
        return dict(row) if row else {}

async def get_pending_handovers_for(staff_id: int) -> list:
    """Входящие неподтверждённые передачи для данного сотрудника."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT ch.*,
                   TRIM(COALESCE(sf.last_name,'') || ' ' || COALESCE(sf.first_name,'')) AS from_name
            FROM cash_handovers ch
            LEFT JOIN staff sf ON sf.id = ch.from_staff_id
            WHERE ch.to_staff_id=$1 AND ch.status='pending'
            ORDER BY ch.created_at DESC
        """, staff_id)
        return [dict(r) for r in rows]

async def get_my_sent_handovers(staff_id: int) -> list:
    """Исходящие передачи наличных от данного сотрудника (все типы: сотрудник / банк / сейф)."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT ch.*,
                   TRIM(COALESCE(st.last_name,'') || ' ' || COALESCE(st.first_name,'')) AS to_name
            FROM cash_handovers ch
            LEFT JOIN staff st ON st.id = ch.to_staff_id
            WHERE ch.from_staff_id = $1
            ORDER BY ch.created_at DESC LIMIT 50
        """, staff_id)
        return [dict(r) for r in rows]

async def get_my_received_handovers(staff_id: int) -> list:
    """Входящие подтверждённые передачи наличных для данного сотрудника."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT ch.*,
                   TRIM(COALESCE(sf.last_name,'') || ' ' || COALESCE(sf.first_name,'')) AS from_name
            FROM cash_handovers ch
            LEFT JOIN staff sf ON sf.id = ch.from_staff_id
            WHERE ch.to_staff_id = $1 AND ch.status = 'confirmed'
            ORDER BY ch.created_at DESC LIMIT 50
        """, staff_id)
        return [dict(r) for r in rows]

async def add_safe_deposit(from_staff_id: int, amount: float, note: str = '') -> dict:
    """Сдача в сейф от администратора: создаёт pending запись для подтверждения."""
    if not pool: return {}
    async with pool.acquire() as conn:
        shift_id = await _get_current_shift_id(conn)
        row = await conn.fetchrow("""
            INSERT INTO cash_handovers (from_staff_id, to_staff_id, amount, note, to_type, status, shift_id)
            VALUES ($1, NULL, $2, $3, 'safe', 'pending', $4) RETURNING *
        """, from_staff_id, amount, note, shift_id)
        return dict(row) if row else {}

async def get_pending_safe_deposits() -> list:
    """Pending сдачи в сейф для отображения в admin.html."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT ch.*,
                   TRIM(COALESCE(sf.last_name,'') || ' ' || COALESCE(sf.first_name,'')) AS from_name
            FROM cash_handovers ch
            LEFT JOIN staff sf ON sf.id = ch.from_staff_id
            WHERE ch.to_type='safe' AND ch.status='pending'
            ORDER BY ch.created_at DESC
        """)
        return [dict(r) for r in rows]

async def create_bank_deposit(from_staff_id: int, amount: float, to_type: str, note: str = '') -> dict:
    """Инкассация: наличные сданы в банк/сейф. Сразу подтверждена."""
    if not pool: return {}
    async with pool.acquire() as conn:
        shift_id = await _get_current_shift_id(conn)
        row = await conn.fetchrow("""
            INSERT INTO cash_handovers (from_staff_id, to_staff_id, amount, note, to_type, status, confirmed_at, shift_id)
            VALUES ($1, NULL, $2, $3, $4, 'confirmed', NOW(), $5) RETURNING *
        """, from_staff_id, amount, note, to_type, shift_id)
        return dict(row) if row else {}

async def get_bank_deposits(limit: int = 100) -> list:
    """История инкассаций (bank/safe)."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT ch.*,
                   TRIM(COALESCE(s.last_name,'') || ' ' || COALESCE(s.first_name,'')) AS from_name
            FROM cash_handovers ch
            LEFT JOIN staff s ON s.id = ch.from_staff_id
            WHERE ch.to_type IN ('bank','safe')
            ORDER BY ch.created_at DESC LIMIT $1
        """, limit)
        return [dict(r) for r in rows]

async def get_cash_dashboard() -> dict:
    """Сводные метрики для дашборда кассы."""
    if not pool: return {}
    cid = _cid()
    balances = await get_cash_balance()
    staff_on_hand   = sum(float(b.get('on_hand', 0)) for b in balances if b.get('role') != 'admin' and not b.get('can_manage_cash'))
    manager_on_hand = sum(float(b.get('on_hand', 0)) for b in balances if b.get('role') != 'admin' and b.get('can_manage_cash'))
    admin_on_hand   = sum(float(b.get('on_hand', 0)) for b in balances if b.get('role') == 'admin')
    from datetime import date as _date
    today = _date.today()
    async with pool.acquire() as conn:
        r1 = await conn.fetchrow(
            "SELECT COALESCE(SUM(amount),0) AS total FROM cash_handovers "
            "WHERE to_type IN ('bank','safe') AND created_at::date=$1 AND company_id=$2", today, cid)
        r3 = await conn.fetchrow(
            "SELECT COALESCE(SUM(amount),0) AS total, COUNT(*) AS cnt FROM expenses "
            "WHERE status IN ('pending','mgr_approved') AND company_id=$1", cid)
        # К оплате: все заказы в работе (не доставлены), у которых есть остаток
        r_pending = await conn.fetchrow("""
            SELECT COALESCE(SUM(GREATEST(0,
                COALESCE(NULLIF((SELECT SUM(COALESCE(price_per_sqm,0)*COALESCE(sqm,0))
                                  FROM order_items WHERE order_id=o.id), 0),
                         COALESCE(o.total_price,0), 0)
                - COALESCE(o.discount_sum,0)
                - COALESCE(o.delivery_discount,0) - COALESCE(o.manual_discount,0)
                - COALESCE((SELECT SUM(op.amount) FROM order_payments op
                             WHERE op.order_id = o.id
                               AND NOT (op.confirmed = FALSE AND op.confirmed_at IS NOT NULL)), 0)
            )), 0) AS total
            FROM orders o
            WHERE o.payment_status IN ('unpaid','partial')
              AND o.status IN ('ready','delivery')
              AND o.company_id=$1
        """, cid)
        # Долги: доставленные с debt_responsible_id, ещё не оплачены полностью
        r_debt = await conn.fetchrow("""
            SELECT
                COUNT(*) AS total_cnt,
                COUNT(CASE WHEN o.debt_due_date IS NOT NULL AND o.debt_due_date < CURRENT_DATE THEN 1 END) AS overdue_cnt,
                COALESCE(SUM(GREATEST(0,
                    COALESCE(NULLIF((SELECT SUM(COALESCE(price_per_sqm,0)*COALESCE(sqm,0))
                                      FROM order_items WHERE order_id=o.id), 0),
                             COALESCE(o.total_price,0), 0)
                    - COALESCE(o.discount_sum,0)
                    - COALESCE(o.delivery_discount,0) - COALESCE(o.manual_discount,0)
                    - COALESCE((SELECT SUM(op.amount) FROM order_payments op
                                 WHERE op.order_id = o.id
                                   AND NOT (op.confirmed = FALSE AND op.confirmed_at IS NOT NULL)), 0)
                )), 0) AS total_debt
            FROM orders o
            WHERE o.debt_responsible_id IS NOT NULL
              AND o.payment_status IN ('unpaid','partial')
              AND o.company_id=$1
        """, cid)
    return {
        'staff_on_hand':          staff_on_hand,
        'manager_on_hand':        manager_on_hand,
        'admin_on_hand':          admin_on_hand,
        'banked_today':           float(r1['total']),
        'pending_client_cash':    float(r_pending['total']),
        'expenses_pending_sum':   float(r3['total']),
        'expenses_pending_count': int(r3['cnt']),
        'debt_total':             float(r_debt['total_debt']),
        'debt_count':             int(r_debt['total_cnt']),
        'debt_overdue_count':     int(r_debt['overdue_cnt']),
    }

async def confirm_payment(payment_id: int, confirmed_by: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE order_payments SET confirmed=TRUE, confirmed_by=$2, confirmed_at=NOW()
            WHERE id=$1 RETURNING *
        """, payment_id, confirmed_by or None)
        return dict(row) if row else {}

async def save_payment_receipt(payment_id: int, receipt_url: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE order_payments SET receipt_url=$2 WHERE id=$1 RETURNING *",
            payment_id, receipt_url)
        return dict(row) if row else {}

async def get_unconfirmed_payments() -> list:
    """Неподтверждённые платежи картой/переводом (только ожидающие, не отклонённые)."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT p.*,
                   o.client_first_name, o.client_last_name, o.short_address,
                   TRIM(COALESCE(s.last_name,'') || ' ' || COALESCE(s.first_name,'')) AS staff_full_name,
                   s.phone AS staff_phone
            FROM order_payments p
            LEFT JOIN orders o ON o.id = p.order_id
            LEFT JOIN staff s ON s.id = p.created_by_staff_id
            WHERE p.method IN ('card','transfer') AND p.confirmed=FALSE AND p.confirmed_at IS NULL
            ORDER BY p.created_at DESC
        """)
        return [dict(r) for r in rows]

async def get_cash_tg_channel() -> str:
    if not pool: return ""
    cid = _cid()
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT cash_tg_channel_id FROM settings WHERE company_id=$1", cid)
            return (row['cash_tg_channel_id'] or "") if row else ""
    except Exception:
        return ""

async def get_media_channel_id() -> str:
    if not pool: return ""
    cid = _cid()
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT media_channel_id FROM settings WHERE company_id=$1", cid)
            return (row['media_channel_id'] or "") if row else ""
    except Exception:
        return ""

# ── plans (roadmap) ──────────────────────────────────────────────────────────

async def ensure_plans_table():
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS plans (
                id          SERIAL PRIMARY KEY,
                title       VARCHAR(500) NOT NULL,
                description TEXT         DEFAULT '',
                status      VARCHAR(20)  DEFAULT 'new',
                priority    VARCHAR(20)  DEFAULT 'normal',
                created_at  TIMESTAMPTZ  DEFAULT NOW(),
                done_at     TIMESTAMPTZ  DEFAULT NULL
            )
        """)
        await conn.execute("ALTER TABLE plans ALTER COLUMN status SET DEFAULT 'new'")
        count = await conn.fetchval("SELECT COUNT(*) FROM plans")
        if count == 0:
            seed = [
                ("Архивация заказов",
                 "Каждые 60 дней переносить доставленные и отменённые заказы в отдельную таблицу orders_archive, чтобы снизить нагрузку на основную таблицу orders. Добавить кнопку ручной архивации в «Обслуживание БД» и автоматический cron.",
                 "high"),
                ("Флоу мастерской (мойщик)",
                 "Реализовать полный цикл работы мойщика: список позиций с названиями услуг и размерами, замеры (ширина/длина/площадь), подтверждение приёмки, смена статусов Получен → Мойка → Сушка → Готов. Мойщик работает только со своими назначенными заказами.",
                 "high"),
                ("Аналитика и отчёты",
                 "Страница отчётов в admin: выручка за период (день/неделя/месяц), количество заказов по статусам, топ услуг по площади и сумме, загруженность мастерской. Экспорт в Excel.",
                 "normal"),
                ("История изменений заказа",
                 "Полный лог всех действий по заказу: кто и когда изменил статус, добавил позицию, добавил/отклонил оплату, изменил адрес. Уже частично есть order_activity — расширить и красиво отобразить в карточке заказа.",
                 "normal"),
                ("SMS / TG уведомления клиенту по статусам",
                 "Автоматически отправлять клиенту сообщение в Telegram при каждой смене статуса заказа (шаблоны уже есть в tg_status_messages). Проверить и доработать: кнопки «Тест отправки», статистика доставки.",
                 "normal"),
                ("Мобильная версия staff (PWA)",
                 "Улучшить работу staff.html на мобильном: добавить иконку на рабочий стол (PWA manifest), офлайн-заглушку, оптимизировать таблицы и модалки для маленьких экранов.",
                 "low"),
            ]
            await conn.executemany(
                "INSERT INTO plans(title, description, priority) VALUES($1, $2, $3)",
                seed
            )

async def get_plans():
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM plans WHERE company_id=$1 ORDER BY status, priority DESC, created_at DESC", cid)
    return [dict(r) for r in rows]

async def create_plan(title: str, description: str, priority: str):
    if not pool: return None
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO plans(title,description,priority,company_id) VALUES($1,$2,$3,$4) RETURNING *",
            title, description, priority, cid
        )
    return dict(row)

async def update_plan(plan_id: int, **kwargs):
    if not pool: return None
    cid = _cid()
    fields = {k: v for k, v in kwargs.items() if k in ('title','description','status','priority','done_at')}
    if not fields: return None
    sets   = ", ".join(f"{k}=${i+2}" for i, k in enumerate(fields))
    values = list(fields.values())
    cid_idx = len(fields) + 2
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE plans SET {sets} WHERE id=$1 AND company_id=${cid_idx} RETURNING *", plan_id, *values, cid
        )
    return dict(row) if row else None

async def delete_plan(plan_id: int):
    if not pool: return
    cid = _cid()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM plans WHERE id=$1 AND company_id=$2", plan_id, cid)


# ── Chat ──────────────────────────────────────────────────────────────────────

async def ensure_chat_tables():
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS chat_sessions (
            id           SERIAL PRIMARY KEY,
            code         VARCHAR(12) UNIQUE NOT NULL,
            client_phone VARCHAR(20) DEFAULT '',
            client_name  VARCHAR(100) DEFAULT '',
            branch       VARCHAR(50) DEFAULT '',
            status       VARCHAR(20) DEFAULT 'pending',
            claimed_by   INTEGER REFERENCES staff(id) ON DELETE SET NULL,
            claimed_name VARCHAR(100) DEFAULT '',
            created_at   TIMESTAMPTZ DEFAULT NOW(),
            updated_at   TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_chat_sessions_status ON chat_sessions(status);
        CREATE INDEX IF NOT EXISTS idx_chat_sessions_code   ON chat_sessions(code);

        CREATE TABLE IF NOT EXISTS chat_messages (
            id          SERIAL PRIMARY KEY,
            session_id  INTEGER REFERENCES chat_sessions(id) ON DELETE CASCADE,
            sender_type VARCHAR(10) NOT NULL,
            sender_name VARCHAR(100) DEFAULT '',
            text        TEXT NOT NULL,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_chat_messages_session ON chat_messages(session_id);
        ALTER TABLE chat_sessions ADD COLUMN IF NOT EXISTS last_client_msg_at TIMESTAMPTZ;
        ALTER TABLE chat_sessions ADD COLUMN IF NOT EXISTS warned_at TIMESTAMPTZ;
        ALTER TABLE chat_sessions ADD COLUMN IF NOT EXISTS lang VARCHAR(5) DEFAULT 'uz';
        """)

async def create_chat_session(code: str, client_phone: str = '', client_name: str = '', branch: str = '', lang: str = 'uz') -> dict:
    if not pool: return None
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO chat_sessions (code, client_phone, client_name, branch, lang, company_id) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *",
            code, client_phone, client_name, branch, lang, cid
        )
        return dict(row) if row else None

async def get_chat_session(code: str) -> dict:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM chat_sessions WHERE code=$1", code)
        return dict(row) if row else None

async def get_active_chat_sessions() -> list:
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM chat_sessions WHERE status IN ('pending','active') AND company_id=$1 ORDER BY created_at DESC",
            cid
        )
        return [dict(r) for r in rows]

async def is_first_client_message(session_id: int) -> bool:
    """True если клиент ещё не писал ни одного сообщения в этой сессии."""
    if not pool: return False
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM chat_messages WHERE session_id=$1 AND sender_type='client'",
            session_id
        )
        return count == 0

async def get_active_chat_by_phone(phone: str) -> dict:
    """Найти активный/pending чат клиента (своей компании) по номеру телефона."""
    if not pool or not phone: return None
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM chat_sessions WHERE client_phone=$1 AND company_id=$2 AND status IN ('pending','active') ORDER BY created_at DESC LIMIT 1",
            phone.strip(), cid
        )
        return dict(row) if row else None

async def get_closed_chat_sessions(limit: int = 50, offset: int = 0,
                                    staff_id: int = None, own_only: bool = False) -> list:
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        if own_only and staff_id:
            rows = await conn.fetch(
                "SELECT * FROM chat_sessions WHERE status='closed' AND claimed_by=$1 AND company_id=$4 ORDER BY updated_at DESC LIMIT $2 OFFSET $3",
                staff_id, limit, offset, cid
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM chat_sessions WHERE status='closed' AND company_id=$3 ORDER BY updated_at DESC LIMIT $1 OFFSET $2",
                limit, offset, cid
            )
        return [dict(r) for r in rows]

async def claim_chat_session(code: str, staff_id: int, staff_name: str) -> dict:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """UPDATE chat_sessions SET status='active', claimed_by=$2, claimed_name=$3, updated_at=NOW()
               WHERE code=$1 AND (claimed_by IS NULL OR claimed_by=$2) RETURNING *""",
            code, staff_id, staff_name
        )
        return dict(row) if row else None

async def close_chat_session(code: str) -> dict:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE chat_sessions SET status='closed', updated_at=NOW() WHERE code=$1 RETURNING *", code
        )
        return dict(row) if row else None

async def add_chat_message(session_id: int, sender_type: str, sender_name: str, text: str) -> dict:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO chat_messages (session_id, sender_type, sender_name, text) VALUES ($1,$2,$3,$4) RETURNING *",
            session_id, sender_type, sender_name, text
        )
        return dict(row) if row else None

async def get_chat_messages(session_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM chat_messages WHERE session_id=$1 ORDER BY created_at ASC", session_id
        )
        return [dict(r) for r in rows]

async def get_staff_for_chat_push(company_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id FROM staff WHERE active=TRUE AND company_id=$1 AND role IN ('admin','manager','callcenter')",
            company_id
        )
        return [r['id'] for r in rows]

async def ensure_chat_templates():
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS chat_templates (
            id         SERIAL PRIMARY KEY,
            key        VARCHAR(30) DEFAULT 'quick',
            lang       VARCHAR(5) NOT NULL DEFAULT 'uz',
            text       TEXT NOT NULL,
            sort_order INT DEFAULT 0,
            active     BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_chat_templates_lang ON chat_templates(lang, key);
        """)
        count = await conn.fetchval("SELECT COUNT(*) FROM chat_templates")
        if count == 0:
            await conn.executemany(
                "INSERT INTO chat_templates (key, lang, text, sort_order) VALUES ($1,$2,$3,$4)",
                _CHAT_TEMPLATE_SEED
            )

async def get_chat_templates(lang: str = None, key: str = None) -> list:
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        conditions, args = ["active=TRUE", "company_id=$1"], [cid]
        if lang:  args.append(lang);  conditions.append(f"lang=${len(args)}")
        if key:   args.append(key);   conditions.append(f"key=${len(args)}")
        rows = await conn.fetch(
            f"SELECT * FROM chat_templates WHERE {' AND '.join(conditions)} ORDER BY lang, sort_order, id",
            *args
        )
        return [dict(r) for r in rows]

async def get_all_chat_templates() -> list:
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM chat_templates WHERE company_id=$1 ORDER BY lang, key, sort_order, id", cid)
        return [dict(r) for r in rows]

async def upsert_chat_template(data: dict) -> dict:
    if not pool: return None
    cid = _cid()
    async with pool.acquire() as conn:
        tid = data.get('id')
        if tid:
            row = await conn.fetchrow(
                "UPDATE chat_templates SET key=$2,lang=$3,text=$4,sort_order=$5,active=$6 WHERE id=$1 AND company_id=$7 RETURNING *",
                tid, data['key'], data['lang'], data['text'],
                data.get('sort_order', 0), data.get('active', True), cid
            )
        else:
            row = await conn.fetchrow(
                "INSERT INTO chat_templates (key,lang,text,sort_order,active,company_id) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *",
                data['key'], data['lang'], data['text'], data.get('sort_order', 0), data.get('active', True), cid
            )
        return dict(row) if row else None

async def delete_chat_template(tid: int):
    if not pool: return
    cid = _cid()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM chat_templates WHERE id=$1 AND company_id=$2", tid, cid)

_CHAT_TEMPLATE_SEED = [
    # ── Авто-сообщения RU ──
    ('welcome',      'ru', "Здравствуйте, {name}! 👋 Рады видеть вас в ARTEZ. Напишите ваш вопрос — оператор ответит в ближайшее время.", 0),
    ('auto_reply',   'ru', "✅ {name}, ваше сообщение принято! Оператор ответит в течение 1–3 минут. Спасибо за ожидание 🙏", 1),
    ('warn_timeout', 'ru', "⏰ {name}, вы давно не отвечаете. Чат будет автоматически закрыт через 2 минуты.", 2),
    ('bye_m',        'ru', "Спасибо за обращение, {name}! Рад был помочь 😊 Если появятся вопросы — мы всегда здесь. Хорошего дня!", 3),
    ('bye_f',        'ru', "Спасибо за обращение, {name}! Рада была помочь 😊 Если появятся вопросы — мы всегда здесь. Хорошего дня!", 4),
    # ── Авто-сообщения UZ ──
    ('welcome',      'uz', "Salom, {name}! 👋 Sizni ARTEZ'da ko'rganimizdan xursandmiz. Savolingizni yozing — operator tez orada javob beradi.", 0),
    ('auto_reply',   'uz', "✅ {name}, xabaringiz qabul qilindi! Operator 1–3 daqiqa ichida javob beradi. Kutganingiz uchun rahmat 🙏", 1),
    ('warn_timeout', 'uz', "⏰ {name}, siz uzoq vaqtdan beri javob bermadingiz. Chat 2 daqiqadan so'ng avtomatik yopiladi.", 2),
    ('bye_m',        'uz', "Murojaat qilganingiz uchun rahmat, {name}! Yordam bera olganim uchun xursandman 😊 Savol bo'lsa — biz doim shu yerdamiz. Yaxshi kun!", 3),
    ('bye_f',        'uz', "Murojaat qilganingiz uchun rahmat, {name}! Yordam bera olganim uchun xursandman 😊 Savol bo'lsa — biz doim shu yerdamiz. Yaxshi kun!", 4),
    # ── Быстрые ответы RU ──
    ('quick', 'ru', "Здравствуйте, {name}! Чем могу помочь? 😊", 10),
    ('quick', 'ru', "Какое изделие нужно почистить? (ковёр, диван, матрас, шторы...)", 11),
    ('quick', 'ru', "Стоимость зависит от размера и состояния. Пришлите фото или назовите размеры? 📐", 12),
    ('quick', 'ru', "Выезд мастера для замера и забора — бесплатно 🚗", 13),
    ('quick', 'ru', "Срок чистки — 1–3 дня. Вернём чистым и свежим 🧹", 14),
    ('quick', 'ru', "Работаем ежедневно с 9:00 до 20:00 🕐", 15),
    ('quick', 'ru', "Оплата при получении — наличными или картой 💳", 16),
    ('quick', 'ru', "Используем профессиональную химию — безопасно для детей и аллергиков ✅", 17),
    ('quick', 'ru', "Уточните адрес, {name}? Выедем в удобное для вас время 📍", 18),
    ('quick', 'ru', "Записываем вас! Мастер свяжется для подтверждения времени ✅", 19),
    ('quick', 'ru', "Если есть ещё вопросы — спрашивайте, с удовольствием помогу 😊", 20),
    ('quick', 'ru', "Спасибо, {name}! Ждём ваше изделие 🙏", 21),
    # ── Быстрые ответы UZ ──
    ('quick', 'uz', "Salom, {name}! Qanday yordam bera olaman? 😊", 10),
    ('quick', 'uz', "Qaysi mahsulotni tozalash kerak? (gilam, divan, matras, parda...)", 11),
    ('quick', 'uz', "Narx o'lcham va holatiga qarab. Rasm yuboring yoki o'lchamlarini ayting? 📐", 12),
    ('quick', 'uz', "Usta o'lchov va olib ketish uchun chiqishi bepul 🚗", 13),
    ('quick', 'uz', "Tozalash muddati — 1–3 kun. Toza va yangi holda qaytaramiz 🧹", 14),
    ('quick', 'uz', "Har kuni soat 9:00 dan 20:00 gacha ishlaymiz 🕐", 15),
    ('quick', 'uz', "To'lov qabul qilishda — naqd yoki karta orqali 💳", 16),
    ('quick', 'uz', "Professional kimyo ishlatamiz — bolalar va allergiklar uchun xavfsiz ✅", 17),
    ('quick', 'uz', "Manzilni ayta olasizmi, {name}? Qulay vaqtingizda chiqamiz 📍", 18),
    ('quick', 'uz', "Yozib olyapmiz! Usta vaqtni tasdiqlash uchun bog'lanadi ✅", 19),
    ('quick', 'uz', "Yana savollar bo'lsa — so'rang, mamnuniyat bilan yordam beraman 😊", 20),
    ('quick', 'uz', "Rahmat, {name}! Mahsulotingizni kutamiz 🙏", 21),
]

async def seed_chat_templates_forced():
    """Обновить auto-шаблоны (welcome/auto_reply/warn/bye) по key+lang,
       добавить quick-шаблоны если текста ещё нет."""
    if not pool: return 0
    updated = 0
    inserted = 0
    async with pool.acquire() as conn:
        existing_texts = {r['text'] for r in await conn.fetch("SELECT text FROM chat_templates")}
        for key, lang, text, sort_order in _CHAT_TEMPLATE_SEED:
            if key == 'quick':
                if text not in existing_texts:
                    await conn.execute(
                        "INSERT INTO chat_templates (key, lang, text, sort_order) VALUES ($1,$2,$3,$4)",
                        key, lang, text, sort_order
                    )
                    inserted += 1
            else:
                row = await conn.fetchrow(
                    "SELECT id FROM chat_templates WHERE key=$1 AND lang=$2 LIMIT 1", key, lang
                )
                if row:
                    await conn.execute(
                        "UPDATE chat_templates SET text=$1, sort_order=$2 WHERE id=$3",
                        text, sort_order, row['id']
                    )
                    updated += 1
                else:
                    await conn.execute(
                        "INSERT INTO chat_templates (key, lang, text, sort_order) VALUES ($1,$2,$3,$4)",
                        key, lang, text, sort_order
                    )
                    inserted += 1
    return updated + inserted

async def get_chat_template_text(key: str, lang: str) -> str:
    """Получить текст шаблона по ключу и языку, fallback на uz."""
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT text FROM chat_templates WHERE key=$1 AND lang=$2 AND active=TRUE LIMIT 1",
            key, lang
        )
        if not row:
            row = await conn.fetchrow(
                "SELECT text FROM chat_templates WHERE key=$1 AND active=TRUE LIMIT 1", key
            )
        return row['text'] if row else None

async def touch_chat_client_activity(code: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE chat_sessions SET last_client_msg_at=NOW(), warned_at=NULL, updated_at=NOW() WHERE code=$1",
            code
        )

async def get_sessions_to_warn() -> list:
    """Активные сессии, где клиент молчит 10+ мин и предупреждение ещё не отправлено."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT cs.*, s.gender AS staff_gender, s.first_name AS staff_first_name
            FROM chat_sessions cs
            LEFT JOIN staff s ON s.id = cs.claimed_by
            WHERE cs.status = 'active'
              AND COALESCE(cs.last_client_msg_at, cs.created_at) < NOW() - INTERVAL '10 minutes'
              AND cs.warned_at IS NULL
        """)
        return [dict(r) for r in rows]

async def get_sessions_to_close() -> list:
    """Активные сессии, где предупреждение было >2 мин назад."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT cs.*, s.gender AS staff_gender, s.first_name AS staff_first_name
            FROM chat_sessions cs
            LEFT JOIN staff s ON s.id = cs.claimed_by
            WHERE cs.status = 'active'
              AND cs.warned_at IS NOT NULL
              AND cs.warned_at < NOW() - INTERVAL '2 minutes'
        """)
        return [dict(r) for r in rows]

async def set_chat_warned(code: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE chat_sessions SET warned_at=NOW(), updated_at=NOW() WHERE code=$1", code
        )

# ══════════════════════════════════════
#  ДОЛГИ ПО ЗАКАЗАМ
# ══════════════════════════════════════

async def get_order_debt_amount(order_id: int) -> float:
    if not pool: return 0.0
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT GREATEST(0,
                COALESCE(o.total_price,0) - COALESCE(o.discount_sum,0)
                - COALESCE(o.delivery_discount,0) - COALESCE(o.manual_discount,0)
                - COALESCE((SELECT SUM(amount) FROM order_payments
                             WHERE order_id=o.id AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL)),0)
            ) AS debt FROM orders o WHERE o.id=$1
        """, order_id)
        return float(row["debt"]) if row else 0.0

async def get_debt_approvers() -> list:
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, first_name, last_name, tg_id FROM staff "
            "WHERE can_approve_debt=TRUE AND active=TRUE AND tg_id IS NOT NULL AND company_id=$1", cid)
        return [dict(r) for r in rows]

async def mark_order_delivered_with_debt(order_id: int, responsible_id: int,
                                         due_date_str: str | None, by_name: str) -> bool:
    if not pool: return False
    from datetime import date, timedelta
    due = None
    if due_date_str:
        try: due = date.fromisoformat(due_date_str)
        except Exception: pass
    if due is None:
        due = date.today() + timedelta(days=7)
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE orders SET status='delivered', debt_responsible_id=$2,
                   debt_due_date=$3, debt_approved_at=NOW() WHERE id=$1
        """, order_id, responsible_id, due)
        await conn.execute(
            "UPDATE route_orders SET stop_status='done' WHERE order_id=$1 AND stop_status='pending'",
            order_id)
        resp_name = await conn.fetchval(
            "SELECT COALESCE(last_name||' '||first_name, login) FROM staff WHERE id=$1", responsible_id)
        await conn.execute(
            "INSERT INTO order_status_history(order_num, new_status, note) "
            "SELECT order_num,'delivered','Закрыт с долгом · '||$2||' (отв: '||$3||')' FROM orders WHERE id=$1",
            order_id, by_name, resp_name or "")
    return True

async def get_orders_with_debt() -> list:
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT o.id, o.order_num, o.status, o.debt_due_date, o.debt_approved_at,
                   o.client_first_name, o.client_last_name, o.client_phone,
                   TRIM(COALESCE(sr.last_name,'') || ' ' || COALESCE(sr.first_name,'')) AS responsible_name,
                   sr.id AS responsible_id,
                   GREATEST(0,
                     -- Сумма к оплате всегда округляется вниз до 1000 (остаток < 1000 не
                     -- входит в долг) — та же формула, что в staff.html (_drvStopCard) и
                     -- прод-версии. Портировано из прода 2026-08-07.
                     (net.net_raw - MOD(net.net_raw, 1000))
                     - COALESCE((SELECT SUM(amount) FROM order_payments
                                  WHERE order_id=o.id
                                    AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL)),0)
                   ) AS debt_amount
              FROM orders o
              LEFT JOIN staff sr ON sr.id = o.debt_responsible_id
              CROSS JOIN LATERAL (
                SELECT GREATEST(0,
                  COALESCE(NULLIF((SELECT SUM(COALESCE(price_per_sqm,0)*COALESCE(sqm,0))
                                    FROM order_items WHERE order_id=o.id), 0),
                           COALESCE(o.total_price,0), 0)
                  - COALESCE(o.discount_sum,0)
                  - COALESCE(o.delivery_discount,0) - COALESCE(o.manual_discount,0)
                ) AS net_raw
              ) net
             WHERE o.debt_responsible_id IS NOT NULL AND o.company_id=$1
             ORDER BY o.debt_due_date ASC NULLS LAST, o.id DESC
        """, cid)
        return [r for r in [dict(r) for r in rows] if r["debt_amount"] > 0]

async def extend_debt_due_date(order_id: int, new_due_date_str: str, note: str = '') -> bool:
    if not pool: return False
    from datetime import date as _date
    try:
        new_due = _date.fromisoformat(new_due_date_str)
    except Exception:
        return False
    async with pool.acquire() as conn:
        res = await conn.execute(
            "UPDATE orders SET debt_due_date=$2 WHERE id=$1 AND debt_responsible_id IS NOT NULL",
            order_id, new_due
        )
        if res == "UPDATE 1":
            suffix = f": {note}" if note else ""
            await conn.execute(
                "INSERT INTO order_status_history(order_num, new_status, note) "
                "SELECT order_num, 'debt_extended', $2 FROM orders WHERE id=$1",
                order_id, f"Срок долга продлён до {new_due_date_str}{suffix}"
            )
        return res == "UPDATE 1"

# ── discount_requests ─────────────────────────────────────────────────────────

async def create_discount_request(order_id: int, order_num: str, driver_tg_id: int, requested_amount: float) -> dict | None:
    if not pool: return None
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO discount_requests(order_id, order_num, driver_tg_id, requested_amount, company_id)
            VALUES($1,$2,$3,$4,$5) RETURNING *
        """, order_id, order_num, driver_tg_id, requested_amount, cid)
        return dict(row) if row else None

async def get_pending_discount_requests() -> list:
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT dr.*, o.client_first_name, o.client_last_name,
                   COALESCE(o.total_price,0) AS order_total
            FROM discount_requests dr
            LEFT JOIN orders o ON o.id = dr.order_id
            WHERE dr.status='pending' AND dr.company_id=$1
            ORDER BY dr.created_at ASC
        """, cid)
        return [dict(r) for r in rows]

async def resolve_discount_request(request_id: int, approved_amount: float, resolved_by: int) -> dict | None:
    if not pool: return None
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE discount_requests
               SET status='approved', approved_amount=$2, resolved_by=$3, resolved_at=NOW()
             WHERE id=$1 AND status='pending' AND company_id=$4
            RETURNING *
        """, request_id, approved_amount, resolved_by, cid)
        if not row:
            return None
        r = dict(row)
        await conn.execute("""
            UPDATE orders SET manual_discount = COALESCE(manual_discount,0) + $2 WHERE id=$1 AND company_id=$3
        """, r["order_id"], approved_amount, cid)
        return r

async def reject_discount_request(request_id: int, resolved_by: int) -> dict | None:
    if not pool: return None
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE discount_requests
               SET status='rejected', resolved_by=$2, resolved_at=NOW()
             WHERE id=$1 AND status='pending' AND company_id=$3
            RETURNING *
        """, request_id, resolved_by, cid)
        return dict(row) if row else None


# ── Портировано из прод-бота (миграция artez_bot → SaaS) ─────────────────────
# Функции ниже принимают company_id явным параметром (не через _cid()), т.к.
# вызываются напрямую из бота — отдельного процесса вне FastAPI request-контекста.

async def get_client_lang(tg_id: int, company_id: int) -> str | None:
    """Сохранённый язык клиента бота ('ru'/'uz') или None, если клиент не найден."""
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT lang FROM crm_clients WHERE tg_id=$1 AND company_id=$2", tg_id, company_id)
    return row["lang"] if row else None

async def set_client_lang(tg_id: int, lang: str, company_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE crm_clients SET lang=$1, updated_at=NOW() WHERE tg_id=$2 AND company_id=$3",
            lang, tg_id, company_id)

async def apply_auto_discount(order_id: int, amount: float, company_id: int) -> bool:
    """Применить авто-скидку (округление суммы) к заказу без ручного согласования."""
    if not pool: return False
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE orders SET manual_discount = COALESCE(manual_discount,0) + $2 WHERE id=$1 AND company_id=$3",
            order_id, amount, company_id)
    return True

async def get_live_promo_id_for_user(user_id: int, company_id: int) -> int | None:
    """Живое (не истёкшее, не использованное) окно акции пользователя — для тега лида.
    Не потребляет окно (used_order_id не трогаем — это делает реальный заказ)."""
    if not pool or not user_id: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT pus.promotion_id
            FROM promo_user_state pus
            JOIN promotions p ON p.id = pus.promotion_id
            WHERE pus.user_id = $1 AND pus.company_id = $2 AND pus.used_order_id IS NULL
              AND pus.expires_at > NOW() AND p.is_active = TRUE
            ORDER BY pus.created_at DESC LIMIT 1
        """, user_id, company_id)
        return row["promotion_id"] if row else None

async def set_lead_promo(lead_num: str, promo_id: int, company_id: int) -> None:
    """Помечает лид принадлежностью к акции (тег для сотрудников)."""
    if not pool or not lead_num or not promo_id: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE leads SET promo_id=$1, updated_at=NOW() WHERE lead_num=$2 AND company_id=$3",
            promo_id, lead_num, company_id)

async def get_managers_with_push(company_id: int) -> list:
    """Менеджеры и админы компании с tg_id — цели пуш-уведомлений о скидках/долгах."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, first_name, last_name, tg_id
            FROM staff
            WHERE role IN ('admin','manager') AND active=TRUE
              AND tg_id IS NOT NULL AND company_id=$1
        """, company_id)
        return [dict(r) for r in rows]

# ── Бот заказов: быстрый заказ (order_bot_handlers.py) ──────────────────────
# clients — собственная таблица бота (НЕ crm_clients, это отдельная общая CRM-
# таблица для админки). company_id передаётся явным параметром, как и выше.

async def upsert_bot_client(tg_id: int, company_id: int, username: str | None,
                             first_name: str | None, last_name: str | None,
                             phone: str | None = None, lang: str = "ru",
                             tg_phone: str | None = None) -> None:
    """Создаёт/обновляет клиента бота. Без ON CONFLICT на конкретный constraint —
    прод-таблица clients (наследие single-tenant artez_bot) имеет ГЛОБАЛЬНЫЙ
    UNIQUE(tg_id), а не UNIQUE(company_id, tg_id) — см. комментарий в create_tables().

    phone — обычный контактный номер (для связи, может быть введён вручную —
    клиент мог указать номер, который не принадлежит ему). tg_phone — ВЕРИФИЦИРОВАННЫЙ
    номер: передаётся ТОЛЬКО когда клиент реально поделился СВОИМ контактом через
    Telegram (проверено по contact.user_id == from_user.id), никогда при ручном
    вводе. Только tg_phone можно использовать как ключ поиска чужих данных
    («Статус заказа») — иначе любой мог бы вписать номер другого человека и
    увидеть его заказы (см. security-фикс 2026-07-29)."""
    if not pool: return
    async with pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT id FROM clients WHERE tg_id=$1 AND company_id=$2", tg_id, company_id)
        if existing:
            await conn.execute("""
                UPDATE clients SET tg_username=$1, first_name=$2, last_name=$3, lang=$4,
                       phone=COALESCE($5, phone), tg_phone=COALESCE($6, tg_phone), updated_at=NOW()
                WHERE tg_id=$7 AND company_id=$8
            """, username, first_name, last_name, lang, phone, tg_phone, tg_id, company_id)
        else:
            await conn.execute("""
                INSERT INTO clients (tg_id, company_id, tg_username, first_name, last_name, phone, tg_phone, lang, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
            """, tg_id, company_id, username, first_name, last_name, phone, tg_phone, lang)

async def get_bot_client_by_tg_id(tg_id: int, company_id: int) -> dict | None:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM clients WHERE tg_id=$1 AND company_id=$2", tg_id, company_id)
        return dict(row) if row else None

async def get_bot_client_lang(tg_id: int, company_id: int) -> str | None:
    """Сохранённый язык клиента бота ('ru'/'uz') или None, если клиент не найден."""
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT lang FROM clients WHERE tg_id=$1 AND company_id=$2", tg_id, company_id)
        return row["lang"] if row else None

async def set_bot_client_lang(tg_id: int, lang: str, company_id: int) -> None:
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE clients SET lang=$1, updated_at=NOW() WHERE tg_id=$2 AND company_id=$3",
            lang, tg_id, company_id)

async def mark_welcome_video_sent(tg_id: int, company_id: int) -> bool:
    """Атомарно помечает, что видео-инструкция отправлена этому клиенту этой
    компании. Возвращает True только если флаг реально был снят (т.е. это
    первый раз) — чтобы не слать видео повторно при каждом /start."""
    if not pool: return False
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE clients SET welcome_video_sent=TRUE "
            "WHERE tg_id=$1 AND company_id=$2 AND COALESCE(welcome_video_sent,FALSE)=FALSE RETURNING id",
            tg_id, company_id
        )
        return row is not None

async def get_services_for_company(company_id: int) -> list[dict]:
    """Список услуг компании (для клавиатуры выбора услуги в боте)."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM services WHERE company_id=$1 ORDER BY order_idx, key", company_id)
        return [dict(r) for r in rows]

async def get_client_orders_by_tg(tg_id: int, company_id: int) -> list[dict]:
    """Заказы клиента бота заказов (order_bot_handlers.py) по client_tg_id — для
    раздела «Мой профиль» (статистика всего/выполнено). Явный company_id (вебхук
    общий на все компании), от новых к старым."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM orders WHERE client_tg_id=$1 AND company_id=$2 ORDER BY created_at DESC",
            tg_id, company_id)
        return [dict(r) for r in rows]

async def get_client_leads_by_tg(tg_id: int, company_id: int) -> list[dict]:
    """Лиды клиента бота заказов по client_tg_id — раздел «Статус заказа».
    Бот всегда создаёт лид (не заказ напрямую), поэтому «новые» заявки клиента
    ищутся здесь, а не в orders. Явный company_id (вебхук общий на все компании)."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM leads WHERE client_tg_id=$1 AND company_id=$2 ORDER BY created_at DESC",
            tg_id, company_id)
        return [dict(r) for r in rows]

async def get_orders_by_phones(phones: list[str], company_id: int) -> list[dict]:
    """Заказы клиента по номерам телефона — «Статус заказа» ищет заказы не
    только по client_tg_id (сотрудник мог создать заказ вручную/конвертировать
    лид в заказ, не сохранив client_tg_id), а по ЛЮБОМУ телефону, который клиент
    когда-либо указывал (свой профильный + все из его лидов — при конвертации
    лида в заказ телефон копируется из лида, но сам лид не привязывается к
    получившемуся order_num, см. convert_lead_to_order в main.py)."""
    if not pool or not phones: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM orders WHERE client_phone = ANY($1) AND company_id=$2 ORDER BY created_at DESC",
            phones, company_id)
        return [dict(r) for r in rows]

async def get_staff_by_role(company_id: int, role: str) -> list:
    """Активные сотрудники компании с указанной ролью (для группового роутинга)."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM staff WHERE company_id=$1 AND role=$2 AND active=TRUE ORDER BY first_name",
            company_id, role)
        return [dict(r) for r in rows]

async def take_lead(lead_id: int, staff_id: int, staff_name: str, company_id: int):
    """Назначает лид на сотрудника (кнопка «Взять лид» в боте).
    Возвращает (status, taker_name, taker_verb): status один из
    'ok'|'already_mine'|'taken'|'not_found'."""
    if not pool: return ('error', '', '')
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT assigned_to, lead_num FROM leads WHERE id=$1 AND company_id=$2", lead_id, company_id)
        if not row:
            return ('not_found', '', '')
        if row['assigned_to'] and row['assigned_to'] != staff_id:
            taker = await conn.fetchrow(
                "SELECT first_name, last_name, gender FROM staff WHERE id=$1", row['assigned_to'])
            taker_name = (f"{taker['first_name'] or ''} {taker['last_name'] or ''}".strip()
                          if taker else 'другой сотрудник')
            taker_verb = 'Взяла' if taker and taker.get('gender') == 'F' else 'Взял'
            return ('taken', taker_name, taker_verb)
        if row['assigned_to'] == staff_id:
            return ('already_mine', '', '')
        await conn.execute(
            "UPDATE leads SET assigned_to=$1, updated_at=NOW() WHERE id=$2 AND company_id=$3",
            staff_id, lead_id, company_id)
        try:
            await conn.execute("""
                INSERT INTO lead_calls (lead_id, operator_id, action, note, created_at)
                VALUES ($1,$2,'note',$3,NOW())
            """, lead_id, staff_id, f"Лид взят через Telegram: {staff_name}")
        except Exception:
            pass
        return ('ok', '', '')

async def get_stats(company_id: int, branch: str = None) -> dict:
    """Статистика заказов компании для админ-команды /stats бота."""
    if not pool: return {}
    async with pool.acquire() as conn:
        if branch:
            row = await conn.fetchrow("""
                SELECT
                    COUNT(*) FILTER (WHERE status='new')       AS new_count,
                    COUNT(*) FILTER (WHERE status='delivered') AS done_count,
                    COUNT(*) FILTER (WHERE status='cancelled') AS cancel_count,
                    COUNT(*)                                    AS total
                FROM orders WHERE company_id=$1 AND branch=$2
            """, company_id, branch)
        else:
            row = await conn.fetchrow("""
                SELECT
                    COUNT(*) FILTER (WHERE status='new')       AS new_count,
                    COUNT(*) FILTER (WHERE status='delivered') AS done_count,
                    COUNT(*) FILTER (WHERE status='cancelled') AS cancel_count,
                    COUNT(*)                                    AS total
                FROM orders WHERE company_id=$1
            """, company_id)
        return dict(row) if row else {}


# ── Долговые одобрения ────────────────────────────────────────────────────────

async def create_debt_approval_request(order_id: int, order_num: str, driver_tg_id: int,
                                        debt_amount: float, mgr_msgs_json: str = '{}') -> dict | None:
    if not pool: return None
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO debt_approval_requests(order_id, order_num, driver_tg_id, debt_amount, mgr_msgs, company_id)
            VALUES($1,$2,$3,$4,$5::jsonb,$6)
            RETURNING id, order_id, order_num, debt_amount, status
        """, order_id, order_num, driver_tg_id, debt_amount, mgr_msgs_json, cid)
        return dict(row) if row else None

async def get_pending_debt_approvals() -> list:
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT dar.id, dar.order_id, dar.order_num, dar.driver_tg_id,
                   dar.debt_amount, dar.mgr_msgs, dar.created_at,
                   o.client_first_name, o.client_last_name, o.client_phone,
                   o.address, o.short_address, o.location, o.location_address,
                   COALESCE((SELECT SUM(COALESCE(sqm*price_per_sqm,0)) FROM order_items WHERE order_id=o.id),
                            COALESCE(o.total_price,0)) AS order_total,
                   COALESCE(o.discount_sum, 0) + COALESCE(o.delivery_discount, 0)
                       + COALESCE(o.manual_discount, 0) AS total_discount,
                   COALESCE((SELECT SUM(amount) FROM order_payments
                              WHERE order_id = o.id
                                AND ((method='cash' AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL))
                                     OR (method<>'cash' AND confirmed=TRUE))), 0) AS paid_amount,
                   COALESCE((SELECT COUNT(*) FROM order_items WHERE order_id = o.id), 0)::int AS item_count
            FROM debt_approval_requests dar
            LEFT JOIN orders o ON o.id = dar.order_id
            WHERE dar.status = 'pending' AND dar.company_id=$1
            ORDER BY dar.created_at ASC
        """, cid)
        return [dict(r) for r in rows]

async def get_order_channel_info(order_id: int) -> dict | None:
    """Возвращает channel_id, msg_id и данные стопа для обновления канального сообщения."""
    import json as _j
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT r.id AS route_id, r.branch, r.tg_delivery_msg_ids,
                   ro.sort_order,
                   o.order_num, o.client_first_name, o.client_last_name, o.client_phone,
                   o.address, o.short_address,
                   COALESCE((SELECT SUM(COALESCE(price_per_sqm,0)*COALESCE(sqm,0))
                              FROM order_items WHERE order_id=o.id),
                            COALESCE(o.total_price,0)) AS items_total,
                   COALESCE(o.discount_sum,0) AS discount_sum,
                   COALESCE(o.delivery_discount,0) AS delivery_discount,
                   COALESCE(o.manual_discount,0) AS manual_discount,
                   COALESCE((SELECT COUNT(*) FROM order_items WHERE order_id=o.id),0)::int AS item_count,
                   COALESCE((SELECT SUM(amount) FROM order_payments
                              WHERE order_id=o.id
                                AND ((method='cash' AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL))
                                     OR (method<>'cash' AND confirmed=TRUE))),0) AS paid_amount
            FROM route_orders ro
            JOIN routes r ON r.id = ro.route_id
            JOIN orders o ON o.id = ro.order_id
            WHERE ro.order_id = $1
            ORDER BY r.created_at DESC LIMIT 1
        """, order_id)
        if not row: return None
        d = dict(row)
        raw = d.get("tg_delivery_msg_ids") or "{}"
        try: msg_ids = _j.loads(raw) if isinstance(raw, str) else (raw or {})
        except: msg_ids = {}
        d["msg_id"] = msg_ids.get(str(order_id))
        stored_ch = msg_ids.get("__channel__")
        if stored_ch:
            d["channel_id"] = int(stored_ch)
        else:
            branch = d.get("branch", "")
            key_ch = "delivery_channel_navoi_id" if branch == "navoi" else "delivery_channel_zarafshan_id"
            d["channel_id"] = 0
            cfg = await conn.fetchrow("SELECT value FROM config WHERE key=$1", key_ch)
            if cfg and cfg["value"]:
                d["channel_id"] = int(cfg["value"])
        num_row = await conn.fetchrow(
            "SELECT COUNT(*)+1 AS num FROM route_orders WHERE route_id=$1 AND sort_order < $2",
            d["route_id"], d["sort_order"])
        d["stop_num"] = int(num_row["num"]) if num_row else 1
        return d

async def resolve_debt_approval(request_id: int, resolution: str, resolved_by: int,
                                 responsible_id: int | None = None) -> dict | None:
    if not pool: return None
    from datetime import date, timedelta
    cid = _cid()
    async with pool.acquire() as conn:
        if responsible_id is not None:
            owns = await conn.fetchval(
                "SELECT 1 FROM staff WHERE id=$1 AND company_id=$2", responsible_id, cid)
            if not owns:
                return None
        row = await conn.fetchrow("""
            UPDATE debt_approval_requests
            SET status=$2, resolution=$3, resolved_by=$4, responsible_id=$5, resolved_at=NOW()
            WHERE id=$1 AND status='pending' AND company_id=$6
            RETURNING *
        """, request_id, resolution, resolution, resolved_by, responsible_id, cid)
        if not row: return None
        r = dict(row)
        if resolution == 'approved' and responsible_id:
            due = date.today() + timedelta(days=7)
            await conn.execute("""
                UPDATE orders SET status='delivered', debt_responsible_id=$2,
                       debt_due_date=$3, debt_approved_at=NOW() WHERE id=$1
            """, r['order_id'], responsible_id, due)
            await conn.execute(
                "UPDATE route_orders SET stop_status='done' WHERE order_id=$1 AND stop_status='pending'",
                r['order_id'])
        return r

async def get_routes_today(company_id: int, branch: str | None = None) -> list:
    # company_id ОБЯЗАТЕЛЕН — раньше запрос фильтровался только по дате и
    # (опционально) branch, БЕЗ company_id вообще: водитель без указанного
    # branch увидел бы маршруты/заказы/телефоны клиентов ВСЕХ компаний SaaS
    # за сегодня; даже с branch — две компании с одинаковым названием филиала
    # смешались бы (см. security-фикс 2026-07-29, тот же класс IDOR).
    if not pool: return []
    async with pool.acquire() as conn:
        where_clause = "WHERE r.company_id = $1 AND r.date = CURRENT_DATE AND r.status != 'cancelled'"
        vals = [company_id]
        if branch:
            where_clause += " AND r.branch = $2"
            vals.append(branch)
        rows = await conn.fetch(f"""
            SELECT r.id AS route_id, r.name, r.date::text, r.type, r.status AS route_status, r.branch,
                   TRIM(COALESCE(s.first_name,'') || ' ' || COALESCE(s.last_name,'')) AS driver_name,
                   ro.sort_order, ro.stop_status, ro.driver_confirmed,
                   o.id AS order_id, o.order_num, o.status AS order_status,
                   o.client_first_name, o.client_last_name, o.client_phone,
                   o.address, o.short_address, o.location, o.location_address,
                   COALESCE((SELECT SUM(COALESCE(sqm*price_per_sqm,0)) FROM order_items WHERE order_id=o.id),
                            COALESCE(o.total_price,0)) AS items_total,
                   COALESCE(o.discount_sum,0)+COALESCE(o.delivery_discount,0)+COALESCE(o.manual_discount,0) AS total_discount,
                   COALESCE((SELECT SUM(amount) FROM order_payments WHERE order_id=o.id
                              AND ((method='cash' AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL))
                                   OR (method<>'cash' AND confirmed=TRUE))), 0) AS paid_amount,
                   COALESCE((SELECT COUNT(*) FROM order_items WHERE order_id=o.id),0)::int AS item_count
            FROM routes r
            LEFT JOIN staff s ON s.id = r.driver_id
            JOIN route_orders ro ON ro.route_id = r.id
            JOIN orders o ON o.id = ro.order_id
            {where_clause}
            ORDER BY r.id, ro.sort_order
        """, *vals)
        routes: dict = {}
        for row in rows:
            rid = row["route_id"]
            if rid not in routes:
                routes[rid] = {"id": rid, "name": row["name"], "date": row["date"],
                               "type": row["type"], "status": row["route_status"],
                               "branch": row["branch"],
                               "driver_name": row["driver_name"] or None,
                               "stops": []}
            routes[rid]["stops"].append({
                "order_id":       row["order_id"],
                "order_num":      row["order_num"],
                "sort_order":     row["sort_order"],
                "stop_status":    row["stop_status"],
                "driver_confirmed": bool(row["driver_confirmed"]),
                "order_status":   row["order_status"],
                "client_first_name": row["client_first_name"],
                "client_last_name":  row.get("client_last_name"),
                "client_phone":   row.get("client_phone"),
                "address":        row.get("address"),
                "short_address":  row.get("short_address"),
                "location":       row.get("location"),
                "location_address": row.get("location_address"),
                "items_total":    float(row["items_total"]),
                "total_discount": float(row["total_discount"]),
                "paid_amount":    float(row["paid_amount"]),
                "item_count":     row["item_count"],
            })
        return list(routes.values())

async def driver_set_stop_status(order_id: int, status: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE route_orders SET stop_status=$2 WHERE order_id=$1 AND stop_status!='done'",
            order_id, status)

async def driver_set_confirmed(order_id: int, confirmed: bool = True):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE route_orders SET driver_confirmed=$2 WHERE order_id=$1", order_id, confirmed)

async def driver_update_order_status(order_id: int, new_status: str, staff_id: int, staff_name: str, note: str = ""):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("UPDATE orders SET status=$2, updated_at=NOW() WHERE id=$1", order_id, new_status)
        await conn.execute(
            "INSERT INTO order_activity (order_id, staff_id, staff_name, action, details) VALUES ($1,$2,$3,$4,$5)",
            order_id, staff_id, staff_name, f"status_{new_status}", note or f"Статус → {new_status}")


# ── Расходы ───────────────────────────────────────────────────────────────────

# ── Seed данные: 2-уровневая структура ───────────────────────────────────────
_EXPENSE_CAT_PARENTS = [
    # (name_ru, name_uz, icon, sort_order)  — имя БЕЗ эмодзи, эмодзи только в icon
    ("Транспорт",        "Transport",          "🚗", 10),
    ("Коммунальные",     "Kommunal",           "💡", 20),
    ("Персонал",         "Xodimlar",           "👷", 30),
    ("Химия/Материалы",  "Kimyo/Materiallar",  "🧴", 40),
    ("Закупки/Склад",    "Xarid/Ombor",        "📦", 50),
    ("Офис",             "Ofis",               "🏢", 60),
    ("Обслуживание",     "Texnik xizmat",      "🔧", 70),
    ("Маркетинг",        "Marketing",          "📣", 80),
    ("Финансы",          "Moliya",             "🏦", 90),
    ("Прочее",           "Boshqalar",          "❓", 100),
]

# (parent_name_ru, [(name_ru, name_uz, icon, approve_level, receipt_required, amount_threshold, sort_order)])
_EXPENSE_CAT_CHILDREN = [
    ("Транспорт", [
        ("Топливо",         "Yoqilg'i",       "⛽", "manager", True,  None, 1),
        ("Ремонт авто",     "Avto ta'miri",   "🔧", "both",    True,  None, 2),
        ("Парковка",        "Parkovka",       "🅿️","manager", False, None, 3),
    ]),
    ("Коммунальные", [
        ("Электричество",   "Elektr",         "💡", "admin",   True,  None, 1),
        ("Вода",            "Suv",            "💧", "admin",   True,  None, 2),
        ("Интернет",        "Internet",       "🌐", "admin",   True,  None, 3),
        ("Газ",             "Gaz",            "🔥", "admin",   True,  None, 4),
    ]),
    ("Персонал", [
        ("Зарплата",        "Maosh",          "💰", "admin",   False, None, 1),
        ("Аванс",           "Avans",          "💸", "both",    False, None, 2),
        ("Питание",         "Ovqat",          "🍽", "manager", False, None, 3),
        ("Медицина",        "Tibbiyot",       "🏥", "both",    True,  None, 4),
    ]),
    ("Химия/Материалы", [
        ("Бытовая химия",   "Kimyo",          "🧴", "manager", True,  None, 1),
        ("Инвентарь",       "Inventar",       "🪣", "manager", False, None, 2),
    ]),
    ("Закупки/Склад", [
        ("Упаковка",        "Qadoqlash",      "📦", "manager", False, None, 1),
        ("Прочие закупки",  "Boshqa xaridlar","🛒", "both",    True,  None, 2),
    ]),
    ("Офис", [
        ("Канцтовары",      "Kantselyariya",  "📎", "manager", False, None, 1),
        ("Продукты",        "Oziq-ovqat",     "☕", "manager", False, None, 2),
        ("Связь/SIM",       "Aloqa/SIM",      "📱", "manager", True,  None, 3),
        ("Аренда",          "Ijara",          "🏢", "admin",   True,  None, 4),
    ]),
    ("Обслуживание", [
        ("Ремонт обор-я",   "Jihoz ta'miri",  "🔩", "both",    True,  None, 1),
        ("Уборка помещ.",   "Xona tozalash",  "🧹", "manager", False, None, 2),
    ]),
    ("Маркетинг", [
        ("Реклама",         "Reklama",        "📣", "both",    True,  None, 1),
        ("Представит.",     "Vakillik",       "🎁", "admin",   True,  None, 2),
    ]),
    ("Финансы", [
        ("Инкассация",      "Inkassatsiya",   "🏦", "admin",   True,  None, 1),
    ]),
    ("Прочее", [
        ("Прочее",             "Boshqalar",             "❓", "admin",   True,  None, 1),
    ]),
]

async def ensure_expense_tables():
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS expense_categories (
                id               SERIAL PRIMARY KEY,
                name_ru          TEXT NOT NULL,
                name_uz          TEXT NOT NULL,
                icon             TEXT DEFAULT '',
                parent_id        INT,
                approve_level    TEXT NOT NULL DEFAULT 'manager',
                receipt_required BOOLEAN NOT NULL DEFAULT FALSE,
                amount_threshold NUMERIC,
                sort_order       INT DEFAULT 0,
                active           BOOLEAN NOT NULL DEFAULT TRUE
            )
        """)
        # Миграция: добавить parent_id если таблица уже существует без него
        await conn.execute("""
            ALTER TABLE expense_categories ADD COLUMN IF NOT EXISTS parent_id INT
        """)
        try:
            await conn.execute("""
                ALTER TABLE expense_categories ADD CONSTRAINT fk_exp_cat_parent
                FOREIGN KEY (parent_id) REFERENCES expense_categories(id) ON DELETE SET NULL
            """)
        except Exception:
            pass  # constraint уже есть

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS expenses (
                id                   SERIAL PRIMARY KEY,
                category_id          INT REFERENCES expense_categories(id),
                amount               NUMERIC NOT NULL,
                description          TEXT DEFAULT '',
                created_by_staff_id  INT REFERENCES staff(id),
                branch               TEXT DEFAULT '',
                status               TEXT NOT NULL DEFAULT 'pending',
                manager_id           INT REFERENCES staff(id),
                manager_at           TIMESTAMPTZ,
                admin_id             INT REFERENCES staff(id),
                admin_at             TIMESTAMPTZ,
                reject_reason        TEXT DEFAULT '',
                receipt_url          TEXT DEFAULT '',
                paid_from            TEXT DEFAULT 'cash',
                created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        # Сеем 2-уровневую структуру если подкатегорий ещё нет
        has_children = await conn.fetchval(
            "SELECT COUNT(*) FROM expense_categories WHERE parent_id IS NOT NULL")
        if has_children == 0:
            # Очищаем старые плоские категории (расходов в production пока нет)
            await conn.execute("TRUNCATE expenses RESTART IDENTITY")
            await conn.execute("TRUNCATE expense_categories RESTART IDENTITY CASCADE")
            # Сеем родителей
            for (name_ru, name_uz, icon, sort_order) in _EXPENSE_CAT_PARENTS:
                await conn.execute(
                    "INSERT INTO expense_categories (name_ru, name_uz, icon, sort_order) VALUES ($1,$2,$3,$4)",
                    name_ru, name_uz, icon, sort_order)
            # Сеем детей
            for (parent_name, children) in _EXPENSE_CAT_CHILDREN:
                pid = await conn.fetchval(
                    "SELECT id FROM expense_categories WHERE name_ru=$1", parent_name)
                if not pid:
                    continue
                for (nm_ru, nm_uz, icon, approve_level, receipt_req, threshold, sord) in children:
                    await conn.execute("""
                        INSERT INTO expense_categories
                            (name_ru, name_uz, icon, parent_id, approve_level, receipt_required, amount_threshold, sort_order)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    """, nm_ru, nm_uz, icon, pid, approve_level, receipt_req, threshold, sord)
        else:
            # Миграция: убираем дублирующий эмодзи из начала name_ru/name_uz
            # если name_ru начинается с icon + пробел (например "⛽ Топливо" → "Топливо")
            await conn.execute("""
                UPDATE expense_categories
                SET name_ru = TRIM(SUBSTRING(name_ru FROM LENGTH(icon) + 2)),
                    name_uz = TRIM(SUBSTRING(name_uz FROM LENGTH(icon) + 2))
                WHERE icon != ''
                  AND LENGTH(name_ru) > LENGTH(icon) + 1
                  AND SUBSTRING(name_ru FROM 1 FOR LENGTH(icon)) = icon
                  AND SUBSTRING(name_ru FROM LENGTH(icon) + 1 FOR 1) = ' '
            """)

async def get_expense_categories_tree() -> list:
    """Возвращает дерево: родители со списком children."""
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM expense_categories WHERE active=TRUE AND company_id=$1 ORDER BY sort_order, id", cid)
        cats = [dict(r) for r in rows]
    parents = [c for c in cats if not c['parent_id']]
    ch_map: dict = {}
    for c in cats:
        if c['parent_id']:
            ch_map.setdefault(c['parent_id'], []).append(c)
    for p in parents:
        p['children'] = ch_map.get(p['id'], [])
    return parents

async def get_expense_categories() -> list:
    """Плоский список всех активных категорий (для обратной совместимости)."""
    if not pool: return []
    cid = _cid()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM expense_categories WHERE active=TRUE AND company_id=$1 ORDER BY sort_order, id", cid)
        return [dict(r) for r in rows]

async def create_expense_category(name_ru: str, name_uz: str, icon: str,
                                   parent_id, approve_level: str,
                                   receipt_required: bool, amount_threshold,
                                   sort_order: int, for_staff: bool = False) -> dict:
    if not pool: return {}
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO expense_categories
                (name_ru, name_uz, icon, parent_id, approve_level, receipt_required, amount_threshold, sort_order, for_staff, company_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *
        """, name_ru, name_uz, icon, parent_id, approve_level,
             receipt_required, amount_threshold, sort_order, for_staff, cid)
        return dict(row) if row else {}

async def update_expense_category(cat_id: int, name_ru: str, name_uz: str, icon: str,
                                   parent_id, approve_level: str,
                                   receipt_required: bool, amount_threshold,
                                   sort_order: int, active: bool, for_staff: bool = False) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE expense_categories
            SET name_ru=$2, name_uz=$3, icon=$4, parent_id=$5,
                approve_level=$6, receipt_required=$7, amount_threshold=$8,
                sort_order=$9, active=$10, for_staff=$11
            WHERE id=$1 RETURNING *
        """, cat_id, name_ru, name_uz, icon, parent_id, approve_level,
             receipt_required, amount_threshold, sort_order, active, for_staff)
        return dict(row) if row else {}

async def delete_expense_category(cat_id: int) -> dict:
    if not pool: return {"ok": False, "error": "no pool"}
    async with pool.acquire() as conn:
        has_expenses = await conn.fetchval(
            "SELECT COUNT(*) FROM expenses WHERE category_id=$1", cat_id)
        if has_expenses:
            return {"ok": False, "error": "has_expenses"}
        has_children = await conn.fetchval(
            "SELECT COUNT(*) FROM expense_categories WHERE parent_id=$1", cat_id)
        if has_children:
            return {"ok": False, "error": "has_children"}
        await conn.execute("DELETE FROM expense_categories WHERE id=$1", cat_id)
        return {"ok": True}

# ── Superadmin: expense_categories catalog (company_id=0) ─────────────
async def get_expense_categories_for_company(company_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM expense_categories WHERE company_id=$1 ORDER BY sort_order, id", company_id)
        return [dict(r) for r in rows]

async def create_expense_category_for_company(company_id: int, name_ru: str, name_uz: str,
        icon: str, parent_id, approve_level: str, receipt_required: bool,
        amount_threshold, sort_order: int, for_staff: bool = False) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO expense_categories
                (name_ru, name_uz, icon, parent_id, approve_level, receipt_required, amount_threshold, sort_order, for_staff, company_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *
        """, name_ru, name_uz, icon, parent_id, approve_level,
             receipt_required, amount_threshold, sort_order, for_staff, company_id)
        return dict(row) if row else {}

async def update_expense_category_for_company(company_id: int, cat_id: int,
        name_ru: str, name_uz: str, icon: str, parent_id, approve_level: str,
        receipt_required: bool, amount_threshold, sort_order: int,
        active: bool, for_staff: bool = False) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE expense_categories
            SET name_ru=$3, name_uz=$4, icon=$5, parent_id=$6,
                approve_level=$7, receipt_required=$8, amount_threshold=$9,
                sort_order=$10, active=$11, for_staff=$12
            WHERE id=$1 AND company_id=$2 RETURNING *
        """, cat_id, company_id, name_ru, name_uz, icon, parent_id, approve_level,
             receipt_required, amount_threshold, sort_order, active, for_staff)
        return dict(row) if row else {}

async def delete_expense_category_for_company(company_id: int, cat_id: int) -> dict:
    if not pool: return {"ok": False}
    async with pool.acquire() as conn:
        has_children = await conn.fetchval(
            "SELECT COUNT(*) FROM expense_categories WHERE parent_id=$1 AND company_id=$2", cat_id, company_id)
        if has_children:
            return {"ok": False, "error": "has_children"}
        await conn.execute("DELETE FROM expense_categories WHERE id=$1 AND company_id=$2", cat_id, company_id)
        return {"ok": True}

async def seed_company_expense_categories(company_id: int, force: bool = False):
    if not pool: return
    async with pool.acquire() as conn:
        if not force:
            existing = await conn.fetchval(
                "SELECT COUNT(*) FROM expense_categories WHERE company_id=$1", company_id)
            if existing > 0:
                return
        template = await conn.fetch(
            "SELECT * FROM expense_categories WHERE company_id=0 ORDER BY sort_order, id")
        if not template:
            return
        if force:
            await conn.execute("DELETE FROM expense_categories WHERE company_id=$1", company_id)
        id_map: dict = {}
        for r in template:
            old_id = r['id']
            new_parent = id_map.get(r['parent_id']) if r['parent_id'] else None
            new_row = await conn.fetchrow("""
                INSERT INTO expense_categories
                    (name_ru, name_uz, icon, parent_id, approve_level, receipt_required,
                     amount_threshold, sort_order, for_staff, active, company_id)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id
            """, r['name_ru'], r['name_uz'], r['icon'], new_parent,
                 r['approve_level'], r['receipt_required'], r['amount_threshold'],
                 r['sort_order'], r['for_staff'], r['active'], company_id)
            if new_row:
                id_map[old_id] = new_row['id']

async def resync_expense_category_template_from_company1():
    """Пересинхронизировать шаблон расходов (company_id=0) из company_id=1."""
    if not pool: return
    async with pool.acquire() as conn:
        # NULLS FIRST гарантирует что родители вставятся раньше детей
        src = await conn.fetch(
            "SELECT * FROM expense_categories WHERE company_id=1 ORDER BY parent_id NULLS FIRST, sort_order, id")
        await conn.execute("DELETE FROM expense_categories WHERE company_id=0")
        id_map: dict = {}
        for r in src:
            new_parent = id_map.get(r['parent_id']) if r['parent_id'] else None
            new_row = await conn.fetchrow("""
                INSERT INTO expense_categories
                    (name_ru, name_uz, icon, parent_id, approve_level, receipt_required,
                     amount_threshold, sort_order, for_staff, active, company_id)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,0) RETURNING id
            """, r['name_ru'], r['name_uz'], r['icon'], new_parent,
                 r['approve_level'], r['receipt_required'], r['amount_threshold'],
                 r['sort_order'], r['for_staff'], r['active'])
            if new_row:
                id_map[r['id']] = new_row['id']

# ── Superadmin: chat_templates catalog (company_id=0) ─────────────────
async def get_chat_templates_for_company(company_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM chat_templates WHERE company_id=$1 ORDER BY lang, key, sort_order, id", company_id)
        return [dict(r) for r in rows]

async def upsert_chat_template_for_company(company_id: int, data: dict) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        tid = data.get('id')
        if tid:
            row = await conn.fetchrow(
                "UPDATE chat_templates SET key=$3,lang=$4,text=$5,sort_order=$6,active=$7 WHERE id=$1 AND company_id=$2 RETURNING *",
                tid, company_id, data['key'], data['lang'], data['text'],
                data.get('sort_order', 0), data.get('active', True)
            )
        else:
            row = await conn.fetchrow(
                "INSERT INTO chat_templates (key,lang,text,sort_order,active,company_id) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *",
                data['key'], data['lang'], data['text'], data.get('sort_order', 0),
                data.get('active', True), company_id
            )
        return dict(row) if row else {}

async def delete_chat_template_for_company(company_id: int, tid: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM chat_templates WHERE id=$1 AND company_id=$2", tid, company_id)

async def seed_chat_templates_for_company0():
    """Засеять шаблоны чата в company_id=0 (шаблон суперадмина)."""
    if not pool: return
    async with pool.acquire() as conn:
        for key, lang, text, sort_order in _CHAT_TEMPLATE_SEED:
            existing = await conn.fetchrow(
                "SELECT id FROM chat_templates WHERE key=$1 AND lang=$2 AND company_id=0", key, lang)
            if key == 'quick':
                existing_text = await conn.fetchval(
                    "SELECT id FROM chat_templates WHERE key='quick' AND lang=$1 AND text=$2 AND company_id=0",
                    lang, text)
                if not existing_text:
                    await conn.execute(
                        "INSERT INTO chat_templates (key,lang,text,sort_order,company_id) VALUES ($1,$2,$3,$4,0)",
                        key, lang, text, sort_order)
            else:
                if existing:
                    await conn.execute(
                        "UPDATE chat_templates SET text=$1, sort_order=$2 WHERE id=$3",
                        text, sort_order, existing['id'])
                else:
                    await conn.execute(
                        "INSERT INTO chat_templates (key,lang,text,sort_order,company_id) VALUES ($1,$2,$3,$4,0)",
                        key, lang, text, sort_order)

async def seed_company_chat_templates(company_id: int, force: bool = False):
    if not pool: return
    async with pool.acquire() as conn:
        if not force:
            existing = await conn.fetchval(
                "SELECT COUNT(*) FROM chat_templates WHERE company_id=$1", company_id)
            if existing > 0:
                return
        template = await conn.fetch(
            "SELECT key, lang, text, sort_order, active FROM chat_templates WHERE company_id=0")
        if not template:
            source = _CHAT_TEMPLATE_SEED
            await conn.executemany(
                "INSERT INTO chat_templates (key,lang,text,sort_order,company_id) VALUES ($1,$2,$3,$4,$5)",
                [(k, l, t, s, company_id) for k, l, t, s in source])
        else:
            await conn.executemany(
                "INSERT INTO chat_templates (key,lang,text,sort_order,active,company_id) VALUES ($1,$2,$3,$4,$5,$6)",
                [(r['key'], r['lang'], r['text'], r['sort_order'], r['active'], company_id) for r in template])

async def create_expense(category_id: int, amount: float, description: str,
                         staff_id: int, branch: str, for_staff_id: int = None) -> dict:
    if not pool: return {}
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO expenses (category_id, amount, description, created_by_staff_id, branch, for_staff_id, company_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *
        """, category_id, amount, description, staff_id, branch, for_staff_id, cid)
        return dict(row) if row else {}

async def get_expenses(branch: str = None, status: str = None,
                       category_id: int = None, limit: int = 100) -> list:
    if not pool: return []
    cid = _cid()
    filters, params = [f"e.company_id=${len([cid])}"], [cid]
    if branch:      filters.append(f"e.branch=${len(params)+1}");           params.append(branch)
    if status == 'paid':
        filters.append("e.status IN ('paid','approved')")
    elif status:
        filters.append(f"e.status=${len(params)+1}"); params.append(status)
    if category_id: filters.append(f"e.category_id=${len(params)+1}"); params.append(category_id)
    where = "WHERE " + " AND ".join(filters)
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT e.*,
                   ec.name_ru AS category_name_ru, ec.name_uz AS category_name_uz,
                   ec.icon AS category_icon, ec.approve_level, ec.receipt_required,
                   ep.name_ru AS parent_name_ru, ep.name_uz AS parent_name_uz, ep.icon AS parent_icon,
                   TRIM(COALESCE(sc.last_name,'') || ' ' || COALESCE(sc.first_name,'')) AS creator_name,
                   TRIM(COALESCE(sm.last_name,'') || ' ' || COALESCE(sm.first_name,'')) AS manager_name,
                   TRIM(COALESCE(sa.last_name,'') || ' ' || COALESCE(sa.first_name,'')) AS admin_name,
                   TRIM(COALESCE(sf.last_name,'') || ' ' || COALESCE(sf.first_name,'')) AS for_staff_name
            FROM expenses e
            LEFT JOIN expense_categories ec ON ec.id = e.category_id
            LEFT JOIN expense_categories ep ON ep.id = ec.parent_id
            LEFT JOIN staff sc ON sc.id = e.created_by_staff_id
            LEFT JOIN staff sm ON sm.id = e.manager_id
            LEFT JOIN staff sa ON sa.id = e.admin_id
            LEFT JOIN staff sf ON sf.id = e.for_staff_id
            {where}
            ORDER BY e.created_at DESC LIMIT {limit}
        """, *params)
        return [dict(r) for r in rows]

async def get_my_expenses(staff_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT e.*,
                   ec.name_ru AS category_name_ru, ec.name_uz AS category_name_uz,
                   ec.icon AS category_icon, ec.approve_level, ec.receipt_required,
                   ep.name_ru AS parent_name_ru, ep.name_uz AS parent_name_uz
            FROM expenses e
            LEFT JOIN expense_categories ec ON ec.id = e.category_id
            LEFT JOIN expense_categories ep ON ep.id = ec.parent_id
            WHERE e.created_by_staff_id = $1
            ORDER BY e.created_at DESC LIMIT 50
        """, staff_id)
        return [dict(r) for r in rows]

async def get_pending_expenses_for_manager(branch: str = None) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        cond = "AND e.branch=$1" if branch else ""
        params = [branch] if branch else []
        rows = await conn.fetch(f"""
            SELECT e.*,
                   ec.name_ru AS category_name_ru, ec.name_uz AS category_name_uz,
                   ec.icon AS category_icon, ec.approve_level, ec.receipt_required,
                   ep.name_ru AS parent_name_ru, ep.name_uz AS parent_name_uz,
                   TRIM(COALESCE(sc.last_name,'') || ' ' || COALESCE(sc.first_name,'')) AS creator_name,
                   sc.phone AS creator_phone
            FROM expenses e
            LEFT JOIN expense_categories ec ON ec.id = e.category_id
            LEFT JOIN expense_categories ep ON ep.id = ec.parent_id
            LEFT JOIN staff sc ON sc.id = e.created_by_staff_id
            WHERE e.status='pending' AND ec.approve_level IN ('manager','both')
            {cond}
            ORDER BY e.created_at DESC
        """, *params)
        return [dict(r) for r in rows]

async def get_pending_expenses_for_admin() -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT e.*,
                   ec.name_ru AS category_name_ru, ec.name_uz AS category_name_uz,
                   ec.icon AS category_icon, ec.approve_level, ec.receipt_required,
                   ep.name_ru AS parent_name_ru, ep.name_uz AS parent_name_uz,
                   TRIM(COALESCE(sc.last_name,'') || ' ' || COALESCE(sc.first_name,'')) AS creator_name,
                   sc.phone AS creator_phone,
                   TRIM(COALESCE(sm.last_name,'') || ' ' || COALESCE(sm.first_name,'')) AS manager_name
            FROM expenses e
            LEFT JOIN expense_categories ec ON ec.id = e.category_id
            LEFT JOIN expense_categories ep ON ep.id = ec.parent_id
            LEFT JOIN staff sc ON sc.id = e.created_by_staff_id
            LEFT JOIN staff sm ON sm.id = e.manager_id
            WHERE (e.status='pending' AND ec.approve_level IN ('admin','both'))
               OR e.status='mgr_approved'
            ORDER BY e.created_at DESC
        """)
        return [dict(r) for r in rows]

async def approve_expense_manager(expense_id: int, manager_id: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        cat = await conn.fetchrow("""
            SELECT ec.approve_level, ec.name_ru AS category_name_ru FROM expenses e
            JOIN expense_categories ec ON ec.id=e.category_id
            WHERE e.id=$1
        """, expense_id)
        new_status = 'paid' if cat and cat['approve_level'] == 'manager' else 'mgr_approved'
        row = await conn.fetchrow("""
            UPDATE expenses SET status=$2, manager_id=$3, manager_at=NOW()
            WHERE id=$1 AND status='pending' RETURNING *
        """, expense_id, new_status, manager_id)
        if row and new_status == 'paid':
            exp = dict(row)
            exp['category_name_ru'] = cat['category_name_ru'] if cat else ''
            await _create_ledger_for_expense(conn, exp)
        return dict(row) if row else {}

async def approve_expense_admin(expense_id: int, admin_id: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE expenses SET status='paid', admin_id=$2, admin_at=NOW()
            WHERE id=$1 AND status IN ('pending','mgr_approved') RETURNING *
        """, expense_id, admin_id)
        if row:
            cat = await conn.fetchval(
                "SELECT name_ru FROM expense_categories WHERE id=$1", row['category_id'])
            exp = dict(row)
            exp['category_name_ru'] = cat or ''
            await _create_ledger_for_expense(conn, exp)
        return dict(row) if row else {}

async def reject_expense(expense_id: int, staff_id: int, reason: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE expenses SET status='rejected', reject_reason=$2,
                manager_id=CASE WHEN manager_id IS NULL THEN $3 ELSE manager_id END,
                admin_id=CASE WHEN status IN ('pending','mgr_approved') THEN $3 ELSE admin_id END
            WHERE id=$1 AND status NOT IN ('rejected','paid') RETURNING *
        """, expense_id, reason, staff_id)
        return dict(row) if row else {}

async def save_expense_receipt(expense_id: int, receipt_url: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE expenses SET receipt_url=$2 WHERE id=$1 RETURNING *",
            expense_id, receipt_url)
        return dict(row) if row else {}


# ── SMS рассылки по расписанию ───────────────────────────────────────────────

async def ensure_salary_ledger_table():
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS salary_ledger (
                id          SERIAL PRIMARY KEY,
                staff_id    INTEGER NOT NULL REFERENCES staff(id) ON DELETE CASCADE,
                period      DATE    NOT NULL,
                type        VARCHAR(30) NOT NULL DEFAULT 'accrual',
                amount      NUMERIC(12,2) NOT NULL,
                note        TEXT DEFAULT '',
                expense_id  INTEGER REFERENCES expenses(id) ON DELETE SET NULL,
                created_by  INTEGER REFERENCES staff(id) ON DELETE SET NULL,
                created_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_salary_ledger_staff_period ON salary_ledger(staff_id, period)")
        await conn.execute(
            "ALTER TABLE staff ADD COLUMN IF NOT EXISTS advance_percent NUMERIC(5,2) DEFAULT NULL")
        await conn.execute(
            "ALTER TABLE settings ADD COLUMN IF NOT EXISTS advance_max_percent NUMERIC(5,2) DEFAULT 50")
        await conn.execute(
            "ALTER TABLE salary_ledger ADD COLUMN IF NOT EXISTS fine_reason TEXT DEFAULT NULL")
        await conn.execute(
            "ALTER TABLE orders ADD COLUMN IF NOT EXISTS packer_login VARCHAR(100) DEFAULT NULL")
        await conn.execute(
            "ALTER TABLE orders ADD COLUMN IF NOT EXISTS washed_at TIMESTAMPTZ DEFAULT NULL")
        await conn.execute(
            "ALTER TABLE orders ADD COLUMN IF NOT EXISTS packed_at TIMESTAMPTZ DEFAULT NULL")
        # Fix staff login uniqueness: per-company instead of global
        await conn.execute("DROP INDEX IF EXISTS staff_login_unique")
        await conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS staff_company_login_unique
            ON staff(company_id, login) WHERE company_id IS NOT NULL
        """)
        # Make config per-company (SaaS isolation)
        await conn.execute("ALTER TABLE config ADD COLUMN IF NOT EXISTS company_id INTEGER NOT NULL DEFAULT 1")
        await conn.execute("ALTER TABLE config DROP CONSTRAINT IF EXISTS config_pkey")
        await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS config_cid_key_uq ON config(company_id, key)")


async def delete_company_cascade(company_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        # Chat
        await conn.execute("DELETE FROM chat_messages  WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM chat_sessions  WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM chat_templates WHERE company_id=$1", company_id)
        # SMS
        await conn.execute("DELETE FROM sms_contacts   WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM sms_dispatches WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM sms_groups     WHERE company_id=$1", company_id)
        # Autodial
        await conn.execute("DELETE FROM autodial_group_members WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM autodial_calls WHERE campaign_id IN (SELECT id FROM autodial_campaigns WHERE company_id=$1)", company_id)
        await conn.execute("DELETE FROM autodial_campaigns  WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM autodial_groups     WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM autodial_callerids  WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM autodial_ivrs       WHERE company_id=$1", company_id)
        # Routes
        await conn.execute("DELETE FROM route_orders WHERE route_id IN (SELECT id FROM routes WHERE company_id=$1)", company_id)
        await conn.execute("DELETE FROM routes WHERE company_id=$1", company_id)
        # Orders (approvals, receipts, activity before orders)
        await conn.execute("DELETE FROM discount_requests       WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM debt_approval_requests  WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM order_receipt_log       WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM order_activity WHERE order_id IN (SELECT id FROM orders WHERE company_id=$1)", company_id)
        await conn.execute("DELETE FROM order_payments WHERE order_id IN (SELECT id FROM orders WHERE company_id=$1)", company_id)
        await conn.execute("DELETE FROM order_photos   WHERE order_id IN (SELECT id FROM orders WHERE company_id=$1)", company_id)
        await conn.execute("DELETE FROM order_item_media WHERE item_id IN (SELECT id FROM order_items WHERE order_id IN (SELECT id FROM orders WHERE company_id=$1))", company_id)
        await conn.execute("DELETE FROM order_items WHERE order_id IN (SELECT id FROM orders WHERE company_id=$1)", company_id)
        await conn.execute("DELETE FROM orders WHERE company_id=$1", company_id)
        # Leads
        await conn.execute("DELETE FROM lead_reminders WHERE lead_id IN (SELECT id FROM leads WHERE company_id=$1)", company_id)
        await conn.execute("DELETE FROM lead_calls     WHERE lead_id IN (SELECT id FROM leads WHERE company_id=$1)", company_id)
        await conn.execute("DELETE FROM leads WHERE company_id=$1", company_id)
        # Staff (salary tables before staff)
        await conn.execute("DELETE FROM salary_ledger          WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM staff_salary_percents  WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM staff_salary_per_unit  WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM staff_salary_kpi       WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM staff_commissions      WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM timesheet              WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM staff_attendance_events WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM staff_personal WHERE staff_id IN (SELECT id FROM staff WHERE company_id=$1)", company_id)
        await conn.execute("DELETE FROM staff WHERE company_id=$1", company_id)
        # Branches
        await conn.execute("DELETE FROM branches WHERE company_id=$1", company_id)
        # Prices & Catalog
        await conn.execute("DELETE FROM prices   WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM services WHERE company_id=$1", company_id)
        # Positions & Departments (no FK to companies, но очищаем мусор)
        await conn.execute("DELETE FROM positions   WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM departments WHERE company_id=$1", company_id)
        # Cash & Expenses
        await conn.execute("DELETE FROM cash_handovers WHERE shift_id IN (SELECT id FROM cash_shifts WHERE company_id=$1)", company_id)
        await conn.execute("DELETE FROM cash_shifts        WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM expense_categories WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM expenses           WHERE company_id=$1", company_id)
        # CRM
        await conn.execute("DELETE FROM crm_clients  WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM contacts     WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM site_contacts WHERE company_id=$1", company_id)
        # Promos
        await conn.execute("DELETE FROM promo_user_state WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM promotions       WHERE company_id=$1", company_id)
        # Notifications & TG
        await conn.execute("DELETE FROM push_subscriptions  WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM agent_notifications WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM washer_notifications WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM tg_status_messages  WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM tg_phone_links      WHERE company_id=$1", company_id)
        # Config & Settings
        await conn.execute("DELETE FROM config   WHERE company_id=$1", company_id)
        await conn.execute("DELETE FROM settings WHERE company_id=$1", company_id)
        # Users
        await conn.execute("DELETE FROM users WHERE company_id=$1", company_id)
        # Plans
        await conn.execute("DELETE FROM plans WHERE company_id=$1", company_id)
        # Finally delete company (saas_subscriptions/payments cascade automatically)
        await conn.execute("DELETE FROM companies WHERE id=$1", company_id)


async def get_advance_max_percent() -> float:
    if not pool: return 50.0
    cid = _cid()
    async with pool.acquire() as conn:
        v = await conn.fetchval("SELECT COALESCE(advance_max_percent, 50) FROM settings WHERE company_id=$1", cid)
        return float(v or 50)

async def save_advance_max_percent(pct: float) -> None:
    if not pool: return
    cid = _cid()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE settings SET advance_max_percent=$1 WHERE company_id=$2", pct, cid)

async def get_salary_balance(staff_id: int) -> dict:
    """Баланс сотрудника: общий остаток, разбивка за текущий месяц, лимит аванса."""
    if not pool: return {}
    cid = _cid()
    from datetime import date as _date
    today = _date.today()
    period = _date(today.year, today.month, 1)
    async with pool.acquire() as conn:
        if not await conn.fetchrow("SELECT id FROM staff WHERE id=$1 AND company_id=$2", staff_id, cid):
            return {}
        # Общий накопленный баланс
        total = await conn.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM salary_ledger WHERE staff_id=$1", staff_id)
        # За текущий месяц
        month_rows = await conn.fetch("""
            SELECT type, COALESCE(SUM(amount),0) AS s
            FROM salary_ledger WHERE staff_id=$1 AND period=$2
            GROUP BY type
        """, staff_id, period)
        month_map = {r['type']: float(r['s']) for r in month_rows}
        accrual_month   = month_map.get('accrual', 0)
        advances_month  = abs(month_map.get('advance', 0))
        # advance_percent: личный или глобальный (settings — своя компания, не случайная строка)
        pct_row = await conn.fetchrow("""
            SELECT s.advance_percent, st.advance_max_percent
            FROM staff s, settings st
            WHERE s.id=$1 AND st.company_id=$2
            LIMIT 1
        """, staff_id, cid)
        pct = float(pct_row['advance_percent'] or pct_row['advance_max_percent'] or 50)
        advance_limit = max(0.0, accrual_month * pct / 100 - advances_month)
        return {
            'balance':        float(total or 0),
            'accrual_month':  accrual_month,
            'advances_month': advances_month,
            'advance_pct':    pct,
            'advance_limit':  advance_limit,
        }

async def get_salary_ledger(staff_id: int, year: int, month: int) -> list:
    if not pool: return []
    cid = _cid()
    from datetime import date as _date
    period = _date(year, month, 1)
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT sl.*, TRIM(COALESCE(sc.last_name,'') || ' ' || COALESCE(sc.first_name,'')) AS creator_name
            FROM salary_ledger sl
            LEFT JOIN staff sc ON sc.id = sl.created_by
            WHERE sl.staff_id=$1 AND sl.period=$2 AND sl.company_id=$3
            ORDER BY sl.created_at
        """, staff_id, period, cid)
        return [dict(r) for r in rows]

async def add_salary_ledger_entry(staff_id: int, period_str: str, type_: str,
                                   amount: float, note: str = '',
                                   expense_id: int = None, created_by: int = None,
                                   fine_reason: str = None) -> dict:
    if not pool: return {}
    cid = _cid()
    from datetime import date as _date
    period = _date.fromisoformat(period_str)
    async with pool.acquire() as conn:
        if not await conn.fetchrow("SELECT id FROM staff WHERE id=$1 AND company_id=$2", staff_id, cid):
            return {}
        row = await conn.fetchrow("""
            INSERT INTO salary_ledger (staff_id, period, type, amount, note, expense_id, created_by, fine_reason, company_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *
        """, staff_id, period, type_, amount, note, expense_id, created_by, fine_reason, cid)
        return dict(row) if row else {}

async def get_salary_daily_breakdown(staff_id: int, year: int, month: int) -> dict:
    """Посуточный регистр: начисление считается из табеля, авансы/штрафы из ledger."""
    if not pool: return {}
    cid = _cid()
    from datetime import date as _date
    import calendar as _cal
    period = _date(year, month, 1)
    next_m = month + 1 if month < 12 else 1
    next_y = year if month < 12 else year + 1
    period_end = _date(next_y, next_m, 1)

    async with pool.acquire() as conn:
        # Сотрудник должен принадлежать компании вызывающего — иначе всё дальше
        # (ledger/timesheet/проценты) читается по чужому staff_id (IDOR).
        if not await conn.fetchrow("SELECT id FROM staff WHERE id=$1 AND company_id=$2", staff_id, cid):
            return {}

        # Накопленный баланс ДО этого месяца (все типы, все периоды < текущего)
        opening_balance = float(await conn.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM salary_ledger WHERE staff_id=$1 AND period < $2",
            staff_id, period) or 0)

        # Ставка и норма из staff
        st = await conn.fetchrow(
            "SELECT salary_type, salary_rate, salary_work_days, login FROM staff WHERE id=$1", staff_id)
        sal_type    = (st['salary_type'] or 'fixed') if st else 'fixed'
        sal_rate    = float((st['salary_rate'] or 0)) if st else 0.0
        norm_days   = int((st['salary_work_days'] or 26)) if st else 26
        staff_login = (st['login'] or '') if st else ''
        norm_hours  = norm_days * 8.0
        hourly_rate = sal_rate / norm_hours if (sal_type in ('fixed','fixed_percent') and norm_hours > 0) else 0.0

        # Табель за месяц
        ts_rows = await conn.fetch(
            "SELECT date, hours, type FROM timesheet WHERE staff_id=$1 AND date>=$2 AND date<$3 ORDER BY date",
            staff_id, period, period_end)

        # Ledger — только НЕ начисления (авансы, штрафы, выплаты) за этот период
        ledger_rows = await conn.fetch("""
            SELECT sl.*, DATE(sl.created_at AT TIME ZONE 'Asia/Tashkent') AS entry_date,
                   TRIM(COALESCE(sc.last_name,'') || ' ' || COALESCE(sc.first_name,'')) AS creator_name
            FROM salary_ledger sl
            LEFT JOIN staff sc ON sc.id = sl.created_by
            WHERE sl.staff_id=$1 AND sl.period=$2 AND sl.type != 'accrual'
            ORDER BY sl.created_at
        """, staff_id, period)

        # Для percent/fixed_percent: посуточный объём мойки
        washing_daily: dict = {}
        if sal_type in ('percent', 'fixed_percent') and staff_login:
            washing_pct_val = await conn.fetchval(
                "SELECT COALESCE(percent,0) FROM staff_salary_percents WHERE staff_id=$1 AND role='washing'",
                staff_id)
            washing_pct = float(washing_pct_val or 0)
            if washing_pct:
                day_rows = await conn.fetch("""
                    SELECT DATE(o.created_at AT TIME ZONE 'Asia/Tashkent') AS day,
                        COALESCE(SUM(COALESCE(oi.actual_total_sum,
                            oi.price_per_sqm * COALESCE(oi.actual_sqm, oi.sqm), 0)), 0) AS day_sum
                    FROM order_items oi
                    JOIN orders o ON o.id = oi.order_id
                    WHERE oi.washer_login = $1
                      AND o.status != 'cancelled'
                      AND o.created_at >= $2::timestamptz
                      AND o.created_at <  $3::timestamptz
                    GROUP BY DATE(o.created_at AT TIME ZONE 'Asia/Tashkent')
                """, staff_login, period, period_end)
                washing_daily = {
                    str(r['day']): round(float(r['day_sum']) * washing_pct / 100, 2)
                    for r in day_rows
                }

    # Карта табеля {date_str: hours}
    ts_map = {str(r['date']): float(r['hours'] or 0) for r in ts_rows
              if r['type'] in ('work', 'overtime')}

    # Карта ledger по дате транзакции
    from collections import defaultdict
    ledger_by_date = defaultdict(list)
    for r in ledger_rows:
        rd = dict(r)
        rd['amount'] = float(r['amount'])
        rd['entry_date'] = str(r['entry_date'])
        ledger_by_date[rd['entry_date']].append(rd)

    # Строим посуточную таблицу
    from datetime import date as _date2
    days_in_month = _cal.monthrange(year, month)[1]
    today = _date2.today()
    last_day = days_in_month if (year < today.year or month < today.month) else min(days_in_month, today.day)

    result_days = []
    running = 0.0  # таблица показывает движения текущего месяца; opening_balance — в шапке
    for d in range(1, last_day + 1):
        date_str = f"{year}-{month:02d}-{d:02d}"
        entries  = ledger_by_date.get(date_str, [])
        op = running

        # Начисление: из табеля (fixed) + мойка по дням (percent)
        hours_today = ts_map.get(date_str, 0.0)
        accrual = round(hourly_rate * hours_today, 2) if hourly_rate > 0 else 0.0
        accrual = round(accrual + washing_daily.get(date_str, 0.0), 2)

        advance = 0.0; salary_payment = 0.0; fine = 0.0; other = 0.0
        for e in entries:
            amt = e['amount']
            t   = e['type']
            if t == 'advance': advance += amt
            elif t == 'salary_payment': salary_payment += amt
            elif t == 'fine': fine += amt
            else: other += amt

        # running — только движения внутри месяца (без opening_balance)
        running = round(op + accrual + advance + salary_payment + fine + other, 2)
        result_days.append({
            'date': date_str,
            'opening': op,
            'accrual': accrual,
            'hours': hours_today,
            'advance': advance,
            'salary_payment': salary_payment,
            'fine': fine,
            'other': other,
            'closing': running,
            'entries': entries,
        })

    # Итоги за месяц
    total_accrual = sum(d['accrual'] for d in result_days)
    total_advance = sum(d['advance'] for d in result_days)
    total_fine    = sum(d['fine']    for d in result_days)
    total_payment = sum(d['salary_payment'] for d in result_days)
    total_other   = sum(d['other']   for d in result_days)
    month_balance = round(total_accrual + total_advance + total_fine + total_payment + total_other, 2)

    return {
        'days': result_days,
        'opening_balance': opening_balance,
        'salary_type': sal_type,
        'hourly_rate': hourly_rate,
        'total_accrual': total_accrual,
        'total_advance': abs(total_advance),
        'total_fine': abs(total_fine),
        'total_payment': abs(total_payment),
        'month_balance': month_balance,
        'final_balance': round(opening_balance + month_balance, 2),
    }

async def get_agent_monitoring_stats() -> dict:
    """Мониторинг активности персонала: order_activity / lead_calls / chat_messages /
    staff_attendance_events / cash_shifts + orders_in_work + presence_status."""
    if not pool: return {"agents": [], "order_status_breakdown": {}, "activity_trend_7d": []}
    cid = _cid()

    today_tk = datetime.now(_TASHKENT).date()
    yesterday_tk = today_tk - timedelta(days=1)
    week_start_tk = today_tk - timedelta(days=6)
    ts_from, ts_to = _tz_range(str(week_start_tk), str(today_tk))
    now_utc = datetime.now(timezone.utc)

    def _secs_ago(dt):
        """Секунд назад от now_utc; поддерживает aware и naive (UTC) datetime."""
        if dt is None:
            return float('inf')
        if dt.tzinfo is None:
            return (datetime.utcnow() - dt).total_seconds()
        return (now_utc - dt).total_seconds()

    async with pool.acquire() as conn:
        staff_rows = await conn.fetch(
            "SELECT id, login, first_name, last_name, role, branch FROM staff WHERE active = true AND company_id=$1", cid)

        oa_rows = await conn.fetch("""
            SELECT staff_id,
                   MAX(created_at) AS last_active,
                   COUNT(*) FILTER (WHERE (created_at AT TIME ZONE 'Asia/Tashkent')::date = $1) AS actions_today,
                   COUNT(*) FILTER (WHERE (created_at AT TIME ZONE 'Asia/Tashkent')::date = $2) AS actions_yesterday
            FROM order_activity
            WHERE staff_id IS NOT NULL AND company_id=$3
            GROUP BY staff_id
        """, today_tk, yesterday_tk, cid)

        lc_rows = await conn.fetch("""
            SELECT operator_id AS staff_id,
                   MAX(created_at) AS last_active,
                   COUNT(*) FILTER (WHERE (created_at AT TIME ZONE 'Asia/Tashkent')::date = $1) AS actions_today,
                   COUNT(*) FILTER (WHERE (created_at AT TIME ZONE 'Asia/Tashkent')::date = $2) AS actions_yesterday
            FROM lead_calls
            WHERE operator_id IS NOT NULL AND company_id=$3
            GROUP BY operator_id
        """, today_tk, yesterday_tk, cid)

        cm_rows = await conn.fetch("""
            SELECT sender_name,
                   MAX(created_at) AS last_active,
                   COUNT(*) FILTER (WHERE (created_at AT TIME ZONE 'Asia/Tashkent')::date = $1) AS actions_today,
                   COUNT(*) FILTER (WHERE (created_at AT TIME ZONE 'Asia/Tashkent')::date = $2) AS actions_yesterday
            FROM chat_messages
            WHERE sender_type = 'staff' AND company_id=$3
            GROUP BY sender_name
        """, today_tk, yesterday_tk, cid)

        # attendance events: приход/уход
        try:
            ae_rows = await conn.fetch("""
                SELECT staff_id,
                       MAX(created_at) AS last_active,
                       COUNT(*) FILTER (WHERE event_type = 'in'
                           AND (created_at AT TIME ZONE 'Asia/Tashkent')::date = $1) AS in_today,
                       MIN(created_at) FILTER (WHERE event_type = 'in'
                           AND (created_at AT TIME ZONE 'Asia/Tashkent')::date = $1) AS first_in_today,
                       COUNT(*) FILTER (WHERE event_type = 'out'
                           AND (created_at AT TIME ZONE 'Asia/Tashkent')::date = $1) AS out_today
                FROM staff_attendance_events
                WHERE company_id=$2
                GROUP BY staff_id
            """, today_tk, cid)
        except Exception:
            ae_rows = []

        # смены кассы
        try:
            cs_rows = await conn.fetch("""
                SELECT opened_by AS staff_id,
                       MAX(opened_at) AS last_active,
                       COUNT(*) FILTER (WHERE status = 'open'
                           AND (opened_at AT TIME ZONE 'Asia/Tashkent')::date = $1) AS shift_open_today
                FROM cash_shifts
                WHERE opened_by IS NOT NULL AND company_id=$2
                GROUP BY opened_by
            """, today_tk, cid)
        except Exception:
            cs_rows = []

        # заказы в работе (linked via login)
        try:
            oiw_rows = await conn.fetch("""
                SELECT s.id AS staff_id, COUNT(*) AS cnt
                FROM orders o
                JOIN staff s ON s.login = o.assigned_to
                WHERE o.status NOT IN ('delivered','cancelled','new')
                  AND o.assigned_to IS NOT NULL
                  AND o.company_id=$1
                GROUP BY s.id
            """, cid)
        except Exception:
            oiw_rows = []

        status_row = await conn.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE status NOT IN ('delivered','cancelled')) AS in_progress,
                COUNT(*) FILTER (
                    WHERE status = 'delivered'
                      AND (updated_at AT TIME ZONE 'Asia/Tashkent')::date = $1
                ) AS delivered_today,
                COUNT(*) FILTER (
                    WHERE status = 'cancelled'
                      AND (updated_at AT TIME ZONE 'Asia/Tashkent')::date = $1
                ) AS cancelled_today
            FROM orders
            WHERE company_id=$2
        """, today_tk, cid)

        trend_oa_rows = await conn.fetch("""
            SELECT (created_at AT TIME ZONE 'Asia/Tashkent')::date AS day, COUNT(*) AS cnt
            FROM order_activity
            WHERE created_at >= $1 AND created_at < $2 AND company_id=$3
            GROUP BY day
        """, ts_from, ts_to, cid)

        trend_lc_rows = await conn.fetch("""
            SELECT (created_at AT TIME ZONE 'Asia/Tashkent')::date AS day, COUNT(*) AS cnt
            FROM lead_calls
            WHERE created_at >= $1 AND created_at < $2 AND company_id=$3
            GROUP BY day
        """, ts_from, ts_to, cid)

    # ── вспомогательная merge-функция ─────────────────────────────────────
    def _merge_id(dest: dict, sid, last_active, at=0, ay=0):
        cur = dest.get(sid)
        if cur is None:
            dest[sid] = {'last_active': last_active, 'actions_today': at, 'actions_yesterday': ay}
        else:
            if last_active and (cur['last_active'] is None or last_active > cur['last_active']):
                cur['last_active'] = last_active
            cur['actions_today'] += at
            cur['actions_yesterday'] += ay

    # ── merge order_activity + lead_calls по staff.id ────────────────────
    by_id: dict = {}
    for r in oa_rows:
        _merge_id(by_id, r['staff_id'], r['last_active'],
                  r['actions_today'] or 0, r['actions_yesterday'] or 0)
    for r in lc_rows:
        _merge_id(by_id, r['staff_id'], r['last_active'],
                  r['actions_today'] or 0, r['actions_yesterday'] or 0)

    # ── merge chat_messages по имени ──────────────────────────────────────
    by_name: dict = {}
    for r in cm_rows:
        name = (r['sender_name'] or '').strip()
        if not name:
            continue
        by_name[name] = {
            'last_active': r['last_active'],
            'actions_today': r['actions_today'] or 0,
            'actions_yesterday': r['actions_yesterday'] or 0,
        }

    # ── индексы дополнительных источников ────────────────────────────────
    ae_by_id: dict = {r['staff_id']: r for r in ae_rows}
    cs_by_id: dict = {r['staff_id']: r for r in cs_rows}
    oiw_by_id: dict = {r['staff_id']: int(r['cnt'] or 0) for r in oiw_rows}

    # ── сборка agents[] ───────────────────────────────────────────────────
    agents = []
    for s in staff_rows:
        sid = s['id']
        full_name = f"{(s['last_name'] or '').strip()} {(s['first_name'] or '').strip()}".strip()
        name_key = ((s['last_name'] or '') + ' ' + (s['first_name'] or '')).strip()

        last_active = None
        actions_today = 0
        actions_yesterday = 0

        m1 = by_id.get(sid)
        if m1:
            last_active = m1['last_active']
            actions_today += m1['actions_today']
            actions_yesterday += m1['actions_yesterday']

        m2 = by_name.get(name_key)
        if m2:
            if m2['last_active'] and (last_active is None or m2['last_active'] > last_active):
                last_active = m2['last_active']
            actions_today += m2['actions_today']
            actions_yesterday += m2['actions_yesterday']

        # attendance
        ae = ae_by_id.get(sid)
        checked_in_today = bool(ae and (ae['in_today'] or 0) > 0)
        checked_out_today = bool(ae and (ae['out_today'] or 0) > 0)
        checked_in_time = None
        if ae and ae['first_in_today']:
            t = ae['first_in_today']
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)
            checked_in_time = t.astimezone(_TASHKENT).strftime('%H:%M')
        if ae and ae['last_active']:
            if last_active is None or ae['last_active'] > last_active:
                last_active = ae['last_active']

        # cash_shifts
        cs = cs_by_id.get(sid)
        shift_open_today = bool(cs and (cs['shift_open_today'] or 0) > 0)
        if cs and cs['last_active']:
            if last_active is None or cs['last_active'] > last_active:
                last_active = cs['last_active']

        # orders in work
        orders_in_work = oiw_by_id.get(sid, 0)

        # presence_status
        secs = _secs_ago(last_active)
        if secs <= 300:
            presence_status = 'online'
        elif checked_in_today and checked_out_today:
            presence_status = 'left'
        elif checked_in_today:
            presence_status = 'today'
        else:
            presence_status = 'offline'

        agents.append({
            'id': sid,
            'name': full_name,
            'role': s['role'],
            'branch': s['branch'],
            'last_active': last_active.isoformat() if last_active else None,
            'actions_today': int(actions_today),
            'actions_yesterday': int(actions_yesterday),
            'checked_in_today': checked_in_today,
            'checked_in_time': checked_in_time,
            'checked_out_today': checked_out_today,
            'shift_open_today': shift_open_today,
            'orders_in_work': orders_in_work,
            'presence_status': presence_status,
        })

    order_status_breakdown = {
        'in_progress': int(status_row['in_progress']) if status_row else 0,
        'delivered_today': int(status_row['delivered_today']) if status_row else 0,
        'cancelled_today': int(status_row['cancelled_today']) if status_row else 0,
    }

    trend_map: dict = {}
    for r in trend_oa_rows:
        trend_map[r['day']] = trend_map.get(r['day'], 0) + (r['cnt'] or 0)
    for r in trend_lc_rows:
        trend_map[r['day']] = trend_map.get(r['day'], 0) + (r['cnt'] or 0)

    activity_trend_7d = []
    for i in range(7):
        d = week_start_tk + timedelta(days=i)
        activity_trend_7d.append({
            'date': str(d),
            'actions': int(trend_map.get(d, 0)),
        })

    return {
        'agents': agents,
        'order_status_breakdown': order_status_breakdown,
        'activity_trend_7d': activity_trend_7d,
    }

async def set_opening_balance(staff_id: int, year: int, month: int,
                               target: float, created_by: int) -> dict:
    """Устанавливает остаток на начало месяца через корректирующую запись в предыдущем периоде."""
    if not pool: return {}
    cid = _cid()
    from datetime import date as _date
    period = _date(year, month, 1)
    async with pool.acquire() as conn:
        if not await conn.fetchrow("SELECT id FROM staff WHERE id=$1 AND company_id=$2", staff_id, cid):
            return {}
        current = float(await conn.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM salary_ledger WHERE staff_id=$1 AND period < $2",
            staff_id, period) or 0)
        diff = round(target - current, 2)
        if diff == 0:
            return {'ok': True, 'diff': 0}
        # Корректировка записывается в предыдущий период
        prev_month = month - 1 if month > 1 else 12
        prev_year  = year if month > 1 else year - 1
        prev_period = _date(prev_year, prev_month, 1)
        row = await conn.fetchrow("""
            INSERT INTO salary_ledger (staff_id, period, type, amount, note, created_by, company_id)
            VALUES ($1,$2,'adjustment',$3,'Ручная корректировка входящего остатка',$4,$5)
            RETURNING id
        """, staff_id, prev_period, diff, created_by, cid)
        return {'ok': True, 'diff': diff, 'entry_id': row['id'] if row else None}

async def delete_month_accruals(staff_id: int, year: int, month: int) -> int:
    """Удаляет все записи типа 'accrual' за указанный период для сотрудника."""
    if not pool: return 0
    cid = _cid()
    from datetime import date as _date
    period = _date(year, month, 1)
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM salary_ledger WHERE staff_id=$1 AND period=$2 AND type='accrual' AND company_id=$3",
            staff_id, period, cid)
        return int(result.split()[-1])

async def update_salary_ledger_entry(entry_id: int, amount: float, note: str) -> dict:
    if not pool: return {}
    cid = _cid()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE salary_ledger SET amount=$2, note=$3 WHERE id=$1 AND company_id=$4 RETURNING *
        """, entry_id, amount, note, cid)
        return dict(row) if row else {}

async def delete_salary_ledger_entry(entry_id: int) -> bool:
    if not pool: return False
    cid = _cid()
    async with pool.acquire() as conn:
        r = await conn.execute("DELETE FROM salary_ledger WHERE id=$1 AND company_id=$2", entry_id, cid)
        return r == "DELETE 1"

async def _accrue_monthly_salaries_for_company(conn, company_id: int, period) -> int:
    staff_rows = await conn.fetch("""
        SELECT id, salary_type, salary_rate
        FROM staff
        WHERE (active = TRUE OR active IS NULL)
          AND role <> 'agent'
          AND salary_type IS NOT NULL AND salary_rate > 0
          AND company_id=$1
    """, company_id)
    count = 0
    for s in staff_rows:
        exists = await conn.fetchval(
            "SELECT 1 FROM salary_ledger WHERE staff_id=$1 AND period=$2 AND type='accrual'",
            s['id'], period)
        if exists:
            continue
        await conn.execute("""
            INSERT INTO salary_ledger (staff_id, period, type, amount, note, company_id)
            VALUES ($1, $2, 'accrual', $3, 'Автоначисление оклада', $4)
        """, s['id'], period, float(s['salary_rate'] or 0), company_id)
        count += 1
    return count

async def auto_accrue_monthly_salaries(company_id: int | None = None) -> int:
    """Начисляет оклады 1-го числа месяца. Пропускает если уже есть accrual за период.
    company_id=None (используется фоновым воркером раз в месяц) — по всем компаниям;
    иначе (ручной вызов админом) — только по своей компании."""
    if not pool: return 0
    from datetime import date as _date
    today = _date.today()
    period = _date(today.year, today.month, 1)
    async with pool.acquire() as conn:
        if company_id is not None:
            return await _accrue_monthly_salaries_for_company(conn, company_id, period)
        total = 0
        companies = await conn.fetch("SELECT id FROM companies WHERE id > 0")
        for c in companies:
            total += await _accrue_monthly_salaries_for_company(conn, c['id'], period)
        return total

async def _create_ledger_for_expense(conn, expense: dict) -> None:
    """Создаёт запись salary_ledger при утверждении расхода на сотрудника."""
    if not expense.get('for_staff_id'):
        return
    cat_name = (expense.get('category_name_ru') or '').lower()
    if 'аванс' in cat_name:
        ltype = 'advance'
    elif 'зарплат' in cat_name:
        ltype = 'salary_payment'
    elif 'бонус' in cat_name or 'премия' in cat_name:
        ltype = 'bonus'
    else:
        ltype = 'deduction'
    from datetime import date as _date
    today = _date.today()
    period = _date(today.year, today.month, 1)
    await conn.execute("""
        INSERT INTO salary_ledger (staff_id, period, type, amount, note, expense_id, created_by)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT DO NOTHING
    """, expense['for_staff_id'], period, ltype, -float(expense['amount']),
        expense.get('description') or '', expense.get('id'), expense.get('admin_id') or expense.get('manager_id'))

async def ensure_sms_dispatch_table():
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS sms_dispatches (
                id           SERIAL PRIMARY KEY,
                name         TEXT NOT NULL DEFAULT 'Рассылка',
                message      TEXT NOT NULL,
                from_nick    TEXT NOT NULL DEFAULT 'ARTEZ',
                phones       JSONB NOT NULL,
                scheduled_at TIMESTAMPTZ NOT NULL,
                status       TEXT NOT NULL DEFAULT 'pending',
                sent_at      TIMESTAMPTZ,
                sent_count   INT DEFAULT 0,
                created_at   TIMESTAMPTZ DEFAULT NOW()
            )
        """)

async def create_sms_dispatch(name: str, message: str, from_nick: str,
                               phones: list, scheduled_at) -> int:
    if not pool: return 0
    cid = _cid()
    import json as _j
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO sms_dispatches (name, message, from_nick, phones, scheduled_at, company_id)
            VALUES ($1, $2, $3, $4, $5, $6) RETURNING id
        """, name, message, from_nick, _j.dumps(phones), scheduled_at, cid)
        return row["id"] if row else 0

async def get_pending_sms_dispatches() -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM sms_dispatches
            WHERE status='pending' AND scheduled_at <= NOW()
            ORDER BY scheduled_at ASC LIMIT 20
        """)
        return [dict(r) for r in rows]

async def mark_sms_dispatch_sent(dispatch_id: int, sent_count: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE sms_dispatches SET status='sent', sent_at=NOW(), sent_count=$2
            WHERE id=$1
        """, dispatch_id, sent_count)

_SMS_OPERATOR_DEFAULTS = [
    {"operator": "beeline",   "display_name": "Beeline",   "prefixes": [90, 91, 92],     "price_service": 115, "price_ad": 300},
    {"operator": "uzmobile",  "display_name": "Uzmobile",  "prefixes": [99, 77, 70, 95], "price_service": 145, "price_ad": 350},
    {"operator": "mobiuz",    "display_name": "MobiUz",    "prefixes": [97, 88, 87],     "price_service": 110, "price_ad": 290},
    {"operator": "ucell",     "display_name": "Ucell",     "prefixes": [93, 94, 50],     "price_service": 160, "price_ad": 340},
    {"operator": "humans",    "display_name": "Humans",    "prefixes": [33],              "price_service": 95,  "price_ad": 95},
    {"operator": "oq",        "display_name": "OQ",        "prefixes": [20],              "price_service": 0,   "price_ad": 0},
    {"operator": "perfectum", "display_name": "Perfectum", "prefixes": [98, 80],         "price_service": 95,  "price_ad": 95},
]

async def ensure_sms_operator_prices():
    if not pool: return
    import json as _j
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS sms_operator_prices (
                id           SERIAL PRIMARY KEY,
                operator     TEXT NOT NULL UNIQUE,
                display_name TEXT NOT NULL,
                prefixes     JSONB NOT NULL DEFAULT '[]',
                price_service INT NOT NULL DEFAULT 0,
                price_ad      INT NOT NULL DEFAULT 0,
                updated_at   TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # Вставить дефолты если таблица пуста
        count = await conn.fetchval("SELECT COUNT(*) FROM sms_operator_prices")
        if count == 0:
            for op in _SMS_OPERATOR_DEFAULTS:
                await conn.execute("""
                    INSERT INTO sms_operator_prices (operator, display_name, prefixes, price_service, price_ad)
                    VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING
                """, op["operator"], op["display_name"], _j.dumps(op["prefixes"]),
                    op["price_service"], op["price_ad"])

async def get_sms_operator_prices() -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM sms_operator_prices ORDER BY id")
        return [dict(r) for r in rows]

async def update_sms_operator_price(op_id: int, display_name: str, prefixes: list,
                                     price_service: int, price_ad: int) -> dict:
    if not pool: return {}
    import json as _j
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE sms_operator_prices
            SET display_name=$2, prefixes=$3, price_service=$4, price_ad=$5, updated_at=NOW()
            WHERE id=$1 RETURNING *
        """, op_id, display_name, _j.dumps(prefixes), price_service, price_ad)
        return dict(row) if row else {}

def _sms_date_range(start_date: str, end_date: str):
    from datetime import datetime, timezone, timedelta
    tz5 = timezone(timedelta(hours=5))
    s = datetime.fromisoformat(start_date).replace(hour=0,  minute=0,  second=0,  tzinfo=tz5)
    e = datetime.fromisoformat(end_date).replace(  hour=23, minute=59, second=59, tzinfo=tz5)
    return s, e

async def get_sms_stats_by_month(start_date: str, end_date: str) -> list:
    if not pool: return []
    cid = _cid()
    s, e = _sms_date_range(start_date, end_date)
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                TO_CHAR(COALESCE(sent_at, scheduled_at) AT TIME ZONE 'Asia/Tashkent', 'YYYY-MM') AS month,
                COUNT(*)::int AS dispatches,
                COALESCE(SUM(sent_count),0)::int AS sent_sms,
                COALESCE(SUM(jsonb_array_length(phones)),0)::int AS total_phones
            FROM sms_dispatches
            WHERE COALESCE(sent_at, scheduled_at) BETWEEN $1 AND $2
              AND company_id=$3
            GROUP BY 1 ORDER BY 1 DESC
        """, s, e, cid)
        return [dict(r) for r in rows]

async def get_sms_stats_by_date(start_date: str, end_date: str) -> list:
    if not pool: return []
    cid = _cid()
    s, e = _sms_date_range(start_date, end_date)
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                TO_CHAR(COALESCE(sent_at, scheduled_at) AT TIME ZONE 'Asia/Tashkent', 'YYYY-MM-DD') AS date,
                COUNT(*)::int AS dispatches,
                COALESCE(SUM(sent_count),0)::int AS sent_sms,
                COALESCE(SUM(jsonb_array_length(phones)),0)::int AS total_phones
            FROM sms_dispatches
            WHERE COALESCE(sent_at, scheduled_at) BETWEEN $1 AND $2
              AND company_id=$3
            GROUP BY 1 ORDER BY 1 DESC
        """, s, e, cid)
        return [dict(r) for r in rows]

async def get_sms_dispatches_report(start_date: str, end_date: str) -> list:
    if not pool: return []
    cid = _cid()
    s, e = _sms_date_range(start_date, end_date)
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, name, from_nick,
                   LEFT(message, 60) AS message_preview,
                   jsonb_array_length(phones)::int AS total_phones,
                   COALESCE(sent_count,0)::int AS sent_count, status,
                   TO_CHAR(scheduled_at AT TIME ZONE 'Asia/Tashkent', 'DD.MM.YYYY HH24:MI') AS scheduled,
                   TO_CHAR(sent_at     AT TIME ZONE 'Asia/Tashkent', 'DD.MM.YYYY HH24:MI') AS sent_at
            FROM sms_dispatches
            WHERE COALESCE(sent_at, scheduled_at) BETWEEN $1 AND $2
              AND company_id=$3
            ORDER BY id DESC LIMIT 200
        """, s, e, cid)
        return [dict(r) for r in rows]

async def get_sms_dispatches_for_export(start_date: str, end_date: str) -> list:
    if not pool: return []
    cid = _cid()
    s, e = _sms_date_range(start_date, end_date)
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, name, from_nick, message,
                   jsonb_array_length(phones)::int AS total_phones,
                   COALESCE(sent_count,0)::int AS sent_count, status,
                   scheduled_at AT TIME ZONE 'Asia/Tashkent' AS scheduled_at,
                   sent_at      AT TIME ZONE 'Asia/Tashkent' AS sent_at,
                   created_at   AT TIME ZONE 'Asia/Tashkent' AS created_at
            FROM sms_dispatches
            WHERE COALESCE(sent_at, scheduled_at) BETWEEN $1 AND $2
              AND company_id=$3
            ORDER BY id DESC
        """, s, e, cid)
        return [dict(r) for r in rows]


# ══════════════════════════════════════
#  SaaS — Компании
# ══════════════════════════════════════

async def get_all_companies():
    if not pool: return []
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT id, name, slug, plan, max_branches, max_staff, active, created_at, "
            "contact_name, contact_phone, contact_email, inn, legal_name, address, notes, logo_url "
            "FROM companies WHERE id > 0 ORDER BY id"
        )

async def get_company(company_id: int):
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM companies WHERE id=$1", company_id
        )

async def get_company_by_contact_tg_id(tg_id: int):
    """Ищет компанию, чей владелец уже подтвердил телефон через Cleano-бота этим Telegram-аккаунтом
    (contact_tg_id заполняется при успешной регистрации через public_register_company)."""
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT id, name, slug FROM companies WHERE contact_tg_id=$1 ORDER BY id DESC LIMIT 1", tg_id
        )

async def create_company(name: str, slug: str, secret_key: str,
                          plan: str = "starter", max_branches: int = 5, max_staff: int = 50):
    if not pool: return None
    async with pool.acquire() as conn:
        try:
            return await conn.fetchrow("""
                INSERT INTO companies (name, slug, secret_key, plan, max_branches, max_staff, active)
                VALUES ($1, $2, $3, $4, $5, $6, TRUE)
                RETURNING *
            """, name, slug, secret_key, plan, max_branches, max_staff)
        except Exception:
            return None  # slug уже занят

async def get_company_slug(company_id: int) -> str:
    if not pool: return ""
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT slug FROM companies WHERE id=$1", company_id) or ""

async def update_company(company_id: int, updates: dict) -> bool:
    """Возвращает False только при конфликте уникального slug — остальные ошибки пробрасываются."""
    if not pool or not updates: return True
    allowed = {"name", "slug", "secret_key", "plan", "max_branches", "max_staff", "active", "timezone", "trial_days",
               "legal_name", "inn", "address", "contact_name", "contact_phone", "contact_email", "notes",
               "whatsapp", "instagram", "tg_group_link", "tg_group_id", "tg_channel_link", "tg_channel_id",
               "tg_admin_link", "tg_admin_id", "logo_url", "contact_tg_id"}
    fields = {k: v for k, v in updates.items() if k in allowed}
    if not fields: return True
    cols = ", ".join(f"{k}=${i+2}" for i, k in enumerate(fields))
    vals = list(fields.values())
    async with pool.acquire() as conn:
        try:
            await conn.execute(
                f"UPDATE companies SET {cols} WHERE id=$1", company_id, *vals
            )
        except asyncpg.exceptions.UniqueViolationError:
            return False
    if "timezone" in fields:
        invalidate_company_tz_cache(company_id)
    return True

async def delete_company(company_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM companies WHERE id=$1", company_id)


# ══════════════════════════════════════
#  SaaS — Филиалы
# ══════════════════════════════════════

async def get_branches(company_id: int):
    if not pool: return []
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM branches WHERE company_id=$1 ORDER BY id", company_id
        )

async def get_branch_by_slug(company_id: int, slug: str):
    """Регистронезависимое сравнение: slug у филиалов не нормализуется при
    создании (может быть введён вручную кириллицей, с заглавной буквы —
    см. баг 2026-08-13, ON CONFLICT-типа проблема с branch-роутингом лидов,
    где 'Зарафшан1' и 'зарафшан1' не матчились точным сравнением)."""
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM branches WHERE company_id=$1 AND LOWER(TRIM(slug))=LOWER(TRIM($2))",
            company_id, slug
        )


_BRANCH_TG_COLUMNS = {"tg_leads_group_id", "tg_orders_channel_id", "tg_delivery_channel_id"}

async def get_branch_tg_group_id(branch_slug: str, column: str):
    """Читает Telegram group/channel id из карточки филиала (по slug, текущая компания через _cid()).
    column — строго из белого списка. Возвращает None, если филиал/значение не найдены."""
    if not pool or not branch_slug or column not in _BRANCH_TG_COLUMNS:
        return None
    row = await get_branch_by_slug(_cid(), branch_slug)
    return row[column] if row else None

async def create_branch(company_id: int, slug: str, name_ru: str, name_uz: str = "",
                         lat=None, lon=None, phones: list = None,
                         workshop_lat=None, workshop_lon=None,
                         tg_orders_channel_id=None,
                         tg_leads_group_id=None, tg_delivery_channel_id=None,
                         tg_delivery_channel_link=None, telegram_link=None,
                         admin_tg_link=None, whatsapp=None, instagram=None,
                         tg_leads_group_link=None, tg_orders_channel_link=None,
                         telegram_group_id=None,
                         admin_tg_id=None):
    if not pool: return None
    import json
    phones_json = json.dumps(phones or [])
    async with pool.acquire() as conn:
        try:
            return await conn.fetchrow("""
                INSERT INTO branches
                    (company_id, slug, name_ru, name_uz, lat, lon, phones,
                     workshop_lat, workshop_lon,
                     tg_orders_channel_id,
                     tg_leads_group_id, tg_delivery_channel_id, tg_delivery_channel_link,
                     telegram_link, admin_tg_link, whatsapp, instagram,
                     tg_leads_group_link, tg_orders_channel_link,
                     telegram_group_id, admin_tg_id, active)
                VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,
                        $18,$19,$20,$21,TRUE)
                RETURNING *
            """, company_id, slug, name_ru, name_uz, lat, lon, phones_json,
                 workshop_lat, workshop_lon,
                 tg_orders_channel_id,
                 tg_leads_group_id, tg_delivery_channel_id, tg_delivery_channel_link,
                 telegram_link, admin_tg_link, whatsapp, instagram,
                 tg_leads_group_link, tg_orders_channel_link,
                 telegram_group_id, admin_tg_id)
        except Exception:
            return None  # slug уже занят

async def update_branch(branch_id: int, company_id: int, updates: dict) -> bool:
    if not pool or not updates: return False
    import json
    allowed = {"name_ru", "name_uz", "lat", "lon", "phones",
               "workshop_lat", "workshop_lon",
               "tg_orders_channel_id",
               "tg_leads_group_id", "tg_delivery_channel_id", "tg_delivery_channel_link",
               "telegram_link", "admin_tg_link", "whatsapp", "instagram", "active",
               "tg_leads_group_link", "tg_orders_channel_link",
               "telegram_group_id", "admin_tg_id"}
    fields = {k: v for k, v in updates.items() if k in allowed}
    if not fields: return False
    # phones сериализуем в JSON
    if "phones" in fields:
        fields["phones"] = json.dumps(fields["phones"])
    # Строим SET: name_ru=$3, lat=$4, ...
    params = [branch_id, company_id]
    set_parts = []
    for k, v in fields.items():
        params.append(v)
        cast = "::text::jsonb" if k == "phones" else ""
        set_parts.append(f"{k}=${len(params)}{cast}")
    sql = f"UPDATE branches SET {', '.join(set_parts)} WHERE id=$1 AND company_id=$2"
    async with pool.acquire() as conn:
        result = await conn.execute(sql, *params)
        return result != "UPDATE 0"

async def delete_branch(branch_id: int, company_id: int) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM branches WHERE id=$1 AND company_id=$2", branch_id, company_id
        )
        return result != "DELETE 0"


# ══════════════════════════════════════
#  ГЛАВНАЯ СТРАНИЦА САЙТА: слайдер + статистика
# ══════════════════════════════════════
async def get_site_slides(company_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM site_slides WHERE company_id=$1 ORDER BY sort_order, id", company_id)
    return [dict(r) for r in rows]


async def create_site_slide(company_id: int, image_url: str, eyebrow_ru: str, eyebrow_uz: str,
                             title_ru: str, title_uz: str, text_ru: str, text_uz: str, sort_order: int) -> dict | None:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO site_slides (company_id, image_url, eyebrow_ru, eyebrow_uz, title_ru, title_uz, text_ru, text_uz, sort_order)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *
        """, company_id, image_url, eyebrow_ru, eyebrow_uz, title_ru, title_uz, text_ru, text_uz, sort_order)
    return dict(row) if row else None


async def update_site_slide(slide_id: int, company_id: int, updates: dict) -> dict | None:
    if not pool: return None
    allowed = {"eyebrow_ru", "eyebrow_uz", "title_ru", "title_uz", "text_ru", "text_uz", "sort_order"}
    fields = {k: v for k, v in updates.items() if k in allowed}
    if not fields: return None
    params = [slide_id, company_id]
    sets = []
    for k, v in fields.items():
        params.append(v)
        sets.append(f"{k}=${len(params)}")
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE site_slides SET {', '.join(sets)} WHERE id=$1 AND company_id=$2 RETURNING *", *params)
    return dict(row) if row else None


async def update_site_slide_image(slide_id: int, company_id: int, image_url: str) -> dict | None:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE site_slides SET image_url=$1 WHERE id=$2 AND company_id=$3 RETURNING *",
            image_url, slide_id, company_id)
    return dict(row) if row else None


async def delete_site_slide(slide_id: int, company_id: int) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM site_slides WHERE id=$1 AND company_id=$2", slide_id, company_id)
    return res != "DELETE 0"


async def get_site_stats(company_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM site_stats WHERE company_id=$1 ORDER BY sort_order, id", company_id)
    return [dict(r) for r in rows]


async def update_site_stat(stat_id: int, company_id: int, updates: dict) -> dict | None:
    if not pool: return None
    allowed = {"value_ru", "value_uz", "label_ru", "label_uz", "sort_order"}
    fields = {k: v for k, v in updates.items() if k in allowed}
    if not fields: return None
    params = [stat_id, company_id]
    sets = []
    for k, v in fields.items():
        params.append(v)
        sets.append(f"{k}=${len(params)}")
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE site_stats SET {', '.join(sets)} WHERE id=$1 AND company_id=$2 RETURNING *", *params)
    return dict(row) if row else None


async def seed_company_site_stats(company_id: int, city_ru: str = "", city_uz: str = ""):
    """Заводит 4 карточки статистики для новой компании — из шаблона (company_id=0, заполняет суперадмин),
    иначе 4 пустые (фиксированная 4-колоночная сетка на сайте).
    3-я карточка (индекс 2) в шаблоне — про города/филиалы (у ARTEZ: 'Навои и Зарафшан');
    если при регистрации указан город клиента — подставляем его вместо шаблонного."""
    if not pool: return
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM site_stats WHERE company_id=$1", company_id)
        if count > 0:
            return
        template = await conn.fetch(
            "SELECT value_ru, value_uz, label_ru, label_uz FROM site_stats WHERE company_id=0 ORDER BY sort_order, id LIMIT 4")
        if template:
            for i, r in enumerate(template):
                value_ru, value_uz, label_ru, label_uz = r["value_ru"], r["value_uz"], r["label_ru"], r["label_uz"]
                if i == 2 and city_ru:
                    value_ru = value_uz = "1 📍"
                    label_ru = f"город: {city_ru}"
                    label_uz = f"shahar: {city_uz or city_ru}"
                await conn.execute(
                    "INSERT INTO site_stats (company_id, value_ru, value_uz, label_ru, label_uz, sort_order) VALUES ($1,$2,$3,$4,$5,$6)",
                    company_id, value_ru, value_uz, label_ru, label_uz, i)
        else:
            await conn.executemany(
                "INSERT INTO site_stats (company_id, value_ru, value_uz, label_ru, label_uz, sort_order) VALUES ($1,'','','','',$2)",
                [(company_id, i) for i in range(4)]
            )


async def seed_company_defaults(company_id: int, name: str):
    """Сидирует непустые дефолты для 'О компании' (футер сайта) и текста чека — чтобы
    после регистрации поля не оставались пустыми (раньше показывали только placeholder 'ARTEZ')."""
    if not pool: return
    defaults = {
        "footer_about_ru": f"{name} — химчистка ковров, мягкой мебели и текстиля. Работаем быстро и с гарантией качества.",
        "footer_about_uz": f"{name} — gilam, yumshoq mebel va to'qimachilikni kimyoviy tozalash. Tez va sifat kafolati bilan ishlaymiz.",
        "receipt_header_text": name,
        "receipt_slogan": "Химчистка ковров, мебели, матрасов и штор",
        "receipt_footer_note": "Спасибо, что выбрали нас!",
    }
    for key, value in defaults.items():
        existing = await get_config_for_company(key, company_id)
        if not existing:
            await set_config_for_company(key, value, company_id)


async def seed_company_site_slides(company_id: int, force: bool = False):
    """Копирует шаблонные слайды из company_id=0 (заполняет суперадмин). Пусто, если шаблона нет."""
    if not pool: return
    async with pool.acquire() as conn:
        if not force:
            exists = await conn.fetchval("SELECT 1 FROM site_slides WHERE company_id=$1 LIMIT 1", company_id)
            if exists: return
        if force:
            await conn.execute("DELETE FROM site_slides WHERE company_id=$1", company_id)
        template = await conn.fetch("SELECT * FROM site_slides WHERE company_id=0 ORDER BY sort_order, id")
        for r in template:
            await conn.execute("""
                INSERT INTO site_slides (company_id, image_url, eyebrow_ru, eyebrow_uz, title_ru, title_uz, text_ru, text_uz, sort_order)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            """, company_id, r["image_url"], r["eyebrow_ru"], r["eyebrow_uz"], r["title_ru"], r["title_uz"],
                 r["text_ru"], r["text_uz"], r["sort_order"])


async def seed_company_site_reviews(company_id: int, force: bool = False, city_ru: str = "", city_uz: str = ""):
    """Копирует шаблонные отзывы из company_id=0 (заполняет суперадмин). Пусто, если шаблона нет.
    Город в каждом отзыве (у ARTEZ: Навои/Зарафшан вперемешку) заменяется на город клиента, если указан."""
    if not pool: return
    async with pool.acquire() as conn:
        if not force:
            exists = await conn.fetchval("SELECT 1 FROM site_reviews WHERE company_id=$1 LIMIT 1", company_id)
            if exists: return
        if force:
            await conn.execute("DELETE FROM site_reviews WHERE company_id=$1", company_id)
        template = await conn.fetch("SELECT * FROM site_reviews WHERE company_id=0 ORDER BY sort_order, id")
        for r in template:
            c_ru = city_ru or r["city_ru"]
            c_uz = city_uz or r["city_uz"]
            await conn.execute("""
                INSERT INTO site_reviews (company_id, author_name, rating, text_ru, text_uz, city_ru, city_uz, sort_order)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            """, company_id, r["author_name"], r["rating"], r["text_ru"], r["text_uz"],
                 c_ru, c_uz, r["sort_order"])


async def seed_company_site_faq(company_id: int, force: bool = False):
    """Копирует шаблонные FAQ из company_id=0 (заполняет суперадмин). Пусто, если шаблона нет."""
    if not pool: return
    async with pool.acquire() as conn:
        if not force:
            exists = await conn.fetchval("SELECT 1 FROM site_faq WHERE company_id=$1 LIMIT 1", company_id)
            if exists: return
        if force:
            await conn.execute("DELETE FROM site_faq WHERE company_id=$1", company_id)
        template = await conn.fetch("SELECT * FROM site_faq WHERE company_id=0 ORDER BY sort_order, id")
        for r in template:
            await conn.execute("""
                INSERT INTO site_faq (company_id, question_ru, question_uz, answer_ru, answer_uz, sort_order)
                VALUES ($1,$2,$3,$4,$5,$6)
            """, company_id, r["question_ru"], r["question_uz"], r["answer_ru"], r["answer_uz"], r["sort_order"])


# ── Отзывы на главной странице ───────────────────────────────────────────────
async def get_site_reviews(company_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM site_reviews WHERE company_id=$1 ORDER BY sort_order, id", company_id)
    return [dict(r) for r in rows]


async def create_site_review(company_id: int, data: dict) -> dict | None:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO site_reviews (company_id, author_name, rating, text_ru, text_uz, city_ru, city_uz, sort_order)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *
        """, company_id, data.get("author_name", ""), data.get("rating", 5),
             data.get("text_ru", ""), data.get("text_uz", ""),
             data.get("city_ru", ""), data.get("city_uz", ""), data.get("sort_order", 0))
    return dict(row) if row else None


async def update_site_review(review_id: int, company_id: int, updates: dict) -> dict | None:
    if not pool: return None
    allowed = {"author_name", "rating", "text_ru", "text_uz", "city_ru", "city_uz", "sort_order"}
    fields = {k: v for k, v in updates.items() if k in allowed}
    if not fields: return None
    params = [review_id, company_id]
    sets = []
    for k, v in fields.items():
        params.append(v)
        sets.append(f"{k}=${len(params)}")
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE site_reviews SET {', '.join(sets)} WHERE id=$1 AND company_id=$2 RETURNING *", *params)
    return dict(row) if row else None


async def delete_site_review(review_id: int, company_id: int) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM site_reviews WHERE id=$1 AND company_id=$2", review_id, company_id)
    return res != "DELETE 0"


# ── FAQ на главной странице ──────────────────────────────────────────────────
async def get_site_faq(company_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM site_faq WHERE company_id=$1 ORDER BY sort_order, id", company_id)
    return [dict(r) for r in rows]


async def create_site_faq_item(company_id: int, data: dict) -> dict | None:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO site_faq (company_id, question_ru, question_uz, answer_ru, answer_uz, sort_order)
            VALUES ($1,$2,$3,$4,$5,$6) RETURNING *
        """, company_id, data.get("question_ru", ""), data.get("question_uz", ""),
             data.get("answer_ru", ""), data.get("answer_uz", ""), data.get("sort_order", 0))
    return dict(row) if row else None


async def update_site_faq_item(faq_id: int, company_id: int, updates: dict) -> dict | None:
    if not pool: return None
    allowed = {"question_ru", "question_uz", "answer_ru", "answer_uz", "sort_order"}
    fields = {k: v for k, v in updates.items() if k in allowed}
    if not fields: return None
    params = [faq_id, company_id]
    sets = []
    for k, v in fields.items():
        params.append(v)
        sets.append(f"{k}=${len(params)}")
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE site_faq SET {', '.join(sets)} WHERE id=$1 AND company_id=$2 RETURNING *", *params)
    return dict(row) if row else None


async def delete_site_faq_item(faq_id: int, company_id: int) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM site_faq WHERE id=$1 AND company_id=$2", faq_id, company_id)
    return res != "DELETE 0"


# ══════════════════════════════════════
#  SAAS PLANS / SUBSCRIPTIONS / PAYMENTS
# ══════════════════════════════════════

async def check_company_field_exists(column: str, value: str) -> bool:
    """Проверка на дубликат перед регистрацией (публичная форма cleano.uz).
    column — строго из белого списка, никогда не из пользовательского ввода напрямую."""
    if not pool: return False
    allowed_cols = {"name", "slug", "contact_phone", "contact_email"}
    if column not in allowed_cols or not value.strip():
        return False
    async with pool.acquire() as conn:
        if column == "contact_phone":
            digits = re.sub(r"\D", "", value)
            if not digits:
                return False
            row = await conn.fetchval(
                "SELECT 1 FROM companies WHERE regexp_replace(contact_phone, '\\D', '', 'g') = $1 "
                "AND contact_phone IS NOT NULL AND contact_phone <> '' LIMIT 1", digits)
        else:
            row = await conn.fetchval(
                f"SELECT 1 FROM companies WHERE lower({column}) = lower($1) LIMIT 1", value.strip())
        return bool(row)


# ── Подтверждение телефона при регистрации на cleano.uz (SMS или Telegram-бот) ──

async def save_cleano_tg_link(phone: str, tg_id: int):
    """Сохраняет связку телефон→tg_id, которую бот Cleano получил через 'поделиться контактом'."""
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO cleano_tg_links (phone, tg_id, created_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (phone) DO UPDATE SET tg_id=$2, created_at=NOW()
        """, phone, tg_id)


async def get_cleano_tg_id_by_phone(phone: str):
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT tg_id FROM cleano_tg_links WHERE phone=$1", phone)
        return row["tg_id"] if row else None


async def mark_cleano_phone_verified(phone: str, method: str, tg_id: int | None = None):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO cleano_phone_verifications (phone, verified_at, method, tg_id)
            VALUES ($1, NOW(), $2, $3)
            ON CONFLICT (phone) DO UPDATE SET verified_at=NOW(), method=$2, tg_id=$3
        """, phone, method, tg_id)


async def get_cleano_phone_verification(phone: str):
    """Возвращает запись подтверждения, если она свежая (не старше 30 минут), иначе None."""
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT phone, verified_at, method, tg_id FROM cleano_phone_verifications
            WHERE phone=$1 AND verified_at > NOW() - INTERVAL '30 minutes'
        """, phone)
        return dict(row) if row else None


async def consume_cleano_phone_verification(phone: str):
    """Одноразовое использование: удаляет подтверждение сразу после успешного создания компании,
    чтобы одно и то же подтверждение (действительное 30 минут) нельзя было переиспользовать
    для другой регистрации с тем же номером."""
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM cleano_phone_verifications WHERE phone=$1", phone)


# ── Персистентная ссылка суперадмина t.me/<bot>?start=company_<id> ──────────

async def save_pending_company_link(chat_id: int, company_id: int):
    """Запоминает на 30 минут: этот chat_id сейчас привязывается к company_id
    (между /start company_X и последующим 'поделиться контактом')."""
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO cleano_pending_company_link (chat_id, company_id, expires_at)
            VALUES ($1, $2, NOW() + INTERVAL '30 minutes')
            ON CONFLICT (chat_id) DO UPDATE SET company_id=$2, expires_at=NOW() + INTERVAL '30 minutes'
        """, chat_id, company_id)


async def get_pending_company_link(chat_id: int):
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT company_id FROM cleano_pending_company_link WHERE chat_id=$1 AND expires_at > NOW()", chat_id)


async def consume_pending_company_link(chat_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM cleano_pending_company_link WHERE chat_id=$1", chat_id)


async def get_saas_plan_by_slug(slug: str):
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM saas_plans WHERE slug=$1", slug)
        return dict(row) if row else None


async def get_saas_plans():
    """Список планов с месячными ценами."""
    if not pool: return []
    async with pool.acquire() as conn:
        plans = await conn.fetch(
            "SELECT * FROM saas_plans ORDER BY sort_order, id"
        )
        pricing = await conn.fetch(
            "SELECT plan_id, month, price FROM saas_plan_pricing ORDER BY plan_id, month"
        )
    # Группируем месячные цены по plan_id
    pricing_map: dict = {}
    for row in pricing:
        pricing_map.setdefault(row["plan_id"], {})[row["month"]] = row["price"]
    result = []
    for p in plans:
        d = dict(p)
        d["monthly_prices"] = pricing_map.get(p["id"], {})
        result.append(d)
    return result


async def update_saas_plan(plan_id: int, updates: dict):
    if not pool: return
    allowed = {"display_name", "max_branches", "max_staff", "base_price", "active", "sort_order"}
    fields = {k: v for k, v in updates.items() if k in allowed}
    if not fields: return
    params = [plan_id]
    sets = []
    for k, v in fields.items():
        params.append(v)
        sets.append(f"{k}=${len(params)}")
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE saas_plans SET {', '.join(sets)} WHERE id=$1", *params
        )


async def update_saas_plan_pricing(plan_id: int, month: int, price: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO saas_plan_pricing (plan_id, month, price)
            VALUES ($1, $2, $3)
            ON CONFLICT (plan_id, month) DO UPDATE SET price = EXCLUDED.price
        """, plan_id, month, price)


async def get_site_templates(active_only: bool = False):
    if not pool: return []
    async with pool.acquire() as conn:
        q = "SELECT * FROM site_templates"
        if active_only: q += " WHERE active=TRUE"
        q += " ORDER BY sort_order, id"
        return [dict(r) for r in await conn.fetch(q)]


async def create_site_template(key: str, name_ru: str, name_uz: str, preview_url: str = None, sort_order: int = 0):
    if not pool: return None
    async with pool.acquire() as conn:
        return dict(await conn.fetchrow("""
            INSERT INTO site_templates (key, name_ru, name_uz, preview_url, sort_order)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING *
        """, key, name_ru, name_uz, preview_url, sort_order))


async def update_site_template(template_id: int, updates: dict):
    if not pool: return
    allowed = {"name_ru", "name_uz", "preview_url", "active", "sort_order"}
    fields = {k: v for k, v in updates.items() if k in allowed}
    if not fields: return
    params = [template_id]
    sets = []
    for k, v in fields.items():
        params.append(v)
        sets.append(f"{k}=${len(params)}")
    async with pool.acquire() as conn:
        await conn.execute(f"UPDATE site_templates SET {', '.join(sets)} WHERE id=$1", *params)


async def delete_site_template(template_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM site_templates WHERE id=$1", template_id)


async def get_site_palettes(active_only: bool = False):
    if not pool: return []
    async with pool.acquire() as conn:
        q = "SELECT * FROM site_palettes"
        if active_only: q += " WHERE active=TRUE"
        q += " ORDER BY sort_order, id"
        rows = [dict(r) for r in await conn.fetch(q)]
        for r in rows:
            if isinstance(r.get("colors"), str):
                r["colors"] = json.loads(r["colors"])
        return rows


async def create_site_palette(key: str, name_ru: str, name_uz: str, colors: dict, sort_order: int = 0):
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO site_palettes (key, name_ru, name_uz, colors, sort_order)
            VALUES ($1, $2, $3, $4::jsonb, $5)
            RETURNING *
        """, key, name_ru, name_uz, json.dumps(colors), sort_order)
        d = dict(row)
        if isinstance(d.get("colors"), str): d["colors"] = json.loads(d["colors"])
        return d


async def update_site_palette(palette_id: int, updates: dict):
    if not pool: return
    allowed = {"name_ru", "name_uz", "colors", "active", "sort_order"}
    fields = {k: v for k, v in updates.items() if k in allowed}
    if not fields: return
    params = [palette_id]
    sets = []
    for k, v in fields.items():
        if k == "colors":
            v = json.dumps(v)
            params.append(v)
            sets.append(f"colors=${len(params)}::jsonb")
        else:
            params.append(v)
            sets.append(f"{k}=${len(params)}")
    async with pool.acquire() as conn:
        await conn.execute(f"UPDATE site_palettes SET {', '.join(sets)} WHERE id=$1", *params)


async def delete_site_palette(palette_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM site_palettes WHERE id=$1", palette_id)


async def get_company_site_design(company_id: int):
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT site_template_key, site_palette_key FROM companies WHERE id=$1", company_id
        )


async def get_company_public_design(company_id: int):
    """Публичные данные для рендера сайта: ключ шаблона + цвета палитры (для index.html)."""
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT c.site_template_key, c.site_palette_key, p.colors
            FROM companies c
            LEFT JOIN site_palettes p ON p.key = c.site_palette_key
            WHERE c.id = $1
        """, company_id)
        if not row: return None
        d = dict(row)
        if isinstance(d.get("colors"), str):
            d["colors"] = json.loads(d["colors"])
        return d


async def update_company_site_design(company_id: int, template_key: str, palette_key: str):
    if not pool: return
    async with pool.acquire() as conn:
        tpl_ok = await conn.fetchval("SELECT 1 FROM site_templates WHERE key=$1 AND active=TRUE", template_key)
        pal_ok = await conn.fetchval("SELECT 1 FROM site_palettes WHERE key=$1 AND active=TRUE", palette_key)
        if not tpl_ok or not pal_ok:
            raise ValueError("unknown or inactive template/palette key")
        await conn.execute(
            "UPDATE companies SET site_template_key=$2, site_palette_key=$3 WHERE id=$1",
            company_id, template_key, palette_key
        )


async def get_saas_subscription(company_id: int):
    """Текущая активная (или последняя) подписка компании с данными плана."""
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow("""
            SELECT s.*, p.slug AS plan_slug, p.display_name AS plan_name,
                   p.max_branches, p.max_staff, p.base_price
            FROM saas_subscriptions s
            JOIN saas_plans p ON p.id = s.plan_id
            WHERE s.company_id = $1
            ORDER BY s.created_at DESC
            LIMIT 1
        """, company_id)


async def create_saas_subscription(company_id: int, plan_id: int,
                                   start_date, end_date, notes=None, status='active'):
    if not pool: return None
    from datetime import date as _date
    if isinstance(start_date, str): start_date = _date.fromisoformat(start_date)
    if isinstance(end_date, str):   end_date   = _date.fromisoformat(end_date)
    async with pool.acquire() as conn:
        return await conn.fetchrow("""
            INSERT INTO saas_subscriptions (company_id, plan_id, start_date, end_date, notes, status)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING *
        """, company_id, plan_id, start_date, end_date, notes, status)


async def get_saas_subscriptions_all(company_id: int):
    """Все подписки компании (история) с данными плана, новые первыми."""
    if not pool: return []
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT s.*, p.slug AS plan_slug, p.display_name AS plan_name,
                   p.max_branches, p.max_staff
            FROM saas_subscriptions s
            JOIN saas_plans p ON p.id = s.plan_id
            WHERE s.company_id = $1
            ORDER BY s.created_at DESC
        """, company_id)


async def update_saas_subscription(sub_id: int, updates: dict):
    if not pool: return
    from datetime import date as _date
    allowed = {"status", "end_date", "balance", "notes"}
    fields = {k: v for k, v in updates.items() if k in allowed}
    if not fields: return
    if "end_date" in fields and isinstance(fields["end_date"], str):
        fields["end_date"] = _date.fromisoformat(fields["end_date"])
    params = [sub_id]
    sets = []
    for k, v in fields.items():
        params.append(v)
        sets.append(f"{k}=${len(params)}")
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE saas_subscriptions SET {', '.join(sets)} WHERE id=$1", *params
        )


async def checkout_plan_test(company_id: int, plan_id: int, months: int) -> dict | None:
    """ТЕСТОВЫЙ режим оплаты (нет платёжного шлюза) — сразу применяет план к компании:
    создаёт подписку + запись оплаты и обновляет companies.plan/max_branches/max_staff."""
    if not pool: return None
    from datetime import date, timedelta
    months = max(1, min(12, months))
    async with pool.acquire() as conn:
        plan = await conn.fetchrow("SELECT * FROM saas_plans WHERE id=$1 AND active=TRUE", plan_id)
        if not plan:
            return None
        price_row = await conn.fetchrow(
            "SELECT price FROM saas_plan_pricing WHERE plan_id=$1 AND month=$2", plan_id, months)
        amount = price_row["price"] if price_row else plan["base_price"] * months
        start = date.today()
        end = start + timedelta(days=30 * months)
        async with conn.transaction():
            sub = await conn.fetchrow("""
                INSERT INTO saas_subscriptions (company_id, plan_id, start_date, end_date, status, notes)
                VALUES ($1, $2, $3, $4, 'active', 'Тестовая оплата (нет платёжного шлюза)')
                RETURNING *
            """, company_id, plan_id, start, end)
            payment = await conn.fetchrow("""
                INSERT INTO saas_payments (company_id, subscription_id, amount, payment_date, note)
                VALUES ($1, $2, $3, $4, 'Тестовая оплата')
                RETURNING *
            """, company_id, sub["id"], amount, start)
            await conn.execute(
                "UPDATE companies SET plan=$1, max_branches=$2, max_staff=$3 WHERE id=$4",
                plan["slug"], plan["max_branches"], plan["max_staff"], company_id)
    return {"subscription": dict(sub), "payment": dict(payment), "plan": dict(plan)}


async def get_saas_payments(company_id: int):
    if not pool: return []
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT * FROM saas_payments
            WHERE company_id = $1
            ORDER BY payment_date DESC, id DESC
        """, company_id)


async def add_saas_payment(company_id: int, subscription_id, amount: int,
                           payment_date=None, note=None):
    if not pool: return None
    from datetime import date as _date
    if isinstance(payment_date, str) and payment_date:
        payment_date = _date.fromisoformat(payment_date)
    elif not payment_date:
        payment_date = _date.today()
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow("""
                INSERT INTO saas_payments (company_id, subscription_id, amount, payment_date, note)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING *
            """, company_id, subscription_id, amount, payment_date, note)
            if subscription_id:
                await conn.execute(
                    "UPDATE saas_subscriptions SET balance = balance + $1 WHERE id = $2",
                    amount, subscription_id
                )
            return row
