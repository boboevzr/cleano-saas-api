"""Хендлеры бота заказов для клиентов компании — общий Router на всех компаний.

Два потока, перенесённые из старого монолитного artez_bot/bot.py:
- "быстрый заказ" (QuickForm) — язык → меню → услуга → телефон → имя → лид создан.
- "полный заказ" (OrderForm) — язык → меню → имя → телефон → филиал → адрес →
  услуга → тип услуги → дата → время → лид создан. Реальный порядок шагов взят
  из старого bot.py (class OrderForm, ~L674 и его хендлеры ~L1941-2253) — там
  этот порядок именно такой (имя и телефон запрашиваются раньше филиала/адреса).
  Упрощение относительно старого бота: без гео-точки/карты (location/web_app) —
  адрес только текстом; филиал берётся динамически из db.get_branches(company_id)
  вместо хардкода "zarafshan"/"navoi".

В обоих случаях — сотрудник обрабатывает лид и конвертирует его в заказ вручную
через CRM, бот заказ напрямую не создаёт. После создания лида сотрудники получают
push + сообщение в TG-группу филиала с кнопкой «Взять лид» (см. cb_take_lead —
использует db.take_lead(), уже company_id-aware).

Главное меню (menu_kb/_build_menu_text) построено динамически по образцу прод-бота:
название компании+тэглайн, филиалы, короткий/основной номер, телефоны по филиалам,
кнопки «Открыть приложение»/«Оставить заявку» (подменю быстрая/полная)/«Статус
заказа»/«Цены»/«Калькулятор»/«Мой профиль»/«Оператор» (эскалация на сотрудника
через группу лидов, с возможностью ответить клиенту).

НЕ перенесено (следующий этап, ОТЛОЖЕНО по просьбе пользователя): скидки, долги,
касса, водительские маршруты — в SaaS для этого уже есть веб/PWA (staff.html), но
пользователь хочет продублировать и в боте, просто не сейчас. См. память проекта
(project_artez_bot_saas_migration) за деталями/находками для этого этапа.
Также не перенесены: admin-команды, autodial, live-chat — вне рамок текущей
миграции. См. artez_bot/artez_bot/bot.py (только чтение).

«Стать Агентом» (menu_agent/agent_confirm/agent_contact_received, конец файла)
перенесена и использует company-scoped _agent_status_for_bot/_agent_apply_for_bot
из main.py напрямую (тот же процесс, без HTTP-петли на себя же).

ВАЖНО: echo_fallback ограничен приватными чатами (F.chat.type == "private") — бот
теперь состоит в группе лидов компании (уведомления/«Взять лид»/«Оператор»), и без
этого ограничения обычная переписка сотрудников в группе получала бы в ответ
клиентское меню.

Лид создаётся через db.create_lead() — ту же функцию, что использует остальной API
(admin.html, /api/bot/lead и т.д.), с явным company_id (вебхук общий на все компании,
request-scoped contextvar _cid() здесь не работает).
"""
from __future__ import annotations

import asyncio
import html
import json
import logging
import re

from aiogram import Router, F
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message, CallbackQuery, BufferedInputFile,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    InputMediaPhoto, InputMediaVideo,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

import database as db

router = Router(name="order_bot")


# ══════════════════════════════════════
#  FSM
# ══════════════════════════════════════
class QuickForm(StatesGroup):
    service = State()
    phone   = State()
    name    = State()


class CalcForm(StatesGroup):
    """Калькулятор стоимости — анонимный, ничего не сохраняет (не создаёт лид).
    Порядок шагов перенесён из старого bot.py (class CalcForm, ~L2282-2361):
    услуга → тип услуги → ширина (см) → длина (см) → результат."""
    service      = State()
    service_type = State()
    width        = State()
    length       = State()


class OrderForm(StatesGroup):
    """Полный заказ — реальный порядок шагов из старого bot.py (class OrderForm):
    имя → телефон → филиал → адрес → услуга → тип услуги → дата → время → лид.
    (Старый бот дополнительно спрашивал гео-точку между адресом и услугой —
    здесь опущено, см. docstring модуля.)"""
    name         = State()
    phone        = State()
    branch       = State()
    address      = State()
    service      = State()
    service_type = State()
    date         = State()
    time         = State()
    time_from    = State()   # выбор начала (grid) после «Указать время»
    time_to      = State()   # выбор конца (grid)


class OperatorForm(StatesGroup):
    """Клиент пишет сообщение оператору — уходит в TG-группу лидов компании
    с кнопкой «Ответить клиенту» (см. admin_reply_start/AdminReplyForm ниже)."""
    message = State()


class RegisterForm(StatesGroup):
    """Регистрация аккаунта на сайте компании прямо из бота (перенесено из
    прод-бота, class RegisterForm ~L674). Пароль генерируется сервером
    (main.register_via_tg) и присылается отдельным сообщением от лица
    СВОЕГО бота компании — здесь нет HTTP-петли на себя же (один процесс)."""
    waiting_name    = State()
    waiting_contact = State()


class AdminReplyForm(StatesGroup):
    """Сотрудник нажал «Ответить клиенту» в группе — следующее его сообщение
    (в ТОЙ ЖЕ группе, от того же tg_id — FSM-ключ включает chat_id+user_id,
    см. order_bot_storage.PostgresStorage) пересылается клиенту."""
    waiting_reply = State()


class AgentForm(StatesGroup):
    """«Стать Агентом» — если верифицированный tg_phone ещё не известен, просим
    поделиться контактом, чтобы найти аккаунт сайта той же компании."""
    waiting_contact = State()


class DriverPaymentForm(StatesGroup):
    """«Доставлено» с оплатой — водитель выбрал способ оплаты (drv_pay_method_),
    ждём сумму текстом. Недоплата < 1000 сум — авто-скидка, иначе заявка
    менеджеру (см. handle_payment_amount в старом bot.py, ~L3792)."""
    waiting_amount = State()


# Группировка статусов заказа для раздела «Статус заказа» — перенесено из
# старого bot.py (STATUS_GROUPS/ORDER_STATUS_NAMES_*, ~L550-582), значения
# статусов сверены с ALL_ORDER_STATUSES в main.py (реальный список, не выдумано).
# "drying" добавлен в "progress" — в прод-боте отсутствовал в словаре группировки,
# но статус существует в ALL_ORDER_STATUSES между washing и packing, логично
# туда же (см. финальный ответ агента про это архитектурное решение).
STATUS_GROUPS = {
    "new":       ["new", "confirmed"],
    "progress":  ["pickup", "received", "washing", "drying", "packing", "ready", "delivery"],
    "done":      ["delivered"],
    "cancelled": ["cancelled"],
}

ORDER_STATUS_NAMES_RU = {
    "new":       "🆕 Новый",
    "confirmed": "✅ Подтверждён",
    "pickup":    "🚗 Вывоз",
    "received":  "📥 В мастерской",
    "washing":   "🧼 Мойка",
    "drying":    "💨 Сушка",
    "packing":   "📦 Упаковка",
    "ready":     "✅ Готов",
    "delivery":  "🚚 Доставка",
    "delivered": "✅ Доставлен",
    "cancelled": "❌ Отменён",
}
ORDER_STATUS_NAMES_UZ = {
    "new":       "🆕 Yangi",
    "confirmed": "✅ Tasdiqlangan",
    "pickup":    "🚗 Olib ketish",
    "received":  "📥 Ustaxonada",
    "washing":   "🧼 Yuvish",
    "drying":    "💨 Quritish",
    "packing":   "📦 Qadoqlash",
    "ready":     "✅ Tayyor",
    "delivery":  "🚚 Yetkazish",
    "delivered": "✅ Yetkazildi",
    "cancelled": "❌ Bekor qilindi",
}


def _order_status_name(lang: str, status: str) -> str:
    names = ORDER_STATUS_NAMES_UZ if lang == "uz" else ORDER_STATUS_NAMES_RU
    return names.get(status, status)


# ══════════════════════════════════════
#  ТЕКСТЫ (только ключи, нужные быстрому заказу — не весь словарь прод-бота)
# ══════════════════════════════════════
T = {
    "ru": {
        "hello":          "👋",
        "lang_set":       "🇷🇺 Выбран русский язык",
        "menu_title":     "🏠 Главное меню",
        "btn_order":      "📄 Оставить заявку",
        "ask_order_type": "📋 Выберите тип заявки:",
        "btn_order_quick": "⚡ Быстрая заявка",
        "btn_webapp":     "🌐 Открыть приложение",
        "btn_status":     "📦 Статус заказа",
        "btn_prices":     "💰 Цены",
        "btn_profile":    "👤 Мой профиль",
        "btn_operator":   "🎧 Оператор",
        "contact_short_line": "☎️ Короткий номер: {num}",
        "contact_main_label": "📞 Оператор:",
        "ask_service":    "🧺 Выберите услугу:",
        "ask_phone":      "📞 Поделитесь номером или введите вручную:\n\nФормат: +998XXXXXXXXX",
        "btn_share_phone": "📱 Поделиться номером",
        "btn_enter_phone": "⌨️ Ввести другой номер",
        "ask_phone_manual": "✏️ Введите номер в формате:\n+998XXXXXXXXX\n\nПример: +998901234567",
        "phone_invalid":  "⚠️ Неверный формат!\n\nВведите номер строго в формате:\n+998XXXXXXXXX",
        "ask_name":       "👤 Введите ваше имя:",
        "order_done":     "✅ Заказ принят!\n\nМы свяжемся с вами в ближайшее время.",
        "order_failed":   "⚠️ Не удалось сохранить заказ. Попробуйте ещё раз чуть позже.",
        "btn_cancel":     "❌ Отмена",
        "btn_menu":       "🏠 Меню",
        "cancelled":      "❌ Отменено.",

        # ── Регистрация на сайте через бота ──
        "menu_register_hint": "<i>📝 Чтобы отслеживать статус заказов в боте или на сайте — зарегистрируйтесь, нажав кнопку «Зарегистрироваться» ниже</i>",
        "btn_reg_start":  "📝 Зарегистрироваться",
        "reg_ask_name":   "👤 Как к вам обращаться?",
        "btn_reg_type_name": "✏️ Ввести другое имя",
        "reg_name_empty": "⚠️ Введите имя",
        "reg_ask_phone":  "📞 Теперь поделитесь номером телефона:",
        "reg_already_registered": "ℹ️ <b>У вас уже есть аккаунт на сайте</b>\n\n📱 {phone}\n\nВойдите с этим номером.",
        "reg_success":    "✅ <b>Готово!</b> Данные для входа отправлены отдельным сообщением.",
        "reg_error":      "❌ Не удалось зарегистрировать. Попробуйте позже.",
        "reg_own_phone_only": "❌ Поделитесь своим номером.",

        # ── Полный заказ (OrderForm) ──
        "btn_order_full": "📅 Заказать с выездом",
        "ask_branch":     "🏢 Выберите филиал:",
        "ask_address":    "📍 Введите адрес (город, улица, дом):",
        "ask_service_type": "⏱ Выберите тип услуги:",
        "btn_type_standard": "🕓 Стандарт",
        "btn_type_express":  "⚡ Экспресс",
        "ask_date":       "📅 Выберите дату самовывоза:",
        "btn_today":      "Сегодня",
        "btn_tomorrow":   "Завтра",
        "btn_pick_date":  "✏️ Указать дату",
        "ask_date_manual": "✏️ Введите дату в формате ДД.ММ.ГГГГ\n\nПример: {example}",
        "date_invalid":   "⚠️ Неверная дата!\n\nВведите дату в формате ДД.ММ.ГГГГ (не раньше сегодняшнего дня).",
        "ask_time":       "🕐 Выберите время самовывоза:",
        "btn_morning":    "🌅 08:00 — 13:00",
        "btn_evening":    "🌆 13:00 — 20:00",
        "btn_custom_time": "✏️ Указать время",
        "ask_time_from":  "🕐 Выберите время начала:",
        "ask_time_to":    "🕐 Выберите время окончания:",
        "full_order_done": "✅ Заявка №{num} принята!\n\nМы свяжемся с вами в ближайшее время.",
        "full_order_failed": "⚠️ Не удалось сохранить заявку. Попробуйте ещё раз чуть позже.",

        # ── Калькулятор стоимости (CalcForm) ──
        "btn_calc":       "🧮 Калькулятор",
        "calc_ask_svc":   "🧮 Калькулятор стоимости\n\nВыберите услугу:",
        "calc_selected_header": "🧮 Калькулятор стоимости\n\nУслуга: {svc}",
        "calc_ask_w":     "Введите ширину в сантиметрах:\n\nПример: 200 (= 2 метра)",
        "calc_ask_l":     "Теперь введите длину в сантиметрах:\n\nПример: 300 (= 3 метра)",
        "calc_result_below_min": "🧮 Расчёт стоимости\n\n📐 Размер: {w} × {l} см = {sqm} {unit}\n{svc}\n💰 {price} сум/{unit}\n\n⚠️ Ваш размер {sqm} {unit} — меньше мин. заказа ({min_order} {unit})\n💵 Итого: {total} сум (за {min_order} {unit})",
        "calc_result_no_min": "🧮 Расчёт стоимости\n\n📐 Размер: {w} × {l} см = {sqm} {unit}\n{svc}\n💰 {price} сум/{unit}\n\n💵 Итого: {total} сум",
        "calc_price_missing": "⚠️ Цена для этой услуги ещё не настроена. Обратитесь в компанию.",
        "invalid_num":    "⚠️ Пожалуйста, введите число. Например: 200",

        # ── Статус заказа ──
        "status_empty":      "📦 Статус заказа\n\nУ вас пока нет заявок.",
        "status_menu_title": "📦 Статус заказа\n\nВыберите категорию:",
        "status_btn_new":       "🆕 Новые",
        "status_btn_progress":  "🔄 В работе",
        "status_btn_done":      "✅ Выполнено",
        "status_btn_cancelled": "❌ Отменены",
        "status_group_empty": "В этой категории заявок нет.",
        "status_lead_line":   "📋 Заявка {num}\n🧺 {service}\n📌 Статус: 🆕 Новая заявка, ожидайте звонка",
        "status_lead_lost_line": "📋 Заявка {num}\n📌 Статус: ❌ Отменена",
        "lbl_created":        "Заказ создан",
        "lbl_pickup":         "Забор",
        "lbl_delivered":      "Доставлен",
        "lbl_status":         "Статус",
        "lbl_items":          "Позиций",
        "lbl_items_total":    "Сумма позиций",
        "lbl_due":            "К оплате",
        "lbl_paid":           "Оплачено",
        "lbl_remaining":      "Осталось оплатить",
        "lbl_fully_paid":     "✅ Оплачено полностью",
        "lbl_currency":       "сум",
        "btn_back_to_status": "◀️ К категориям",
        "btn_order_detail":   "📋 №{num}: подробнее",
        "btn_positions":      "📄 Позиции",
        "btn_measure_media":  "📐 Фото/видео замера",
        "btn_photos":         "📸 Фото: до/после/повреждения",
        "btn_back_to_order":  "◀️ К заказу",
        "order_positions_title": "📄 Позиции заказа №{num}",
        "no_positions":       "Позиций пока нет.",
        "no_measure_media":   "Фото/видео замера пока нет.",
        "no_photos":          "Фото пока нет.",
        "order_measure_caption": "📐 Замер · Позиция {idx}: {service}\n🧾 Заказ №{num}",
        "photo_type_before":  "До",
        "photo_type_after":   "После",
        "photo_type_damage":  "⚠️ Повреждение",
        "order_not_found":    "⚠️ Заказ не найден.",

        # ── Мой профиль ──
        "profile_text":   "👤 Ваш профиль\n\n📛 Имя: {name}\n📞 Телефон: {phone}\n🆔 ID: {uid}\n\n📊 Заявок всего: {total}\n✅ Выполнено: {done}{last}",
        "profile_last":   "\nПоследний заказ: {date}",
        "profile_nophone": "не указан",
        "btn_settings":   "⚙️ Настройки",
        "settings_text":  "⚙️ Настройки",
        "btn_change_lang": "🌐 Сменить язык",
        "btn_help":       "🆘 Помощь",
        "help_text":      "🆘 Помощь\n\nЕсли у вас возник вопрос — воспользуйтесь кнопкой «🎧 Оператор» в главном меню, или позвоните нам.",

        # ── Оператор ──
        "operator_text":  "🎧 Напишите ваше сообщение для оператора:",
        "operator_fwd":   "✅ Сообщение передано оператору.",

        # ── Цены ──
        "prices_title":   "💰 Цены",
        "prices_empty":   "⚠️ Цены ещё не настроены. Обратитесь в компанию.",
        "prices_min_order_title": "📦 Мин. заказ: ",
        "prices_min_order_line": "{min_order} {unit} ({services})",
        "prices_footer_types": "Стандарт / Экспресс",
        "prices_footer_delivery": "🚚 Вывоз и доставка — бесплатно",

        # ── Стать Агентом ──
        "btn_agent":      "🤝 Стать Агентом",
        "agent_info_text": "🤝 <b>Стать Агентом</b>\n\nАгенты привлекают клиентов и получают комиссию с каждого заказа.\n\n📋 Условия:\n• Быть зарегистрированным на сайте компании\n• Приводить клиентов по реферальной ссылке\n• Размер комиссии: зависит от суммы заказа\n\nНажмите «Подтвердить», чтобы продолжить:",
        "btn_agent_confirm": "✅ Подтвердить — Стать Агентом",
        "agent_checking": "⏳ Проверяем…",
        "agent_ask_contact": "🤝 Стать Агентом\n\nАккаунт на сайте не найден.\n\nНажмите кнопку ниже — бот получит ваш номер и найдёт ваш аккаунт на сайте компании",
        "agent_already":  "✅ Вы уже являетесь Агентом!\n\nВойдите в кабинет агента:\n🔗 {url}\n\nЛогин: ваш номер телефона",
        "agent_success":  "🎉 Ура! Вы стали Агентом!\n\nЛогин: {phone}\nПароль: как на сайте компании\n\nВойдите в кабинет:\n🔗 {url}",
        "agent_not_found": "❌ Номер {phone} не найден на сайте компании.\n\nЗарегистрируйтесь на сайте с этим номером, затем снова нажмите «Стать Агентом»",
        "agent_failed":   "❌ Не удалось проверить/зарегистрировать. Попробуйте позже или через сайт компании.",
        "btn_open_agent_cabinet": "🎯 Открыть кабинет агента",
        "btn_register_site": "🌐 Зарегистрироваться",

        # ── Маршрут водителя (/route) ──
        "route_no_access": "❌ Доступно только водителям компании.",
        "route_empty": "🚚 На сегодня маршрутов нет.",
        "route_header": "🚚 Маршрут на сегодня: {count} точ(ка/ки/ек)",
        "route_stop_line": "📋 {num}\n👤 {client}\n📞 {phone}\n📍 {address}\n📦 {items} поз.",
        "btn_route_take": "🚚 Взял",
        "btn_route_deliver": "✅ Доставлено",
        "route_taken": "🚚 Взял для доставки.",
        "route_delivered": "✅ Доставлено.",
        "route_delivered_debt": "✅ Доставлено.\n❗ Долг: {debt} сум",
        "route_action_error": "⚠️ Не удалось выполнить: {error}",

        # ── Оплата при доставке (этап 3, под-этап 2) ──
        "pay_ask_method": "💰 Ожидаемая сумма: {expected} сум\n\nКак оплатил клиент?",
        "btn_pay_cash": "💵 Наличные",
        "btn_pay_card": "💳 Карта",
        "btn_pay_transfer": "🏦 Перевод",
        "btn_pay_none": "🚫 Без оплаты",
        "pay_ask_amount": "✏️ Введите полученную сумму (например: {expected}):",
        "pay_invalid_amount": "⚠️ Введите число (0 или больше). Например: {expected}",
        "pay_delivered_full": "✅ Доставлено. Оплата получена полностью ({amount} сум).",
        "pay_delivered_auto_discount": "✅ Доставлено.\n💸 Недоплата {shortfall} сум — скидка применена автоматически.",
        "pay_delivered_discount_pending": "✅ Доставлено.\n💸 Недоплата {shortfall} сум — заявка на скидку отправлена менеджеру на согласование.",
        "pay_mgr_notify": "💸 Заявка на скидку\nЗаказ: {num}\nСумма: {shortfall} сум\n\nВодитель: {driver}\n\nСогласование — в панели сотрудника.",
    },
    "uz": {
        "hello":          "👋",
        "lang_set":       "🇺🇿 O'zbek tili tanlandi",
        "menu_title":     "🏠 Asosiy menyu",
        "btn_order":      "📄 Ariza qoldirish",
        "ask_order_type": "📋 Ariza turini tanlang:",
        "btn_order_quick": "⚡ Tezkor ariza",
        "btn_webapp":     "🌐 Ilovani ochish",
        "btn_status":     "📦 Buyurtma holati",
        "btn_prices":     "💰 Narxlar",
        "btn_profile":    "👤 Mening profilim",
        "btn_operator":   "🎧 Operator",
        "contact_short_line": "☎️ Qisqa raqam: {num}",
        "contact_main_label": "📞 Operator:",
        "ask_service":    "🧺 Xizmatni tanlang:",
        "ask_phone":      "📞 Raqamingizni ulashing yoki qo'lda kiriting:\n\nFormat: +998XXXXXXXXX",
        "btn_share_phone": "📱 Raqamni ulashish",
        "btn_enter_phone": "⌨️ Boshqa raqam kiritish",
        "ask_phone_manual": "✏️ Raqamni quyidagi formatda kiriting:\n+998XXXXXXXXX\n\nMisol: +998901234567",
        "phone_invalid":  "⚠️ Noto'g'ri format!\n\nRaqamni qat'iy formatda kiriting:\n+998XXXXXXXXX",
        "ask_name":       "👤 Ismingizni kiriting:",
        "order_done":     "✅ Buyurtma qabul qilindi!\n\nTez orada siz bilan bog'lanamiz.",
        "order_failed":   "⚠️ Buyurtmani saqlab bo'lmadi. Birozdan keyin qayta urinib ko'ring.",
        "btn_cancel":     "❌ Bekor qilish",
        "btn_menu":       "🏠 Menyu",
        "cancelled":      "❌ Bekor qilindi.",

        # ── Bot orqali saytda ro'yxatdan o'tish ──
        "menu_register_hint": "<i>📝 Botda yoki saytda buyurtmalar holatini kuzatish uchun — pastdagi «Ro'yxatdan o'tish» tugmasini bosib ro'yxatdan o'ting</i>",
        "btn_reg_start":  "📝 Ro'yxatdan o'tish",
        "reg_ask_name":   "👤 Sizga qanday murojaat qilish kerak?",
        "btn_reg_type_name": "✏️ Boshqa ism kiritish",
        "reg_name_empty": "⚠️ Ismingizni kiriting",
        "reg_ask_phone":  "📞 Endi telefon raqamingizni ulashing:",
        "reg_already_registered": "ℹ️ <b>Sizda akkaunt allaqachon bor</b>\n\n📱 {phone}\n\nShu raqam bilan kiring.",
        "reg_success":    "✅ <b>Tayyor!</b> Kirish uchun ma'lumotlar alohida xabarda yuborildi.",
        "reg_error":      "❌ Ro'yxatdan o'tkazib bo'lmadi. Keyinroq urinib ko'ring.",
        "reg_own_phone_only": "❌ O'z raqamingizni ulashing.",

        # ── To'liq buyurtma (OrderForm) ──
        "btn_order_full": "📅 Chiqib olib ketish bilan buyurtma",
        "ask_branch":     "🏢 Filialni tanlang:",
        "ask_address":    "📍 Manzilni kiriting (shahar, ko'cha, uy):",
        "ask_service_type": "⏱ Xizmat turini tanlang:",
        "btn_type_standard": "🕓 Standart",
        "btn_type_express":  "⚡ Ekspress",
        "ask_date":       "📅 Olib ketish sanasini tanlang:",
        "btn_today":      "Bugun",
        "btn_tomorrow":   "Ertaga",
        "btn_pick_date":  "✏️ Sanani kiritish",
        "ask_date_manual": "✏️ Sanani KK.OO.YYYY formatida kiriting\n\nMisol: {example}",
        "date_invalid":   "⚠️ Noto'g'ri sana!\n\nSanani KK.OO.YYYY formatida kiriting (bugungidan oldin bo'lmasin).",
        "ask_time":       "🕐 Olib ketish vaqtini tanlang:",
        "btn_morning":    "🌅 08:00 — 13:00",
        "btn_evening":    "🌆 13:00 — 20:00",
        "btn_custom_time": "✏️ Vaqtni kiritish",
        "ask_time_from":  "🕐 Boshlanish vaqtini tanlang:",
        "ask_time_to":    "🕐 Tugash vaqtini tanlang:",
        "full_order_done": "✅ Ariza №{num} qabul qilindi!\n\nTez orada siz bilan bog'lanamiz.",
        "full_order_failed": "⚠️ Arizani saqlab bo'lmadi. Birozdan keyin qayta urinib ko'ring.",

        # ── Narx kalkulyatori (CalcForm) ──
        "btn_calc":       "🧮 Kalkulyator",
        "calc_ask_svc":   "🧮 Narx kalkulyatori\n\nXizmatni tanlang:",
        "calc_selected_header": "🧮 Narx kalkulyatori\n\nXizmat: {svc}",
        "calc_ask_w":     "Enini santimetrda kiriting:\n\nMisol: 200 (= 2 metr)",
        "calc_ask_l":     "Endi bo'yini santimetrda kiriting:\n\nMisol: 300 (= 3 metr)",
        "calc_result_below_min": "🧮 Narx hisobi\n\n📐 O'lcham: {w} × {l} sm = {sqm} {unit}\n{svc}\n💰 {price} so'm/{unit}\n\n⚠️ Sizning o'lchamingiz {sqm} {unit} — minimal buyurtmadan kam ({min_order} {unit})\n💵 Jami: {total} so'm ({min_order} {unit} uchun)",
        "calc_result_no_min": "🧮 Narx hisobi\n\n📐 O'lcham: {w} × {l} sm = {sqm} {unit}\n{svc}\n💰 {price} so'm/{unit}\n\n💵 Jami: {total} so'm",
        "calc_price_missing": "⚠️ Bu xizmat uchun narx hali sozlanmagan. Kompaniyaga murojaat qiling.",
        "invalid_num":    "⚠️ Iltimos, son kiriting. Masalan: 200",

        # ── Buyurtma holati ──
        "status_empty":      "📦 Buyurtma holati\n\nSizda hali buyurtmalar yo'q.",
        "status_menu_title": "📦 Buyurtma holati\n\nKategoriyani tanlang:",
        "status_btn_new":       "🆕 Yangi",
        "status_btn_progress":  "🔄 Bajarilmoqda",
        "status_btn_done":      "✅ Bajarildi",
        "status_btn_cancelled": "❌ Bekor qilindi",
        "status_group_empty": "Bu kategoriyada buyurtmalar yo'q.",
        "status_lead_line":   "📋 Ariza {num}\n🧺 {service}\n📌 Holat: 🆕 Yangi ariza, qo'ng'iroqni kuting",
        "status_lead_lost_line": "📋 Ariza {num}\n📌 Holat: ❌ Bekor qilindi",
        "lbl_created":        "Buyurtma yaratildi",
        "lbl_pickup":         "Olib ketish",
        "lbl_delivered":      "Yetkazildi",
        "lbl_status":         "Holat",
        "lbl_items":          "Pozitsiyalar",
        "lbl_items_total":    "Pozitsiyalar summasi",
        "lbl_due":            "To'lov uchun",
        "lbl_paid":           "To'landi",
        "lbl_remaining":      "Qolgan to'lov",
        "lbl_fully_paid":     "✅ To'liq to'landi",
        "lbl_currency":       "so'm",
        "btn_back_to_status": "◀️ Kategoriyalarga",
        "btn_order_detail":   "📋 №{num}: batafsil",
        "btn_positions":      "📄 Pozitsiyalar",
        "btn_measure_media":  "📐 O'lchash foto/video",
        "btn_photos":         "📸 Foto: oldin/keyin/shikastlanish",
        "btn_back_to_order":  "◀️ Buyurtmaga",
        "order_positions_title": "📄 №{num} buyurtma pozitsiyalari",
        "no_positions":       "Pozitsiyalar hali yo'q.",
        "no_measure_media":   "O'lchash foto/videosi hali yo'q.",
        "no_photos":          "Foto hali yo'q.",
        "order_measure_caption": "📐 O'lchash · Pozitsiya {idx}: {service}\n🧾 Buyurtma №{num}",
        "photo_type_before":  "Oldin",
        "photo_type_after":   "Keyin",
        "photo_type_damage":  "⚠️ Shikastlanish",
        "order_not_found":    "⚠️ Buyurtma topilmadi.",

        # ── Mening profilim ──
        "profile_text":   "👤 Sizning profilingiz\n\n📛 Ism: {name}\n📞 Telefon: {phone}\n🆔 ID: {uid}\n\n📊 Jami buyurtmalar: {total}\n✅ Bajarildi: {done}{last}",
        "profile_last":   "\nOxirgi buyurtma: {date}",
        "profile_nophone": "ko'rsatilmagan",
        "btn_settings":   "⚙️ Sozlamalar",
        "settings_text":  "⚙️ Sozlamalar",
        "btn_change_lang": "🌐 Tilni o'zgartirish",
        "btn_help":       "🆘 Yordam",
        "help_text":      "🆘 Yordam\n\nSavolingiz bo'lsa — asosiy menyudagi «🎧 Operator» tugmasidan foydalaning, yoki bizga qo'ng'iroq qiling.",

        # ── Operator ──
        "operator_text":  "🎧 Operator uchun xabaringizni yozing:",
        "operator_fwd":   "✅ Xabaringiz operatorga yuborildi.",

        # ── Narxlar ──
        "prices_title":   "💰 Narxlar",
        "prices_empty":   "⚠️ Narxlar hali sozlanmagan. Kompaniyaga murojaat qiling.",
        "prices_min_order_title": "📦 Min buyurtma: ",
        "prices_min_order_line": "{min_order} {unit} ({services})",
        "prices_footer_types": "Standart / Ekspress",
        "prices_footer_delivery": "🚚 Olib ketish va yetkazish — bepul",

        # ── Agent bo'lish ──
        "btn_agent":      "🤝 Agent bo'lish",
        "agent_info_text": "🤝 <b>Agent bo'lish</b>\n\nAgentlar mijozlarni jalb qilish orqali har bir buyurtmadan komissiya oladi.\n\n📋 Shartlar:\n• Kompaniya saytida ro'yxatdan o'tgan bo'lish\n• Referral havola orqali mijoz topib kelish\n• Komissiya miqdori: buyurtma summasiga qarab\n\nDavom etish uchun tasdiqlang:",
        "btn_agent_confirm": "✅ Tasdiqlash — Agent bo'lish",
        "agent_checking": "⏳ Tekshirilmoqda…",
        "agent_ask_contact": "🤝 Agent bo'lish\n\nSaytda akkaunt topilmadi.\n\nQuyidagi tugmani bosing — bot raqamingizni oladi va kompaniya saytidagi akkauntingizni topadi",
        "agent_already":  "✅ Siz allaqachon Agentsiz!\n\nAgent kabinetiga kiring:\n🔗 {url}\n\nLogin: telefon raqamingiz",
        "agent_success":  "🎉 Tabriklaymiz! Siz Agent bo'ldingiz!\n\nLogin: {phone}\nParol: saytdagi kabi\n\nKabinetga kiring:\n🔗 {url}",
        "agent_not_found": "❌ {phone} raqami kompaniya saytida topilmadi.\n\nUshbu raqam bilan saytda ro'yxatdan o'ting, so'ng yana «Agent bo'lish» tugmasini bosing",
        "agent_failed":   "❌ Tekshirib/ro'yxatdan o'tkazib bo'lmadi. Keyinroq yoki kompaniya sayti orqali urinib ko'ring.",
        "btn_open_agent_cabinet": "🎯 Agent kabinetini ochish",
        "btn_register_site": "🌐 Ro'yxatdan o'tish",

        # ── Haydovchi marshruti (/route) ──
        "route_no_access": "❌ Faqat kompaniya haydovchilari uchun.",
        "route_empty": "🚚 Bugunga marshrutlar yo'q.",
        "route_header": "🚚 Bugungi marshrut: {count} ta nuqta",
        "route_stop_line": "📋 {num}\n👤 {client}\n📞 {phone}\n📍 {address}\n📦 {items} dona",
        "btn_route_take": "🚚 Oldim",
        "btn_route_deliver": "✅ Yetkazildi",
        "route_taken": "🚚 Yetkazish uchun oldim.",
        "route_delivered": "✅ Yetkazildi.",
        "route_delivered_debt": "✅ Yetkazildi.\n❗ Qarz: {debt} so'm",
        "route_action_error": "⚠️ Bajarib bo'lmadi: {error}",

        # ── Yetkazishda to'lov (3-bosqich, 2-kichik bosqich) ──
        "pay_ask_method": "💰 Kutilayotgan summa: {expected} so'm\n\nMijoz qanday to'ladi?",
        "btn_pay_cash": "💵 Naqd",
        "btn_pay_card": "💳 Karta",
        "btn_pay_transfer": "🏦 O'tkazma",
        "btn_pay_none": "🚫 To'lovsiz",
        "pay_ask_amount": "✏️ Olingan summani kiriting (masalan: {expected}):",
        "pay_invalid_amount": "⚠️ Son kiriting (0 yoki undan katta). Masalan: {expected}",
        "pay_delivered_full": "✅ Yetkazildi. To'lov to'liq olindi ({amount} so'm).",
        "pay_delivered_auto_discount": "✅ Yetkazildi.\n💸 Kam to'lov {shortfall} so'm — chegirma avtomatik qo'llandi.",
        "pay_delivered_discount_pending": "✅ Yetkazildi.\n💸 Kam to'lov {shortfall} so'm — chegirma so'rovi menejerga yuborildi.",
        "pay_mgr_notify": "💸 Chegirma so'rovi\nBuyurtma: {num}\nSumma: {shortfall} so'm\n\nHaydovchi: {driver}\n\nTasdiqlash — xodim panelida.",
    },
}


def t(lang: str, key: str) -> str:
    return T.get(lang, T["ru"]).get(key, key)


# ══════════════════════════════════════
#  КЛАВИАТУРЫ
# ══════════════════════════════════════
def lang_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🇷🇺 Русский язык", callback_data="lang_ru")],
        [InlineKeyboardButton(text="🇺🇿 O'zbek tili",  callback_data="lang_uz")],
    ])


def menu_kb(lang: str, site_url: str, show_register: bool = False) -> InlineKeyboardMarkup:
    """Главное меню — структура рядов как в прод-боте (menu_kb ~L719-729):
    ссылка на сайт → «Оставить заявку» (ведёт в подменю быстрая/полная,
    см. menu_order ниже) → статус+цены → калькулятор+профиль → оператор →
    (если нет сайтового аккаунта) «Зарегистрироваться».
    "Стать Агентом" — кнопка внутри «Мой профиль» (см. menu_profile), не здесь."""
    rows = [
        [InlineKeyboardButton(text=t(lang, "btn_webapp"), url=site_url)],
        [InlineKeyboardButton(text=t(lang, "btn_order"), callback_data="menu_order")],
        [InlineKeyboardButton(text=t(lang, "btn_status"), callback_data="menu_status"),
         InlineKeyboardButton(text=t(lang, "btn_prices"), callback_data="menu_prices")],
        [InlineKeyboardButton(text=t(lang, "btn_calc"), callback_data="menu_calc"),
         InlineKeyboardButton(text=t(lang, "btn_profile"), callback_data="menu_profile")],
        [InlineKeyboardButton(text=t(lang, "btn_operator"), callback_data="menu_operator")],
    ]
    if show_register:
        rows.append([InlineKeyboardButton(text=t(lang, "btn_reg_start"), callback_data="reg_start")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def status_menu_kb(lang: str, counts: dict) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{t(lang, 'status_btn_new')} ({counts['new']})", callback_data="status_new"),
         InlineKeyboardButton(text=f"{t(lang, 'status_btn_progress')} ({counts['progress']})", callback_data="status_progress")],
        [InlineKeyboardButton(text=f"{t(lang, 'status_btn_done')} ({counts['done']})", callback_data="status_done"),
         InlineKeyboardButton(text=f"{t(lang, 'status_btn_cancelled')} ({counts['cancelled']})", callback_data="status_cancelled")],
        [InlineKeyboardButton(text=t(lang, "btn_menu"), callback_data="go_menu")],
    ])


def back_to_status_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "btn_back_to_status"), callback_data="menu_status")],
        [InlineKeyboardButton(text=t(lang, "btn_menu"), callback_data="go_menu")],
    ])


def cancel_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=t(lang, "btn_cancel"), callback_data="cancel_order"),
    ]])


def back_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=t(lang, "btn_menu"), callback_data="go_menu"),
    ]])


def service_kb(lang: str, services: list[dict]) -> InlineKeyboardMarkup:
    rows = []
    if services:
        for s in services:
            name = s.get(f"name_{lang}") or s.get("name_ru") or s["key"]
            emoji = s.get("emoji") or ""
            label = f"{emoji} {name}".strip()
            rows.append([InlineKeyboardButton(text=label, callback_data=f"svc_{s['key']}")])
    else:
        # Фоллбек, если у компании ещё не заполнен каталог услуг
        rows.append([InlineKeyboardButton(
            text=("🧺 Химчистка" if lang == "ru" else "🧺 Kimyoviy tozalash"),
            callback_data="svc_default")])
    rows.append([InlineKeyboardButton(text=t(lang, "btn_cancel"), callback_data="cancel_order")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def phone_kb(lang: str) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[
            KeyboardButton(text=t(lang, "btn_share_phone"), request_contact=True),
            KeyboardButton(text=t(lang, "btn_enter_phone")),
        ]],
        resize_keyboard=True, one_time_keyboard=True,
    )


# ── Клавиатуры полного заказа (OrderForm) ──
def branch_kb(lang: str, branches: list[dict]) -> InlineKeyboardMarkup:
    rows = []
    for b in branches:
        name = b.get(f"name_{lang}") or b.get("name_ru") or b.get("slug", "")
        rows.append([InlineKeyboardButton(text=name, callback_data=f"of_branch_{b['slug']}")])
    rows.append([InlineKeyboardButton(text=t(lang, "btn_cancel"), callback_data="cancel_order")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def service_type_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "btn_type_standard"), callback_data="of_svctype_standard")],
        [InlineKeyboardButton(text=t(lang, "btn_type_express"),  callback_data="of_svctype_express")],
        [InlineKeyboardButton(text=t(lang, "btn_cancel"), callback_data="cancel_order")],
    ])


_WD_RU = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
_WD_UZ = ["Du", "Se", "Ch", "Pa", "Ju", "Sh", "Ya"]


def date_kb(lang: str) -> InlineKeyboardMarkup:
    from datetime import date, timedelta
    today = date.today()
    rows, row = [], []
    for i in range(7):
        d = today + timedelta(days=i)
        date_str = d.strftime("%d.%m.%Y")
        if i == 0:
            label = t(lang, "btn_today") + f" ({d.strftime('%d.%m')})"
        elif i == 1:
            label = t(lang, "btn_tomorrow") + f" ({d.strftime('%d.%m')})"
        else:
            wd = (_WD_UZ if lang == "uz" else _WD_RU)[d.weekday()]
            label = f"{wd} {d.strftime('%d.%m')}"
        row.append(InlineKeyboardButton(text=label, callback_data=f"of_date_{date_str}"))
        if len(row) == 2:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text=t(lang, "btn_pick_date"), callback_data="of_date_pick")])
    rows.append([InlineKeyboardButton(text=t(lang, "btn_cancel"),    callback_data="cancel_order")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def time_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "btn_morning"),     callback_data="of_time_morning")],
        [InlineKeyboardButton(text=t(lang, "btn_evening"),     callback_data="of_time_evening")],
        [InlineKeyboardButton(text=t(lang, "btn_custom_time"), callback_data="of_time_custom")],
        [InlineKeyboardButton(text=t(lang, "btn_cancel"),      callback_data="cancel_order")],
    ])


_TIME_SLOTS = [f"{h:02d}:00" for h in range(8, 20)]  # 08:00..19:00


def time_from_kb(lang: str) -> InlineKeyboardMarkup:
    rows = []
    for i in range(0, 12, 3):
        rows.append([InlineKeyboardButton(text=_TIME_SLOTS[j], callback_data=f"of_tslot_from_{j+8}")
                     for j in range(i, i + 3)])
    rows.append([InlineKeyboardButton(text=t(lang, "btn_cancel"), callback_data="cancel_order")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def time_to_kb(lang: str, from_h: int) -> InlineKeyboardMarkup:
    slots = [h for h in range(8, 20) if h > from_h]
    rows, row = [], []
    for h in slots:
        row.append(InlineKeyboardButton(text=f"{h:02d}:00", callback_data=f"of_tslot_to_{h}"))
        if len(row) == 3:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text=t(lang, "btn_cancel"), callback_data="cancel_order")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ══════════════════════════════════════
#  ХЕЛПЕРЫ
# ══════════════════════════════════════
_PHONE_RE = re.compile(r"^\+998\d{9}$")


def normalize_phone_bot(raw: str) -> str:
    v = raw.strip().replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if v.startswith("998") and not v.startswith("+"):
        v = "+" + v
    return v if _PHONE_RE.match(v) else ""


async def _resolve_lang(tg_id: int, company_id: int, state: FSMContext) -> str | None:
    """Язык из FSM-данных (быстрее) или из БД (после рестарта FSM ещё жива, но на
    всякий случай проверяем и БД — переживает даже сброс состояния)."""
    data = await state.get_data()
    lang = data.get("lang")
    if lang in ("ru", "uz"):
        return lang
    try:
        lang = await db.get_bot_client_lang(tg_id, company_id)
    except Exception as e:
        logging.warning(f"get_bot_client_lang error: {e}")
        lang = None
    return lang if lang in ("ru", "uz") else None


def _notify_staff_new_lead(lead: dict) -> None:
    """Уведомляет сотрудников (веб-пуш + TG-группа филиала) о новом лиде из бота —
    та же функция, что и для лидов с сайта/CRM (admin.html, /api/bot/lead). Ленивый
    импорт из main — на момент вызова (внутри уже обработанного вебхука) main.py
    полностью загружен, обратный импорт на уровне модуля не нужен."""
    if not lead:
        return
    try:
        from main import _notify_new_lead
        bot_staff = {"role": "bot", "first_name": "Telegram", "last_name": "", "login": "bot"}
        asyncio.create_task(_notify_new_lead(lead, bot_staff))
    except Exception as e:
        logging.warning(f"_notify_staff_new_lead error: {e}")


def _h(value) -> str:
    """HTML-экранирование для текста, идущего в message.answer()/bot.send_message() —
    бот заказов создаётся с DefaultBotProperties(parse_mode="HTML") (main.py,
    _get_order_bot_instance), поэтому любой пользовательский текст (имя из Telegram,
    сообщение оператору, ответ сотрудника и т.п.), вставляемый в текст сообщения,
    должен быть экранирован — иначе Telegram отклонит sendMessage при '<'/'&'/'>'
    в исходных данных (can't parse entities)."""
    return html.escape(str(value)) if value else ""


def _company_site_url(slug: str) -> str:
    """Ссылка на витрину компании для кнопки «Открыть приложение» — НЕ поддомен:
    artez (единственная старая прод-компания на своём домене) → artez.uz,
    остальные компании SaaS → cleano.uz/?company_slug=... (та же логика, что уже
    используется в кнопке 🌐 на superadmin.html)."""
    if slug == "artez":
        return "https://artez.uz/"
    return f"https://cleano.uz/?company_slug={slug}"


async def _site_url(company_id: int) -> str:
    try:
        company = await db.get_company(company_id)
    except Exception as e:
        logging.warning(f"get_company error: {e}")
        company = None
    slug = (company["slug"] if company else "") or ""
    return _company_site_url(slug) if slug else "https://cleano.uz/"


async def _agent_cabinet_url(company_id: int) -> str:
    """Ссылка на кабинет агента (staff.html) — тот же принцип, что и _site_url."""
    try:
        company = await db.get_company(company_id)
    except Exception as e:
        logging.warning(f"get_company error: {e}")
        company = None
    slug = (company["slug"] if company else "") or ""
    if slug == "artez":
        return "https://artez.uz/staff.html"
    return f"https://cleano.uz/staff.html?company_slug={slug}" if slug else "https://cleano.uz/staff.html"


def _join_names(lang: str, names: list[str]) -> str:
    """Соединяет названия филиалов через 'и'/'va' (2 языка), 1 филиал — без союза."""
    names = [n for n in names if n]
    if not names:
        return ""
    if len(names) == 1:
        return names[0]
    sep = " va " if lang == "uz" else " и "
    return ", ".join(names[:-1]) + sep + names[-1]


def _branch_phone_list(branch: dict) -> list[str]:
    """Номера телефонов филиала — phones это JSONB-массив объектов {n, receipt, site}.
    asyncpg НЕ декодирует jsonb в dict/list автоматически (нет set_type_codec) —
    приходит сырой JSON-строкой, тот же паттерн уже используется в main.py (~L6588,
    d['phones'] = json.loads(...) if isinstance(str) else ...). Для меню бота нужен
    только 'n' (receipt/site игнорируем, см. задание)."""
    raw = branch.get("phones") or []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw) if raw else []
        except (ValueError, TypeError):
            raw = []
    out = []
    for p in raw:
        if isinstance(p, dict):
            if p.get("bot") is False:
                continue
            num = p.get("n")
        else:
            num = p
        if num:
            out.append(str(num))
    return out


async def _build_menu_text(lang: str, company_id: int, registered: bool = True) -> str:
    """Динамический текст главного меню (аналог статичного текста прод-бота,
    см. задание): название компании + тэглайн, филиалы, короткий номер (если
    задан), основной номер (всегда 2 строки), телефоны по филиалам.
    registered=False — добавляет подсказку про регистрацию (см. _show_menu)."""
    try:
        company = await db.get_company(company_id)
    except Exception as e:
        logging.warning(f"get_company error: {e}")
        company = None
    company_name = (company["name"] if company else "") or ""

    try:
        tagline = await db.get_config_for_company(f"footer_about_{lang}", company_id)
    except Exception as e:
        logging.warning(f"get_config_for_company(footer_about_{lang}) error: {e}")
        tagline = None

    try:
        branches = [dict(b) for b in await db.get_branches(company_id)]
    except Exception as e:
        logging.warning(f"get_branches error: {e}")
        branches = []

    try:
        contact_short = await db.get_config_for_company("contact_short", company_id)
    except Exception as e:
        logging.warning(f"get_config_for_company(contact_short) error: {e}")
        contact_short = None

    try:
        contact_main = await db.get_config_for_company("contact_main", company_id)
    except Exception as e:
        logging.warning(f"get_config_for_company(contact_main) error: {e}")
        contact_main = None

    lines = [t(lang, "menu_title"), ""]

    header_line = _h(company_name)
    if tagline:
        header_line = f"{header_line} — {_h(tagline)}" if header_line else _h(tagline)
    if header_line:
        lines.append(header_line)

    branch_names = [b.get(f"name_{lang}") or b.get("name_ru") or b.get("slug", "") for b in branches]
    joined = _join_names(lang, branch_names)
    if joined:
        lines.append(f"📍 {_h(joined)}")

    contact_lines = []
    if contact_short:
        contact_lines.append(t(lang, "contact_short_line").format(num=_h(contact_short)))
    if contact_main:
        contact_lines.append(t(lang, "contact_main_label"))
        contact_lines.append(_h(contact_main))
    if contact_lines:
        lines.append("")
        lines.extend(contact_lines)

    if len(branches) == 1:
        phones = _branch_phone_list(branches[0])
        if phones:
            lines.append("")
            lines.extend(f"📱 {_h(p)}" for p in phones)
    elif len(branches) > 1:
        for b in branches:
            phones = _branch_phone_list(b)
            if not phones:
                continue
            lines.append("")
            bname = b.get(f"name_{lang}") or b.get("name_ru") or b.get("slug", "")
            lines.append(_h(bname))
            lines.extend(f"📱 {_h(p)}" for p in phones)

    if not registered:
        lines.append("")
        lines.append(t(lang, "menu_register_hint"))

    return "\n".join(lines)


async def _show_menu(target, lang: str, company_id: int, uid: int | None = None) -> None:
    """target — Message/CallbackQuery.message. Показывает динамическое главное меню
    (текст + клавиатура с корректной ссылкой на витрину компании). Если передан
    uid — проверяет, есть ли у него сайтовый аккаунт этой компании, и добавляет
    подсказку + кнопку «Зарегистрироваться» (аналог _menu_text_and_kb в прод-боте)."""
    registered = True
    if uid is not None:
        try:
            registered = bool(await db.get_user_by_tg_id(uid, company_id))
        except Exception as e:
            logging.warning(f"get_user_by_tg_id error: {e}")
            registered = True  # при сбое лучше не показывать лишнюю кнопку
    header = await _build_menu_text(lang, company_id, registered)
    site_url = await _site_url(company_id)
    await target.answer(header, reply_markup=menu_kb(lang, site_url, show_register=not registered))


async def _build_prices_text(lang: str, company_id: int) -> str:
    """Текстовый прайс-лист — переиспользует те же источники данных, что и
    CalcForm (db.get_services_for_company/db.get_all_prices/_unit_symbol),
    вдохновлено build_prices_text() из прод-бота (не копия 1:1, см. задание)."""
    try:
        services = await db.get_services_for_company(company_id)
    except Exception as e:
        logging.warning(f"get_services_for_company error: {e}")
        services = []
    try:
        prices = await db.get_all_prices()
    except Exception as e:
        logging.warning(f"get_all_prices error: {e}")
        prices = {}

    currency = "so'm" if lang == "uz" else "сум"
    lines = [t(lang, "prices_title"), ""]
    any_price = False
    min_groups: dict = {}  # {(min_val, unit_sym): [svc_name,...]} — как в прод-боте

    ordered = sorted(services, key=lambda s: s.get("order_idx", 0)) if services else []
    for s in ordered:
        key = s.get("key")
        entry = prices.get(key, {})
        std = entry.get("standard")
        exp = entry.get("express")
        if not std and not exp:
            continue
        any_price = True
        name = _svc_display_name(lang, services, key)
        unit_key = (std or exp).get("unit_key") or "m2"
        unit_sym = await _unit_symbol(unit_key, lang)
        parts = []
        if std:
            parts.append(f"{std['price']:,}".replace(",", " "))
        if exp:
            parts.append(f"{exp['price']:,}".replace(",", " "))
        lines.append(f"🔹 {_h(name)}")
        lines.append(f"— {' / '.join(parts)} {currency}/{unit_sym}")
        if std and std.get("min_order"):
            mo = std["min_order"]
            mo_str = int(mo) if mo == int(mo) else mo
            min_groups.setdefault((mo_str, unit_sym), []).append(_h(name))

    if not any_price:
        lines.append(t(lang, "prices_empty"))
        return "\n".join(lines)

    lines.append("")
    if min_groups:
        lines.append(t(lang, "prices_min_order_title"))
        for (mo_str, unit_sym), svc_names in min_groups.items():
            lines.append(t(lang, "prices_min_order_line").format(min_order=mo_str, unit=unit_sym, services=", ".join(svc_names)))
    lines.append(t(lang, "prices_footer_types"))
    lines.append(t(lang, "prices_footer_delivery"))

    return "\n".join(lines)


async def _resolve_operator_group_id(company_id: int) -> str | None:
    """Группа для эскалации «Оператор» — переиспользует тот же конфиг-ключ,
    которым роутится обычный поток лидов без привязки к филиалу (main.py/
    _notify_new_lead, ключ leads_group_id — общий fallback, когда branch неизвестен).
    Филиальный роутинг по tg_leads_group_id здесь не нужен: OperatorForm не
    спрашивает у клиента филиал (в отличие от QuickForm/OrderForm)."""
    try:
        return await db.get_config_for_company("leads_group_id", company_id)
    except Exception as e:
        logging.warning(f"get_config_for_company(leads_group_id) error: {e}")
        return None


# ══════════════════════════════════════
#  /start и язык
# ══════════════════════════════════════
@router.message(CommandStart())
async def start(message: Message, company_id: int, state: FSMContext) -> None:
    await state.clear()
    uid = message.from_user.id
    lang = await _resolve_lang(uid, company_id, state)

    try:
        await db.upsert_bot_client(
            tg_id=uid, company_id=company_id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            last_name=message.from_user.last_name,
            lang=lang or "ru",
        )
    except Exception as e:
        logging.warning(f"upsert_bot_client error: {e}")

    if lang:
        await state.update_data(lang=lang)
        await _show_menu(message, lang, company_id, uid)
    else:
        await message.answer(t("ru", "hello"), reply_markup=lang_kb())


@router.callback_query(F.data.in_({"lang_ru", "lang_uz"}))
async def set_language(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    uid = call.from_user.id
    lang = "ru" if call.data == "lang_ru" else "uz"
    await state.update_data(lang=lang)
    try:
        await db.set_bot_client_lang(uid, lang, company_id)
    except Exception as e:
        logging.warning(f"set_bot_client_lang error: {e}")
    await call.message.edit_text(t(lang, "lang_set"))
    # Видео-инструкция — только первый раз для этого клиента (welcome_video_sent
    # в БД, а не в памяти — переживает рестарт бота), на выбранном языке. Хранится
    # через глобальный Cleano-бот (см. fetch_bot_welcome_video_bytes в main.py) —
    # своему боту компании (message.bot) нужны сырые байты, а не чужой file_id.
    try:
        if await db.mark_welcome_video_sent(uid, company_id):
            from main import fetch_bot_welcome_video_bytes
            video_bytes = await fetch_bot_welcome_video_bytes(company_id, lang)
            if video_bytes:
                await call.message.answer_video(BufferedInputFile(video_bytes, filename=f"welcome-{lang}.mp4"))
    except Exception as e:
        logging.warning(f"welcome video error: {e}")
    await _show_menu(call.message, lang, company_id, uid)


@router.callback_query(F.data == "go_menu")
async def go_menu(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    await state.clear()
    uid = call.from_user.id
    lang = await _resolve_lang(uid, company_id, state)
    if not lang:
        await call.message.answer(t("ru", "hello"), reply_markup=lang_kb())
        return
    await state.update_data(lang=lang)
    await _show_menu(call.message, lang, company_id, uid)


@router.callback_query(F.data == "cancel_order")
async def cancel_order(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    await state.clear()
    await state.update_data(lang=lang)
    site_url = await _site_url(company_id)
    await call.message.answer(t(lang, "cancelled"), reply_markup=menu_kb(lang, site_url))


# ══════════════════════════════════════
#  РЕГИСТРАЦИЯ НА САЙТЕ ЧЕРЕЗ БОТА (перенесено из прод-бота, ~L1466-1568).
#  Кнопка «Зарегистрироваться» появляется в главном меню только когда у
#  этого tg_id ещё нет сайтового аккаунта ЭТОЙ компании (см. _show_menu).
# ══════════════════════════════════════
@router.callback_query(F.data == "reg_start")
async def register_start(call: CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    fname = (call.from_user.first_name or "").strip()
    if call.from_user.last_name:
        fname = (fname + " " + call.from_user.last_name).strip()
    rows = []
    if fname:
        rows.append([InlineKeyboardButton(text=f"✅ {fname}", callback_data="reg_use_tgname")])
    rows.append([InlineKeyboardButton(text=t(lang, "btn_reg_type_name"), callback_data="reg_type_name")])
    rows.append([InlineKeyboardButton(text=t(lang, "btn_menu"), callback_data="go_menu")])
    await state.set_state(RegisterForm.waiting_name)
    await call.message.answer(t(lang, "reg_ask_name"), reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


@router.callback_query(RegisterForm.waiting_name, F.data == "reg_use_tgname")
async def register_name_from_tg(call: CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    name = (call.from_user.first_name or "").strip()
    if call.from_user.last_name:
        name = (name + " " + call.from_user.last_name).strip()
    await state.update_data(name=name)
    await state.set_state(RegisterForm.waiting_contact)
    await call.message.answer(t(lang, "reg_ask_phone"), reply_markup=ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text=t(lang, "btn_share_phone"), request_contact=True)],
    ], resize_keyboard=True, one_time_keyboard=True))


@router.callback_query(RegisterForm.waiting_name, F.data == "reg_type_name")
async def register_name_prompt_typed(call: CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    await call.message.answer(t(lang, "reg_ask_name"))


@router.message(RegisterForm.waiting_name)
async def register_name_typed(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    lang = data.get("lang", "ru")
    name = (message.text or "").strip()
    if not name:
        await message.answer(t(lang, "reg_name_empty"))
        return
    await state.update_data(name=name)
    await state.set_state(RegisterForm.waiting_contact)
    await message.answer(t(lang, "reg_ask_phone"), reply_markup=ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text=t(lang, "btn_share_phone"), request_contact=True)],
    ], resize_keyboard=True, one_time_keyboard=True))


@router.message(RegisterForm.waiting_contact, F.contact)
async def register_contact_received(message: Message, company_id: int, state: FSMContext) -> None:
    uid = message.from_user.id
    data = await state.get_data()
    lang = data.get("lang", "ru")
    if message.contact.user_id and int(message.contact.user_id) != uid:
        await message.answer(t(lang, "reg_own_phone_only"), reply_markup=ReplyKeyboardRemove())
        return

    phone = normalize_phone_bot(message.contact.phone_number or "")
    if not phone:
        await message.answer(t(lang, "phone_invalid"), reply_markup=ReplyKeyboardRemove())
        return

    name = (data.get("name") or message.from_user.first_name or "").strip()
    await state.clear()
    await state.update_data(lang=lang)

    await message.answer("⏳", reply_markup=ReplyKeyboardRemove())

    try:
        await db.upsert_bot_client(
            tg_id=uid, company_id=company_id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            last_name=message.from_user.last_name,
            phone=phone, lang=lang,
            tg_phone=phone,
        )
    except Exception as e:
        logging.warning(f"upsert_bot_client (register) error: {e}")

    kb_back = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=t(lang, "btn_menu"), callback_data="go_menu")]])

    try:
        from main import bot_register_client
        result = await bot_register_client(phone, name, lang, company_id, uid)
    except Exception as e:
        logging.warning(f"bot_register_client error: {e}")
        result = {"ok": False}

    if result.get("already_registered"):
        await message.answer(t(lang, "reg_already_registered").format(phone=phone), reply_markup=kb_back)
        return

    if not result.get("ok"):
        await message.answer(t(lang, "reg_error"), reply_markup=kb_back)
        return

    # Без кнопки меню — кредсы придут отдельным сообщением, там уже своя кнопка «Меню».
    await message.answer(t(lang, "reg_success"))
    creds_text = (
        f"🎉 Регистрация завершена!\n\n"
        f"📱 Номер / Логин: <code>{phone}</code>\n"
        f"🔑 Пароль: <code>{result['password']}</code>\n\n"
        f"⚠️ Пароль можно сменить в личном кабинете на сайте."
        if lang != "uz" else
        f"🎉 Ro'yxatdan o'tish yakunlandi!\n\n"
        f"📱 Raqam / Login: <code>{phone}</code>\n"
        f"🔑 Parol: <code>{result['password']}</code>\n\n"
        f"⚠️ Parolni saytdagi shaxsiy kabinetda almashtirish mumkin."
    )
    await message.answer(creds_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "btn_menu"), callback_data="go_menu")]
    ]))


# ══════════════════════════════════════
#  «ОСТАВИТЬ ЗАЯВКУ»: подменю быстрая/полная (как menu_order() в прод-боте,
#  ~L1674) → order_type_quick ведёт в QuickForm, order_type_full — в OrderForm
#  (те же имена callback'ов, что и в проде, ~L1679/1693 — задание явно просит
#  зарезервировать эти имена под новую структуру).
# ══════════════════════════════════════
@router.callback_query(F.data == "menu_order")
async def menu_order(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "btn_order_quick"), callback_data="order_type_quick")],
        [InlineKeyboardButton(text=t(lang, "btn_order_full"), callback_data="order_type_full")],
        [InlineKeyboardButton(text=t(lang, "btn_cancel"), callback_data="cancel_order")],
    ])
    await call.message.answer(t(lang, "ask_order_type"), reply_markup=kb)


@router.callback_query(F.data == "order_type_quick")
async def menu_order_quick(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    try:
        services = await db.get_services_for_company(company_id)
    except Exception as e:
        logging.warning(f"get_services_for_company error: {e}")
        services = []
    await state.set_state(QuickForm.service)
    await call.message.answer(t(lang, "ask_service"), reply_markup=service_kb(lang, services))


@router.callback_query(QuickForm.service, F.data.startswith("svc_"))
async def quick_service(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    svc_key = call.data.replace("svc_", "")
    await state.update_data(service=svc_key)
    await state.set_state(QuickForm.phone)
    await call.message.answer(t(lang, "ask_phone"), reply_markup=phone_kb(lang))


async def _finish_phone_step(message: Message, company_id: int, state: FSMContext, phone: str, verified: bool = False) -> None:
    data = await state.get_data()
    lang = data.get("lang", "ru")
    await state.update_data(phone=phone)
    # Сохраняем телефон в clients — иначе «Мой профиль» никогда не узнает номер
    # (upsert_bot_client в /start вызывается без phone). COALESCE в самом
    # upsert_bot_client не даст затереть уже сохранённый номер пустым.
    # tg_phone передаём ТОЛЬКО когда номер пришёл через «Поделиться контактом» и
    # проверен (contact.user_id == from_user.id) — см. quick_phone_contact.
    # Он используется как ключ поиска чужих заказов в «Статус заказа»/«Профиль»,
    # поэтому ручной ввод (quick_phone_text) НИКОГДА не должен его передавать.
    try:
        await db.upsert_bot_client(
            tg_id=message.from_user.id, company_id=company_id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            last_name=message.from_user.last_name,
            phone=phone, lang=lang,
            tg_phone=phone if verified else None,
        )
    except Exception as e:
        logging.warning(f"upsert_bot_client (phone) error: {e}")
    await state.set_state(QuickForm.name)
    await message.answer("✅", reply_markup=ReplyKeyboardRemove())
    await message.answer(t(lang, "ask_name"), reply_markup=cancel_kb(lang))


@router.message(QuickForm.phone, F.contact)
async def quick_phone_contact(message: Message, company_id: int, state: FSMContext) -> None:
    data = await state.get_data()
    lang = data.get("lang", "ru")
    # Клиент мог переслать ЧУЖОЙ контакт (Telegram это разрешает) — засчитываем как
    # верифицированный (tg_phone) только если это его собственный номер.
    is_own = message.contact.user_id == message.from_user.id
    norm = normalize_phone_bot(message.contact.phone_number or "")
    if not norm:
        await message.answer(t(lang, "phone_invalid"), reply_markup=phone_kb(lang))
        return
    await _finish_phone_step(message, company_id, state, norm, verified=is_own)


@router.message(QuickForm.phone, F.text)
async def quick_phone_text(message: Message, company_id: int, state: FSMContext) -> None:
    data = await state.get_data()
    lang = data.get("lang", "ru")
    raw = (message.text or "").strip()
    if raw == t(lang, "btn_enter_phone"):
        await message.answer(t(lang, "ask_phone_manual"), reply_markup=cancel_kb(lang))
        return
    norm = normalize_phone_bot(raw)
    if not norm:
        await message.answer(t(lang, "phone_invalid"), reply_markup=phone_kb(lang))
        return
    await _finish_phone_step(message, company_id, state, norm)


@router.message(QuickForm.name)
async def quick_name(message: Message, company_id: int, state: FSMContext) -> None:
    data = await state.get_data()
    lang = data.get("lang", "ru")
    name = (message.text or "").strip()
    if not name:
        await message.answer(t(lang, "ask_name"), reply_markup=cancel_kb(lang))
        return

    uid = message.from_user.id
    saved = False
    try:
        lead = await db.create_lead({
            "client_name":  name,
            "client_phone": data.get("phone", ""),
            "service":      data.get("service", ""),
            "note":         "Быстрая заявка (бот)",
            "status":       "new",
            "source":       "bot",
            "client_tg_id": uid,
        }, company_id)
        saved = bool(lead)
    except Exception as e:
        logging.error(f"create_lead error: {e}")

    if saved:
        _notify_staff_new_lead(lead)

    await state.clear()
    await state.update_data(lang=lang)
    if saved:
        await message.answer(t(lang, "order_done"), reply_markup=back_kb(lang))
    else:
        await message.answer(t(lang, "order_failed"), reply_markup=back_kb(lang))


# ══════════════════════════════════════
#  ПОЛНЫЙ ЗАКАЗ: имя → телефон → филиал → адрес → услуга → тип → дата → время → сохранение
#  (реальный порядок шагов из старого bot.py, см. docstring модуля и класса OrderForm)
# ══════════════════════════════════════
@router.callback_query(F.data == "order_type_full")
async def menu_order_full(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    await state.set_state(OrderForm.name)
    await call.message.answer(t(lang, "ask_name"), reply_markup=cancel_kb(lang))


@router.message(OrderForm.name)
async def full_name(message: Message, company_id: int, state: FSMContext) -> None:
    data = await state.get_data()
    lang = data.get("lang", "ru")
    name = (message.text or "").strip()
    if not name:
        await message.answer(t(lang, "ask_name"), reply_markup=cancel_kb(lang))
        return
    await state.update_data(name=name)
    await state.set_state(OrderForm.phone)
    await message.answer(t(lang, "ask_phone"), reply_markup=phone_kb(lang))


async def _advance_after_phone(message: Message, company_id: int, state: FSMContext, phone: str, verified: bool = False) -> None:
    data = await state.get_data()
    lang = data.get("lang", "ru")
    await state.update_data(phone=phone)
    # См. комментарий в _finish_phone_step (QuickForm) — тот же фикс для OrderForm.
    try:
        await db.upsert_bot_client(
            tg_id=message.from_user.id, company_id=company_id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            last_name=message.from_user.last_name,
            phone=phone, lang=lang,
            tg_phone=phone if verified else None,
        )
    except Exception as e:
        logging.warning(f"upsert_bot_client (phone) error: {e}")
    try:
        branches = await db.get_branches(company_id)
    except Exception as e:
        logging.warning(f"get_branches error: {e}")
        branches = []
    await message.answer("✅", reply_markup=ReplyKeyboardRemove())
    if branches:
        await state.set_state(OrderForm.branch)
        await message.answer(t(lang, "ask_branch"), reply_markup=branch_kb(lang, [dict(b) for b in branches]))
    else:
        # У компании ещё не заведены филиалы — пропускаем шаг, branch останется пустым
        await state.set_state(OrderForm.address)
        await message.answer(t(lang, "ask_address"), reply_markup=cancel_kb(lang))


@router.message(OrderForm.phone, F.contact)
async def full_phone_contact(message: Message, company_id: int, state: FSMContext) -> None:
    data = await state.get_data()
    lang = data.get("lang", "ru")
    is_own = message.contact.user_id == message.from_user.id
    norm = normalize_phone_bot(message.contact.phone_number or "")
    if not norm:
        await message.answer(t(lang, "phone_invalid"), reply_markup=phone_kb(lang))
        return
    await _advance_after_phone(message, company_id, state, norm, verified=is_own)


@router.message(OrderForm.phone, F.text)
async def full_phone_text(message: Message, company_id: int, state: FSMContext) -> None:
    data = await state.get_data()
    lang = data.get("lang", "ru")
    raw = (message.text or "").strip()
    if raw == t(lang, "btn_enter_phone"):
        await message.answer(t(lang, "ask_phone_manual"), reply_markup=cancel_kb(lang))
        return
    norm = normalize_phone_bot(raw)
    if not norm:
        await message.answer(t(lang, "phone_invalid"), reply_markup=phone_kb(lang))
        return
    await _advance_after_phone(message, company_id, state, norm)


@router.callback_query(OrderForm.branch, F.data.startswith("of_branch_"))
async def full_branch(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    slug = call.data[len("of_branch_"):]
    await state.update_data(branch=slug)
    await state.set_state(OrderForm.address)
    await call.message.answer(t(lang, "ask_address"), reply_markup=cancel_kb(lang))


@router.message(OrderForm.address)
async def full_address(message: Message, company_id: int, state: FSMContext) -> None:
    data = await state.get_data()
    lang = data.get("lang", "ru")
    address = (message.text or "").strip()
    if not address:
        await message.answer(t(lang, "ask_address"), reply_markup=cancel_kb(lang))
        return
    await state.update_data(address=address)
    try:
        services = await db.get_services_for_company(company_id)
    except Exception as e:
        logging.warning(f"get_services_for_company error: {e}")
        services = []
    await state.set_state(OrderForm.service)
    await message.answer(t(lang, "ask_service"), reply_markup=service_kb(lang, services))


@router.callback_query(OrderForm.service, F.data.startswith("svc_"))
async def full_service(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    svc_key = call.data.replace("svc_", "")
    await state.update_data(service=svc_key)
    await state.set_state(OrderForm.service_type)
    await call.message.answer(t(lang, "ask_service_type"), reply_markup=service_type_kb(lang))


@router.callback_query(OrderForm.service_type, F.data.startswith("of_svctype_"))
async def full_service_type(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    kind = call.data.replace("of_svctype_", "")
    label = t(lang, "btn_type_standard") if kind == "standard" else t(lang, "btn_type_express")
    await state.update_data(service_type=label)
    await state.set_state(OrderForm.date)
    await call.message.answer(t(lang, "ask_date"), reply_markup=date_kb(lang))


@router.callback_query(OrderForm.date, F.data.startswith("of_date_") & (F.data != "of_date_pick"))
async def full_date_btn(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    date_val = call.data[len("of_date_"):]
    await state.update_data(date=date_val)
    await state.set_state(OrderForm.time)
    await call.message.answer(t(lang, "ask_time"), reply_markup=time_kb(lang))


@router.callback_query(OrderForm.date, F.data == "of_date_pick")
async def full_date_pick(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    from datetime import date as _dt, timedelta
    example = (_dt.today() + timedelta(days=7)).strftime("%d.%m.%Y")
    await call.message.answer(t(lang, "ask_date_manual").format(example=example), reply_markup=cancel_kb(lang))


@router.message(OrderForm.date)
async def full_date_manual(message: Message, company_id: int, state: FSMContext) -> None:
    data = await state.get_data()
    lang = data.get("lang", "ru")
    text = (message.text or "").strip()
    m = re.fullmatch(r"(\d{2})\.(\d{2})\.(\d{4})", text)
    valid = False
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            from datetime import date as dt_date
            d = dt_date(year, month, day)
            if d >= dt_date.today():
                valid = True
        except ValueError:
            valid = False
    if not valid:
        await message.answer(t(lang, "date_invalid"), reply_markup=cancel_kb(lang))
        return
    await state.update_data(date=text)
    await state.set_state(OrderForm.time)
    await message.answer(t(lang, "ask_time"), reply_markup=time_kb(lang))


async def _finish_full_order(message: Message, company_id: int, state: FSMContext,
                              time_txt: str, tg_user) -> None:
    data = await state.get_data()
    lang = data.get("lang", "ru")
    uid = tg_user.id if tg_user else None

    note_parts = ["Полная заявка (бот)"]
    if data.get("service_type"):
        note_parts.append(f"Тип: {data['service_type']}")

    saved = False
    lead = None
    try:
        lead = await db.create_lead({
            "client_name":  data.get("name", ""),
            "client_phone": data.get("phone", ""),
            "service":      data.get("service", ""),
            "branch":       data.get("branch", ""),
            "address":      data.get("address", ""),
            "note":         " · ".join(note_parts),
            "status":       "new",
            "source":       "bot",
            "client_tg_id": uid,
            "pickup_date":  data.get("date", ""),
            "pickup_time":  time_txt,
        }, company_id)
        saved = bool(lead)
    except Exception as e:
        logging.error(f"create_lead error (full order): {e}")

    if saved:
        _notify_staff_new_lead(lead)

    await state.clear()
    await state.update_data(lang=lang)
    if saved:
        await message.answer(
            t(lang, "full_order_done").format(num=lead.get("lead_num", "")),
            reply_markup=back_kb(lang))
    else:
        await message.answer(t(lang, "full_order_failed"), reply_markup=back_kb(lang))


@router.callback_query(OrderForm.time, F.data.in_({"of_time_morning", "of_time_evening", "of_time_custom"}))
async def full_time_choice(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    if call.data == "of_time_morning":
        await _finish_full_order(call.message, company_id, state, "08:00 — 13:00", call.from_user)
    elif call.data == "of_time_evening":
        await _finish_full_order(call.message, company_id, state, "13:00 — 20:00", call.from_user)
    else:
        await state.set_state(OrderForm.time_from)
        await call.message.answer(t(lang, "ask_time_from"), reply_markup=time_from_kb(lang))


@router.callback_query(OrderForm.time_from, F.data.startswith("of_tslot_from_"))
async def full_time_from(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    from_h = int(call.data.split("_")[-1])
    await state.update_data(time_from_h=from_h)
    await state.set_state(OrderForm.time_to)
    await call.message.answer(t(lang, "ask_time_to"), reply_markup=time_to_kb(lang, from_h))


@router.callback_query(OrderForm.time_to, F.data.startswith("of_tslot_to_"))
async def full_time_to(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    from_h = data.get("time_from_h", 8)
    to_h = int(call.data.split("_")[-1])
    time_txt = f"{from_h:02d}:00 — {to_h:02d}:00"
    await _finish_full_order(call.message, company_id, state, time_txt, call.from_user)


# ══════════════════════════════════════
#  КАЛЬКУЛЯТОР СТОИМОСТИ (CalcForm) — услуга → тип услуги → ширина → длина →
#  результат. Анонимный, ничего не сохраняет, никакого лида не создаёт —
#  чисто информационный расчёт (площадь × цена). Перенесено из старого
#  bot.py (class CalcForm, ~L2282-2361), без module-level кэша цен: бот общий
#  на все компании в одном процессе, поэтому цены/юниты берутся заново на
#  шаге показа результата через db.get_all_prices()/db.get_all_units()
#  (contextvar company_id уже резолвится корректно к моменту вызова).
# ══════════════════════════════════════

# Фолбек символов единиц измерения — только для отображения, если в таблице
# units почему-то нет нужной записи (в норме она сидируется при init_db).
_UNIT_SYMBOL_FALLBACK = {
    "m2":  {"symbol_ru": "м²", "symbol_uz": "m²"},
    "m":   {"symbol_ru": "м",  "symbol_uz": "m"},
    "pcs": {"symbol_ru": "шт", "symbol_uz": "dona"},
    "cm":  {"symbol_ru": "см", "symbol_uz": "sm"},
    "cm2": {"symbol_ru": "см²", "symbol_uz": "sm²"},
    "kg":  {"symbol_ru": "кг", "symbol_uz": "kg"},
}


def _svc_display_name(lang: str, services: list[dict], svc_key: str) -> str:
    """Название услуги (эмодзи + имя на нужном языке) по её ключу, из уже
    полученного списка услуг компании. Фоллбек — сам ключ, если услугу
    почему-то не нашли (например, каталог поменялся между шагами)."""
    for s in services:
        if s.get("key") == svc_key:
            name = s.get(f"name_{lang}") or s.get("name_ru") or svc_key
            emoji = s.get("emoji") or ""
            return f"{emoji} {name}".strip()
    return svc_key


async def _unit_symbol(unit_key: str, lang: str) -> str:
    try:
        units = await db.get_all_units()
    except Exception as e:
        logging.warning(f"get_all_units error: {e}")
        units = []
    for u in units:
        if u["key"] == unit_key:
            return u["symbol_uz"] if lang == "uz" else u["symbol_ru"]
    fallback = _UNIT_SYMBOL_FALLBACK.get(unit_key) or _UNIT_SYMBOL_FALLBACK["m2"]
    return fallback["symbol_uz"] if lang == "uz" else fallback["symbol_ru"]


@router.callback_query(F.data == "menu_calc")
async def menu_calc(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    try:
        services = await db.get_services_for_company(company_id)
    except Exception as e:
        logging.warning(f"get_services_for_company error: {e}")
        services = []
    await state.set_state(CalcForm.service)
    await call.message.answer(t(lang, "calc_ask_svc"), reply_markup=service_kb(lang, services))


@router.callback_query(CalcForm.service, F.data.startswith("svc_"))
async def calc_service(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    svc_key = call.data.replace("svc_", "")
    try:
        services = await db.get_services_for_company(company_id)
    except Exception as e:
        logging.warning(f"get_services_for_company error: {e}")
        services = []
    svc_name = _svc_display_name(lang, services, svc_key)
    await state.update_data(calc_svc=svc_key, calc_svc_name=svc_name)
    await state.set_state(CalcForm.service_type)
    await call.message.answer(t(lang, "ask_service_type"), reply_markup=service_type_kb(lang))


@router.callback_query(CalcForm.service_type, F.data.startswith("of_svctype_"))
async def calc_service_type(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    svctype = call.data.replace("of_svctype_", "")
    type_label = t(lang, "btn_type_standard") if svctype == "standard" else t(lang, "btn_type_express")
    await state.update_data(calc_svctype=svctype, calc_svctype_label=type_label)
    await state.set_state(CalcForm.width)
    svc_display = f"{data.get('calc_svc_name', '')} ({type_label})".strip()
    header = t(lang, "calc_selected_header").format(svc=svc_display)
    await call.message.answer(header + "\n\n" + t(lang, "calc_ask_w"), reply_markup=cancel_kb(lang))


@router.message(CalcForm.width)
async def calc_width(message: Message, company_id: int, state: FSMContext) -> None:
    data = await state.get_data()
    lang = data.get("lang", "ru")
    raw = (message.text or "").strip()
    try:
        width = float(raw.replace(",", "."))
    except ValueError:
        await message.answer(t(lang, "invalid_num"))
        return
    await state.update_data(calc_w=width)
    await state.set_state(CalcForm.length)
    svc_display = f"{data.get('calc_svc_name', '')} ({data.get('calc_svctype_label', '')})".strip()
    header = t(lang, "calc_selected_header").format(svc=svc_display)
    await message.answer(header + "\n\n" + t(lang, "calc_ask_l"), reply_markup=cancel_kb(lang))


@router.message(CalcForm.length)
async def calc_length(message: Message, company_id: int, state: FSMContext) -> None:
    data = await state.get_data()
    lang = data.get("lang", "ru")
    raw = (message.text or "").strip()
    try:
        length = float(raw.replace(",", "."))
    except ValueError:
        await message.answer(t(lang, "invalid_num"))
        return

    svc = data.get("calc_svc", "")
    svctype = data.get("calc_svctype", "standard")
    width = data.get("calc_w", 0.0)

    try:
        prices = await db.get_all_prices()
    except Exception as e:
        logging.warning(f"get_all_prices error: {e}")
        prices = {}
    entry = prices.get(svc, {}).get(svctype)

    if not entry:
        await state.clear()
        await state.update_data(lang=lang)
        await message.answer(t(lang, "calc_price_missing"), reply_markup=back_kb(lang))
        return

    sqm_real = (width / 100) * (length / 100)
    min_order = entry.get("min_order")
    sqm_bill = max(sqm_real, min_order) if min_order else sqm_real
    price = entry["price"]
    total = int(sqm_bill * price)
    unit_sym = await _unit_symbol(entry.get("unit_key") or "m2", lang)
    svc_display = f"{data.get('calc_svc_name', '')} ({data.get('calc_svctype_label', '')})".strip()

    fmt_args = dict(
        w=int(width), l=int(length), sqm=round(sqm_real, 2), unit=unit_sym,
        svc=svc_display,
        price=f"{price:,}".replace(",", " "),
        total=f"{total:,}".replace(",", " "),
        min_order=min_order,
    )
    if min_order and sqm_real < min_order:
        result = t(lang, "calc_result_below_min").format(**fmt_args)
    else:
        result = t(lang, "calc_result_no_min").format(**fmt_args)

    await state.clear()
    await state.update_data(lang=lang)
    await message.answer(result, reply_markup=back_kb(lang))


# ══════════════════════════════════════
#  СТАТУС ЗАКАЗА — бот всегда создаёт ЛИД (никогда заказ напрямую), поэтому
#  «Статус заказа» показывает ДВА разных источника, не только orders:
#  - 🆕 Новые   = лиды клиента (client_tg_id), ещё не converted/lost — лид
#                 существует, но заказ из него ещё не создан сотрудником.
#  - 🔄 В работе = заказы (orders), НЕ delivered/cancelled — найдены НЕ по
#                 client_tg_id (при конвертации лида в заказ client_tg_id не
#                 копируется, см. convert_lead_to_order в main.py), а по
#                 clients.tg_phone — телефону клиента, ВЕРИФИЦИРОВАННОМУ через
#                 «Поделиться номером» в Telegram (contact.user_id проверен
#                 в quick_phone_contact/full_phone_contact).
#  - ✅ Выполнено = заказы со статусом delivered (по тому же телефону).
#  - ❌ Отменены  = лиды со статусом lost + заказы со статусом cancelled.
#
#  ⚠️ БЕЗОПАСНОСТЬ: раньше сюда же добавлялись телефоны из СОБСТВЕННЫХ лидов
#  клиента (client_phone на leads) и обычный clients.phone (мог быть введён
#  вручную) — это позволяло любому клиенту вписать ЧУЖОЙ номер телефона (свой
#  или через фейковый лид) и увидеть чужие заказы/статусы. Используем ТОЛЬКО
#  clients.tg_phone — его нельзя подделать, Telegram сам не даст поделиться
#  чужим контактом через кнопку request_contact, а ручной ввод его не трогает.
# ══════════════════════════════════════
async def _gather_status_data(uid: int, company_id: int) -> tuple[list[dict], list[dict]]:
    try:
        leads = await db.get_client_leads_by_tg(uid, company_id)
    except Exception as e:
        logging.warning(f"get_client_leads_by_tg error: {e}")
        leads = []

    try:
        client = await db.get_bot_client_by_tg_id(uid, company_id)
        verified_phone = (client or {}).get("tg_phone")
    except Exception as e:
        logging.warning(f"get_bot_client_by_tg_id error: {e}")
        verified_phone = None

    try:
        orders = await db.get_orders_by_phones([verified_phone], company_id) if verified_phone else []
    except Exception as e:
        logging.warning(f"get_orders_by_phones error: {e}")
        orders = []
    return leads, orders


@router.callback_query(F.data == "menu_status")
async def menu_status(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    uid = call.from_user.id

    leads, orders = await _gather_status_data(uid, company_id)

    if not leads and not orders:
        kb_empty = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=t(lang, "btn_order"), callback_data="menu_order")],
            [InlineKeyboardButton(text=t(lang, "btn_menu"), callback_data="go_menu")],
        ])
        await call.message.answer(t(lang, "status_empty"), reply_markup=kb_empty)
        return

    counts = {
        "new":       sum(1 for l in leads if l.get("status") not in ("converted", "lost")),
        "progress":  sum(1 for o in orders if o.get("status") not in ("delivered", "cancelled")),
        "done":      sum(1 for o in orders if o.get("status") == "delivered"),
        "cancelled": sum(1 for l in leads if l.get("status") == "lost")
                     + sum(1 for o in orders if o.get("status") == "cancelled"),
    }

    await call.message.answer(t(lang, "status_menu_title"), reply_markup=status_menu_kb(lang, counts))


@router.callback_query(F.data.in_({"status_new", "status_progress", "status_done", "status_cancelled"}))
async def show_status_group(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    group = call.data.replace("status_", "")
    uid = call.from_user.id

    leads, orders = await _gather_status_data(uid, company_id)

    lines = []
    order_nums: list[str] = []  # заказы этой группы — под каждым кнопка "подробнее" (не для лидов, у них нет детального просмотра)
    if group == "new":
        for l in leads:
            if l.get("status") in ("converted", "lost"):
                continue
            lines.append(t(lang, "status_lead_line").format(
                num=_h(l.get("lead_code") or l.get("lead_num") or f"#{l.get('id','')}"),
                service=_h(l.get("service") or l.get("note") or ""),
            ))
    elif group == "progress":
        for o in orders:
            if o.get("status") in ("delivered", "cancelled"):
                continue
            lines.append(_format_order_summary(lang, o))
            if o.get("order_num"):
                order_nums.append(o["order_num"])
    elif group == "done":
        for o in orders:
            if o.get("status") != "delivered":
                continue
            lines.append(_format_order_summary(lang, o))
            if o.get("order_num"):
                order_nums.append(o["order_num"])
    elif group == "cancelled":
        for l in leads:
            if l.get("status") != "lost":
                continue
            lines.append(t(lang, "status_lead_lost_line").format(
                num=_h(l.get("lead_code") or l.get("lead_num") or f"#{l.get('id','')}")))
        for o in orders:
            if o.get("status") != "cancelled":
                continue
            lines.append(_format_order_summary(lang, o))
            if o.get("order_num"):
                order_nums.append(o["order_num"])

    group_title_keys = {
        "new": "status_btn_new", "progress": "status_btn_progress",
        "done": "status_btn_done", "cancelled": "status_btn_cancelled",
    }
    title = t(lang, group_title_keys.get(group, "status_btn_new"))

    if not lines:
        text = f"{title}\n\n" + t(lang, "status_group_empty")
    else:
        text = "\n\n".join([f"{title}\n"] + lines)

    if order_nums:
        kb_rows = [[InlineKeyboardButton(text=t(lang, "btn_order_detail").format(num=_h(num)),
                                          callback_data=f"ordet_{num}")] for num in order_nums]
        kb_rows += back_to_status_kb(lang).inline_keyboard
        kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    else:
        kb = back_to_status_kb(lang)

    await call.message.answer(text, reply_markup=kb)


def _fmt_dt(v) -> str:
    return v.strftime("%d.%m.%Y %H:%M") if hasattr(v, "strftime") else (str(v) if v else "")


def _format_order_summary(lang: str, o: dict) -> str:
    """Сводка заказа: даты (создан/забор/доставлен), статус, кол-во позиций,
    оплата — 1-в-1 формат прод-бота (_format_order_summary в artez_bot/bot.py).
    Услуга (order.service) намеренно не показывается — заказ может содержать
    разные услуги по позициям, точный список смотрите в кнопке «Позиции»."""
    lines = [f"📋 №{_h(o.get('order_num', ''))}"]
    created = o.get("created_at")
    if created:
        lines.append(f"📅 {t(lang,'lbl_created')}: {_fmt_dt(created)}")
    if o.get("pickup_date"):
        pu = str(o["pickup_date"]) + (f" {o['pickup_time']}" if o.get("pickup_time") else "")
        lines.append(f"🚚 {t(lang,'lbl_pickup')}: {_h(pu)}")
    if o.get("delivered_at"):
        lines.append(f"✅ {t(lang,'lbl_delivered')}: {_fmt_dt(o['delivered_at'])}")
    lines.append(f"📍 {t(lang,'lbl_status')}: {_order_status_name(lang, o['status'])}")
    lines.append(f"📦 {t(lang,'lbl_items')}: {o.get('item_count') or 0}")

    pay = db.order_payment_summary(o)
    cur = t(lang, 'lbl_currency')
    if pay['items_total'] > 0:
        lines.append(f"🧾 {t(lang,'lbl_items_total')}: {_fmt_sum(pay['items_total'])} {cur}")
    lines.append(f"💰 {t(lang,'lbl_due')}: {_fmt_sum(pay['net'])} {cur}")
    if pay['paid'] > 0:
        lines.append(f"✅ {t(lang,'lbl_paid')}: {_fmt_sum(pay['paid'])} {cur}")
    if pay['debt'] >= 1000:
        lines.append(f"❗ {t(lang,'lbl_remaining')}: {_fmt_sum(pay['debt'])} {cur}")
    elif pay['net'] > 0:
        lines.append(t(lang,'lbl_fully_paid'))
    return "\n".join(lines)


# ══════════════════════════════════════
#  ДЕТАЛЬНЫЙ ПРОСМОТР ЗАКАЗА — позиции / фото-видео замера / фото до-после-
#  повреждение прямо в боте (перенесено из прод-бота artez_bot/bot.py).
#  Плоская маршрутизация по префиксу callback_data (без FSM), как в проде:
#  ordet_{num} → карточка заказа, opos_/omed_/ophoto_{num} → вкладки. Только
#  для заказов (не лидов — у лида ещё нет позиций/фото).
# ══════════════════════════════════════
def _svc_label(lang: str, item: dict) -> str:
    if lang == "uz" and item.get("service_uz"):
        return item["service_uz"]
    return item.get("service_ru") or item.get("service") or ""


def _item_meta_str(lang: str, item: dict) -> str:
    unit_m2 = "m²" if lang == "uz" else "м²"
    unit_sum = "so'm" if lang == "uz" else "сум"
    parts = []
    w, l = item.get("width_cm"), item.get("length_cm")
    if w and l:
        parts.append(f"{w}×{l} {'sm' if lang == 'uz' else 'см'}")
    if item.get("sqm"):
        parts.append(f"{item['sqm']} {unit_m2}")
    if item.get("price_per_sqm"):
        parts.append(f"{item['price_per_sqm']} {unit_sum}/{unit_m2}")
    if item.get("total_sum"):
        parts.append(f"= {item['total_sum']} {unit_sum}")
    return " · ".join(str(p) for p in parts)


def _order_detail_kb(lang: str, order_num: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "btn_positions"),     callback_data=f"opos_{order_num}")],
        [InlineKeyboardButton(text=t(lang, "btn_measure_media"), callback_data=f"omed_{order_num}")],
        [InlineKeyboardButton(text=t(lang, "btn_photos"),        callback_data=f"ophoto_{order_num}")],
        [InlineKeyboardButton(text=t(lang, "btn_back_to_status"), callback_data="menu_status")],
        [InlineKeyboardButton(text=t(lang, "btn_menu"), callback_data="go_menu")],
    ])


def _back_to_order_kb(lang: str, order_num: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "btn_back_to_order"), callback_data=f"ordet_{order_num}")],
        [InlineKeyboardButton(text=t(lang, "btn_menu"), callback_data="go_menu")],
    ])


async def _client_owned_order(order_num: str, uid: int, company_id: int) -> dict | None:
    """Заказ по номеру, только если принадлежит этому tg_id (через верифицированный
    tg_phone, тот же путь владения, что в _gather_status_data)."""
    try:
        client = await db.get_bot_client_by_tg_id(uid, company_id)
        phone = (client or {}).get("tg_phone")
    except Exception as e:
        logging.warning(f"get_bot_client_by_tg_id error: {e}")
        phone = None
    if not phone:
        return None
    try:
        order = await db.get_order_by_num_and_phone(order_num, phone, company_id)
    except Exception as e:
        logging.warning(f"get_order_by_num_and_phone error: {e}")
        return None
    return order or None


@router.callback_query(F.data.startswith("ordet_"))
async def show_order_detail(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    order_num = call.data[len("ordet_"):]
    uid = call.from_user.id

    order = await _client_owned_order(order_num, uid, company_id)
    if not order:
        await call.message.answer(t(lang, "order_not_found"), reply_markup=back_to_status_kb(lang))
        return

    text = _format_order_summary(lang, order)
    await call.message.answer(text, reply_markup=_order_detail_kb(lang, order_num))


@router.callback_query(F.data.startswith("opos_"))
async def show_order_positions(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    order_num = call.data[len("opos_"):]
    uid = call.from_user.id

    order = await _client_owned_order(order_num, uid, company_id)
    if not order:
        await call.message.answer(t(lang, "order_not_found"), reply_markup=back_to_status_kb(lang))
        return

    try:
        items = await db.get_order_items(order["id"])
    except Exception as e:
        logging.warning(f"get_order_items error: {e}")
        items = []

    if not items:
        await call.message.answer(t(lang, "no_positions"), reply_markup=_back_to_order_kb(lang, order_num))
        return

    lines = [t(lang, "order_positions_title").format(num=_h(order_num))]
    for i, it in enumerate(items, 1):
        line = f"{i}. {_h(_svc_label(lang, it))}"
        meta = _item_meta_str(lang, it)
        if meta:
            line += f"\n{meta}"
        lines.append(line)
    await call.message.answer("\n\n".join(lines), reply_markup=_back_to_order_kb(lang, order_num))


@router.callback_query(F.data.startswith("omed_"))
async def show_order_measure_media(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    order_num = call.data[len("omed_"):]
    uid = call.from_user.id

    order = await _client_owned_order(order_num, uid, company_id)
    if not order:
        await call.message.answer(t(lang, "order_not_found"), reply_markup=back_to_status_kb(lang))
        return

    try:
        items = await db.get_order_items(order["id"])
    except Exception as e:
        logging.warning(f"get_order_items error: {e}")
        items = []

    from main import fetch_global_bot_file_bytes

    sent_any = False
    for i, it in enumerate(items, 1):
        try:
            media = await db.get_item_media(it["id"])
        except Exception as e:
            logging.warning(f"get_item_media error: {e}")
            media = []
        if not media:
            continue
        caption = t(lang, "order_measure_caption").format(idx=i, service=_h(_svc_label(lang, it)), num=_h(order_num))
        meta = _item_meta_str(lang, it)
        if meta:
            caption += f"\n{meta}"
        try:
            # tg_file_id принадлежит глобальному Cleano-боту (media_ch), а не боту
            # компании — reuse напрямую не работает, поэтому байты скачиваются и
            # пересылаются заново (см. fetch_global_bot_file_bytes в main.py).
            if len(media) == 1:
                m = media[0]
                fbytes = await fetch_global_bot_file_bytes(m["tg_file_id"])
                if not fbytes:
                    continue
                ext = "mp4" if m.get("tg_file_type") == "video" else "jpg"
                fin = BufferedInputFile(fbytes, filename=f"measure-{it['id']}.{ext}")
                if m.get("tg_file_type") == "video":
                    await call.bot.send_video(uid, fin, caption=caption, protect_content=True)
                else:
                    await call.bot.send_photo(uid, fin, caption=caption, protect_content=True)
            else:
                # Лимит Telegram-альбома — 10 файлов; caption можно задать только первому элементу.
                group = []
                for idx, m in enumerate(media[:10]):
                    fbytes = await fetch_global_bot_file_bytes(m["tg_file_id"])
                    if not fbytes:
                        continue
                    ext = "mp4" if m.get("tg_file_type") == "video" else "jpg"
                    fin = BufferedInputFile(fbytes, filename=f"measure-{it['id']}-{idx}.{ext}")
                    cls = InputMediaVideo if m.get("tg_file_type") == "video" else InputMediaPhoto
                    group.append(cls(media=fin, caption=caption if idx == 0 else None))
                if not group:
                    continue
                await call.bot.send_media_group(uid, group, protect_content=True)
            # Отмечаем успех ТОЛЬКО после реальной отправки (как в прод-боте) — иначе
            # при сбое отправки (напр. невалидный/чужой tg_file_id) sent_any уже был бы
            # True и фолбэк "Фото/видео замера пока нет" не показался бы — бот молчал
            # бы совсем, без единого сообщения пользователю.
            sent_any = True
        except Exception as e:
            logging.warning(f"send measure media error: {e}")

    if not sent_any:
        await call.message.answer(t(lang, "no_measure_media"), reply_markup=_back_to_order_kb(lang, order_num))


@router.callback_query(F.data.startswith("ophoto_"))
async def show_order_photos(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    order_num = call.data[len("ophoto_"):]
    uid = call.from_user.id

    order = await _client_owned_order(order_num, uid, company_id)
    if not order:
        await call.message.answer(t(lang, "order_not_found"), reply_markup=back_to_status_kb(lang))
        return

    try:
        photos = await db.get_order_photos(order["id"])
    except Exception as e:
        logging.warning(f"get_order_photos error: {e}")
        photos = []

    if not photos:
        await call.message.answer(t(lang, "no_photos"), reply_markup=_back_to_order_kb(lang, order_num))
        return

    from main import fetch_global_bot_file_bytes

    type_keys = {"before": "photo_type_before", "after": "photo_type_after", "damage": "photo_type_damage"}
    for p in photos:
        caption = t(lang, type_keys.get(p.get("photo_type"), "photo_type_damage"))
        try:
            # tg_file_id принадлежит глобальному Cleano-боту (media_ch), а не боту
            # компании — reuse напрямую не работает, поэтому байты скачиваются и
            # пересылаются заново (см. fetch_global_bot_file_bytes в main.py).
            fbytes = await fetch_global_bot_file_bytes(p["tg_file_id"])
            if not fbytes:
                continue
            ext = "mp4" if p.get("tg_file_type") == "video" else "jpg"
            fin = BufferedInputFile(fbytes, filename=f"photo-{p.get('id', 0)}.{ext}")
            if p.get("tg_file_type") == "video":
                await call.bot.send_video(uid, fin, caption=caption, protect_content=True)
            else:
                await call.bot.send_photo(uid, fin, caption=caption, protect_content=True)
        except Exception as e:
            logging.warning(f"send order photo error: {e}")

    # Как в прод-боте: подтверждение шлётся ВСЕГДА после цикла — иначе, если
    # send_photo/send_video упадёт на всех фото (напр. невалидный tg_file_id),
    # пользователь не получит от бота вообще ничего.
    await call.message.answer("✅", reply_markup=_back_to_order_kb(lang, order_num))


# ══════════════════════════════════════
#  ЦЕНЫ — текстовый прайс-лист (см. _build_prices_text выше).
# ══════════════════════════════════════
@router.callback_query(F.data == "menu_prices")
async def menu_prices(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    text = await _build_prices_text(lang, company_id)
    await call.message.answer(text, reply_markup=back_kb(lang))


@router.message(Command("prices"))
async def cmd_prices(message: Message, company_id: int, state: FSMContext) -> None:
    """Слэш-команда /prices — раньше был только доступ через кнопку меню,
    /prices как текст не совпадал ни с одним обработчиком и падал в общий
    фоллбэк (главное меню)."""
    lang = await _resolve_lang(message.from_user.id, company_id, state) or "ru"
    text = await _build_prices_text(lang, company_id)
    await message.answer(text, reply_markup=back_kb(lang))


# ══════════════════════════════════════
#  МОЙ ПРОФИЛЬ — имя из Telegram (как в прод-боте, НЕ из БД), телефон/заказы —
#  из БД. Формат сверен с прод-ботом (📛/📞/🆔/📊/✅). «Стать Агентом» — кнопка
#  здесь же, см. menu_agent/agent_confirm ниже (company_id теперь корректно
#  прокинут через get_user_by_tg_id/get_user_by_phone, см. security-фикс).
#
#  Заказы для статистики ищутся ТАК ЖЕ, как в «Статус заказа» (_gather_status_data,
#  по телефону, не по client_tg_id) — бот никогда не создаёт заказ напрямую, и
#  при конвертации лида в заказ client_tg_id не переносится, так что поиск по
#  одному только tg_id почти всегда возвращал бы 0 заказов у реальных клиентов.
# ══════════════════════════════════════
@router.callback_query(F.data == "menu_profile")
async def menu_profile(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    uid = call.from_user.id

    try:
        client = await db.get_bot_client_by_tg_id(uid, company_id)
    except Exception as e:
        logging.warning(f"get_bot_client_by_tg_id error: {e}")
        client = None
    _, orders = await _gather_status_data(uid, company_id)

    total = len(orders)
    done = sum(1 for o in orders if o.get("status") in STATUS_GROUPS["done"])
    last_line = ""
    if orders:
        ts = orders[0].get("created_at")
        if ts:
            last_d = ts.strftime("%d.%m.%Y") if hasattr(ts, "strftime") else str(ts)[:10]
            last_line = t(lang, "profile_last").format(date=last_d)

    name_parts = [call.from_user.first_name or "", call.from_user.last_name or ""]
    name = " ".join(p for p in name_parts if p) or "—"
    phone = (client or {}).get("phone") or t(lang, "profile_nophone")

    text = t(lang, "profile_text").format(
        name=_h(name), phone=_h(phone), uid=uid, total=total, done=done, last=last_line,
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "btn_agent"), callback_data="menu_agent")],
        [InlineKeyboardButton(text=t(lang, "btn_settings"), callback_data="menu_settings")],
        [InlineKeyboardButton(text=t(lang, "btn_menu"), callback_data="go_menu")],
    ])
    await call.message.answer(text, reply_markup=kb)


def settings_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "btn_change_lang"), callback_data="settings_lang")],
        [InlineKeyboardButton(text=t(lang, "btn_help"), callback_data="menu_help")],
        [InlineKeyboardButton(text=t(lang, "btn_menu"), callback_data="go_menu")],
    ])


@router.callback_query(F.data == "menu_settings")
async def menu_settings(call: CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    await call.message.answer(t(lang, "settings_text"), reply_markup=settings_kb(lang))


@router.callback_query(F.data == "settings_lang")
async def settings_lang(call: CallbackQuery, state: FSMContext) -> None:
    """Показывает выбор языка повторно — обрабатывается уже существующим
    set_language (callback_data lang_ru/lang_uz), как при первом /start."""
    await call.answer()
    await call.message.answer(t("ru", "hello"), reply_markup=lang_kb())


@router.callback_query(F.data == "menu_help")
async def menu_help(call: CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    await call.message.answer(t(lang, "help_text"), reply_markup=back_kb(lang))


# ══════════════════════════════════════
#  СТАТЬ АГЕНТОМ — перенесено из прод-бота (menu_agent/agent_confirm/
#  agent_contact_received, ~L1214-1304). Использует уже company-scoped
#  _agent_status_for_bot/_agent_apply_for_bot из main.py (см. security-фикс
#  2026-07-29: get_user_by_tg_id/get_user_by_phone/link_user_tg_id теперь
#  ОБЯЗАТЕЛЬНО принимают company_id — раньше могли зацепить чужую компанию).
#
#  Если верифицированный tg_phone уже известен (клиент когда-то делился СВОИМ
#  контактом — см. security-фикс phone spoofing) — используем его сразу, не
#  спрашивая контакт повторно. Иначе просим поделиться контактом здесь же.
# ══════════════════════════════════════
async def _do_agent_check(uid: int, company_id: int, phone: str | None, lang: str, message: Message) -> None:
    """phone=None — ещё не знаем номер (первое нажатие «Подтвердить», ничего не
    просили) → при "не найдено" просто просим контакт. phone задан (уже
    верифицирован — либо из tg_phone, либо только что получен через контакт) →
    при "не найдено" показываем номер и предлагаем зарегистрироваться на сайте."""
    from main import _agent_status_for_bot, _agent_apply_for_bot

    try:
        status = await _agent_status_for_bot(uid, phone, company_id)
    except Exception as e:
        logging.warning(f"_agent_status_for_bot error: {e}")
        await message.answer(t(lang, "agent_failed"), reply_markup=back_kb(lang))
        return

    cabinet_url = await _agent_cabinet_url(company_id)

    if status.get("is_agent"):
        await message.answer(
            t(lang, "agent_already").format(url=cabinet_url),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=t(lang, "btn_open_agent_cabinet"), url=cabinet_url)],
                [InlineKeyboardButton(text=t(lang, "btn_menu"), callback_data="go_menu")],
            ]))
        return

    if status.get("has_site_account"):
        try:
            result = await _agent_apply_for_bot(uid, phone, company_id)
        except Exception as e:
            logging.warning(f"_agent_apply_for_bot error: {e}")
            result = {"ok": False}
        if result.get("ok"):
            p = result.get("phone", "")
            await message.answer(
                t(lang, "agent_success").format(phone=_h(p), url=cabinet_url),
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text=t(lang, "btn_open_agent_cabinet"), url=cabinet_url)],
                    [InlineKeyboardButton(text=t(lang, "btn_menu"), callback_data="go_menu")],
                ]))
        else:
            await message.answer(t(lang, "agent_failed"), reply_markup=back_kb(lang))
        return

    if phone:
        await message.answer(
            t(lang, "agent_not_found").format(phone=_h(phone)),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=t(lang, "btn_register_site"), url=await _site_url(company_id))],
                [InlineKeyboardButton(text=t(lang, "btn_menu"), callback_data="go_menu")],
            ]))
    else:
        await message.answer(
            t(lang, "agent_ask_contact"),
            reply_markup=ReplyKeyboardMarkup(keyboard=[
                [KeyboardButton(text=t(lang, "btn_share_phone"), request_contact=True)],
            ], resize_keyboard=True, one_time_keyboard=True))


@router.callback_query(F.data == "menu_agent")
async def menu_agent(call: CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    await call.message.answer(t(lang, "agent_info_text"), reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "btn_agent_confirm"), callback_data="agent_confirm")],
        [InlineKeyboardButton(text=t(lang, "btn_cancel"), callback_data="go_menu")],
    ]))


@router.callback_query(F.data == "agent_confirm")
async def agent_confirm(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    uid = call.from_user.id
    await call.message.answer(t(lang, "agent_checking"))

    try:
        client = await db.get_bot_client_by_tg_id(uid, company_id)
    except Exception as e:
        logging.warning(f"get_bot_client_by_tg_id error: {e}")
        client = None
    tg_phone = (client or {}).get("tg_phone")

    if not tg_phone:
        await state.set_state(AgentForm.waiting_contact)
    await _do_agent_check(uid, company_id, tg_phone, lang, call.message)


@router.message(AgentForm.waiting_contact, F.contact)
async def agent_contact_received(message: Message, company_id: int, state: FSMContext) -> None:
    data = await state.get_data()
    lang = data.get("lang", "ru")
    await state.clear()
    await state.update_data(lang=lang)
    uid = message.from_user.id
    await message.answer(t(lang, "agent_checking"), reply_markup=ReplyKeyboardRemove())

    # Клиент мог переслать ЧУЖОЙ контакт — считаем верифицированным (и сохраняем
    # как tg_phone) ТОЛЬКО если это его собственный номер (см. security-фикс
    # phone spoofing, 2026-07-29 — тот же принцип и здесь: иначе можно было бы
    # привязать чужой номер/сайт-аккаунт к своему tg_id и стать агентом за него).
    is_own = message.contact.user_id == message.from_user.id
    norm = normalize_phone_bot(message.contact.phone_number or "")
    if not norm or not is_own:
        await _do_agent_check(uid, company_id, norm or None, lang, message)
        return

    try:
        await db.upsert_bot_client(
            tg_id=uid, company_id=company_id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            last_name=message.from_user.last_name,
            phone=norm, lang=lang, tg_phone=norm,
        )
    except Exception as e:
        logging.warning(f"upsert_bot_client (agent contact) error: {e}")

    await _do_agent_check(uid, company_id, norm, lang, message)


# ══════════════════════════════════════
#  ОПЕРАТОР — клиент пишет сообщение (OperatorForm) → уходит в TG-группу лидов
#  компании (_resolve_operator_group_id выше) с кнопкой «Ответить клиенту» →
#  сотрудник нажимает её (admin_reply_start/AdminReplyForm) → следующее его
#  сообщение пересылается клиенту (admin_reply_send). Перенесено из прод-бота
#  (menu_operator/operator_message/admin_reply_start, ~L1577-1671).
# ══════════════════════════════════════
@router.callback_query(F.data == "menu_operator")
async def menu_operator(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    await call.answer()
    data = await state.get_data()
    lang = data.get("lang", "ru")
    await state.set_state(OperatorForm.message)
    await call.message.answer(t(lang, "operator_text"), reply_markup=cancel_kb(lang))


@router.message(OperatorForm.message)
async def operator_message(message: Message, company_id: int, state: FSMContext) -> None:
    data = await state.get_data()
    lang = data.get("lang", "ru")
    uid = message.from_user.id
    username = message.from_user.username or ""
    fullname = f"{message.from_user.first_name or ''} {message.from_user.last_name or ''}".strip() or "—"
    text_body = (message.text or "").strip()

    if not text_body:
        await message.answer(t(lang, "operator_text"), reply_markup=cancel_kb(lang))
        return

    group_id = await _resolve_operator_group_id(company_id)
    if group_id:
        tg_link = f"tg://user?id={uid}"
        group_text = (
            f"💬 <b>Сообщение от клиента</b>\n"
            f"━━━━━━━━━━\n"
            f"👤 {_h(fullname)}" + (f" | @{_h(username)}" if username else "") + "\n"
            f"🆔 <code>{uid}</code>\n"
            f"━━━━━━━━━━\n"
            f"📝 {_h(text_body)}\n"
            f"━━━━━━━━━━"
        )
        reply_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="↩️ Ответить клиенту", callback_data=f"reply_to_{uid}")],
            [InlineKeyboardButton(text="📱 Открыть чат", url=tg_link)],
        ])
        try:
            await message.bot.send_message(int(group_id), group_text, reply_markup=reply_kb)
        except Exception as e:
            logging.error(f"operator_message: send to group failed: {e}")
    else:
        logging.warning(f"operator_message: no leads_group_id configured for company_id={company_id}")

    await state.clear()
    await state.update_data(lang=lang)
    await message.answer(t(lang, "operator_fwd"), reply_markup=back_kb(lang))


@router.callback_query(F.data.startswith("reply_to_"))
async def admin_reply_start(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    """Сотрудник (не клиент!) нажал «Ответить клиенту» в группе — FSM-ключ здесь
    строится по (chat_id группы, tg_id сотрудника), см. PostgresStorage.

    БЕЗОПАСНОСТЬ: без этой проверки ЛЮБОЙ участник группы (не обязательно
    сотрудник) мог нажать кнопку и написать клиенту сообщение от имени
    компании — см. cb_take_lead, тот же паттерн проверки."""
    staff = await db.get_staff_by_tg_id_and_company(call.from_user.id, company_id)
    if not staff:
        await call.answer("Ваш Telegram не привязан к аккаунту сотрудника.", show_alert=True)
        return
    await call.answer()
    try:
        client_id = int(call.data.replace("reply_to_", ""))
    except ValueError:
        return
    await state.set_state(AdminReplyForm.waiting_reply)
    await state.update_data(reply_to_client=client_id)
    await call.message.answer(
        f"✏️ Напишите ответ клиенту {client_id}:\n(следующее сообщение будет отправлено ему)",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_admin_reply"),
        ]]),
    )


@router.callback_query(F.data == "cancel_admin_reply")
async def admin_reply_cancel(call: CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    await state.clear()
    try:
        await call.message.edit_text("❌ Отменено.")
    except Exception:
        pass


@router.message(AdminReplyForm.waiting_reply)
async def admin_reply_send(message: Message, company_id: int, state: FSMContext) -> None:
    data = await state.get_data()
    client_id = data.get("reply_to_client")
    reply_text = (message.text or "").strip()

    if client_id and reply_text:
        try:
            client_lang = await db.get_bot_client_lang(client_id, company_id) or "ru"
        except Exception as e:
            logging.warning(f"get_bot_client_lang error: {e}")
            client_lang = "ru"
        client_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=t(client_lang, "btn_operator"), callback_data="menu_operator")],
            [InlineKeyboardButton(text=t(client_lang, "btn_menu"), callback_data="go_menu")],
        ])
        try:
            await message.bot.send_message(
                client_id,
                f"📩 <b>Сообщение от оператора</b>\n\n{_h(reply_text)}",
                reply_markup=client_kb,
            )
            await message.answer(f"✅ Ответ отправлен клиенту {client_id}")
        except Exception as e:
            logging.error(f"admin_reply_send error: {e}")
            await message.answer(f"⚠️ Не удалось отправить: {e}")
    else:
        await message.answer("⚠️ Не удалось определить клиента для ответа.")

    await state.clear()


# ══════════════════════════════════════
#  СОТРУДНИКИ: «Взять лид» — кнопка в уведомлении о новом лиде (_notify_new_lead,
#  main.py). Логика зеркалит /api/tg/webhook (легаси, single-tenant), но здесь —
#  через db.take_lead(), уже принимающий явный company_id.
# ══════════════════════════════════════
@router.callback_query(F.data.startswith("take_lead_"))
async def cb_take_lead(call: CallbackQuery, company_id: int) -> None:
    try:
        lead_id = int(call.data.replace("take_lead_", ""))
    except ValueError:
        await call.answer("Ошибка", show_alert=True)
        return

    staff = await db.get_staff_by_tg_id_and_company(call.from_user.id, company_id)
    if not staff:
        await call.answer("Ваш Telegram не привязан к аккаунту сотрудника.", show_alert=True)
        return
    if staff.get("role") == "agent":
        await call.answer("Агенты не могут брать лиды через Telegram.", show_alert=True)
        return

    staff_id = staff["id"]
    staff_name = f"{staff.get('first_name','')} {staff.get('last_name','')}".strip() or staff.get("login", "")

    try:
        status, taker_name, taker_verb = await db.take_lead(lead_id, staff_id, staff_name, company_id)
    except Exception as e:
        logging.error(f"take_lead error: {e}")
        await call.answer("Ошибка сервера. Попробуйте ещё раз.", show_alert=True)
        return

    orig_text = call.message.text or call.message.caption or ""

    if status == "not_found":
        await call.answer("Лид не найден", show_alert=True)
    elif status == "taken":
        await call.answer(f"Лид уже взят: {taker_name or 'другой сотрудник'}", show_alert=True)
        new_text = orig_text.rstrip("━").rstrip() + f"\n━━━━━━━━━━\n✅ {taker_verb}: {taker_name or 'другой сотрудник'}"
        try:
            await call.message.edit_text(new_text)
        except Exception:
            pass
    elif status == "already_mine":
        await call.answer("Этот лид уже ваш!")
    elif status == "ok":
        took_verb = "Взяла" if staff.get("gender") == "F" else "Взял"
        await call.answer("Лид взят! Откройте приложение.")
        new_text = orig_text.rstrip("━").rstrip() + f"\n━━━━━━━━━━\n✅ {took_verb}: {staff_name}"
        try:
            await call.message.edit_text(new_text)
        except Exception:
            pass
    else:
        await call.answer("Ошибка сервера. Попробуйте ещё раз.", show_alert=True)


# ══════════════════════════════════════
#  МАРШРУТ ВОДИТЕЛЯ — этап 3 (первый под-этап: только просмотр + «Взял»/
#  «Доставлено», БЕЗ приёма оплаты — оплата/долг/скидка отдельным под-этапом,
#  см. память project_artez_bot_saas_migration). Личный чат бот↔водитель (НЕ
#  группа компании — водитель не должен видеть чужие заказы).
#
#  Переиспользует ТУ ЖЕ бизнес-логику, что и web (`staff.html` → «Доставка»):
#  _driver_take_delivery_core/_driver_deliver_core вынесены в main.py из
#  /api/staff/my-route/stops/{id}/take-delivery и .../deliver (тот же паттерн,
#  что _agent_status_for_bot/_agent_apply_for_bot — прямой вызов в процессе,
#  без HTTP-петли на себя же). db.get_routes_today ТЕПЕРЬ требует company_id
#  (см. security-фикс 2026-07-29 — раньше отдавала маршруты ВСЕХ компаний).
# ══════════════════════════════════════
def _can_drive_bot(staff: dict) -> bool:
    return bool(staff.get("can_drive")) or staff.get("role") == "driver"


async def _format_route_stop(lang: str, stop: dict) -> str:
    client = " ".join(filter(None, [stop.get("client_first_name"), stop.get("client_last_name")])) or "—"
    return t(lang, "route_stop_line").format(
        num=_h(stop.get("order_num") or stop["order_id"]),
        client=_h(client),
        phone=_h(stop.get("client_phone") or "—"),
        address=_h(stop.get("short_address") or stop.get("address") or "—"),
        items=stop.get("item_count", 0),
    )


def _route_stop_kb(lang: str, order_id: int, order_status: str, stop_status: str) -> InlineKeyboardMarkup | None:
    rows = []
    if order_status == "ready":
        rows.append([InlineKeyboardButton(text=t(lang, "btn_route_take"), callback_data=f"drv_take_{order_id}")])
    if stop_status != "done" and order_status in ("ready", "delivery"):
        rows.append([InlineKeyboardButton(text=t(lang, "btn_route_deliver"), callback_data=f"drv_deliver_{order_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None


async def _show_route(message: Message, lang: str, company_id: int, staff: dict) -> None:
    try:
        routes = await db.get_routes_today(company_id, staff.get("branch"))
    except Exception as e:
        logging.warning(f"get_routes_today error: {e}")
        routes = []
    stops = [s for r in routes for s in r.get("stops", [])]
    if not stops:
        await message.answer(t(lang, "route_empty"))
        return
    await message.answer(t(lang, "route_header").format(count=len(stops)))
    for stop in stops:
        text = await _format_route_stop(lang, stop)
        kb = _route_stop_kb(lang, stop["order_id"], stop.get("order_status"), stop.get("stop_status"))
        await message.answer(text, reply_markup=kb)


@router.message(Command("route"))
async def cmd_route(message: Message, company_id: int, state: FSMContext) -> None:
    lang = await _resolve_lang(message.from_user.id, company_id, state) or "ru"
    try:
        staff = await db.get_staff_by_tg_id_and_company(message.from_user.id, company_id)
    except Exception as e:
        logging.warning(f"get_staff_by_tg_id_and_company error: {e}")
        staff = None
    if not staff or not _can_drive_bot(staff):
        await message.answer(t(lang, "route_no_access"))
        return
    await _show_route(message, lang, company_id, dict(staff))


@router.callback_query(F.data.startswith("drv_take_"))
async def drv_take(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    data = await state.get_data()
    lang = data.get("lang", "ru")
    try:
        order_id = int(call.data.replace("drv_take_", ""))
    except ValueError:
        await call.answer()
        return
    staff = await db.get_staff_by_tg_id_and_company(call.from_user.id, company_id)
    if not staff or not _can_drive_bot(staff):
        await call.answer(t(lang, "route_no_access"), show_alert=True)
        return
    await call.answer()
    from main import _driver_take_delivery_core
    try:
        await _driver_take_delivery_core(order_id, dict(staff), source="бот")
    except Exception as e:
        error = getattr(e, "detail", str(e))
        await call.message.answer(t(lang, "route_action_error").format(error=_h(error)))
        return
    await call.message.answer(t(lang, "route_taken"))
    try:
        await call.message.edit_reply_markup(reply_markup=_route_stop_kb(lang, order_id, "delivery", "pending"))
    except Exception:
        pass


def _fmt_sum(v) -> str:
    return f"{int(round(v)):,}".replace(",", " ")


@router.callback_query(F.data.startswith("drv_deliver_"))
async def drv_deliver(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    """«✅ Доставлено» — сначала спрашиваем способ оплаты (drv_pay_method),
    сама доставка (_driver_deliver_core) выполняется ПОСЛЕ ввода суммы, см.
    driver_payment_method/driver_payment_amount ниже."""
    data = await state.get_data()
    lang = data.get("lang", "ru")
    try:
        order_id = int(call.data.replace("drv_deliver_", ""))
    except ValueError:
        await call.answer()
        return
    staff = await db.get_staff_by_tg_id_and_company(call.from_user.id, company_id)
    if not staff or not _can_drive_bot(staff):
        await call.answer(t(lang, "route_no_access"), show_alert=True)
        return
    await call.answer()
    try:
        expected = await db.get_order_debt_amount(order_id)
    except Exception as e:
        logging.warning(f"get_order_debt_amount error: {e}")
        expected = 0
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "btn_pay_cash"), callback_data=f"drv_paym_cash_{order_id}"),
         InlineKeyboardButton(text=t(lang, "btn_pay_card"), callback_data=f"drv_paym_card_{order_id}")],
        [InlineKeyboardButton(text=t(lang, "btn_pay_transfer"), callback_data=f"drv_paym_transfer_{order_id}"),
         InlineKeyboardButton(text=t(lang, "btn_pay_none"), callback_data=f"drv_paym_none_{order_id}")],
    ])
    await call.message.answer(t(lang, "pay_ask_method").format(expected=_fmt_sum(expected)), reply_markup=kb)
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass


async def _finish_delivery_with_payment(message: Message, company_id: int, lang: str,
                                         staff: dict, order_id: int, method: str, amount: float) -> None:
    """Общий хвост: записать оплату (если есть) → если недоплата — авто-скидка
    (<1000 сум) или заявка менеджеру → отметить заказ доставленным. Тот же
    триггер, что handle_payment_amount в старом bot.py (~L3792), которого не
    хватало в SaaS (см. память project_artez_bot_saas_migration, находка #5)."""
    try:
        expected = await db.get_order_debt_amount(order_id)
    except Exception as e:
        logging.warning(f"get_order_debt_amount error: {e}")
        expected = 0

    if amount > 0:
        try:
            await db.add_order_payment(order_id, amount, method, "delivery",
                                        "Оплата при доставке (бот)", _driver_name_bot(staff),
                                        created_by_staff_id=staff["id"])
        except Exception as e:
            logging.warning(f"add_order_payment error: {e}")

    shortfall = max(0.0, expected - amount)
    AUTO_THRESHOLD = 1000.0
    if shortfall <= 0:
        result_text = t(lang, "pay_delivered_full").format(amount=_fmt_sum(amount))
    elif shortfall < AUTO_THRESHOLD:
        try:
            await db.apply_auto_discount(order_id, shortfall, company_id)
        except Exception as e:
            logging.warning(f"apply_auto_discount error: {e}")
        result_text = t(lang, "pay_delivered_auto_discount").format(shortfall=_fmt_sum(shortfall))
    else:
        order_num = str(order_id)
        try:
            order = await db.get_order_by_id(order_id)
            order_num = (order or {}).get("order_num") or order_num
        except Exception as e:
            logging.warning(f"get_order_by_id error: {e}")
        try:
            await db.create_discount_request(order_id, order_num, message.from_user.id, shortfall)
        except Exception as e:
            logging.warning(f"create_discount_request error: {e}")
        result_text = t(lang, "pay_delivered_discount_pending").format(shortfall=_fmt_sum(shortfall))
        try:
            managers = await db.get_managers_with_push(company_id)
        except Exception as e:
            logging.warning(f"get_managers_with_push error: {e}")
            managers = []
        mgr_text = t(lang, "pay_mgr_notify").format(
            num=_h(order_num), shortfall=_fmt_sum(shortfall), driver=_h(_driver_name_bot(staff)))
        for mgr in managers:
            try:
                await message.bot.send_message(int(mgr["tg_id"]), mgr_text)
            except Exception as e:
                logging.warning(f"discount notify mgr {mgr.get('id')}: {e}")

    from main import _driver_deliver_core
    try:
        await _driver_deliver_core(order_id, staff, method="", amount=0, source="бот")
    except Exception as e:
        error = getattr(e, "detail", str(e))
        result_text += "\n" + t(lang, "route_action_error").format(error=_h(error))
    await message.answer(result_text)


def _driver_name_bot(staff: dict) -> str:
    return " ".join(filter(None, [staff.get("last_name"), staff.get("first_name")])) or staff.get("login", "Водитель")


@router.callback_query(F.data.startswith("drv_paym_"))
async def driver_payment_method(call: CallbackQuery, company_id: int, state: FSMContext) -> None:
    data = await state.get_data()
    lang = data.get("lang", "ru")
    try:
        rest = call.data.replace("drv_paym_", "")
        method, order_id_str = rest.rsplit("_", 1)
        order_id = int(order_id_str)
    except ValueError:
        await call.answer()
        return
    staff = await db.get_staff_by_tg_id_and_company(call.from_user.id, company_id)
    if not staff or not _can_drive_bot(staff):
        await call.answer(t(lang, "route_no_access"), show_alert=True)
        return
    await call.answer()
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    if method == "none":
        await _finish_delivery_with_payment(call.message, company_id, lang, dict(staff), order_id, "", 0.0)
        return

    try:
        expected = await db.get_order_debt_amount(order_id)
    except Exception as e:
        logging.warning(f"get_order_debt_amount error: {e}")
        expected = 0
    await state.set_state(DriverPaymentForm.waiting_amount)
    await state.update_data(lang=lang, pay_order_id=order_id, pay_method=method)
    await call.message.answer(t(lang, "pay_ask_amount").format(expected=_fmt_sum(expected)))


@router.message(DriverPaymentForm.waiting_amount, F.text)
async def driver_payment_amount(message: Message, company_id: int, state: FSMContext) -> None:
    data = await state.get_data()
    lang = data.get("lang", "ru")
    order_id = data.get("pay_order_id")
    method = data.get("pay_method", "")
    raw = (message.text or "").strip().replace(" ", "").replace("\xa0", "").replace(",", ".")
    try:
        amount = float(raw)
        if amount < 0:
            raise ValueError
    except ValueError:
        try:
            expected = await db.get_order_debt_amount(order_id)
        except Exception:
            expected = 0
        await message.answer(t(lang, "pay_invalid_amount").format(expected=_fmt_sum(expected)))
        return

    staff = await db.get_staff_by_tg_id_and_company(message.from_user.id, company_id)
    await state.clear()
    await state.update_data(lang=lang)
    if not staff or not _can_drive_bot(staff) or order_id is None:
        await message.answer(t(lang, "route_no_access"))
        return
    await _finish_delivery_with_payment(message, company_id, lang, dict(staff), int(order_id), method, amount)


# ══════════════════════════════════════
#  ФОЛЛБЕК: любой другой текст вне известных состояний
# ══════════════════════════════════════
@router.message(F.chat.type == "private", F.text)
async def echo_fallback(message: Message, company_id: int, state: FSMContext) -> None:
    """Ловит текст вне известных шагов формы (например, если ждали нажатия кнопки) —
    просто возвращает клиента в главное меню. Ограничено приватными чатами: бот
    теперь состоит в группе лидов (уведомления/«Взять лид»/«Оператор»→«Ответить»),
    и без этого фильтра любое сообщение сотрудника в группе (не по теме "Ответить")
    получало бы в ответ клиентское меню — фильтр по chat.type это предотвращает."""
    data = await state.get_data()
    lang = data.get("lang") or await _resolve_lang(message.from_user.id, company_id, state) or "ru"
    await state.clear()
    await state.update_data(lang=lang)
    await _show_menu(message, lang, company_id, message.from_user.id)
