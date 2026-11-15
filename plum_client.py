"""PLUM Payment Gate API v2.0.1 (pay.myuzcard.uz) — тонкий HTTP-клиент. Cleano SaaS: перенос Э3 ARTEZ (2026-11-15).

SaaS: у КАЖДОЙ компании свой договор PLUM — настройки (base_url, login, password, proxy_key, prefix)
берутся НЕ из env, а из настроек компании; pay.py ставит их на время операции: `with using(conf, test): …`.

Взят из ARTEZ (artez_api/plum_client.py) ← Bus App (app/plum/client.py).
Ответ PLUM: {"result": {...}, "error": {"errorCode", "errorMessage"} | null}.
Неясный исход платёжного запроса (таймаут, обрыв, «проверьте позже», неизвестная ошибка) → PlumPending:
деньги могли списаться, решает сверка getTransactionByExt.
НОМЕР КАРТЫ, СРОК И КОД НЕ ЛОГИРУЮТСЯ: в лог идут только путь и код ошибки.

Тестировщики платят ПОДСТАВНЫМ банком (plum_fake.FakePlum) прямо внутри API: `with test_mode(True): …`.
"""

from __future__ import annotations

import base64
import contextvars
import logging
import os
from contextlib import contextmanager

import httpx

log = logging.getLogger("plum")
TIMEOUT = 20.0
TRANSPORT: httpx.AsyncBaseTransport | None = None  # тесты могут подставить свой транспорт для «настоящего» банка

_GROUPS: dict[str, tuple[int, ...]] = {
    "plum_otp_invalid": (-137, -269),
    "plum_otp_expired": (-113, -270),
    "plum_otp_limit": (-110, -111, -139),
    "plum_no_money": (-116, -240, -260, 116, 51),
    "plum_card_invalid": (-102, -202, -212, -216, -31101, -31102),
    "plum_card_expired": (-103, -140, -201),
    "plum_card_blocked": (-104, -141, -204, -205, -261, -31700),
    "plum_no_sms": (-127, -206),
    "plum_corporate": (-115, -131, -142, -258),
    "plum_limit": (-135, -151, -262, -292, -293),
    "plum_min_amount": (-129, -130),
    "plum_card_exists": (-108,),
    "plum_card_other": (-153,),
    "plum_unavailable": (-120, -125, -126, -143, -144, -300, -303, -304, -305, -306, -401, -500, 500, -999),
}
ERRORS: dict[int, str] = {c: k for k, codes in _GROUPS.items() for c in codes}
# платёж закрываем как «не прошёл» только при явном отказе, когда деньги точно не списаны
DEFINITIVE = frozenset(c for k, codes in _GROUPS.items() if k != "plum_unavailable" for c in codes) | {-101, -114, -122, -401, -143, -144}
NOT_FOUND = frozenset({-121, -208, -267})
MERCHANT = frozenset({-401, -143, -144})

# статусы транзакции PLUM (getTransactionByExt)
ST_PAID = 1
ST_FINAL_FAIL = frozenset({2, 4, 11, 12})
ST_NEEDS_REVERSE = 9
ST_WAIT = frozenset({3, 6, 10})


class PlumError(Exception):
    def __init__(self, code: str, plum_code: int | None = None):
        super().__init__(code if plum_code is None else f"{code} ({plum_code})")
        self.code, self.plum_code = code, plum_code


class PlumPending(Exception):
    """Исход неизвестен — проверить позже по extraId."""


_TEST: contextvars.ContextVar[bool] = contextvars.ContextVar("plum_test", default=False)


@contextmanager
def test_mode(on: bool):
    token = _TEST.set(bool(on))
    try:
        yield
    finally:
        _TEST.reset(token)


def is_test() -> bool:
    return _TEST.get()


_CONF: contextvars.ContextVar[dict | None] = contextvars.ContextVar("plum_conf", default=None)


@contextmanager
def using(conf: dict | None, test: bool = False):
    """Настройки PLUM компании + режим подставного банка на время операции."""
    t1, t2 = _CONF.set(conf), _TEST.set(bool(test))
    try:
        yield
    finally:
        _TEST.reset(t2)
        _CONF.reset(t1)


def conf_ready(c: dict | None) -> bool:
    return bool(c and c.get("base_url") and c.get("login") and c.get("password"))


def real_configured() -> bool:
    return conf_ready(_CONF.get())


def enabled() -> bool:
    return True if _TEST.get() else real_configured()


def prefix() -> str:
    """Префикс userId/extraId компании (общий посредник/договор не путает компании); по умолчанию c{id}-."""
    c = _CONF.get() or {}
    return c.get("prefix") or "cln-"


def mask(number: str) -> str:
    d = "".join(ch for ch in number if ch.isdigit())
    return f"{d[:4]} **** **** {d[-4:]}" if len(d) >= 12 else "****"


async def _call(method: str, path: str, *, json: dict | None = None, params: dict | None = None,
                lang: str = "ru", payment: bool = False) -> dict:
    if _TEST.get():
        import plum_fake
        transport, base, login, password, proxy_key = await plum_fake.transport(), "http://plum-fake.local/api/", "fake", "fake", ""
    else:
        if not real_configured():
            raise PlumError("plum_disabled")
        c = _CONF.get() or {}
        transport, base = TRANSPORT, c.get("base_url", "")
        login, password, proxy_key = c.get("login", ""), c.get("password", ""), c.get("proxy_key", "")
    headers = {"Accept": "application/json", "Language": "uz" if (lang or "").startswith("uz") else "ru"}
    if proxy_key:  # через посредника: его ключ + Basic auth в отдельном заголовке (хостинг срезает Authorization)
        token = base64.b64encode(f"{login}:{password}".encode()).decode()
        headers |= {"X-ZC-Key": proxy_key, "X-ZC-Auth": f"Basic {token}"}
    try:
        async with httpx.AsyncClient(base_url=base.rstrip("/") + "/", timeout=TIMEOUT, transport=transport,
                                     auth=(login, password)) as c:
            r = await c.request(method, path, json=json, params=params, headers=headers)
    except httpx.HTTPError as e:
        log.warning("plum %s: %s", path, type(e).__name__)
        if payment:
            raise PlumPending() from None
        raise PlumError("plum_unavailable") from None
    finally:
        if _TEST.get():
            import plum_fake
            await plum_fake.save()
    try:
        body = r.json()
    except ValueError:
        body = None
    if not isinstance(body, dict):
        log.warning("plum %s: http %s, not json", path, r.status_code)
        if payment:
            raise PlumPending()
        raise PlumError("plum_unavailable")
    err = body.get("error")
    if err:
        code = err.get("errorCode") if isinstance(err.get("errorCode"), int) else None
        (log.error if code in MERCHANT else log.info)("plum %s: error %s", path, code)
        if payment and code not in DEFINITIVE:
            raise PlumPending()
        if code in NOT_FOUND:
            raise PlumError("plum_not_found", code)
        raise PlumError(ERRORS.get(code, "plum_failed"), code)
    if r.status_code != 200 or not isinstance(body.get("result"), dict):
        log.warning("plum %s: http %s without result", path, r.status_code)
        if payment:
            raise PlumPending()
        raise PlumError("plum_unavailable")
    return body["result"]


async def create_card(user_id: str, number: str, expire: str, phone: str, lang: str = "ru") -> dict:
    return await _call("POST", "UserCard/createUserCard", lang=lang,
                       json={"userId": user_id, "cardNumber": number, "expireDate": expire, "userPhone": phone})


async def confirm_card(session: int, otp: str, lang: str = "ru") -> dict:
    res = await _call("POST", "UserCard/confirmUserCardCreate", lang=lang, json={"session": session, "otp": otp, "isTrusted": 1})
    return res.get("card") or res


async def user_cards(user_id: str, lang: str = "ru") -> list[dict]:
    return (await _call("GET", "UserCard/getAllUserCards", params={"userId": user_id}, lang=lang)).get("cards") or []


async def delete_card(binding_id: int) -> bool:
    return bool((await _call("DELETE", "UserCard/deleteUserCard", params={"userCardId": binding_id})).get("success"))


async def resend_otp(session: int, lang: str = "ru") -> dict:
    return await _call("GET", "UserCard/resendOtp", params={"session": session}, lang=lang)


async def pay_new(amount: int, number: str, expire: str, extra_id: str, data: str, lang: str = "ru") -> dict:
    return await _call("POST", "Payment/paymentWithoutRegistration", lang=lang, payment=True,
                       json={"amount": amount, "cardNumber": number, "expireDate": expire, "extraId": extra_id,
                             "transactionData": data})


async def pay_card(user_id: str, card_id: int, amount: int, extra_id: str, send_otp: bool, data: str, lang: str = "ru") -> dict:
    return await _call("POST", "Payment/payment", lang=lang, payment=True,
                       json={"userId": user_id, "cardId": card_id, "amount": amount, "extraId": extra_id,
                             "sendOtp": send_otp, "transactionData": data})


async def confirm_pay(session: int, otp: str, lang: str = "ru") -> dict:
    return await _call("POST", "Payment/confirmPayment", lang=lang, payment=True, json={"session": session, "otp": otp})


async def transaction(extra_id: str) -> dict | None:
    try:
        res = await _call("GET", "Payment/getTransactionByExt", params={"extraId": extra_id})
    except PlumError as e:
        if e.code == "plum_not_found":
            return None
        raise
    return res.get("transaction") or res


async def reverse(transaction_id: int) -> dict:
    res = await _call("POST", "Payment/paymentReverse", json={"transactionId": transaction_id})
    return res.get("transaction") or res
