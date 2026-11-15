"""Подставной банк PLUM для тестировщиков на боевом сервере и для автотестов (Э3, 2026-11-15).

Взят из Bus App (app/plum/fake.py). Настоящие деньги не списываются.
- Код из SMS всегда 111111.
- Принимает ТОЛЬКО тестовые карты (TEST_CARDS) — чтобы никто не ввёл сюда настоящий номер;
  номер карты не хранится, только отпечаток sha256.
- Состояние (карты, платежи) хранится в таблице pay_fake_state — переживает перезапуск API.
Рычаги для автотестов: fail_next {путь: errorCode}, timeout_next {путь}, balance, force_status {extraId: статус}.
"""

from __future__ import annotations

import asyncio
import hashlib
import itertools
import json
import random
from datetime import datetime

import httpx

OTP = "111111"
TEST_CARDS = {"8600123456789012": "12/30", "9860123456789015": "12/30"}  # Uzcard, Humo (проходят Luhn)
_lock = asyncio.Lock()


def _h(n: str) -> str:
    return hashlib.sha256(n.encode()).hexdigest()


class FakePlum:
    def __init__(self) -> None:
        self.fail_next: dict[str, int] = {}
        self.timeout_next: set[str] = set()
        self.balance = 100_000_000
        self.force_status: dict[str, int] = {}
        self.reversed: list[int] = []
        self.calls: list[str] = []
        self._ids = itertools.count(random.randrange(10**6, 10**9))
        self.sessions: dict[int, dict] = {}
        self.cards: dict[int, dict] = {}
        self.tx: dict[str, dict] = {}
        self.transport = httpx.MockTransport(self.handle)

    def dump(self) -> dict:
        nid = next(self._ids)
        self._ids = itertools.count(nid)
        return {"balance": self.balance, "cards": self.cards, "tx": self.tx, "next_id": nid,
                "sessions": {str(k): v for k, v in self.sessions.items()}}

    def load(self, d: dict) -> None:
        self.balance = d.get("balance", self.balance)
        self.cards = {int(k): v for k, v in (d.get("cards") or {}).items()}
        self.tx = d.get("tx") or {}
        self.sessions = {int(k): v for k, v in (d.get("sessions") or {}).items()}
        if d.get("next_id"):
            self._ids = itertools.count(int(d["next_id"]))

    def _ok(self, result: dict) -> httpx.Response:
        return httpx.Response(200, json={"result": result, "error": None})

    def _err(self, code: int) -> httpx.Response:
        return httpx.Response(200, json={"result": None, "error": {"errorCode": code, "errorMessage": "fake"}})

    def _session(self, **data) -> dict:
        sid = next(self._ids)
        self.sessions[sid] = data
        return {"session": sid, "otpSentPhone": "99890*****01"}

    @staticmethod
    def _mask(n: str) -> str:
        return f"{n[:6]}******{n[-4:]}"

    def _new_tx(self, extra: str, amount: int, card_id: int, user_id: str, number_mask: str) -> dict:
        if extra in self.tx:
            raise KeyError(extra)
        t = {"id": next(self._ids), "userId": user_id, "cardId": card_id, "cardNumber": number_mask, "extraId": extra,
             "amount": amount, "totalAmount": amount, "utrno": f"U{extra}", "status": 0}
        self.tx[extra] = t
        return t

    def _finish(self, t: dict) -> httpx.Response | None:
        if self.balance < t["amount"]:
            t["status"] = 2
            return self._err(-116)
        self.balance -= t["amount"]
        t["status"] = 1
        return None

    def _tx_out(self, t: dict) -> dict:
        return {"transactionId": t["id"], "utrno": t["utrno"], "status": t["status"], "cardNumber": t["cardNumber"],
                "amount": t["amount"], "cardId": t["cardId"], "commission": 0, "totalAmount": t["amount"],
                "date": datetime.now().isoformat()}

    def handle(self, req: httpx.Request) -> httpx.Response:
        path = req.url.path.split("/api/", 1)[-1].lstrip("/")
        self.calls.append(path)
        if path in self.fail_next:
            return self._err(self.fail_next.pop(path))
        body = json.loads(req.content or b"{}") if req.method in ("POST", "PUT") else {}
        resp = self._route(path, body, dict(req.url.params))
        if path in self.timeout_next:
            self.timeout_next.discard(path)
            raise httpx.ReadTimeout("fake timeout", request=req)
        return resp

    @staticmethod
    def _card_ok(n: str, exp: str) -> bool:
        return TEST_CARDS.get(n) is not None

    def _route(self, path: str, b: dict, q: dict) -> httpx.Response:
        if path == "UserCard/createUserCard":
            n = b["cardNumber"]
            if not self._card_ok(n, b.get("expireDate", "")):
                return self._err(-102)
            if any(c["number_hash"] == _h(n) and c["userId"] == b["userId"] for c in self.cards.values()):
                return self._err(-108)
            return self._ok(self._session(kind="bind", user=b["userId"], number_hash=_h(n), number_mask=self._mask(n),
                                          pc=1 if n.startswith("9860") else 0, expire=b["expireDate"]))
        if path == "UserCard/confirmUserCardCreate":
            s = self.sessions.get(b["session"])
            if not s or s["kind"] != "bind":
                return self._err(-101)
            if b["otp"] != OTP:
                return self._err(-137)
            self.sessions.pop(b["session"])
            bid, cid = next(self._ids), next(self._ids)
            card = {"id": bid, "userId": s["user"], "cardId": cid, "owner": "TEST OWNER", "number": s["number_mask"],
                    "expireDate": s["expire"], "isTrusted": b.get("isTrusted", 0), "status": 0,
                    "pcType": s["pc"], "number_hash": s["number_hash"]}
            self.cards[bid] = card
            return self._ok({"card": {k: v for k, v in card.items() if k != "number_hash"}})
        if path == "UserCard/getAllUserCards":
            return self._ok({"cards": [{k: v for k, v in c.items() if k != "number_hash"}
                                       for c in self.cards.values() if c["userId"] == q.get("userId")]})
        if path == "UserCard/deleteUserCard":
            return self._ok({"success": self.cards.pop(int(q["userCardId"]), None) is not None})
        if path == "UserCard/resendOtp":
            s = self.sessions.pop(int(q["session"]), None)
            return self._err(-101) if s is None else self._ok(self._session(**s))
        if path == "Payment/paymentWithoutRegistration":
            if not self._card_ok(b["cardNumber"], b.get("expireDate", "")):
                return self._err(-102)
            try:
                t = self._new_tx(b["extraId"], b["amount"], 0, "", self._mask(b["cardNumber"]))
            except KeyError:
                return self._err(-122)
            return self._ok({**self._session(kind="pay", extra=b["extraId"]), "transactionId": t["id"]})
        if path == "Payment/payment":
            card = next((c for c in self.cards.values() if c["cardId"] == b["cardId"] and c["userId"] == b["userId"]), None)
            if card is None:
                return self._err(-114)
            try:
                t = self._new_tx(b["extraId"], b["amount"], card["cardId"], b["userId"], card["number"])
            except KeyError:
                return self._err(-122)
            if b.get("sendOtp") or not card["isTrusted"]:
                return self._ok({**self._session(kind="pay", extra=b["extraId"]), "transactionId": t["id"]})
            fail = self._finish(t)
            return fail or self._ok(self._tx_out(t))
        if path == "Payment/confirmPayment":
            s = self.sessions.get(b["session"])
            if not s or s["kind"] != "pay":
                return self._err(-101)
            if b["otp"] != OTP:
                return self._err(-137)
            self.sessions.pop(b["session"])
            t = self.tx[s["extra"]]
            fail = self._finish(t)
            return fail or self._ok(self._tx_out(t))
        if path == "Payment/getTransactionByExt":
            t = self.tx.get(q.get("extraId", ""))
            if t is None:
                return self._err(-121)
            return self._ok({"transaction": {**t, "status": self.force_status.get(t["extraId"], t["status"])}})
        if path == "Payment/paymentReverse":
            t = next((x for x in self.tx.values() if x["id"] == b["transactionId"]), None)
            if t is None:
                return self._err(-121)
            self.reversed.append(t["id"])
            if t["status"] == 1:
                self.balance += t["amount"]
            t["status"] = 4
            self.force_status.pop(t["extraId"], None)
            return self._ok({"transaction": dict(t)})
        return self._err(-101)


FAKE: FakePlum | None = None


async def _pool():
    import database as db
    return db.pool


async def transport() -> httpx.MockTransport:
    """Транспорт подставного банка; при первом обращении поднимаем состояние из БД."""
    global FAKE
    if FAKE is None:
        async with _lock:
            if FAKE is None:
                f = FakePlum()
                pool = await _pool()
                if pool:
                    async with pool.acquire() as c:
                        raw = await c.fetchval("SELECT state FROM pay_fake_state WHERE id=1")
                    if raw:
                        try:
                            f.load(json.loads(raw) if isinstance(raw, str) else raw)
                        except (ValueError, TypeError):
                            pass
                FAKE = f
    return FAKE.transport


async def save() -> None:
    if FAKE is None:
        return
    pool = await _pool()
    if not pool:
        return
    data = json.dumps(FAKE.dump())
    async with pool.acquire() as c:
        await c.execute("INSERT INTO pay_fake_state (id, state, updated_at) VALUES (1, $1::jsonb, NOW()) "
                        "ON CONFLICT (id) DO UPDATE SET state=EXCLUDED.state, updated_at=NOW()", data)
