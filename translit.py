"""Узбекская латиница → кириллица для языка «Ўзбекча» (uz_cyrl).

Основа — C:\\Projects\\_shared\\snippets\\translit.py (как в EEET Bus) + правки:
- «ts» НЕ превращается в «ц» (иначе ketsa → кеца, aytsa → айца), кроме заимствований
  -tsiya → -ция (pozitsiya → позиция), -ansiya → -анция (kvitansiya), aksiya → акция;
- «e» → «э» только в начале слова (после не-буквы);
- не трогаем: код (`...`, <code>), ссылки, домены, @ники, /команды, {переменные},
  HTML-теги/сущности, слова с цифрами (номера заказов), бренды (ARTEZ, Telegram, ...);
- словарь ручных исправлений FIX (слова, которые побуквенно пишутся иначе).
Кириллица и прочие символы проходят как есть, поэтому функция безопасна для
русского текста и повторного вызова.
"""

import re

_APOS = "'’‘`ʼʻ"

# Ручные исправления: латиница (нижний регистр) → кириллица
FIX = {
    "yanvar": "январь", "fevral": "февраль", "aprel": "апрель", "iyun": "июнь", "iyul": "июль",
    "sentabr": "сентябрь", "sentyabr": "сентябрь", "oktabr": "октябрь", "oktyabr": "октябрь",
    "noyabr": "ноябрь", "dekabr": "декабрь",
    "kalkulyator": "калькулятор", "kalkulyatori": "калькулятори",
    "obyekt": "объект", "subyekt": "субъект",
    "litsenziya": "лицензия", "sertifikat": "сертификат",
    "ya": "як", "ch": "чо", "sh": "ша",   # сокращения дней недели (Ya/Ch/Sh)
    "sex": "цех", "sexi": "цехи", "sexda": "цехда", "sexga": "цехга",
}

# Не транслитерируем (как есть)
_KEEP_WORDS = r"ARTEZ|Artez|Telegram|WhatsApp|Instagram|Click|Payme|Uzum|PLUM|Plum|SmartBill|Face ID|Touch ID|PIN|SMS|ID|OK|QR|GPS|Wi-Fi|Expo|Excel|PDF|Yandex|Google|Uzcard|UzCard|UZCARD|Humo|HUMO|Visa|VISA|Mastercard|MasterCard"
_PROTECT = re.compile(
    r"`[^`]*`"                       # Markdown-код
    r"|<code>.*?</code>|<pre>.*?</pre>"
    r"|<[^>]+>|&[#\w]+;"             # HTML-теги и сущности
    r"|https?://\S+|tg://\S+|t\.me/\S+"
    r"|\b[\w.\\-]+\\?\.(?:uz|com|ru|me|org|net|html)\b(?:/\S*)?"   # домены, файлы (artez.uz, staff.html)
    r"|(?<![\w/])[@/][A-Za-z_]\w*"   # @ники и /команды (не «so'm/m²»)
    r"|\$?\{[^{}]*\}"                # {name}, ${x}
    r"|\b[A-Z]{2,}-\d+|\b[A-Za-z]+\d\w*"   # номера заказов (ART-1023), коды (ab12)
    r"|\b(?:" + _KEEP_WORDS + r")",    # бренд + узбекское окончание: Excelga → Excelга
    re.S,
)

_MULTI = [("Tsiya", "Ция"), ("tsiya", "ция"), ("tsiy", "ций"), ("ansiya", "анция"), ("Aksiya", "Акция"), ("aksiya", "акция"),
          ("Kuryer", "Курьер"), ("kuryer", "курьер"),
          ("shʼ", "сҳ"), ("Sh", "Ш"), ("SH", "Ш"), ("sh", "ш"), ("Ch", "Ч"), ("CH", "Ч"), ("ch", "ч"),
          ("Yoʻ", "Йў"), ("YOʻ", "ЙЎ"), ("yoʻ", "йў"), ("Oʻ", "Ў"), ("oʻ", "ў"), ("Gʻ", "Ғ"), ("gʻ", "ғ"),
          ("YO", "Ё"), ("Yo", "Ё"), ("yo", "ё"), ("YU", "Ю"), ("Yu", "Ю"), ("yu", "ю"),
          ("YA", "Я"), ("Ya", "Я"), ("ya", "я"), ("YE", "Е"), ("Ye", "Е"), ("ye", "е"), ("ʼ", "ъ")]
_ONE = dict(zip("ABDEFGHIJKLMNOPQRSTUVXYZabdefghijklmnopqrstuvxyz",
                "АБДЕФГҲИЖКЛМНОПҚРСТУВХЙЗабдефгҳижклмнопқрстувхйз"))
_ONE.update({"C": "К", "c": "к"})   # «c» в узбекской латинице нет (кроме ch): Call-markaz → Калл-марказ
_LAT = "A-Za-zʻʼ"
_WORD = re.compile(rf"[{_LAT}]+")


def norm_uz(s: str) -> str:
    """O'zbek / Oʼzbek / O‘zbek → Oʻzbek; апостроф между буквами → ʼ (кавычки не трогаем)."""
    s = re.sub(rf"([OoGg])[{_APOS}]", lambda m: m.group(1) + "ʻ", s)
    return re.sub(rf"(?<=[A-Za-z])['’‘`ʼ](?=[A-Za-z])", "ʼ", s)


def _word(w: str) -> str:
    fix = FIX.get(w.lower())
    if fix:
        if w.isupper() and len(w) > 1:
            return fix.upper()
        return fix[0].upper() + fix[1:] if w[0].isupper() else fix
    # e в начале слова → э
    if w[0] in "Ee":
        w = ("Э" if w[0] == "E" else "э") + w[1:]
    out, i = [], 0
    while i < len(w):
        for lat, cyr in _MULTI:
            if w.startswith(lat, i):
                out.append(cyr)
                i += len(lat)
                break
        else:
            out.append(_ONE.get(w[i], w[i]))
            i += 1
    return "".join(out)


def _plain(s: str) -> str:
    return _WORD.sub(lambda m: _word(m.group(0)), norm_uz(s))


def to_cyrl(s):
    """Латиница → кириллица, защищённые куски (код, ссылки, бренды...) остаются как есть."""
    if not s or not isinstance(s, str):
        return s
    out, pos = [], 0
    for m in _PROTECT.finditer(s):
        out.append(_plain(s[pos:m.start()]))
        out.append(m.group(0))
        pos = m.end()
    out.append(_plain(s[pos:]))
    return "".join(out)
