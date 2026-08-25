"""
Генерация JPEG-чека заказа (для отправки клиенту в Telegram).

Макет — под 80мм термопринтер (576px по ширине), чёрный текст на белом фоне
без градаций серого (термопринтеры печатают 1-bit чёрное/белое).
"""
import io
import os
import re

from PIL import Image, ImageDraw, ImageFont

_EMOJI_RE = re.compile(
    "[⌀-➿]"           # misc technical, symbols, dingbats
    "|[⬀-⯿]"          # misc symbols and arrows
    "|[︀-️]"          # variation selectors
    "|[\U0001F000-\U0001FFFF]"  # emoji, pictographs, symbols
)

def _strip_emoji(text: str) -> str:
    return _EMOJI_RE.sub('', text).strip()

FONT_DIR = os.path.join(os.path.dirname(__file__), "assets", "fonts")
REGULAR = os.path.join(FONT_DIR, "DejaVuSans.ttf")
BOLD    = os.path.join(FONT_DIR, "DejaVuSans-Bold.ttf")
ITALIC  = os.path.join(FONT_DIR, "DejaVuSans-Oblique.ttf")
EMOJI   = os.path.join(FONT_DIR, "OpenMoji-Black.ttf")

W = 576  # 80mm thermal printer standard raster width
PAD = 20
BLACK = (0, 0, 0)
WHITE = (255, 255, 255)


def _fonts():
    return {
        "logo":      ImageFont.truetype(BOLD, 40),
        "h2":        ImageFont.truetype(REGULAR, 22),
        "item_name": ImageFont.truetype(BOLD, 19),
        "item_sub":  ImageFont.truetype(REGULAR, 19),
        "total":     ImageFont.truetype(BOLD, 28),
        "footer":       ImageFont.truetype(REGULAR, 18),
        "footer_bold":  ImageFont.truetype(BOLD, 20),
        "footer_italic":ImageFont.truetype(ITALIC, 18),
        "emoji":        ImageFont.truetype(EMOJI, 18),
    }


def fmt_n(n) -> str:
    return f"{int(n):,}".replace(",", " ")


def _measure(draw, text, font):
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0], bbox[3] - bbox[1]


def _center_text(draw, y, text, font, fill=BLACK):
    w, h = _measure(draw, text, font)
    draw.text(((W - w) / 2, y), text, font=font, fill=fill)
    return h


def _right_text(draw, y, text, font, fill=BLACK):
    w, h = _measure(draw, text, font)
    draw.text((W - PAD - w, y), text, font=font, fill=fill)
    return h


def _center_mixed_line(draw, y, segments):
    """segments: list of (text, font) tuples. Centers the whole line as one unit."""
    widths = [_measure(draw, t, fnt)[0] for t, fnt in segments]
    heights = [_measure(draw, t, fnt)[1] for t, fnt in segments]
    total_w = sum(widths)
    max_h = max(heights) if heights else 0
    x = (W - total_w) / 2
    for (text, font), w in zip(segments, widths):
        draw.text((x, y), text, font=font, fill=BLACK)
        x += w
    return max_h


def _left_right_line(draw, y, left_text, right_text, font):
    """Draws left_text at PAD and right_text right-aligned, same row/font."""
    draw.text((PAD, y), left_text, font=font, fill=BLACK)
    _, h_l = _measure(draw, left_text, font)
    h_r = _right_text(draw, y, right_text, font)
    return max(h_l, h_r)


def _dashed_line(draw, y):
    x = PAD
    while x < W - PAD:
        draw.line([(x, y), (x + 6, y)], fill=BLACK, width=2)
        x += 12


def generate_receipt_jpeg(order: dict, items: list[dict], branch_contacts: list[str],
                           header_text: str = "ARTEZ",
                           slogan: str = "Химчистка ковров, мебели, матрасов и штор",
                           footer_note: str = "",
                           bot_link: str = "",
                           site_link: str = "artez.uz",
                           grand_total: float = None) -> bytes:
    """
    Рисует JPEG-чек заказа и возвращает его байты.

    order: словарь заказа (как из db.get_order_by_id) — используются поля
           id/order_num, created_at, имя и телефон клиента.
    items: список позиций заказа (как из db.get_order_items) — service,
           width_cm, length_cm, sqm, price_per_sqm, total_sum. Тип услуги
           (Стандарт/Экспресс), если есть, уже входит в текст service.
    branch_contacts: список строк контактов компании (Основной номер + Номер 1
                      филиала) — выводятся в один ряд в подвале.
    header_text: текст шапки-логотипа чека (по умолчанию "ARTEZ").
    slogan: слоган в подвале чека.
    footer_note: доп. строка в подвале чека (курсивом, не выводится, если пустая).
    bot_link: ссылка/юзернейм Telegram-бота заявок (не выводится, если пусто).
    site_link: ссылка на сайт.
    """
    f = _fonts()

    order_num = order.get("order_num") or order.get("id") or ""
    created_at = order.get("created_at")
    if hasattr(created_at, "strftime"):
        order_date_str = created_at.strftime("%d.%m.%Y %H:%M")
    else:
        order_date_str = str(created_at or "")

    client_name = " ".join(
        p for p in [order.get("client_first_name"), order.get("client_last_name")] if p
    ).strip() or order.get("client_name") or ""
    client_phone = order.get("client_phone") or ""

    # Оверсайз-холст, обрезаем в конце по фактической высоте.
    img = Image.new("RGB", (W, max(1200, 400 + 200 * len(items))), WHITE)
    draw = ImageDraw.Draw(img)

    y = PAD
    y += _center_text(draw, y, header_text, f["logo"]) + 10
    y += _center_text(draw, y, f"Заказ №{order_num}  ·  {order_date_str}", f["h2"]) + 6
    if client_name or client_phone:
        y += _left_right_line(draw, y, client_name, client_phone, f["h2"]) + 12
    _dashed_line(draw, y); y += 18

    for i, it in enumerate(items, 1):
        name = _strip_emoji(it.get("service") or "—")
        line1 = f"{i}. {name}"
        draw.text((PAD, y), line1, font=f["item_name"], fill=BLACK)
        y += _measure(draw, line1, f["item_name"])[1] + 8

        w_cm, l_cm, sqm = it.get("width_cm"), it.get("length_cm"), it.get("sqm")
        price, total = it.get("price_per_sqm"), it.get("total_sum")

        if w_cm and l_cm:
            dims_line = f"{int(w_cm)}×{int(l_cm)} см · {float(sqm or 0):.2f} м²"
            draw.text((PAD + 14, y), dims_line, font=f["item_sub"], fill=BLACK)
            y += _measure(draw, dims_line, f["item_sub"])[1] + 6

        price_line = f"{fmt_n(price)} сум/м²" if price else ""

        if price_line and total:
            total_line = f"{fmt_n(total)} сум"
            price_w, price_h = _measure(draw, price_line, f["item_sub"])
            total_w, total_h = _measure(draw, total_line, f["item_name"])
            available = W - 2 * PAD
            min_gap = 12
            if price_w + total_w + min_gap > available:
                draw.text((PAD + 14, y), price_line, font=f["item_sub"], fill=BLACK)
                y += price_h + 6
                draw.text((W - PAD - total_w, y), total_line, font=f["item_name"], fill=BLACK)
                y += total_h + 4
            else:
                bottom = y + price_h
                draw.text((PAD + 14, y), price_line, font=f["item_sub"], fill=BLACK)
                draw.text((W - PAD - total_w, bottom - total_h), total_line, font=f["item_name"], fill=BLACK)
                y += max(price_h, total_h) + 4
        elif total:
            total_line = f"{fmt_n(total)} сум"
            total_h = _right_text(draw, y, total_line, f["item_name"])
            y += total_h + 4

        y += 12
        _dashed_line(draw, y)
        y += 16

    grand = grand_total if grand_total is not None else sum(float(it.get("total_sum") or 0) for it in items)
    y += 8
    y += _right_text(draw, y, f"ИТОГО: {fmt_n(grand)} сум", f["total"]) + 16
    _dashed_line(draw, y); y += 20

    if branch_contacts:
        contacts_line = "   ·   ".join(c for c in branch_contacts if c)
        y += _center_text(draw, y, contacts_line, f["footer_bold"]) + 6

    link_segments = []
    if bot_link:
        if link_segments:
            link_segments.append(("   ·   ", f["footer"]))
        link_segments += [("🤖 ", f["emoji"]), (f"Бот заявок: {bot_link}", f["footer"])]
    if site_link:
        if link_segments:
            link_segments.append(("   ·   ", f["footer"]))
        link_segments += [("🌐 ", f["emoji"]), (site_link, f["footer"])]
    if link_segments:
        y += _center_mixed_line(draw, y, link_segments) + 16
    else:
        y += 4

    y += _center_text(draw, y, slogan, f["footer"]) + 6
    if footer_note:
        y += _center_text(draw, y, footer_note, f["footer_italic"]) + 6
    y += PAD

    final = img.crop((0, 0, W, int(y)))
    buf = io.BytesIO()
    final.save(buf, "JPEG", quality=95)
    return buf.getvalue()
