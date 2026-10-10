"""Human-readable Matrix replies; structured outbox data stays internal."""

from __future__ import annotations

import re
from html import escape
from urllib.parse import quote


def display_duration(value):
    match = re.fullmatch(r"(\d+)(ms|s|m|h|d|w)", str(value))
    if not match:
        return str(value)
    number = int(match[1])
    forms = {
        "ms": ("миллисекунда", "миллисекунды", "миллисекунд"),
        "s": ("секунда", "секунды", "секунд"),
        "m": ("минута", "минуты", "минут"),
        "h": ("час", "часа", "часов"),
        "d": ("день", "дня", "дней"),
        "w": ("неделя", "недели", "недель"),
    }[match[2]]
    index = 2 if 11 <= number % 100 <= 14 else 0 if number % 10 == 1 else (
        1 if 2 <= number % 10 <= 4 else 2
    )
    return f"{number} {forms[index]}"


def reply_content(result):
    """Render both new results and unsent replies saved by older bot versions."""
    message = str(result.get("message") or "")
    if result.get("code") == "NOT_ROOM_ADMIN":
        message = "Изменять срок хранения может только администратор выбранной комнаты."
    elif message == "Нужен запас между min_lifetime и max_lifetime.":
        message = (
            "Максимальный срок должен быть больше минимального. "
            "Пришлите срок, который больше текущего минимума."
        )
    elif message == "min_lifetime превышает допустимый max_lifetime.":
        message = (
            "Минимальный срок превышает максимальный срок, разрешённый сервером. "
            "Укажите меньший минимальный срок."
        )
    paragraphs = []
    html = []
    if result.get("room"):
        name = str(result["room"])
        paragraphs.append(name)
        heading = f"<strong>{escape(name)}</strong>"
        if result.get("room_id"):
            url = "https://matrix.to/#/" + quote(str(result["room_id"]), safe="")
            heading += f' · <a href="{escape(url, quote=True)}">Открыть комнату</a>'
        html.append(f"<p>{heading}</p>")
    if message:
        paragraphs.append(message)
        html.append("<p>" + escape(message).replace("\n", "<br>") + "</p>")
    if "max_lifetime" in result:
        maximum = display_duration(result["max_lifetime"])
        minimum = display_duration(result.get("min_lifetime", "не задан"))
        paragraphs.append(f"Срок хранения: {maximum}\nМинимальный срок: {minimum}")
        html.append(
            f"<p>Срок хранения: <strong>{escape(maximum)}</strong><br>"
            f"Минимальный срок: {escape(minimum)}</p>"
        )
    choices = result.get("choices") or []
    if choices:
        paragraphs.append("\n".join(str(choice) for choice in choices))
        html.append("<ol>" + "".join(
            "<li>" + escape(re.sub(r"^\d+\.\s*", "", str(choice))) + "</li>"
            for choice in choices
        ) + "</ol>")
    body = "\n\n".join(paragraphs) or "Не удалось подготовить ответ. Отправьте команду ещё раз."
    return {
        "msgtype": "m.notice",
        "body": body,
        "format": "org.matrix.custom.html",
        "formatted_body": "".join(html) or f"<p>{escape(body)}</p>",
    }
