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
        message = (
            "Изменять срок хранения может только администратор выбранной комнаты "
            "с необходимыми правами. Права могли измениться после выбора комнаты. "
            "Попросите администратора проверить их и отправьте «Перечислить комнаты» заново."
        )
    elif result.get("code") in {"NOT_JOINED", "M_FORBIDDEN", "NOT_RETENTION_ROOM"}:
        message = (
            "Сейчас вы не можете настроить эту комнату: проверьте, что вступили в неё "
            "и имеете права менять срок. Пространства и чаты управления ботом не подходят. "
            "Отправьте «Перечислить комнаты» и выберите доступную комнату."
        )
    elif message == "Серверный срок хранения действует для этой комнаты.":
        message = (
            "Отключить автоудаление полностью нельзя: на сервере действует обязательный срок. "
            "Команда не изменила настройки. Отправьте «Статус», чтобы увидеть текущий срок, "
            "или задайте другой разрешённый срок, например «7 дней»."
        )
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
    if result.get("room_details"):
        details = str(result["room_details"])
        paragraphs.append(details)
        html.append("<p>" + escape(details).replace("\n", "<br>") + "</p>")
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
            "<li>" + escape(re.sub(r"^\d+\.\s*", "", str(choice))).replace("\n", "<br>")
            + "</li>"
            for choice in choices
        ) + "</ol>")
    if "allowed_min" in result:
        limits = (
            "Минимальный разрешённый срок: " + display_duration(result["allowed_min"])
            + ". Максимальный разрешённый срок: " + display_duration(result["allowed_max"]) + "."
            if result["allowed_min"] != "не задан" and result["allowed_max"] != "не задан"
            else "Допустимые сроки ограничиваются правилами сервера."
        )
        timing = (
            "Удаление обычно начинается немного раньше срока; запас на этом сервере: "
            + display_duration(result.get("redaction_lead", "не задан")) + ". "
            "При нагрузке или отсутствии сети результат на iPhone может появиться позже. "
            "Сначала может появиться «Сообщение удалено», затем запись исчезнет из истории. "
            "Этот срок не гарантирует удаления скачанных файлов, скриншотов и копий сообщений."
        )
        paragraphs.extend([limits, timing])
        html.extend(["<p>" + escape(limits) + "</p>", "<p>" + escape(timing) + "</p>"])
    body = "\n\n".join(paragraphs) or "Не удалось подготовить ответ. Отправьте команду ещё раз."
    return {
        "msgtype": "m.notice",
        "body": body,
        "format": "org.matrix.custom.html",
        "formatted_body": "".join(html) or f"<p>{escape(body)}</p>",
    }
