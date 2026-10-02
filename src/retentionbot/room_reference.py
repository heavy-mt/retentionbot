"""Room identifiers and share links; never fetch a URL supplied in a command."""

from urllib.parse import unquote, urlsplit


def reference(value: str) -> tuple[str, str]:
    if not isinstance(value, str) or not value.strip() or len(value) > 2048:
        raise ValueError("Пришлите название комнаты или ссылку на неё.")
    value = value.strip()
    if value.startswith(("https://", "http://")):
        url = urlsplit(value)
        if url.scheme != "https" or url.netloc != "matrix.to" or not url.fragment.startswith("/"):
            raise ValueError("Нужна ссылка matrix.to на комнату, её ID или название.")
        value = unquote(url.fragment[1:].split("?", 1)[0].split("/", 1)[0])
    elif value.startswith("matrix:"):
        url = urlsplit(value)
        parts = url.path.split("/")
        if len(parts) < 2 or parts[0] not in {"r", "roomid"}:
            raise ValueError("Эта Matrix-ссылка не указывает на комнату.")
        value = ("#" if parts[0] == "r" else "!") + unquote(parts[1])
    if value.startswith(("!", "#")):
        if ":" not in value or any(c.isspace() for c in value) or len(value) > 1024:
            raise ValueError("Некорректный ID или адрес комнаты.")
        return ("id" if value.startswith("!") else "alias"), value
    if value.startswith("@"):
        raise ValueError("Это адрес пользователя. Пришлите название комнаты или ссылку на неё.")
    return "name", value
