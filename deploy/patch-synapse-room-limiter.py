"""Keep event creation and persistence inside Synapse's per-room limiter."""

from __future__ import annotations

import ast
import copy
import importlib.util
from pathlib import Path


def find_method(tree: ast.Module) -> ast.AsyncFunctionDef:
    handler = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "EventCreationHandler"
    )
    return next(
        node for node in handler.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "create_and_send_nonmember_event"
    )


def is_limiter(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.AsyncWith)
        and len(node.items) == 1
        and ast.unparse(node.items[0].context_expr) == "self.limiter.queue(room_id)"
    )


def is_send_block(node: ast.AST) -> bool:
    if not isinstance(node, ast.AsyncWith) or len(node.items) != 1:
        return False
    expression = node.items[0].context_expr
    if not isinstance(expression, ast.Call):
        return False
    if ast.unparse(expression.func) != (
        "self._worker_lock_handler.acquire_read_write_lock"
    ):
        return False
    if [ast.unparse(arg) for arg in expression.args] != [
        "NEW_EVENT_DURING_PURGE_LOCK_NAME", "room_id"
    ]:
        return False
    if len(expression.keywords) != 1:
        return False
    keyword = expression.keywords[0]
    if keyword.arg != "write" or not isinstance(keyword.value, ast.Constant):
        return False
    if keyword.value.value is not False or len(node.body) != 1:
        return False
    result = node.body[0]
    return (
        isinstance(result, ast.Return)
        and isinstance(result.value, ast.Await)
        and isinstance(result.value.value, ast.Call)
        and ast.unparse(result.value.value.func)
        == "self._create_and_send_nonmember_event_locked"
    )


def patch_source(source: str) -> str:
    tree = ast.parse(source)
    method = find_method(tree)
    if method.body and is_limiter(method.body[-1]):
        if is_send_block(method.body[-1].body[-1]):
            return source  # Already fixed upstream or by an earlier build.
    if len(method.body) < 2:
        raise ValueError("Unsupported Synapse event creation layout")
    limiter, send = method.body[-2:]
    if not is_limiter(limiter) or not is_send_block(send):
        raise ValueError("Unsupported Synapse limiter/purge-lock layout; refusing to patch")

    expected = copy.deepcopy(tree)
    expected_method = find_method(expected)
    expected_method.body[-2].body.append(expected_method.body.pop())
    lines = source.splitlines(keepends=True)
    for index in range(send.lineno - 1, send.end_lineno):
        if lines[index].strip():
            lines[index] = "    " + lines[index]
    patched = "".join(lines)
    actual = ast.parse(patched)
    if ast.dump(actual, include_attributes=False) != ast.dump(expected, include_attributes=False):
        raise ValueError("Patch changed more than the limiter scope")
    compile(patched, "synapse/handlers/message.py", "exec")
    return patched


def main() -> None:
    spec = importlib.util.find_spec("synapse.handlers.message")
    if spec is None or spec.origin is None:
        raise RuntimeError("Synapse message handler not found")
    path = Path(spec.origin)
    source = path.read_text()
    patched = patch_source(source)
    if patched != source:
        path.write_text(patched)
        print(f"Fixed room limiter scope: {path}")
    else:
        print(f"Room limiter scope already fixed: {path}")


if __name__ == "__main__":
    main()
