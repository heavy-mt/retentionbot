# Разработка и карта проверок

## Рабочий цикл

Прочитать архитектуру, выбрать слой задачи, проверить интерфейсы в исходниках.
Изменять поведение вместе с относящимися тестами и документацией. Любое изменение
patch scripts должно отвергать неизвестную структуру Synapse и быть идемпотентным.
Не проверять только исходный patch: CI отдельно проверяет handler в готовом образе.

Локально из корня checkout:

```bash
(
    set -euo pipefail
    python3 -m venv .venv
    .venv/bin/pip install -e '.[dev]'
    .venv/bin/ruff check src tests deploy/patch-synapse-room-limiter.py \
        deploy/check-synapse-room-limiter.py
    .venv/bin/pytest -q
)
```

Без SYNAPSE_PYTHON/RABBITMQ_TEST_URL часть интеграционных тестов будет skipped.
Без TEST_DATABASE_URL проверяется SQLite, а не PostgreSQL. Это не полный release
gate. Точный запуск окружений содержится в .github/workflows/tests.yml.
CI ставит настоящий Synapse 1.162.0 отдельно, применяет room limiter patch,
запускает набор для SQLite/PostgreSQL, RabbitMQ, сборки и Compose smoke.

## Карта тестов

| Изменяемая область | Основные проверки |
| --- | --- |
| Политика и интервалы | test_policy.py, test_integration.py |
| Cursor, lease, FIFO/scheduling, компактация | test_queue.py, test_service.py, test_invalidation_cleanup.py |
| Worker результаты, retries, broker delivery | test_worker.py, test_integration.py |
| Synapse redaction, purge, permissions, federation semantics | test_integration.py |
| Client reset sentinel | test_cache_reset.py и интеграционные проверки reset |
| Согласованная очистка и потерянный ответ | test_invalidation_cleanup.py и native-purge интеграционные сценарии |
| Command dialog, permissions, E2EE restart | test_command_bot.py |
| Plain/HTML ответы, escaping, legacy outbox | test_bot_messages.py |
| DAG/room limiter patch | test_synapse_room_limiter.py, actual-image CI check |
| Docker/secrets/networks | test_deployment.py, .github/smoke-compose.sh |

Наличие теста в файле не означает прохождение на production. Живые результаты
помечаются отдельно в acceptance.md и датированных отчётах. Матрица типов контента,
медиа storage, iOS и multi-process topology не получает галочку автоматически.

## Как менять протоколы

- Для внутреннего API учитывать порядок обновления module/observer/worker.
  Старый модуль не знает compact-invalidations: ошибка должна сохранять записи,
  а не удалять локальные подтверждения. Новый модуль совместим со старыми invalidate.
- Для БД предусмотреть existing rows, unknown fields, транзакционность и обе
  поддержанные СУБД. Проверять pending и null completion отдельно от terminal.
- Для reset сохранять ignored users и durable generations; тестировать reconnect
  после нескольких поколений, а не только мгновенную доставку в открытый чат.
- Для bot UI держать internal JSON и пользовательское оформление раздельно.
  Сохранять deterministic txn_id, outbox, проверку DM/actor и HTML escaping.
- Для версии обновлять pyproject.toml, Compose image tags, CI build tags и README
  согласованно. Исторические версии в отчётах оставлять прежними.

## Выпуск

Объединить нужные изменения в один commit/ветку, прогнать полный CI именно для
итогового head, проверить README/CHANGELOG и upgrade path. Релизные notes описывают
поведение, проверенное окружение и ограничения. Не обещать точный срок удаления,
100 msg/s, media deletion или поддержку клиента без измерений.

После обновления стенда проверить установленный SHA/package version, health и
согласованную очистку на disposable aged данных. Рабочие таблицы нельзя искусственно
состаривать для демонстрации. Семь суток реального ожидания не требуются в тестах.
Не смешивать публикацию кандидата в GitHub с его фактическим развёртыванием.
