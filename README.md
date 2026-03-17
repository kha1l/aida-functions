# aida_functions

Dagster‑сервис, который запускает jobs и публикует сообщения в RabbitMQ очередь `aida`.

## Структура проекта
- `config.py` — загрузка настроек из env.
- `connection.py` — фабрики подключений (БД, RabbitMQ).
- `defs.py` — единая точка входа Dagster (`Definitions`).
- `messaging.py` — сборка/валидация payload и публикация в RabbitMQ.
- `resources/` — ресурсы Dagster (`db`, `rabbitmq`).
- `database/` — модели и слой работы с БД.
- `functions/` — пакеты `func_*` (по одному на файл из старого проекта). `func_statistic` пока не добавляем.

## Запуск локально
1. Установить зависимости (если еще не установлены):
```
. .venv/bin/activate
pip install -r requirements.txt
```
2. Запустить Dagster:
```
dagster dev -m defs
```

## Запуск в Docker
```
docker build -t aida-functions .
docker run --env-file .env -p 3000:3000 aida-functions
```

## Основные env‑переменные
### RabbitMQ
- `RABBIT_URL` (пример: `amqp://user:pass@host:5672/`)
- `RABBIT_QUEUE` (default: `aida`)
- `RABBIT_EXCHANGE`
- `RABBIT_ROUTING_KEY`
- `RABBIT_EXCHANGE_TYPE` (default: `direct`)
- `RABBIT_DURABLE` (default: `true`)
- `RABBIT_ENABLED` (default: `true`)

### База данных
База собирается из частей:
- `DBASE_NAME`
- `DBASE_USER`
- `DBASE_PASSWORD`
- `DBASE_HOST`
- `DBASE_PORT` (optional, default: `5432`)
- `DBASE_DRIVER` (optional, default: `postgresql`)
- `DATABASE_ECHO` (default: `false`)
- `DATABASE_POOL_PRE_PING` (default: `true`)

### Расписания
Для каждого `func_<name>`:
- `SCHEDULE_<NAME>_CRON` (например `SCHEDULE_BIRTHDAY_CRON`)
- `SCHEDULE_<NAME>_TZ` (например `SCHEDULE_BIRTHDAY_TZ`)
- `SCHEDULE_<NAME>_ENABLED` (например `SCHEDULE_BIRTHDAY_ENABLED`)
- `SCHEDULE_TZ_DEFAULT` — общий дефолтный часовой пояс

## Примечания
- В `ops.py` уже ожидаются ресурсы `db` и `rabbitmq` через `context.resources`.
- Публикация сообщений — через `messaging.build_payload()` и `RabbitMQPublisher.publish()`.
