# aida_functions

`aida_functions` — Dagster-сервис с набором jobs для отчётов и уведомлений Aida.  
Он читает данные из PostgreSQL, при необходимости запрашивает Dodo API и публикует готовые сообщения в RabbitMQ для дальнейшей отправки в Telegram.

## Что делает сервис

- регистрирует Dagster jobs и schedules через `defs.py`;
- предоставляет ресурсы `db`, `rabbitmq` и `dodo_api`;
- собирает payload для очереди через `messaging.py`;
- хранит SQLAlchemy-модели и bulk-запросы в директории `database/`;
- содержит отдельные пакеты `functions/func_*` для каждого типа отчёта.

## Стек

- Python 3.12
- Dagster
- SQLAlchemy + asyncpg
- aio-pika
- aiohttp

## Структура проекта

| Путь | Назначение |
| --- | --- |
| `defs.py` | Корневой `Definitions` с jobs, schedules и ресурсами |
| `config.py` | Загрузка настроек из env |
| `connection.py` | Фабрики подключения к БД и RabbitMQ вне Dagster-ресурсов |
| `messaging.py` | Валидация payload и публикация сообщений в RabbitMQ |
| `resources/database.py` | Dagster-ресурс БД |
| `resources/rabbitmq.py` | Dagster-ресурс RabbitMQ |
| `resources/dodo_api.py` | Async-клиент и Dagster-ресурс для Dodo API |
| `database/models.py` | SQLAlchemy-модели схем `special` и `aida` |
| `database/requests.py` | Bulk-запросы к БД для jobs |
| `functions/func_*` | Пакеты с `ops.py`, `jobs.py`, `definitions.py`, `schedulers.py` |

## Доступные jobs

- `birthday_job`
- `couriers_job`
- `metrics_job`
- `refusal_job`
- `revenue_job`
- `staff_job`
- `stationary_job`
- `stock_job`
- `stops_job`
- `tariff_job`
- `tickets_job`

## Установка

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Запуск

Локальная разработка:

```bash
dagster dev -m defs
```

Запуск конкретной job:

```bash
dagster job execute -m defs -j couriers_job
```

Docker/gRPC:

```bash
docker build -t aida-functions .
docker run --env-file .env -p 4000:4000 aida-functions
```

Контейнер стартует командой `dagster api grpc -m defs`.

## Переменные окружения

### RabbitMQ

- `RABBIT_URL`
- `RABBIT_QUEUE`
- `RABBIT_EXCHANGE`
- `RABBIT_ROUTING_KEY`
- `RABBIT_EXCHANGE_TYPE`
- `RABBIT_DURABLE`
- `RABBIT_ENABLED`

### База данных

Поддерживаются два варианта конфигурации:

- `DATABASE_URL`
- или набор `DBASE_NAME`, `DBASE_USER`, `DBASE_PASSWORD`, `DBASE_HOST`, `DBASE_PORT`, `DBASE_DRIVER`

Дополнительно:

- `DATABASE_ECHO`
- `DATABASE_POOL_PRE_PING`

### Dodo API

- `DODO_API_TIMEOUT_SECONDS`
- `DODO_API_RETRIES_500`
- `DODO_API_MAX_CONCURRENT_REQUESTS`

Токены и account-данные для Dodo API читаются из таблиц БД.

### Couriers и погода

- `COURIERS_MAX_CONCURRENT_UNITS`
- `WEATHER_API_KEY` или `API_WEATHER_KEY`
- `WEATHER_API_TIMEOUT_SECONDS`
- `WEATHER_FORECAST_COUNT`
- `WEATHER_LANG`
- `WEATHER_API_RETRIES`
- `WEATHER_MAX_CONCURRENT_REQUESTS`

Погодные данные используются в `func_couriers`; координаты точек берутся из `special.units`.

### Расписания

Для каждого job доступны переменные:

- `SCHEDULE_<NAME>_CRON`
- `SCHEDULE_<NAME>_TZ`
- `SCHEDULE_<NAME>_ENABLED`

Общий timezone по умолчанию задаётся через `SCHEDULE_TZ_DEFAULT`.

## Примечания

- Основная точка интеграции с очередью — `RabbitMQPublisher.publish_async()`.
- `resources/dodo_api.py` поддерживает retry, shared `aiohttp` session и ограничение параллелизма запросов.
- `database/requests.py` содержит bulk-запросы и retry на временных DB-ошибках.
- `func_couriers` дополнительно собирает погодный прогноз и переиспользует общие runtime-ресурсы в рамках job.
