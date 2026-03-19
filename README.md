# aida_functions

`aida_functions` — сервис на `Dagster`, который собирает отчеты и уведомления для экосистемы Aida, читает данные из PostgreSQL, ходит в Dodo API и публикует готовые payload в RabbitMQ.

## Что внутри

- daily jobs по расписанию: курьеры, ресторан, метрики, стопы, тикеты и другие;
- manual jobs без расписания, которые запускаются из `aida3` по нажатию кнопок;
- shared Dagster-ресурсы для БД, RabbitMQ и Dodo API;
- bulk-запросы к БД в `database/requests.py`;
- общая логика runtime для manual-отчетов в `functions/shared/manual_report_runtime.py`.

## Основные директории

| Путь | Назначение |
| --- | --- |
| `defs.py` | Корневой `Definitions` со всеми jobs, schedules и ресурсами |
| `config.py` | Настройки из env |
| `database/` | SQLAlchemy-модели, engine и bulk-запросы |
| `resources/` | Dagster-ресурсы `db`, `rabbitmq`, `dodo_api` |
| `messaging.py` | Формирование и публикация payload в RabbitMQ |
| `functions/func_couriers/` | Отчеты по курьерам и delivery-manual jobs |
| `functions/func_stationary/` | Отчеты по ресторану и stationary-manual jobs |
| `functions/shared/` | Общий runtime и shared helpers для job'ов |

## Daily jobs

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

## Manual jobs

### Delivery / couriers

- `certificates_job`
- `later_job`
- `waiting_job`
- `sectors_job`
- `longers_job`

### Stationary

- `later_rest_job`
- `assembly_rest_job`
- `drink_rest_job`

## Как это работает

1. Job получает подписки и связанные аккаунты из PostgreSQL.
2. Для каждой точки выбирается рабочий токен Dodo API.
3. Собирается один или несколько текстовых блоков отчета.
4. Для каждого подписчика строится payload вида `chat_id + post + text + topic_id + unit_uuid`.
5. Payload публикуется в RabbitMQ, откуда его забирает `aida3`.

## Локальный запуск

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Запуск Dagster UI и job'ов локально:

```bash
dagster dev -m defs
```

Запуск конкретной job:

```bash
dagster job execute -m defs -j couriers_job
```

## Основные переменные окружения

### PostgreSQL

- `DATABASE_URL`
- или `DBASE_NAME`, `DBASE_USER`, `DBASE_PASSWORD`, `DBASE_HOST`, `DBASE_PORT`, `DBASE_DRIVER`
- `DATABASE_ECHO`
- `DATABASE_POOL_PRE_PING`

### RabbitMQ

- `RABBIT_URL`
- `RABBIT_QUEUE`
- `RABBIT_EXCHANGE`
- `RABBIT_ROUTING_KEY`
- `RABBIT_EXCHANGE_TYPE`
- `RABBIT_DURABLE`
- `RABBIT_ENABLED`

### Dodo API

- `DODO_API_TIMEOUT_SECONDS`
- `DODO_API_RETRIES_500`
- `DODO_API_MAX_CONCURRENT_REQUESTS`

Токены и `account.properties` читаются из БД.

### Couriers и погода

- `COURIERS_MAX_CONCURRENT_UNITS`
- `WEATHER_API_KEY` или `API_WEATHER_KEY`
- `WEATHER_API_TIMEOUT_SECONDS`
- `WEATHER_FORECAST_COUNT`
- `WEATHER_LANG`
- `WEATHER_API_RETRIES`
- `WEATHER_MAX_CONCURRENT_REQUESTS`

### Schedules

- `SCHEDULE_<NAME>_CRON`
- `SCHEDULE_<NAME>_TZ`
- `SCHEDULE_<NAME>_ENABLED`
- `SCHEDULE_TZ_DEFAULT`

## Полезные детали

- `resources/dodo_api.py` умеет retry на `429/500`, сетевые ошибки и shared `aiohttp` session.
- `database/requests.py` строит bulk-выборки, чтобы job'ы меньше ходили в БД.
- `functions/shared/manual_report_runtime.py` дает единый lifecycle для manual-отчетов.
- `aida3` запускает manual jobs через Dagster GraphQL и получает готовые сообщения из очереди.
