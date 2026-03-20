# functions

Сюда переносим все функции из старого проекта.

Правило:
- Для каждой логической группы создаем пакет `func_<name>`.
- Внутри пакета: `definitions.py`, `jobs.py`, `ops.py`, `schedulers.py` (паттерн как в `dagster_dodostats`).
- `func_statistic` пока не добавляем.

Добавленные пакеты под ваши файлы:
- `func_birthday`
- `func_couriers`
- `func_defect`
- `func_metrics`
- `func_refusal`
- `func_revenue`
- `func_staff`
- `func_stationary`
- `func_stock`
- `func_stops` — регулярный job по стопам продаж, ингредиентов, ключевых ингредиентов и секторов
- `func_tickets` — регулярный job по клиентским тикетам из Pyrus
- `func_tracking` — manual job по агрегированным данным трекинга производства
- `func_writeoff`

Сейчас все модульные `definitions.py` используют общий словарь ресурсов из
`functions/shared/definition_resources.py`, чтобы их можно было запускать отдельно
и они не расходились с корневым `defs.py`.

Тексты отчетов локализуются в `functions/shared/report_text_i18n.py`: базовый язык
формирования — русский, а при отправке в RabbitMQ отчет автоматически адаптируется
под язык чата (`ru` / `en`) из `special.settings.country`.

Пример (только структура):
```
functions/
 └── func_example/
     ├── __init__.py
     ├── definitions.py
     ├── jobs.py
     ├── ops.py
     └── schedulers.py
```
