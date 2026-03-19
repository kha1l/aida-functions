# functions

Сюда переносим все функции из старого проекта.

Правило:
- Для каждой логической группы создаем пакет `func_<name>`.
- Внутри пакета: `definitions.py`, `jobs.py`, `ops.py`, `schedulers.py` (паттерн как в `dagster_dodostats`).
- `func_statistic` пока не добавляем.

Добавленные пакеты под ваши файлы:
- `func_birthday`
- `func_couriers`
- `func_metrics`
- `func_refusal`
- `func_revenue`
- `func_staff`
- `func_stationary`
- `func_stock`
- `func_stops`
- `func_tickets`

Сейчас все модульные `definitions.py` используют общий словарь ресурсов из
`functions/shared/definition_resources.py`, чтобы их можно было запускать отдельно
и они не расходились с корневым `defs.py`.

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
