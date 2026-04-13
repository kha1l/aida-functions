"""Запись временных результатов WebApp-отчетов в общую таблицу."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text


TABLE_NAME = "aida.webapp_report_results"


async def store_webapp_report_result(
    *,
    db,
    request_id: str,
    payload: dict[str, Any],
    status: str,
    error: str | None,
) -> None:
    """Сохраняет готовый JSON-результат WebApp-отчета по request_id."""
    sessionmaker = db.get_sessionmaker()
    async with sessionmaker() as session:
        await session.execute(text("CREATE SCHEMA IF NOT EXISTS aida"))
        await session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS aida.webapp_report_results (
                    request_id uuid PRIMARY KEY,
                    report_type varchar(50) NOT NULL,
                    status varchar(20) NOT NULL,
                    input_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
                    result_json jsonb,
                    error text,
                    run_id varchar(100),
                    created_at timestamptz NOT NULL DEFAULT now(),
                    finished_at timestamptz,
                    expires_at timestamptz NOT NULL
                )
                """
            )
        )
        await session.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_webapp_report_results_expires_at
                    ON aida.webapp_report_results (expires_at)
                """
            )
        )
        await session.execute(
            text(
                """
                UPDATE aida.webapp_report_results
                SET status = :status,
                    result_json = CAST(:result_json AS jsonb),
                    error = :error,
                    finished_at = now()
                WHERE request_id = CAST(:request_id AS uuid)
                """
            ),
            {
                "status": status,
                "result_json": json.dumps(payload, ensure_ascii=False),
                "error": error,
                "request_id": request_id,
            },
        )
        await session.commit()
