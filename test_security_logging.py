from __future__ import annotations

import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from security_logging import (
    SafeStructuredLogger,
    build_structured_log,
    pseudonymize,
    redact_text,
    safe_identifier,
    safe_log_context,
)

try:
    from dagster import job, op
except ModuleNotFoundError:
    job = None
    op = None

try:
    from functions.shared.report_job_runtime import log_auth_error
    from resources.zveno import ZvenoClient, ZvenoResult
except ModuleNotFoundError as exc:
    log_auth_error = None
    ZvenoClient = None
    ZvenoResult = None
    OPTIONAL_IMPORT_ERROR = str(exc)
else:
    OPTIONAL_IMPORT_ERROR = ""


class SecurityLoggingTests(unittest.TestCase):
    def test_structured_log_uses_allowlist_and_correlation_fields(self) -> None:
        event = build_structured_log(
            level="WARNING",
            message="unit=%s Authorization: Bearer %s",
            args=("987654321", "synthetic-secret"),
            component="synthetic_op",
            request_id="request-1",
            trace_id="trace-1",
        )

        self.assertEqual(
            set(event),
            {
                "timestamp",
                "stream",
                "level",
                "message",
                "service",
                "version",
                "component",
                "request_id",
                "trace_id",
            },
        )
        self.assertEqual(event["request_id"], "request-1")
        self.assertEqual(event["trace_id"], "trace-1")
        self.assertNotIn("987654321", event["message"])
        self.assertNotIn("synthetic-secret", event["message"])

    def test_safe_context_emits_redacted_json_with_stable_trace(self) -> None:
        messages: list[str] = []
        delegate = SimpleNamespace(
            info=messages.append,
            warning=messages.append,
            error=messages.append,
        )
        context = SimpleNamespace(
            run_id="synthetic-run",
            op=SimpleNamespace(name="synthetic_op"),
            log=delegate,
        )
        safe_context = safe_log_context(context)
        safe_context.log.warning(
            "email=%s phone=%s",
            "synthetic.employee@example.test",
            "+7 (999) 123-45-67",
        )

        event = json.loads(messages[0])
        self.assertEqual(event["component"], "synthetic_op")
        self.assertRegex(event["request_id"], r"^[0-9a-f]{32}$")
        self.assertRegex(event["trace_id"], r"^[0-9a-f]{32}$")
        self.assertNotIn("synthetic.employee@example.test", messages[0])
        self.assertNotIn("+7 (999) 123-45-67", messages[0])

    def test_structured_logger_drops_unapproved_keyword_fields(self) -> None:
        messages: list[str] = []
        logger = SafeStructuredLogger(
            SimpleNamespace(info=messages.append),
            component="synthetic_op",
            request_id="request-1",
            trace_id="trace-1",
        )
        logger.info(
            "completed",
            raw_payload="must-not-survive",
            access_token="synthetic-secret",
        )

        self.assertNotIn("raw_payload", messages[0])
        self.assertNotIn("must-not-survive", messages[0])
        self.assertNotIn("access_token", messages[0])
        self.assertNotIn("synthetic-secret", messages[0])

    @unittest.skipIf(op is None, "dagster is not installed")
    def test_safe_context_works_with_real_dagster_op(self) -> None:
        @op
        def synthetic_security_logging_op(context) -> None:
            safe_context = safe_log_context(context)
            self.assertEqual(
                safe_context.log._component,
                "synthetic_security_logging_op",
            )
            safe_context.log.info("synthetic dagster message")

        @job
        def synthetic_security_logging_job() -> None:
            synthetic_security_logging_op()

        result = synthetic_security_logging_job.execute_in_process()
        self.assertTrue(result.success)

    def test_pseudonym_is_hmac_and_versioned(self) -> None:
        raw_identifier = "synthetic-employee-42"
        with patch.dict(
            os.environ,
            {
                "SECURITY_PSEUDONYM_KEY": "synthetic-test-key",
                "SECURITY_PSEUDONYM_KEY_VERSION": "test-v1",
            },
            clear=False,
        ):
            pseudonym = pseudonymize(raw_identifier)

        self.assertRegex(pseudonym or "", r"^test-v1:[0-9a-f]{64}$")
        self.assertNotIn(raw_identifier, pseudonym or "")

    def test_identifier_is_omitted_without_key(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(pseudonymize("synthetic-employee-42"))
            self.assertEqual(safe_identifier("synthetic-employee-42"), "[REDACTED_ID]")

    def test_redaction_removes_sensitive_synthetic_values(self) -> None:
        source = json.dumps(
            {
                "email": "synthetic.employee@example.test",
                "phone": "+7 (999) 123-45-67",
                "chat_id": 987654321,
                "Authorization": "Bearer synthetic-secret",
            }
        )
        with patch.dict(os.environ, {}, clear=True):
            redacted = redact_text(source)

        self.assertNotIn("synthetic.employee@example.test", redacted)
        self.assertNotIn("+7 (999) 123-45-67", redacted)
        self.assertNotIn("987654321", redacted)
        self.assertNotIn("synthetic-secret", redacted)


@unittest.skipIf(ZvenoClient is None, f"optional runtime dependencies unavailable: {OPTIONAL_IMPORT_ERROR}")
class ZvenoLoggingTests(unittest.IsolatedAsyncioTestCase):
    async def test_failure_log_does_not_contain_employee_or_response_body(self) -> None:
        employee_name = "Synthetic Employee"
        response_body = "synthetic.employee@example.test upstream body"
        client = ZvenoClient(
            api_key="synthetic-api-key",
            base_url="https://example.test",
            default_model="synthetic-model",
        )
        client.generate_greeting = AsyncMock(
            return_value=ZvenoResult(
                ok=False,
                text=None,
                error_html=response_body,
                status=502,
                model="synthetic-model",
            )
        )
        messages: list[str] = []

        with patch.dict(
            os.environ,
            {
                "SECURITY_PSEUDONYM_KEY": "synthetic-test-key",
                "SECURITY_PSEUDONYM_KEY_VERSION": "test-v1",
            },
            clear=False,
        ):
            result = await client.generate_greeting_or_fallback(
                employee_name=employee_name,
                occasion="synthetic occasion",
                fallback=lambda: "synthetic fallback",
                normalize=lambda value: value,
                log_warning=lambda template, *args: messages.append(template % args),
                log_prefix="synthetic_op",
                subject=employee_name,
            )

        serialized = "\n".join(messages)
        self.assertEqual(result, "synthetic fallback")
        self.assertRegex(serialized, r"subject_hash=test-v1:[0-9a-f]{64}")
        self.assertNotIn(employee_name, serialized)
        self.assertNotIn(response_body, serialized)
        self.assertIn("status=502", serialized)


@unittest.skipIf(log_auth_error is None, f"optional runtime dependencies unavailable: {OPTIONAL_IMPORT_ERROR}")
class ReportLoggingTests(unittest.TestCase):
    def test_dodo_error_log_drops_body_and_hashes_identifiers(self) -> None:
        raw_unit_uuid = "synthetic-unit-uuid"
        raw_account_id = 987654321
        response_body = (
            "<pre>Synthetic Employee synthetic.employee@example.test "
            "Authorization: Bearer synthetic-secret</pre>"
        )
        messages: list[str] = []
        log = SimpleNamespace(
            warning=lambda template, *args: messages.append(template % args),
        )
        context = SimpleNamespace(log=log)

        with patch.dict(
            os.environ,
            {
                "SECURITY_PSEUDONYM_KEY": "synthetic-test-key",
                "SECURITY_PSEUDONYM_KEY_VERSION": "test-v1",
            },
            clear=False,
        ):
            log_auth_error(
                context,
                log_prefix="synthetic_op",
                unit_uuid=raw_unit_uuid,
                account_id=raw_account_id,
                error_html=response_body,
                error_status=401,
            )

        serialized = "\n".join(messages)
        self.assertNotIn(raw_unit_uuid, serialized)
        self.assertNotIn(str(raw_account_id), serialized)
        self.assertNotIn("Synthetic Employee", serialized)
        self.assertNotIn("synthetic.employee@example.test", serialized)
        self.assertNotIn("synthetic-secret", serialized)
        self.assertEqual(len(__import__("re").findall(r"test-v1:[0-9a-f]{64}", serialized)), 2)
        self.assertIn("status=401", serialized)


if __name__ == "__main__":
    unittest.main()
