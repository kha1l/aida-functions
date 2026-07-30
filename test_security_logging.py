from __future__ import annotations

import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from security_logging import pseudonymize, redact_text, safe_identifier

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
