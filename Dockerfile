FROM python:3.12-slim@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN groupadd --gid 10001 appuser \
 && useradd --uid 10001 --gid appuser --create-home --no-log-init appuser \
 && install -d -o appuser -g appuser /app

WORKDIR /app

COPY requirements.txt ./

RUN python - <<'PY'
import codecs
from pathlib import Path

p = Path("requirements.txt")
data = p.read_bytes()
if data.startswith(codecs.BOM_UTF16_LE) or data.startswith(codecs.BOM_UTF16_BE):
    p.write_text(data.decode("utf-16"), encoding="utf-8")
PY

RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=appuser:appuser . .

USER appuser

EXPOSE 4000

CMD ["dagster", "api", "grpc", "-m", "defs", "-h", "0.0.0.0", "-p", "4000"]
