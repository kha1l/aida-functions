FROM python:3.14-slim@sha256:cea0e6040540fb2b965b6e7fb5ffa00871e632eef63719f0ea54bca189ce14a6

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
