FROM python:3.12-slim

WORKDIR /app

RUN useradd -m appuser

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Persistent state lives in the volume mounted at /data (Railway volume or the
# compose `liq_data` named volume). Pre-creating + chowning the dir in the
# image means the mount inherits appuser ownership on first use.
ENV LIQ_DATA_DIR=/data
RUN mkdir -p /data && chown -R appuser:appuser /data /app

USER appuser

# default: run all pipeline stages in this one container (Railway-style
# single-service hosts). For docker-compose's multi-container setup, each
# service overrides this with its own `command:`.
CMD ["python", "entrypoint.py"]
