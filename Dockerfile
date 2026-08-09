FROM python:3.11-slim

WORKDIR /app

RUN useradd -m appuser

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Persistent state lives in the volume mounted at /tmp/data (Railway volume
# or the compose `liq_data` named volume). Pre-creating + chowning the dir in
# the image means the mount inherits appuser ownership on first use. This
# must happen BEFORE the USER directive switches us to the unprivileged user,
# otherwise appuser has no permission to create files under the mount point.
ENV LIQ_DATA_DIR=/tmp/data
RUN mkdir -p /tmp/data && chown -R appuser:appuser /tmp/data /app

USER appuser

# default: run all pipeline stages in this one container (Railway-style
# single-service hosts). For docker-compose's multi-container setup, each
# service overrides this with its own `command:`.
CMD ["python", "entrypoint.py"]
