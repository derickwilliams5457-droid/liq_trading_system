FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# default: run all three pipeline stages in this one container (Railway-style
# single-service hosts). For docker-compose's multi-container setup, each
# service overrides this with its own `command:`.
CMD ["python", "entrypoint.py"]

