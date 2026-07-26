FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# each service overrides this with its own `command:` in docker-compose.yml
CMD ["python", "run_bot.py"]
