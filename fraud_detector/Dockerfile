FROM python:3.12-slim AS base

WORKDIR /app

# Создание директории для логов
RUN mkdir -p /app/logs && \
    touch /app/logs/service.log && \
    chmod -R 777 /app/logs  # Права на запись для всех пользователей

# Установка зависимостей
COPY requirements/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Копирование исходного кода
FROM base AS development
COPY requirements/dev_requirements.txt /tmp/dev_requirements.txt
RUN pip install --no-cache-dir -r /tmp/dev_requirements.txt
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]

FROM base as inference
COPY app ./app
COPY src ./src
COPY artifacts ./artifacts
CMD ["python", "./app/app.py"]