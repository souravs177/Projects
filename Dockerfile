FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SPARK_LOCAL_IP=127.0.0.1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY conf ./conf
COPY data ./data
COPY docs ./docs
COPY orchestration ./orchestration
COPY sql ./sql
COPY src ./src
COPY tests ./tests

RUN python -m pip install --upgrade pip \
    && python -m pip install -e .[dev]

CMD ["python", "-m", "healthcare_medallion.pipeline", "--layer", "all"]
