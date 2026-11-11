FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml ./
COPY app.py kpi.py ./
COPY scripts ./scripts

RUN pip install --no-cache-dir . \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin kpi

USER kpi

CMD ["kpi-processor"]
