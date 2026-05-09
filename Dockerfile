FROM python:3.13-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TONLE_HOST=0.0.0.0 \
    TONLE_PORT=8080

COPY pyproject.toml README.md LICENSE ./
COPY tonle ./tonle

RUN pip install --no-cache-dir .

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin tonle \
    && chown -R tonle:tonle /app

USER tonle

CMD ["tonle"]
