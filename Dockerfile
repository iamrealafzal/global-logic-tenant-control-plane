FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app
COPY pyproject.toml ./
COPY controlplane ./controlplane
RUN pip install --no-cache-dir .

RUN useradd --create-home --uid 65532 app \
    && mkdir -p /data \
    && chown app:app /data
USER app
EXPOSE 8080
CMD ["python", "-m", "controlplane"]
