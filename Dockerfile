# Imagen del motor (FastAPI + SQLite). Sin dependencias de desarrollo ni tests.
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=/usr/local/bin/python3 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Capa de dependencias (cacheable) y luego el proyecto, ambas desde el lockfile.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev

# Usuario sin privilegios; solo /app/data es escribible (volumen con nombre).
RUN useradd --system --uid 10001 --no-create-home engine \
    && mkdir -p /app/data \
    && chown engine /app/data
USER engine

ENV PATH=/app/.venv/bin:$PATH \
    RECOVERY_DATA_DIR=/app/data

EXPOSE 8000

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=10 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2)"]

# Un único proceso: SQLite tiene un solo escritor y el worker es seguro ante concurrencia,
# pero no hace falta más de un proceso uvicorn para el MVP.
CMD ["uvicorn", "recovery_engine.api.app:create_app", "--factory", \
     "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--no-server-header"]
