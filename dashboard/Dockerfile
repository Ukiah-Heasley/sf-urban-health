FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_SYSTEM_PYTHON=1 \
    UV_PROJECT_ENVIRONMENT=/usr/local

# uv for fast, reproducible installs from the project's lockfile.
COPY --from=ghcr.io/astral-sh/uv:0.5.4 /uv /usr/local/bin/uv

WORKDIR /app

# Install the dashboard dependency group only — keeps the image lean.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project --only-group dashboard

COPY dashboard ./dashboard

EXPOSE 8050

CMD ["gunicorn", "-b", "0.0.0.0:8050", "dashboard.app:server"]
