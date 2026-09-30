# ---------- Builder ----------
FROM python:3.12-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --upgrade pip

# Copy project metadata
COPY pyproject.toml .

# Copy source code
COPY src ./src

# Install application and dependencies
RUN pip install --no-cache-dir .


# ---------- Runner ----------
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONPATH=/app/src

# Copy installed virtual environment
COPY --from=builder /opt/venv /opt/venv

# Copy source
COPY --from=builder /app/src ./src

EXPOSE 9000

CMD ["uvicorn", "integration_hub.main:app", "--host", "0.0.0.0", "--port", "9000"]