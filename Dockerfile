FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    PYTHONPATH=/app/src

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt \
 && python -m playwright install --with-deps chromium \
 && rm -rf /var/lib/apt/lists/*

# /data holds the re-run history (a named volume inherits this ownership).
RUN useradd --create-home --uid 10001 app && install -d -o app -g app /data
COPY src/ src/
USER app

HEALTHCHECK --interval=60s --timeout=10s --start-period=60s --retries=3 \
  CMD ["python", "-m", "nesi", "healthcheck"]

ENTRYPOINT ["python", "-m", "nesi"]
CMD ["scheduler"]
