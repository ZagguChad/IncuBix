FROM python:3.12-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock
COPY incubrix ./incubrix
RUN pip install --no-cache-dir --no-deps .
RUN useradd --create-home --uid 10001 appuser && mkdir -p /app/data && chown -R appuser:appuser /app
USER appuser
EXPOSE 8000
CMD ["uvicorn", "incubrix.main:app", "--host", "0.0.0.0", "--port", "8000"]

FROM runtime AS test
USER root
COPY requirements-test.lock ./
RUN pip install --no-cache-dir -r requirements-test.lock
COPY tests ./tests
COPY schemas ./schemas
COPY infra ./infra
COPY testpack ./testpack
USER appuser
CMD ["python", "-m", "pytest"]
