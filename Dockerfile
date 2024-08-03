# The builder image, used to build the virtual environment
FROM python:3.12.3-slim AS runtime

WORKDIR /app

COPY requirements.lock ./
RUN PYTHONDONTWRITEBYTECODE=1 pip install --no-cache-dir -r requirements.lock

COPY alembic.ini ./

# Create empty secrets and settings files, if they don't exist, so the subsequent COPY doesn't fail.
RUN touch ./client_secrets.json
RUN touch ./settings.yaml
COPY client_secrets.json settings.yaml ./

COPY src ./

ENTRYPOINT ["python", "main.py"]