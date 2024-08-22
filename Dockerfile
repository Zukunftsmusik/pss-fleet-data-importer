# The builder image, used to build the virtual environment
FROM python:3.12.3-slim AS runtime

WORKDIR /app

COPY requirements.lock ./
RUN PYTHONDONTWRITEBYTECODE=1 pip install --no-cache-dir -r requirements.lock

COPY alembic.ini main.py ./

# Create empty secrets and settings files, if they don't exist, so the subsequent COPY doesn't fail.
COPY requirements.lock client_secrets.jso[n] settings.yam[l] ./

COPY src ./src

ENTRYPOINT ["python", "main.py"]