# The builder image, used to build the virtual environment
FROM python:3.12.3-slim as runtime

WORKDIR /app

COPY requirements.lock ./
RUN PYTHONDONTWRITEBYTECODE=1 pip install --no-cache-dir -r requirements.lock

COPY alembic.ini ./alembic.ini
COPY src ./src

RUN alembic upgrade head

ENTRYPOINT ["fastapi", "run", "src/api/main.py", "--port", "80"]