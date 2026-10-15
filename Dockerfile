FROM python:3.12-slim

ARG GIT_SHA=unknown
ENV GIT_SHA=${GIT_SHA}

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./
COPY stations/ ./stations/

EXPOSE ${HTTP_PORT:-8000}
CMD uvicorn main:app --host 0.0.0.0 --port ${HTTP_PORT:-8000}
