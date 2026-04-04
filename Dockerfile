# syntax=docker/dockerfile:1

FROM python:3.12-slim-bookworm

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "rapidfuzz_query.py"]
