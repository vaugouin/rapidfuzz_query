# syntax=docker/dockerfile:1
FROM python:3.12-slim-bookworm
WORKDIR /app

# Install system dependencies and update SQLite to version >= 3.35.0
RUN apt-get update && apt-get install -y \
    build-essential \
    libmariadb-dev \
    pkg-config \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install newer SQLite from source (3.40.1 for compatibility with ChromaDB)
RUN wget https://www.sqlite.org/2022/sqlite-autoconf-3400100.tar.gz \
    && tar xzf sqlite-autoconf-3400100.tar.gz \
    && cd sqlite-autoconf-3400100 \
    && ./configure --prefix=/usr/local \
    && make && make install \
    && cd .. && rm -rf sqlite-autoconf-3400100* \
    && ldconfig

# Set environment variable to use the new SQLite
ENV LD_LIBRARY_PATH=/usr/local/lib

COPY requirements.txt /app/
RUN pip install --upgrade pip && pip install -r requirements.txt
COPY . /app/
CMD ["python", "./rapidfuzz_query.py"]
