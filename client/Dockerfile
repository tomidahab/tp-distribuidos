FROM python:3.9-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
COPY ../common ./common

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]