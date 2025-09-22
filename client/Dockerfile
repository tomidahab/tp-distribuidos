FROM python:3.10-slim

WORKDIR /app

COPY client/ ./client
COPY common/ ./common

COPY client/main.py /app/main.py
CMD ["python", "/app/main.py", "/data/input"]
