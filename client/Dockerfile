FROM python:3.10-slim

WORKDIR /app

COPY common /app/common
COPY client.py /app/

CMD ["python", "client.py", "/data/input"]
