FROM python:3.10-slim

WORKDIR /app

COPY common /app/common
COPY client/main.py /app/

CMD ["python", "main.py", "/data/input"]
