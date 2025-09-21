
FROM python:3.10-slim

WORKDIR /app

COPY main.py .

COPY ../../common ./common

RUN pip install pika

CMD ["python", "main.py"]
