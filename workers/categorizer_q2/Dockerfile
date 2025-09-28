FROM python:3.10-slim

WORKDIR /app

COPY common /app/common
COPY workers/categorizer_q2/main.py /app/main.py

RUN pip install pika

CMD ["python", "main.py"]