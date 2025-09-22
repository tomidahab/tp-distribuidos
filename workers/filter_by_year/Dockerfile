
FROM python:3.10-slim

WORKDIR /app

COPY common /app/common
COPY workers/filter_by_year/main.py /app/main.py

RUN pip install pika

CMD ["python", "main.py"]
