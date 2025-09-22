
FROM python:3.10-slim

COPY common /app/common
COPY workers/filter_by_amount/main.py /app/main.py

RUN pip install pika

CMD ["python", "main.py"]