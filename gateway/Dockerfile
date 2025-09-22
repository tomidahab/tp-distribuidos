FROM python:3.10-slim

WORKDIR /app

COPY gateway/ ./gateway
COPY common/ ./common

RUN pip install pika

EXPOSE 5000

ENV PYTHONPATH="/app/common"
ENV PYTHONPATH="/app:/app/common"
CMD ["python", "gateway/main.py"]
