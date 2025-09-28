FROM python:3.10-slim

WORKDIR /app

COPY common /app/common
COPY gateway/main.py /app/main.py
COPY gateway/gateway_protocol.py /app/gateway_protocol.py


RUN pip install pika

EXPOSE 5000

ENV PYTHONPATH="/app/common"
ENV PYTHONPATH="/app:/app/common"
CMD ["python", "main.py"]
