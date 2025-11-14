# FROM python:3.10-slim
FROM python:3.10-alpine

WORKDIR /app

COPY common /app/common
COPY workers/health_checker/main.py /app/main.py

RUN pip install pika
RUN apk update
RUN apk add docker

# Set Python path so it can find the common modules
ENV PYTHONPATH="/app:/app/common"
ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "main.py"]