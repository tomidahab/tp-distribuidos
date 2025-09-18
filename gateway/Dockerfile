FROM python:3.10-slim

WORKDIR /app

COPY common /app/common
COPY gateway.py /app/

EXPOSE 5000

CMD ["python", "gateway.py"]
