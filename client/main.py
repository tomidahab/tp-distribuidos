import os
import sys
import re
import logging
from socket import socket, AF_INET, SOCK_STREAM
from time import sleep

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.protocol_utils import *

# Configuración del logger
logging.basicConfig(
    level=logging.INFO,
    format="[CLIENT] %(asctime)s - %(levelname)s - %(message)s"
)

SERVER_HOST = "gateway"  # local test, cambiar a "gateway" en docker
SERVER_PORT = 5000

PATTERNS = {
    "transactions": re.compile(r"^transactions_\d{4}\d{2}\.csv$"),
    "transaction_items": re.compile(r"^transaction_items_\d{4}\d{2}\.csv$"),
    "users": re.compile(r"^users_\d{4}\d{2}\.csv$"),
    "menu_items": re.compile(r"^menu_items\.csv$")
}

def collect_files(folder):
    files = os.listdir(folder)
    transactions = sorted([f for f in files if PATTERNS["transactions"].match(f)])
    transaction_items = sorted([f for f in files if PATTERNS["transaction_items"].match(f)])
    users = sorted([f for f in files if PATTERNS["users"].match(f)])
    menu_items = [f for f in files if PATTERNS["menu_items"].match(f)]
    ordered = menu_items + transactions + transaction_items + users
    return [os.path.join(folder, f) for f in ordered]

def send_file(skt, file_path, last_file):
    filename = os.path.basename(file_path)
    filesize = os.path.getsize(file_path)

    send_h_str(skt, filename)
    send_long(skt, filesize)
    send_bool(skt, last_file)

    CHUNK_TARGET = 4096
    with open(file_path, "rb") as f:
        buffer = b""
        for line in f:
            if len(buffer) + len(line) > CHUNK_TARGET and buffer:
                send_h_bytes(skt, buffer)
                buffer = b""
            buffer += line
        if buffer:
            send_h_bytes(skt, buffer)

    logging.info(f"Archivo {filename} enviado ({filesize} bytes).")

def main(folder):
    file_list = collect_files(folder)
    if not file_list:
        logging.warning("No se encontraron archivos para enviar.")
        return

    with socket(AF_INET, SOCK_STREAM) as skt:
        skt.connect((SERVER_HOST, SERVER_PORT))
        logging.info(f"Conectado a {SERVER_HOST}:{SERVER_PORT}")

        for file in file_list[:-1]:
            send_file(skt, file, False)
        send_file(skt, file_list[-1], True)

    logging.info("Todos los archivos fueron enviados correctamente.")

if __name__ == "__main__":
    folder = os.path.join("data", "input") if len(sys.argv) < 2 else sys.argv[1]
    main(folder)
