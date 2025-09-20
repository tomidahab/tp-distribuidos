import os
import struct
import sys
import re
import logging
from socket import socket, AF_INET, SOCK_STREAM

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.socket_utils import send_all

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
    ordered = transactions + transaction_items + users + menu_items
    return [os.path.join(folder, f) for f in ordered]

def send_file(skt, file_path):
    filename = os.path.basename(file_path)
    filesize = os.path.getsize(file_path)

    filename_bytes = filename.encode("utf-8")
    send_all(skt, struct.pack("!I", len(filename_bytes)))
    send_all(skt, filename_bytes)
    send_all(skt, struct.pack("!Q", filesize))

    with open(file_path, "rb") as f:
        while chunk := f.read(4096):
            send_all(skt, chunk)

    logging.info(f"Archivo {filename} enviado ({filesize} bytes).")

def main(folder):
    file_list = collect_files(folder)
    if not file_list:
        logging.warning("No se encontraron archivos para enviar.")
        return

    with socket(AF_INET, SOCK_STREAM) as skt:
        skt.connect((SERVER_HOST, SERVER_PORT))
        logging.info(f"Conectado a {SERVER_HOST}:{SERVER_PORT}")

        for file_path in file_list:
            send_file(skt, file_path)

    logging.info("Todos los archivos fueron enviados correctamente.")

if __name__ == "__main__":
    folder = os.path.join("data", "input") if len(sys.argv) < 2 else sys.argv[1]
    main(folder)
