import os
import sys
import logging
from socket import socket, AF_INET, SOCK_STREAM

from gateway_protocol import filename_to_type, handle_and_forward_chunk
from common.protocol_utils import *

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configuración del logger
logging.basicConfig(
    level=logging.INFO,
    format="[GATEWAY] %(asctime)s - %(levelname)s - %(message)s"
)

HOST = "0.0.0.0"
PORT = 5000
OUTPUT_DIR = os.path.join("data", "received")

os.makedirs(OUTPUT_DIR, exist_ok=True)

def handle_client(conn, addr):
    logging.info(f"Conexión recibida de {addr}")
    try:
        while True:
            # NOTE: `recv` is unreliable, and `recv_all` guarantees that we will either receive all the bytes
            # or raise a ConnectionError. Therefore, we should not treat it as a function that can return None.
            # To know when to stop receiving file data, we need explicit flags.
            
            filename = recv_h_str(conn)
            filesize = recv_long(conn)
            last_file = recv_bool(conn)

            filepath = os.path.join(OUTPUT_DIR, filename)

            received = 0
            with open(filepath, "wb") as f:
                file_code = filename_to_type(filename)
                while received < filesize:
                    if received != 0:
                        # handle_and_forward_chunk(0, file_code, 0, chunk)
                        print("todo! forward chunk")
                    chunk = recv_h_bytes(conn)
                    f.write(chunk)
                    received += len(chunk)
                #handle_and_forward_chunk(0, file_code, 1, chunk)

            logging.info(f"Archivo recibido: {filename} ({filesize} bytes)")
            if last_file:
                break

    except Exception as e:
        logging.error(f"Error manejando cliente {addr}: {e}")

def main():
    with socket(AF_INET, SOCK_STREAM) as server:
        server.bind((HOST, PORT))
        server.listen(5)
        logging.info(f"Escuchando en {HOST}:{PORT}...")

        while True:
            conn, addr = server.accept()
            with conn:
                handle_client(conn, addr)

if __name__ == "__main__":
    main()
