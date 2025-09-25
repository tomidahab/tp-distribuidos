import os
import sys
import struct
import logging
from socket import socket, AF_INET, SOCK_STREAM

from gateway_protocol import filename_to_type, handle_and_forward_chunk

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.socket_utils import recv_all

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
            header = conn.recv(4)
            if not header:
                break
            filename_len = struct.unpack("!I", header)[0]
            filename = recv_all(conn, filename_len).decode("utf-8")
            filesize = struct.unpack("!Q", recv_all(conn, 8))[0]

            filepath = os.path.join(OUTPUT_DIR, filename)

            received = 0
            with open(filepath, "wb") as f:
                file_code = filename_to_type(filename)
                while received < filesize:
                    if received != 0:
                        handle_and_forward_chunk(0, file_code, 0, chunk)
                        #print("todo! forward chunk")
                    chunk_header = recv_all(conn, 4)
                    if not chunk_header:
                        break
                    chunk_size = struct.unpack("!I", chunk_header)[0]
                    chunk = recv_all(conn, chunk_size)
                    f.write(chunk)
                    received += chunk_size
                #handle_and_forward_chunk(0, file_code, 1, chunk)

            logging.info(f"Archivo recibido: {filename} ({filesize} bytes)")

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
