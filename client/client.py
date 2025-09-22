from socket import socket, AF_INET, SOCK_STREAM
import os
import signal
from common.protocol_utils import *
import logging

SERVER_HOST = "gateway"  # local test, cambiar a "gateway" en docker
SERVER_PORT = 5000
CHUNK_TARGET = 4096

class Client:
    def __init__(self):
        self.skt = None
        self.current_file = None
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        signal.signal(signal.SIGINT, self._sigterm_handler)

    def _sigterm_handler(self, signum, _):
        if self.skt:
            self.skt.close()
            self.skt = None
        if self.current_file:
            self.current_file.close()
            self.current_file = None
        logging.info('closing server socket [sigterm]')
    
    def _send_file(self, file_path, last_file):
        filename = os.path.basename(file_path)
        filesize = os.path.getsize(file_path)

        send_h_str(self.skt, filename)
        send_long(self.skt, filesize)
        send_bool(self.skt, last_file)

        self.current_file = open(file_path, "rb")
        buffer = b""
        for line in self.current_file:            
            if len(buffer) + len(line) > CHUNK_TARGET and buffer:
                send_h_bytes(self.skt, buffer)
                buffer = b""
            buffer += line
        if buffer:
            send_h_bytes(self.skt, buffer)
            
        self.current_file.close()
        logging.info(f"Archivo {filename} enviado ({filesize} bytes).")

    def run(self, file_list):
        try:
            self.skt = socket(AF_INET, SOCK_STREAM)
            self.skt.connect((SERVER_HOST, SERVER_PORT))
            logging.info(f"Conectado a {SERVER_HOST}:{SERVER_PORT}")
            for file in file_list[:-1]:
                    self._send_file(file, False)
            self._send_file(file_list[-1], True)

            logging.info("Todos los archivos fueron enviados correctamente.")
        except OSError as e:
            logging.error(f"Error: {e}")
        finally:
            if self.skt:
                self.skt.close()
            if self.current_file:
                self.current_file.close()
