from socket import socket, AF_INET, SOCK_STREAM
import os
import signal
from common.protocol_utils import *
from client.client_protocol import *
import logging
import common.response_types as response_types

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
        logging.info('closing file descriptors and shutdown [sigterm]')
    
    def _send_file(self, file_path, last_file, last_dataset):
        filename = os.path.basename(file_path)
        filesize = os.path.getsize(file_path)

        self.current_file = open(file_path, "rb")
        header_line = self.current_file.readline() # Read and skip header

        send_h_str(self.skt, filename)
        send_long(self.skt, filesize - len(header_line))
        send_bool(self.skt, last_file)
        send_bool(self.skt, last_dataset)

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

    def recv_query_result(self, query):
        is_last = False
        print(f"-------{query}-------")
        count = 5 # Showing only first 5 results (for debug only)
        while not is_last:
            data_type, lines_batch, is_last = recv_lines_batch(self.skt)
            for line in lines_batch:
                if count > 0:
                    print(f"[{query} result]: ", line.strip())
                    count -= 1
        

    def run(self, data_sets, q4_dataset):
        try:
            self.skt = socket(AF_INET, SOCK_STREAM)
            self.skt.connect((SERVER_HOST, SERVER_PORT))
            logging.info(f"Conectado a {SERVER_HOST}:{SERVER_PORT}")
            for data_set_files in data_sets[:-1]:
                for file in data_set_files[:-1]:
                    self._send_file(file, False, False)
                self._send_file(data_set_files[-1], True, False)

            for file in data_sets[-1][:-1]:
                self._send_file(file, False, True)
            self._send_file(data_sets[-1][-1], True, True)

            logging.info("Todos los archivos fueron enviados correctamente.")

            # Now we expect to receive the q4 signal to send the extra data 
            if q4_dataset:
                type, _ = recv_response(self.skt)
                if type != response_types.Q4_STEP:
                    logging.error(f"Tipo de respuesta inesperada: {type}")
                    return

                # Send extra data for Q4
                for file in q4_dataset[:-1]:
                    self._send_file(file, False, True)
                self._send_file(q4_dataset[-1], True, True)

            # Wait for results
            self.recv_query_result("Q1")
            # self.recv_query_result("Q2")
            self.recv_query_result("Q3")

            if q4_dataset:
                # self.recv_query_result("Q4")
                pass

            logging.info("Los resultados fueron recibidos correctamete.")
        except OSError as e:
            logging.error(f"Error: {e}")
        finally:
            if self.skt:
                self.skt.close()
            if self.current_file:
                self.current_file.close()
