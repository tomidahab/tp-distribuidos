import logging
import os
import signal
from socket import AF_INET, SOCK_STREAM, socket
import threading
from time import sleep

from common.middleware import MessageMiddlewareQueue
from common.protocol import parse_message, unpack_response_message
import common.config as config
from gateway_protocol import *

HOST = "0.0.0.0"
PORT = 5000
OUTPUT_DIR = os.path.join("data", "received")

RESULT_Q1_QUEUE = os.environ.get('RESULT_Q1_QUEUE', 'query1_result_receiver_queue')
RESULT_Q1_FILE = os.path.join(OUTPUT_DIR, 'result_q1.csv')

RESULT_Q2_QUEUE = os.environ.get('RESULT_Q2_QUEUE', 'query2_result_receiver_queue')
RESULT_Q2_FILE = os.path.join(OUTPUT_DIR, 'result_q2.csv')

RESULT_Q3_QUEUE = os.environ.get('RESULT_Q3_QUEUE', 'query3_result_receiver_queue')
RESULT_Q3_FILE = os.path.join(OUTPUT_DIR, 'result_q3.csv')

QUERIES_TO_COMPLETE = 1

class Gateway:
    def __init__(self):
        self.client_skt = None
        self.cond = threading.Condition()
        self.queries_done = 0
        self.stop_by_sigterm = False
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        signal.signal(signal.SIGINT, self._sigterm_handler)

    def _sigterm_handler(self, signum, _):
        self.stop_by_sigterm = True
        with self.cond:
            self.cond.notify_all()
        if self.client_skt:
            self.skt.close()
            self.skt = None
        logging.info('closing file descriptors and shutdown [sigterm]')

    def send_response_from_file(self, response_type, source_file):
        # Depending of the architecture, we might need semaphores somewhere to send all query responses
        try:
            with open(source_file, 'r') as f:
                if self.stop_by_sigterm:
                    return
                response = f.read()
                send_response(self.client_skt, response_type, response)
        except Exception as e:
            logging.error(f"Error leyendo archivo de respuesta {source_file}: {e}")

    def listen_queue_result_dictionary(self, result_queue, result_file, query_type):
        sleep(config.MIDDLEWARE_UP_TIME)  # Esperar a que RabbitMQ esté listo
        queue = MessageMiddlewareQueue(os.environ.get('RABBITMQ_HOST', 'rabbitmq_server'), result_queue)

        def on_message_callback(message: bytes):
            # Write message to result_q1.csv (append mode)
            logging.info(f"[{result_queue}] Mensaje recibido en cola: {message}")
            size, dictionary_str = unpack_response_message(message)
            with open(result_file, 'w+') as f:
                if self.stop_by_sigterm:
                    return
                f.write(dictionary_str)
                f.write('\n')
            logging.info(f"[{result_queue}] Mensaje guardado en {result_file}")
            logging.info(f"[{result_queue}] Enviando respuesta de tamaño {size} al cliente con contenido: {dictionary_str}")
            send_response(self.client_skt, query_type + 2, dictionary_str) # Q2=4, Q3=5
            logging.info(f"[{result_queue}] Respuesta enviada.")

        try:
            queue.start_consuming(on_message_callback)
        except Exception as e:
            logging.error(f"[{result_queue}] Error consumiendo mensajes: {e}")

    def listen_queue_result(self, result_queue, result_file):
        sleep(config.MIDDLEWARE_UP_TIME)  # Esperar a que RabbitMQ esté listo
        queue = MessageMiddlewareQueue(os.environ.get('RABBITMQ_HOST', 'rabbitmq_server'), result_queue)

        def on_message_callback(message: bytes):
            # Write message to result_q1.csv (append mode)
            logging.info(f"[{result_queue}] Mensaje recibido en cola: {result_queue}")
            parsed_message = parse_message(message)
            rows = parsed_message['rows']
            with open(result_file, 'a+') as f:
                for row in rows:
                    if self.stop_by_sigterm:
                        return
                    f.write(row)
                    f.write('\n')
            logging.info(f"[{result_queue}] Mensaje guardado en {result_file}")

            if parsed_message["is_last"]:
                with self.cond:
                    self.queries_done += 1
                    self.cond.notify_all()
                    logging.info(f"[{result_queue}] Query completada. Total queries done: {self.queries_done}")

                # logging.info(f"[{result_queue}] Mensaje final recibido, enviando respuesta al cliente.")
                # self.send_response_from_file(parsed_message['csv_type'], result_file)
                # os.remove(result_file)
                # logging.info(f"[{result_queue}] Respuesta enviada, persistencia eliminada.")

        try:
            queue.start_consuming(on_message_callback)
        except Exception as e:
            logging.error(f"[{result_queue}] Error consumiendo mensajes: {e}")

    def handle_client(self, addr):
        logging.info(f"Conexión recibida de {addr}")
        try:
            while True:
                # NOTE: `recv` is unreliable, and `recv_all` guarantees that we will either receive all the bytes
                # or raise a ConnectionError. Therefore, we should not treat it as a function that can return None.
                # To know when to stop receiving file data, we need explicit flags.
                
                filename = recv_h_str(self.client_skt)
                filesize = recv_long(self.client_skt)
                last_file = recv_bool(self.client_skt)
                last_dataset = recv_bool(self.client_skt)

                filepath = os.path.join(OUTPUT_DIR, filename)
    
                received = 0
                with open(filepath, "wb") as f:
                    file_code = filename_to_type(filename)
                    while received < filesize:
                        chunk = recv_h_bytes(self.client_skt)
                        if self.stop_by_sigterm:
                            return
                        f.write(chunk)
                        received += len(chunk)  
                        if file_code == 1:
                            logging.info(f"[GATEWAY] Receiving chunk for file {filename}, total received: {received}/{filesize} bytes, len: {len(chunk)}")
                        if len(chunk) != 0:
                            handle_and_forward_chunk(0, file_code, 1 if received >= filesize and last_file else 0, chunk)
    
                logging.info(f"Archivo recibido: {filename} ({filesize} bytes)")
                if last_file and last_dataset:
                    break
    
            # All files sent, now wait for queries to finish
            with self.cond:
                while self.queries_done < QUERIES_TO_COMPLETE and not self.stop_by_sigterm:
                    self.cond.wait()
                # Reset for next client
                self.queries_done = 0
    
            # All queries done, now send results from files
            if self.stop_by_sigterm:
                return

            self.send_file_result(RESULT_Q1_FILE)
            os.remove(RESULT_Q1_FILE)
            # self.send_file_result(RESULT_Q2_FILE)
            # os.remove(RESULT_Q2_FILE)
            # self.send_file_result(RESULT_Q3_FILE)
            # os.remove(RESULT_Q3_FILE)

            # self.send_response_from_file(1, RESULT_Q1_FILE)
            # self.send_response_from_file(1, RESULT_Q2_FILE)
            # self.send_response_from_file(1, RESULT_Q3_FILE)

            logging.info(f"Respuesta enviada, persistencia eliminada.")
    
        except Exception as e:
            logging.error(f"Error manejando cliente {addr}: {e}")
            
    def send_file_result(self, file):
        with open(file, 'r') as f:
            lines_batch = []
            for line in f:
                lines_batch.append(line)
                if len(lines_batch) == 10:
                    send_lines_batch(self.client_skt, 1, lines_batch, False)
                    # logging.info(f"Enviado resultado parcial de {len(lines_batch)} líneas al cliente.")
                    lines_batch = []
                if self.stop_by_sigterm:
                    return
            send_lines_batch(self.client_skt,1, lines_batch, True)
            logging.info(f"Envio completado al cliente.")

    def run(self):
        # Start result queue listener thread
        q1_result_thread = threading.Thread(target=self.listen_queue_result, args=(RESULT_Q1_QUEUE, RESULT_Q1_FILE), daemon=True)
        q1_result_thread.start()
        q2_result_thread = threading.Thread(target=self.listen_queue_result_dictionary, args=(RESULT_Q2_QUEUE, RESULT_Q2_FILE, 2), daemon=True)
        q2_result_thread.start()
        q3_result_thread = threading.Thread(target=self.listen_queue_result, args=(RESULT_Q3_QUEUE, RESULT_Q3_FILE), daemon=True)
        q3_result_thread.start()
        
        # Creates the directory if it doesn't exist
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        with socket(AF_INET, SOCK_STREAM) as server:
            server.bind((HOST, PORT))
            server.listen(5)
            logging.info(f"Escuchando en {HOST}:{PORT}...")

            while True:
                conn, addr = server.accept()
                self.client_skt = conn
                self.handle_client(addr)