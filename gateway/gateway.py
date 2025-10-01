import logging
import os
import signal
from socket import AF_INET, SOCK_STREAM, socket
import threading
from time import sleep

from common import response_types
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

Q4_DATA_REQUESTS_QUEUE = os.environ.get('Q4_DATA_REQUESTS_QUEUE', 'birthday_dictionary_client_request_queue')
RESULT_Q4_QUEUE = os.environ.get('RESULT_Q4_QUEUE', 'query4_answer_queue') # Modify to match others
RESULT_Q4_FILE = os.path.join(OUTPUT_DIR, 'result_q4.csv')

QUERY_1_TOTAL_WORKERS = int(os.environ.get('QUERY_1_TOTAL_WORKERS', 3))
QUERY_3_TOTAL_WORKERS = int(os.environ.get('QUERY_3_TOTAL_WORKERS', 2))

QUERIES_TO_COMPLETE = 3

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

    def listen_queue_result_dictionary(self, result_queue, result_file, query_type):
        sleep(config.MIDDLEWARE_UP_TIME)  # Esperar a que RabbitMQ esté listo
        queue = MessageMiddlewareQueue(os.environ.get('RABBITMQ_HOST', 'rabbitmq_server'), result_queue)

        def on_message_callback(message: bytes):
            # Write message to result_q1.csv (append mode)
            logging.info(f"[{result_queue}] Mensaje recibido en cola: {message}")
            size, dictionary_str = unpack_response_message(message)

            self.save_temp_results(result_file, [dictionary_str])

            # To easily ientify when the query is done
            with open(result_file, 'r') as f:
                lines = f.readlines()
                if len(lines) >= 12:
                    self.mark_query_completed(result_queue)
            
            logging.info(f"[{result_queue}] Mensaje guardado en {result_file}")
            # logging.info(f"[{result_queue}] Enviando respuesta de tamaño {size} al cliente con contenido: {dictionary_str}")
            # send_response(self.client_skt, query_type + 2, dictionary_str) # Q2=4, Q3=5
            # logging.info(f"[{result_queue}] Respuesta enviada.")

        try:
            queue.start_consuming(on_message_callback)
        except Exception as e:
            logging.error(f"[{result_queue}] Error consumiendo mensajes: {e}")

    def save_temp_results(self, result_file, iterable):
        with open(result_file, 'a') as f:  # Changed from 'w+' to 'a' to append instead of overwrite
            for line in iterable:
                if self.stop_by_sigterm:
                    return
                f.write(line)
                f.write('\n')

    def mark_query_completed(self, result_queue):
        with self.cond:
            self.queries_done += 1
            self.cond.notify_all()
            logging.info(f"[{result_queue}] Query completada. Total queries done: {self.queries_done}")

    def listen_for_q4_requests(self):
        sleep(config.MIDDLEWARE_UP_TIME)  # Esperar a que RabbitMQ esté listo
        queue = MessageMiddlewareQueue(os.environ.get('RABBITMQ_HOST', 'rabbitmq_server'), Q4_DATA_REQUESTS_QUEUE)

        def on_message_callback(message: bytes):
            logging.info(f"[{Q4_DATA_REQUESTS_QUEUE}] Mensaje recibido en cola: {message}")
            # This message could be anything, just request de data to the connected client
            send_response(self.client_skt, response_types.Q4_STEP, "")
            self.receive_datasets()
            logging.info(f"[{Q4_DATA_REQUESTS_QUEUE}] Datos recibidos y reenviados al cliente.")

        try:
            queue.start_consuming(on_message_callback)
        except Exception as e:
            logging.error(f"[{Q4_DATA_REQUESTS_QUEUE}] Error consumiendo mensajes: {e}")
    
    def listen_queue_result(self, result_queue, result_file):
        sleep(config.MIDDLEWARE_UP_TIME)  # Esperar a que RabbitMQ esté listo
        logging.info(f"[{result_queue}] Waiting for messages in {result_queue}")
        queue = MessageMiddlewareQueue(os.environ.get('RABBITMQ_HOST', 'rabbitmq_server'), result_queue)
        
        # Separate counters for each query
        q1_messages_received = 0
        q3_messages_received = 0
        
        def on_message_callback(message: bytes):
            nonlocal q1_messages_received, q3_messages_received
            
            # Write message to result file (append mode)
            logging.info(f"[{result_queue}] Mensaje recibido en cola: {result_queue}")
            parsed_message = parse_message(message)
            rows = parsed_message['rows']

            # Increment counter for specific query
            if result_queue == RESULT_Q1_QUEUE:
                q1_messages_received += 1
                logging.info(f"[{result_queue}] Received Q1 message {q1_messages_received} with {len(rows)} rows, is_last={parsed_message['is_last']}")
            
            if result_queue == RESULT_Q3_QUEUE:
                q3_messages_received += 1
                logging.info(f"[{result_queue}] Received Q3 message {q3_messages_received} with {len(rows)} rows, is_last={parsed_message['is_last']}")

            self.save_temp_results(result_file, rows)
            logging.info(f"[{result_queue}] Mensaje guardado en {result_file}")

            # Check completion using specific counter for each query
            if parsed_message['is_last'] and q1_messages_received == QUERY_1_TOTAL_WORKERS and result_queue == RESULT_Q1_QUEUE:
                logging.info(f"[{result_queue}] Q1 completed: received {q1_messages_received}/{QUERY_1_TOTAL_WORKERS} is_last messages")
                self.mark_query_completed(result_queue)

            if parsed_message["is_last"] and q3_messages_received == QUERY_3_TOTAL_WORKERS and result_queue == RESULT_Q3_QUEUE:
                logging.info(f"[{result_queue}] Q3 completed: received {q3_messages_received}/{QUERY_3_TOTAL_WORKERS} is_last messages")
                self.mark_query_completed(result_queue)

        try:
            queue.start_consuming(on_message_callback)
        except Exception as e:
            logging.error(f"[{result_queue}] Error consumiendo mensajes: {e}")

    def receive_datasets(self):
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

    def handle_client(self, addr):
        logging.info(f"Conexión recibida de {addr}")
        self.clear_temp_files()
        try:
            self.receive_datasets()
            logging.info("Todos los archivos fueron recibidos correctamente.")
            
            # Send END messages to all filter_by_year workers
            logging.info("Sending END messages to filter_by_year workers...")
            send_end_messages_to_filter_by_year()
            logging.info("END messages sent to filter_by_year workers.")
            
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
            self.send_file_result(RESULT_Q2_FILE)
            self.send_file_result(RESULT_Q3_FILE)

            self.clear_temp_files()
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

    def clear_temp_files(self):
        try:
            if os.path.exists(RESULT_Q1_FILE):
                os.remove(RESULT_Q1_FILE)
            if os.path.exists(RESULT_Q2_FILE):
                os.remove(RESULT_Q2_FILE)
            if os.path.exists(RESULT_Q3_FILE):
                os.remove(RESULT_Q3_FILE)
        except Exception as e:
            logging.error(f"Error eliminando archivos temporales: {e}")

    def run(self):
        # Start result queue listener thread
        q1_result_thread = threading.Thread(target=self.listen_queue_result, args=(RESULT_Q1_QUEUE, RESULT_Q1_FILE), daemon=True)
        q1_result_thread.start()
        q2_result_thread = threading.Thread(target=self.listen_queue_result_dictionary, args=(RESULT_Q2_QUEUE, RESULT_Q2_FILE, 2), daemon=True)
        q2_result_thread.start()
        q3_result_thread = threading.Thread(target=self.listen_queue_result, args=(RESULT_Q3_QUEUE, RESULT_Q3_FILE), daemon=True)
        q3_result_thread.start()
        
        q4_request_thread = threading.Thread(target=self.listen_for_q4_requests, args=(), daemon=True)
        q4_request_thread.start()
        q4_result_thread = threading.Thread(target=self.listen_queue_result, args=(RESULT_Q4_QUEUE, RESULT_Q4_FILE), daemon=True)
        q4_result_thread.start()

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