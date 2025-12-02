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
from client_handler import ClientHandler
from gateway_protocol import *
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

QUERIES_TO_COMPLETE = 4

class Gateway:
    def __init__(self):
        self.clients = {}  # {client_id: {'socket': socket, 'queries_done': int, 'thread': thread}}
        self.clients_lock = threading.RLock()
        self.cond = threading.Condition()
        self.stop_by_sigterm = False
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        signal.signal(signal.SIGINT, self._sigterm_handler)
        
        # Track results per client
        self.client_results = {}  # {client_id: {query_type: {worker_responses: int}}}
        self.results_lock = threading.RLock()
        
        # Track duplicate messages by sender to prevent duplicate results (in-memory only)
        self.last_message_id_by_sender = {}  # {sender: last_message_id}
        self.duplicates_lock = threading.RLock()

    def _sigterm_handler(self, signum, _):
        self.stop_by_sigterm = True
        
        # Close all client connections
        with self.clients_lock:
            for id in self.clients:
                self.clients[id].kill()
            for id in self.clients:
                self.clients[id].join()
            self.clients.clear()
        
        logging.info('closing file descriptors and shutdown [sigterm]')
            
    def listen_for_q4_requests(self):
        sleep(config.MIDDLEWARE_UP_TIME)  # Esperar a que RabbitMQ esté listo
        queue = MessageMiddlewareQueue(os.environ.get('RABBITMQ_HOST', 'rabbitmq_server'), Q4_DATA_REQUESTS_QUEUE)

        def on_message_callback(message: bytes, delivery_tag=None, channel=None):
            try:
                # Parse message to get client_id
                parsed_msg = parse_message(message)
                client_id = parsed_msg.get('client_id', 'unknown')
                logging.info(f'Get a USERS DATA Request for {client_id}')
                
                with self.clients_lock:
                    self.clients[client_id].handle_q4_data_request()
                
                # Manual ACK after successful processing
                if delivery_tag and channel:
                    channel.basic_ack(delivery_tag=delivery_tag)
                        
            except Exception as e:
                logging.error(f"[{Q4_DATA_REQUESTS_QUEUE}] Error handling Q4 request: {e}")
                # NACK on error to requeue
                if delivery_tag and channel:
                    channel.basic_nack(delivery_tag=delivery_tag, requeue=True)

        try:
            queue.start_consuming(on_message_callback, auto_ack=False)
        except Exception as e:
            logging.error(f"[{Q4_DATA_REQUESTS_QUEUE}] Error consumiendo mensajes: {e}")
    

    def queue_to_query(self, queue):
        queue_to_query = {
                RESULT_Q1_QUEUE: "q1",
                RESULT_Q2_QUEUE: "q2",
                RESULT_Q3_QUEUE: "q3",
                RESULT_Q4_QUEUE: "q4",
            }
        return queue_to_query[queue]

    def listen_queue_result(self, result_queue):
        sleep(config.MIDDLEWARE_UP_TIME)  # Esperar a que RabbitMQ esté listo
        logging.info(f"[{result_queue}] Waiting for messages in {result_queue}")
        queue = MessageMiddlewareQueue(os.environ.get('RABBITMQ_HOST', 'rabbitmq_server'), result_queue)
        
        def on_message_callback(message: bytes, delivery_tag=None, channel=None):
            try:
                # Handle message by client
                parsed_message = parse_message(message)
                client_id = parsed_message.get('client_id')
                message_id = parsed_message.get('message_id', '')
                sender = parsed_message.get('sender', 'unknown')
                
                # Check for duplicate messages from the same sender (simple last message_id comparison)
                with self.duplicates_lock:
                    last_msg_id = self.last_message_id_by_sender.get(sender)
                    
                    if message_id and message_id == last_msg_id:
                        logging.info(f"[{result_queue}] DUPLICATE message detected from sender {sender}, message_id {message_id} (same as last) - skipping")
                        # ACK duplicate to avoid requeue
                        if delivery_tag and channel:
                            channel.basic_ack(delivery_tag=delivery_tag)
                        return
                    
                    # Update last message_id for this sender
                    if message_id:
                        self.last_message_id_by_sender[sender] = message_id
                        logging.debug(f"[{result_queue}] Updated last message_id for sender {sender}: {message_id}")

                with self.clients_lock:
                    self.clients[client_id].handle_message(self.queue_to_query(result_queue), parsed_message)
                
                # Manual ACK after successful processing
                if delivery_tag and channel:
                    channel.basic_ack(delivery_tag=delivery_tag)
                    
            except Exception as e:
                logging.error(f"[{result_queue}] Error handling result message: {e}")
                # NACK on error to requeue
                if delivery_tag and channel:
                    channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
        try:
            queue.start_consuming(on_message_callback, auto_ack=False)
        except Exception as e:
            logging.error(f"[{result_queue}] Error consumiendo mensajes: {e}")

    def get_available_id(self):
        id = 1 # Starting id with 1
        while f"client_{id}" in self.clients:
            id += 1
        return f"client_{id}"

    def run(self):
        # Start result queue listener threads
        q1_result_thread = threading.Thread(target=self.listen_queue_result, args=(RESULT_Q1_QUEUE,), daemon=True)
        q1_result_thread.start()
        q2_result_thread = threading.Thread(target=self.listen_queue_result, args=(RESULT_Q2_QUEUE,), daemon=True)
        q2_result_thread.start()
        q3_result_thread = threading.Thread(target=self.listen_queue_result, args=(RESULT_Q3_QUEUE,), daemon=True)
        q3_result_thread.start()
        
        q4_request_thread = threading.Thread(target=self.listen_for_q4_requests, args=(), daemon=True)
        q4_request_thread.start()
        q4_result_thread = threading.Thread(target=self.listen_queue_result, args=(RESULT_Q4_QUEUE,), daemon=True)
        q4_result_thread.start()

        # Creates the directory if it doesn't exist
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        with socket(AF_INET, SOCK_STREAM) as server:
            server.bind((HOST, PORT))
            server.listen(10)  # Increased backlog for multiple clients
            logging.info(f"Multi-client server listening on {HOST}:{PORT}...")

            while not self.stop_by_sigterm:
                try:
                    conn, addr = server.accept()
                    with self.clients_lock:
                        client_id = self.get_available_id()
                        self.clients[client_id] = ClientHandler(client_id, conn)
                        self.clients[client_id].start()
                        logging.info(f"Started thread for {client_id} from {addr}")

                        # Cleanup of death clients
                        for id in list(self.clients.keys()):
                            if self.clients[id].killed:
                                self.clients[id].join()
                                del self.clients[id]
                    
                except Exception as e:
                    if not self.stop_by_sigterm:
                        logging.error(f"Error accepting client connection: {e}")
                        
            logging.info("Server shutting down...")