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
QUERY_2_TOTAL_WORKERS = int(os.environ.get('QUERY_2_TOTAL_WORKERS', 3))
QUERY_3_TOTAL_WORKERS = int(os.environ.get('QUERY_3_TOTAL_WORKERS', 2))
QUERY_4_TOTAL_WORKERS = int(os.environ.get('QUERY_4_TOTAL_WORKERS', 3))

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

    def _sigterm_handler(self, signum, _):
        self.stop_by_sigterm = True
        with self.cond:
            self.cond.notify_all()
        
        # Close all client connections
        with self.clients_lock:
            for client_id, client_info in self.clients.items():
                try:
                    if client_info['socket']:
                        client_info['socket'].close()
                        logging.info(f'Closed connection for client {client_id}')
                except Exception as e:
                    logging.error(f'Error closing client {client_id}: {e}')
            self.clients.clear()
        
        logging.info('closing file descriptors and shutdown [sigterm]')

    def listen_queue_result_dictionary(self, result_queue, result_file, query_type):
        sleep(config.MIDDLEWARE_UP_TIME)  # Esperar a que RabbitMQ esté listo
        queue = MessageMiddlewareQueue(os.environ.get('RABBITMQ_HOST', 'rabbitmq_server'), result_queue)

        def on_message_callback(message: bytes):
            try:
                # Parse message to extract client_id
                parsed_msg = parse_message(message)
                client_id = parsed_msg.get('client_id', 'unknown')
                rows = parsed_msg.get('rows', [])
                
                logging.info(f"[{result_queue}] Mensaje recibido para client {client_id}")
                
                # Convert rows to result string
                dictionary_str = '\n'.join(rows) if rows else ''

                # Save results with client_id prefix
                client_result_file = f"{result_file.replace('.csv', f'_client_{client_id}.csv')}"
                self.save_temp_results(client_result_file, [dictionary_str])

                # Track worker responses per client
                with self.results_lock:
                    if client_id not in self.client_results:
                        self.client_results[client_id] = {}
                    if query_type not in self.client_results[client_id]:
                        self.client_results[client_id][query_type] = {'worker_responses': 0}
                    
                    self.client_results[client_id][query_type]['worker_responses'] += 1
                    
                    # Check if query is complete for this client
                    expected_workers = {
                        1: QUERY_1_TOTAL_WORKERS,
                        2: QUERY_2_TOTAL_WORKERS, 
                        3: QUERY_3_TOTAL_WORKERS,
                        4: QUERY_4_TOTAL_WORKERS
                    }.get(query_type, 1)
                    
                    if self.client_results[client_id][query_type]['worker_responses'] >= expected_workers:
                        self.mark_query_completed_for_client(client_id, query_type, dictionary_str)
                
                logging.info(f"[{result_queue}] Mensaje guardado en {client_result_file} para client {client_id}")
                
            except Exception as e:
                logging.error(f"Error processing message in {result_queue}: {e}")
                # Fallback: try old format
                try:
                    size, dictionary_str = unpack_response_message(message)
                    self.save_temp_results(result_file, [dictionary_str])
                except:
                    logging.error(f"Failed to parse message in both formats: {e}")
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

    def mark_query_completed_for_client(self, client_id, query_type, result_data):
        """Mark query as completed for specific client and send result"""
        with self.clients_lock:
            if client_id in self.clients:
                client_socket = self.clients[client_id]['socket']
                self.clients[client_id]['queries_done'] += 1
                
                try:
                    # Send result to specific client
                    send_response(client_socket, query_type + 2, result_data)  # Q1=3, Q2=4, Q3=5, Q4=6
                    logging.info(f"[CLIENT {client_id}] Query {query_type} result sent")
                    
                    # Check if all queries completed for this client
                    if self.clients[client_id]['queries_done'] >= QUERIES_TO_COMPLETE:
                        logging.info(f"[CLIENT {client_id}] All queries completed, closing connection")
                        client_socket.close()
                        del self.clients[client_id]
                        
                except Exception as e:
                    logging.error(f"Error sending result to client {client_id}: {e}")
                    if client_id in self.clients:
                        del self.clients[client_id]

    def mark_query_completed(self, result_queue):
        """Mark a query as completed based on the result queue"""
        # This function is called when a query completes processing
        # In the current implementation, it's used to mark completion for all clients
        logging.info(f"[{result_queue}] Query marked as completed globally")
        # Additional logic can be added here if needed

    def handle_client_protocol(self, client_socket, client_id):
        """Handle protocol communication with specific client"""
        try:
            self.receive_datasets(client_socket, client_id)
            logging.info(f"[CLIENT {client_id}] Datasets received and processed")
            
            # Send END messages to all workers for this client
            logging.info(f"[CLIENT {client_id}] Sending END messages to all workers...")
            from gateway_protocol import send_end_messages_to_all_workers
            send_end_messages_to_all_workers(client_id)
            logging.info(f"[CLIENT {client_id}] END messages sent to all workers")
            
            # Wait for all queries to complete
            while True:
                with self.clients_lock:
                    if client_id not in self.clients:
                        break
                    if self.clients[client_id]['queries_done'] >= QUERIES_TO_COMPLETE:
                        break
                sleep(1)
                if self.stop_by_sigterm:
                    break
                    
        except Exception as e:
            logging.error(f"[CLIENT {client_id}] Protocol error: {e}")
    
    def clear_temp_files_for_client(self, client_id):
        """Clear temporary result files for specific client"""
        try:
            files_to_clear = [
                f"{RESULT_Q1_FILE.replace('.csv', f'_client_{client_id}.csv')}",
                f"{RESULT_Q2_FILE.replace('.csv', f'_client_{client_id}.csv')}",
                f"{RESULT_Q3_FILE.replace('.csv', f'_client_{client_id}.csv')}",
                f"{RESULT_Q4_FILE.replace('.csv', f'_client_{client_id}.csv')}"
            ]
            for file_path in files_to_clear:
                if os.path.exists(file_path):
                    os.remove(file_path)
        except Exception as e:
            logging.error(f"Error clearing temp files for client {client_id}: {e}")
    
    def handle_client_connection(self, client_socket, client_address, assigned_client_id):
        """Handle individual client connection in separate thread"""
        try:
            logging.info(f"[CLIENT] New connection from {client_address}")
            
            # Receive client_id from client
            try:
                received_client_id = recv_h_str(client_socket)
                client_id = received_client_id if received_client_id else assigned_client_id
                logging.info(f"[CLIENT {client_id}] Received client_id '{received_client_id}' from client at {client_address}")
            except Exception as e:
                client_id = assigned_client_id
                logging.warning(f"[CLIENT {client_id}] Could not receive client_id ({e}), using assigned: {assigned_client_id}")
            
            # Register client
            with self.clients_lock:
                self.clients[client_id] = {
                    'socket': client_socket,
                    'queries_done': 0,
                    'thread': threading.current_thread()
                }
            
            # Initialize client results tracking
            with self.results_lock:
                self.client_results[client_id] = {}
            
            # Clear temp files for this client
            self.clear_temp_files_for_client(client_id)
            
            # Handle client protocol
            self.handle_client_protocol(client_socket, client_id)
            
        except Exception as e:
            logging.error(f"[CLIENT {client_id}] Error handling client: {e}")
        finally:
            # Cleanup
            with self.clients_lock:
                if client_id in self.clients:
                    del self.clients[client_id]
            try:
                client_socket.close()
            except:
                pass
            logging.info(f"[CLIENT {client_id}] Connection closed")
            
    def listen_for_q4_requests(self):
        sleep(config.MIDDLEWARE_UP_TIME)  # Esperar a que RabbitMQ esté listo
        queue = MessageMiddlewareQueue(os.environ.get('RABBITMQ_HOST', 'rabbitmq_server'), Q4_DATA_REQUESTS_QUEUE)

        def on_message_callback(message: bytes):
            try:
                # Parse message to get client_id
                parsed_msg = parse_message(message)
                client_id = parsed_msg.get('client_id', 'unknown')
                
                logging.info(f"[{Q4_DATA_REQUESTS_QUEUE}] Q4 request for client {client_id}")
                
                # Send Q4 step request to specific client
                with self.clients_lock:
                    if client_id in self.clients:
                        client_socket = self.clients[client_id]['socket']
                        send_response(client_socket, response_types.Q4_STEP, "")
                        self.receive_datasets(client_socket, client_id)
                        logging.info(f"[{Q4_DATA_REQUESTS_QUEUE}] Q4 data received from client {client_id}")
                    else:
                        logging.warning(f"[{Q4_DATA_REQUESTS_QUEUE}] Client {client_id} not found for Q4 request")
                        
            except Exception as e:
                logging.error(f"[{Q4_DATA_REQUESTS_QUEUE}] Error handling Q4 request: {e}")

        try:
            queue.start_consuming(on_message_callback)
        except Exception as e:
            logging.error(f"[{Q4_DATA_REQUESTS_QUEUE}] Error consumiendo mensajes: {e}")
    
    def listen_queue_result(self, result_queue, result_file):
        sleep(config.MIDDLEWARE_UP_TIME)  # Esperar a que RabbitMQ esté listo
        logging.info(f"[{result_queue}] Waiting for messages in {result_queue}")
        queue = MessageMiddlewareQueue(os.environ.get('RABBITMQ_HOST', 'rabbitmq_server'), result_queue)
        
        # Separate counters for each query
        q1_last_messages_received = 0
        q2_messages_received = 0
        q3_messages_received = 0
        q4_messages_received = 0
        
        def on_message_callback(message: bytes):
            nonlocal q1_last_messages_received, q2_messages_received, q3_messages_received, q4_messages_received
            
            # Write message to result file (append mode)
            # logging.info(f"[{result_queue}] Mensaje recibido en cola: {result_queue}")
            parsed_message = parse_message(message)
            rows = parsed_message['rows']
            client_id = parsed_message.get('client_id')
            logging.info(f"[{result_queue}] Parsed message: client_id={client_id}, csv_type={parsed_message.get('csv_type')}, is_last={parsed_message.get('is_last')}, rows={len(rows)}")

            # Increment counter for specific query
            if result_queue == RESULT_Q1_QUEUE:
                # q1_messages_received += 1
                pass
                # logging.info(f"[{result_queue}] Received Q1 message {q1_last_messages_received} with {len(rows)} rows, is_last={parsed_message['is_last']}")
            
            if result_queue == RESULT_Q2_QUEUE:
                q2_messages_received += 1
                logging.info(f"[{result_queue}] Received Q2 message {q2_messages_received} with {len(rows)} rows, is_last={parsed_message['is_last']}")
            
            if result_queue == RESULT_Q3_QUEUE:
                q3_messages_received += 1
                logging.info(f"[{result_queue}] Received Q3 message {q3_messages_received} with {len(rows)} rows, is_last={parsed_message['is_last']}")

            if result_queue == RESULT_Q4_QUEUE:
                logging.info(f"[{result_queue}] Received Q4 message {q4_messages_received} with {len(rows)} rows, is_last={parsed_message['is_last']}")

            # Use client_id to create separate result files per client
            if client_id:
                client_result_file = result_file.replace('.csv', f'_{client_id}.csv')
                self.save_temp_results(client_result_file, rows)
                logging.info(f"[{result_queue}] Message saved to {client_result_file} for client {client_id}")
            else:
                self.save_temp_results(result_file, rows)
                logging.warning(f"[{result_queue}] No client_id found, saving to default file {result_file}")
            # logging.info(f"[{result_queue}] Mensaje guardado en {result_file}")

            # Check completion using specific counter for each query
            if parsed_message['is_last'] and result_queue == RESULT_Q1_QUEUE:
                q1_last_messages_received += 1
                if q1_last_messages_received >= QUERY_1_TOTAL_WORKERS:
                    logging.info(f"[{result_queue}] Q1 completed: received {q1_last_messages_received}/{QUERY_1_TOTAL_WORKERS} is_last messages")
                    self.mark_query_completed(result_queue)

            if parsed_message['is_last'] and q2_messages_received == QUERY_2_TOTAL_WORKERS and result_queue == RESULT_Q2_QUEUE:
                logging.info(f"[{result_queue}] Q2 completed: received {q2_messages_received}/{QUERY_2_TOTAL_WORKERS} is_last messages")
                self.mark_query_completed(result_queue)

            if parsed_message["is_last"] and q3_messages_received == QUERY_3_TOTAL_WORKERS and result_queue == RESULT_Q3_QUEUE:
                logging.info(f"[{result_queue}] Q3 completed: received {q3_messages_received}/{QUERY_3_TOTAL_WORKERS} is_last messages")
                self.mark_query_completed(result_queue)

            if parsed_message['is_last'] and result_queue == RESULT_Q4_QUEUE:
                q4_messages_received += 1
                if q4_messages_received >= QUERY_4_TOTAL_WORKERS:
                    logging.info(f"[{result_queue}] Q4 completed: received {q4_messages_received}/{QUERY_4_TOTAL_WORKERS} is_last messages")
                    self.mark_query_completed(result_queue)

        try:
            queue.start_consuming(on_message_callback)
        except Exception as e:
            logging.error(f"[{result_queue}] Error consumiendo mensajes: {e}")

    def receive_datasets(self, client_socket, client_id):
        total_files_received = 0
        total_bytes_received = 0
        
        while True:
            # NOTE: `recv` is unreliable, and `recv_all` guarantees that we will either receive all the bytes
            # or raise a ConnectionError. Therefore, we should not treat it as a function that can return None.
            # To know when to stop receiving file data, we need explicit flags.
            
            filename = recv_h_str(client_socket)
            filesize = recv_long(client_socket)
            last_file = recv_bool(client_socket)
            last_dataset = recv_bool(client_socket)
            
            logging.info(f"[CLIENT {client_id}] Starting to receive file: {filename} ({filesize} bytes), last_file={last_file}, last_dataset={last_dataset}")
            
            # Add client_id to filename to separate client data
            base_filename, ext = os.path.splitext(filename)
            client_filename = f"{base_filename}_client_{client_id}{ext}"
            filepath = os.path.join(OUTPUT_DIR, client_filename)

            received = 0
            chunks_processed = 0
            with open(filepath, "wb") as f:
                file_code = filename_to_type(filename)
                while received < filesize:
                    chunk = recv_h_bytes(client_socket)
                    if self.stop_by_sigterm:
                        return
                    f.write(chunk)
                    received += len(chunk)
                    chunks_processed += 1
                    
                    if file_code in [1, 2]:  # transactions or transaction_items
                        if chunks_processed % 10 == 0 or received >= filesize:  # Log every 10th chunk or at end
                            logging.info(f"[CLIENT {client_id}] Processing chunk {chunks_processed} for {filename}, received: {received}/{filesize} bytes")
                    
                    if len(chunk) != 0:
                        handle_and_forward_chunk(client_id, file_code, 1 if received >= filesize and last_file else 0, chunk)

            total_files_received += 1
            total_bytes_received += filesize
            logging.info(f"[CLIENT {client_id}] File completed: {filename} ({filesize} bytes), chunks processed: {chunks_processed}")
            logging.info(f"[CLIENT {client_id}] Progress: {total_files_received} files, {total_bytes_received} total bytes received")
            
            if last_file and last_dataset:
                logging.info(f"[CLIENT {client_id}] All datasets received - total files: {total_files_received}, total bytes: {total_bytes_received}")
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
            
            logging.info(f"Queries completadas, enviando resultados al cliente...")

            self.send_file_result(RESULT_Q1_FILE)
            self.send_file_result(RESULT_Q2_FILE)
            self.send_file_result(RESULT_Q3_FILE)
            self.send_file_result(RESULT_Q4_FILE)

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
                    logging.info(f"Enviado resultado parcial de {len(lines_batch)} líneas al cliente.")
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
            if os.path.exists(RESULT_Q4_FILE):
                os.remove(RESULT_Q4_FILE)
        except Exception as e:
            logging.error(f"Error eliminando archivos temporales: {e}")

    def run(self):
        # Start result queue listener threads
        q1_result_thread = threading.Thread(target=self.listen_queue_result, args=(RESULT_Q1_QUEUE, RESULT_Q1_FILE), daemon=True)
        q1_result_thread.start()
        q2_result_thread = threading.Thread(target=self.listen_queue_result, args=(RESULT_Q2_QUEUE, RESULT_Q2_FILE), daemon=True)
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
            server.listen(10)  # Increased backlog for multiple clients
            logging.info(f"Multi-client server listening on {HOST}:{PORT}...")

            client_counter = 0
            while not self.stop_by_sigterm:
                try:
                    conn, addr = server.accept()
                    client_counter += 1
                    client_id = f"client_{client_counter}"
                    
                    # Handle each client in separate thread
                    client_thread = threading.Thread(
                        target=self.handle_client_connection,
                        args=(conn, addr, client_id),
                        daemon=True
                    )
                    client_thread.start()
                    
                    logging.info(f"Started thread for {client_id} from {addr}")
                    
                except Exception as e:
                    if not self.stop_by_sigterm:
                        logging.error(f"Error accepting client connection: {e}")
                        
            logging.info("Server shutting down...")