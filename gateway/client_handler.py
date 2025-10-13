import logging
import os
from socket import socket
import threading

from common import response_types
from gateway_protocol import *

QUERIES_TO_COMPLETE = 4

OUTPUT_DIR = os.path.join("data", "received")

MAX_QUERY_WORKERS = { "q1":2, "q2":3, "q3":2, "q4":1 }

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')

class ClientHandler(threading.Thread):
    def __init__(self, client_id, skt: socket):
        super().__init__(daemon=True)
        self.client_id = client_id
        self.skt = skt
        self.cond = threading.Condition()
        self.q4_request_cond = threading.Condition()
        self.q4_data_requested = False
        self.queries_done = 0
        self.killed = False
        self.query_workers = { "q1":0, "q2":0, "q3":0, "q4":0 }

        # Create the exchanges and queues for this client
        self.transactions_exchange = MessageMiddlewareExchange(
            RABBITMQ_HOST, 
            'filter_by_year_transactions_exchange', 
            'topic',
            ""
        )
        self.transaction_items_exchange = MessageMiddlewareExchange(
            RABBITMQ_HOST, 
            'filter_by_year_transaction_items_exchange', 
            'topic',
            ""
        )
        self.categorizer_items_exchange = MessageMiddlewareExchange(
            RABBITMQ_HOST,
            'categorizer_q2_items_fanout_exchange',
            "fanout",
            ""
        )  
        self.birth_dic_queue = MessageMiddlewareQueue(
            RABBITMQ_HOST, 
            os.environ.get('Q4_DATA_RESPONSES_QUEUE', 'gateway_client_data_queue')
        )

    def save_temp_results(self, result_file, iterable):
        with open(result_file, 'a') as f:  # Changed from 'w+' to 'a' to append instead of overwrite
            for line in iterable:
                if self.killed:
                    return
                f.write(line)
                f.write('\n')

    def handle_q4_data_request(self):
        with self.q4_request_cond:
            self.q4_data_requested = True
            self.q4_request_cond.notify_all()
        
    def handle_message(self, query, parsed_message):

        # Write message to result file (append mode)
        rows = parsed_message['rows']
        client_id = self.client_id
        logging.info(f"[{query}][{self.client_id}] Parsed message: client_id={client_id}, csv_type={parsed_message.get('csv_type')}, is_last={parsed_message.get('is_last')}, rows={len(rows)}")
        
        self.save_temp_results(self.query_path(query), rows)

        if parsed_message["is_last"]:
            self.query_workers[query] += 1
            if self.query_workers[query] == MAX_QUERY_WORKERS[query]:
                logging.info(f"[{query.upper()}] completed: received {self.query_workers[query]}/{MAX_QUERY_WORKERS[query]} is_last messages")
                self.mark_query_completed()

    def receive_datasets(self):
        total_files_received = 0
        total_bytes_received = 0
        
        while True:
            # NOTE: `recv` is unreliable, and `recv_all` guarantees that we will either receive all the bytes
            # or raise a ConnectionError. Therefore, we should not treat it as a function that can return None.
            # To know when to stop receiving file data, we need explicit flags.
            
            filename = recv_h_str(self.skt)
            filesize = recv_long(self.skt)
            last_file = recv_bool(self.skt)
            last_dataset = recv_bool(self.skt)
            
            logging.info(f"[CLIENT {self.client_id}] Starting to receive file: {filename} ({filesize} bytes), last_file={last_file}, last_dataset={last_dataset}")
            
            # Add client_id to filename to separate client data
            base_filename, ext = os.path.splitext(filename)
            client_filename = f"{base_filename}_client_{self.client_id}{ext}"
            filepath = os.path.join(OUTPUT_DIR, client_filename)

            received = 0
            chunks_processed = 0
            with open(filepath, "wb") as f:
                file_code = filename_to_type(filename)
                while received < filesize:
                    chunk = recv_h_bytes(self.skt)
                    if self.killed:
                        return
                    f.write(chunk)
                    received += len(chunk)
                    chunks_processed += 1
                    
                    if file_code in [1, 2]:  # transactions or transaction_items
                        if chunks_processed % 10 == 0 or received >= filesize:  # Log every 10th chunk or at end
                            logging.info(f"[CLIENT {self.client_id}] Processing chunk {chunks_processed} for {filename}, received: {received}/{filesize} bytes")
                    
                    if len(chunk) != 0:
                        from gateway_protocol import handle_and_forward_chunk
                        handle_and_forward_chunk(self.client_id, file_code, 1 if received >= filesize and last_file else 0, chunk,
                                               self.transactions_exchange, self.transaction_items_exchange, 
                                               self.categorizer_items_exchange, self.birth_dic_queue)

            total_files_received += 1
            total_bytes_received += filesize
            logging.info(f"[CLIENT {self.client_id}] File completed: {filename} ({filesize} bytes), chunks processed: {chunks_processed}")
            logging.info(f"[CLIENT {self.client_id}] Progress: {total_files_received} files, {total_bytes_received} total bytes received")
            
            if last_file and last_dataset:
                logging.info(f"[CLIENT {self.client_id}] All datasets received - total files: {total_files_received}, total bytes: {total_bytes_received}")
                break

    def mark_query_completed(self):
        with self.cond:
            self.queries_done += 1
            self.cond.notify_all()

    def send_file_result(self, file):
        with open(file, 'r') as f:
            lines_batch = []
            for line in f:
                lines_batch.append(line)
                if len(lines_batch) == 10:
                    send_lines_batch(self.skt, 1, lines_batch, False)
                    logging.info(f"Enviado resultado parcial de {len(lines_batch)} líneas al cliente.")
                    lines_batch = []
                if self.killed:
                    return
            send_lines_batch(self.skt,1, lines_batch, True)
            logging.info(f"Envio completado al cliente.")

    def query_path(self, query):
        return os.path.join(OUTPUT_DIR, f'result_{query}_client_{self.client_id}.csv')

    def clear_temp_files(self):
        """Clear temporary result files for specific client"""
        try:
            files_to_clear = [
                self.query_path("q1"),
                self.query_path("q2"),
                self.query_path("q3"),
                self.query_path("q4")
            ]
            for file_path in files_to_clear:
                if os.path.exists(file_path):
                    os.remove(file_path)
        except Exception as e:
            logging.error(f"Error clearing temp files for client {self.client_id}: {e}")

    def run(self):
        try:
            self.clear_temp_files()

            self.receive_datasets()
            logging.info(f"[CLIENT {self.client_id}] Datasets received and processed")
            
            # Send END messages to all workers for this client
            logging.info(f"[CLIENT {self.client_id}] Sending END messages to all workers...")
            from gateway_protocol import send_end_messages_to_all_workers
            send_end_messages_to_all_workers(self.client_id, self.transactions_exchange, self.transaction_items_exchange)
            logging.info(f"[CLIENT {self.client_id}] END messages sent to all workers")
            
            with self.q4_request_cond:
                while not self.q4_data_requested and not self.killed:
                    self.q4_request_cond.wait()

            send_response(self.skt, response_types.Q4_STEP, "")
            self.receive_datasets()

            # All files sent, now wait for queries to finish
            with self.cond:
                while self.queries_done < QUERIES_TO_COMPLETE and not self.killed:
                    self.cond.wait()
    
            # All queries done, now send results from files
            if self.killed:
                return
            
            # Send results...
            self.send_file_result(self.query_path("q1"))
            self.send_file_result(self.query_path("q2"))
            self.send_file_result(self.query_path("q3"))
            self.send_file_result(self.query_path("q4"))

            # Cleanup
            # self.clear_temp_files()
            self.skt.close()
            self.killed = True
            
            logging.info(f"[CLIENT {self.client_id}] Connection closed")

        except Exception as e:
            logging.error(f"[CLIENT {self.client_id}] Protocol error: {e}")

    def kill(self):
        if self.skt:
            self.skt.close()
        self.killed = True