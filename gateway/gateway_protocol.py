from socket import socket
import sys
import os
import time
from common.protocol_utils import *
from common.protocol import build_message, parse_message, CSV_TYPES_REVERSE
from common.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
FILTER_BY_YEAR_TRANSACTIONS_EXCHANGE = 'filter_by_year_transactions_exchange'
FILTER_BY_YEAR_TRANSACTION_ITEMS_EXCHANGE = 'filter_by_year_transaction_items_exchange'
NUMBER_OF_YEAR_WORKERS = int(os.environ.get('NUMBER_OF_YEAR_WORKERS', '3'))
CATEGORIZER_QUERY2_ITEMS_QUEUE = 'categorizer_q2_items_queue'
CATEGORIZER_QUERY2_TRANSACTIONS_QUEUE = 'categorizer_q2_receiver_queue'
CATEGORIZER_Q2_ITEMS_FANOUT_EXCHANGE = 'categorizer_q2_items_fanout_exchange'

BIRTH_DIC_DATA_RESPONSES_QUEUE = os.environ.get('Q4_DATA_RESPONSES_QUEUE', 'gateway_client_data_queue')

# Instancias de exchanges y queues (inicialización perezosa)
filter_by_year_transactions_exchange = None
filter_by_year_transaction_items_exchange = None
categorizer_query2_items_queue = None
categorizer_query2_transactions_queue = None
categorizer_query2_items_exchange = None

birth_dic_data_responses_queue = None

# Round-robin counters for distributing messages across workers
transactions_worker_counter = 0
transaction_items_worker_counter = 0


def recv_lines_batch(skt: socket):
    data_type = recv_int(skt)
    length = recv_int(skt)
    is_last = recv_bool(skt)
    lines_batch = []
    for _ in range(0, length):
        lines_batch.append(recv_h_str(skt))
    return data_type, lines_batch, is_last

def send_lines_batch(skt: socket, data_type, lines_batch, is_last):
    send_int(skt, data_type)
    send_int(skt, len(lines_batch))
    send_bool(skt, is_last)
    for line in lines_batch:
        send_h_str(skt, line)

def send_response(skt: socket, response_type, response = ""):
    send_int(skt, response_type)
    send_h_str(skt, response)
    return response_type, response

def handle_and_forward_chunk(client_id: int, csv_type: int, is_last: int, chunk: bytes) -> int:
    """
    Construye el mensaje usando el protocolo y lo envía al exchange correspondiente por RabbitMQ.
    Usa round-robin para distribuir entre múltiples workers de filter_by_year.
    """
    global filter_by_year_transactions_exchange, filter_by_year_transaction_items_exchange, categorizer_query2_items_queue, categorizer_query2_transactions_queue, categorizer_query2_items_exchange
    global transactions_worker_counter, transaction_items_worker_counter
    global birth_dic_data_responses_queue

    rows = chunk.decode("utf-8").splitlines()
    message, _ = build_message(client_id, csv_type, is_last, rows)
    MAX_RETRIES = 10
    RETRY_DELAY = 3
    try:
            
        if csv_type == CSV_TYPES_REVERSE["transaction_items"]:  # transaction_items
            for attempt in range(MAX_RETRIES):
                try:
                    if filter_by_year_transaction_items_exchange is None:
                        filter_by_year_transaction_items_exchange = MessageMiddlewareExchange(
                            RABBITMQ_HOST, 
                            FILTER_BY_YEAR_TRANSACTION_ITEMS_EXCHANGE, 
                            'topic',
                            ""  # Empty queue name since we're only sending
                        )
                    
                    # Round-robin distribution to workers
                    worker_index = transaction_items_worker_counter % NUMBER_OF_YEAR_WORKERS
                    routing_key = f"year.{worker_index}"
                    filter_by_year_transaction_items_exchange.send(message, routing_key=routing_key)
                    transaction_items_worker_counter += 1
                    print(f"[gateway_protocol] Sent transaction_items to worker {worker_index} with routing key {routing_key}", flush=True)
                    break
                except Exception as e:
                    print(f"[gateway_protocol] Retry {attempt+1}/{MAX_RETRIES} for transaction_items exchange: {e}", file=sys.stderr)
                    filter_by_year_transaction_items_exchange = None
                    time.sleep(RETRY_DELAY)
            else:
                print(f"[gateway_protocol] Failed to connect to transaction_items exchange after {MAX_RETRIES} retries", file=sys.stderr)
                return -1
        elif csv_type == CSV_TYPES_REVERSE["transactions"]:  # transactions
            # Enviar a filter_by_year_transactions_exchange
            for attempt in range(MAX_RETRIES):
                try:
                    if filter_by_year_transactions_exchange is None:
                        filter_by_year_transactions_exchange = MessageMiddlewareExchange(
                            RABBITMQ_HOST, 
                            FILTER_BY_YEAR_TRANSACTIONS_EXCHANGE, 
                            'topic',
                            ""  # Empty queue name since we're only sending
                        )
                    
                    # Round-robin distribution to workers
                    worker_index = transactions_worker_counter % NUMBER_OF_YEAR_WORKERS
                    routing_key = f"year.{worker_index}"
                    filter_by_year_transactions_exchange.send(message, routing_key=routing_key)
                    transactions_worker_counter += 1
                    print(f"[gateway_protocol] Sent transactions to worker {worker_index} with routing key {routing_key}", flush=True)
                    break
                except Exception as e:
                    print(f"[gateway_protocol] Retry {attempt+1}/{MAX_RETRIES} for transactions exchange: {e}", file=sys.stderr)
                    filter_by_year_transactions_exchange = None
                    time.sleep(RETRY_DELAY)
            else:
                print(f"[gateway_protocol] Failed to connect to transactions exchange after {MAX_RETRIES} retries", file=sys.stderr)
                return -1
            
        elif csv_type == CSV_TYPES_REVERSE["menu_items"]:  # menu_items
            print(f"[gateway_protocol] Forwarding menu_items to categorizer_q2_items_fanout_exchange")
            for attempt in range(MAX_RETRIES):
                try:
                    if categorizer_query2_items_exchange is None:
                        categorizer_query2_items_exchange = MessageMiddlewareExchange(
                            RABBITMQ_HOST,
                            CATEGORIZER_Q2_ITEMS_FANOUT_EXCHANGE,
                            "fanout",
                            ""  
                        )
                    categorizer_query2_items_exchange.send(message)
                    print(f"[gateway_protocol] Successfully sent menu_items to fanout exchange")
                    break
                except Exception as e:
                    print(f"[gateway_protocol] Retry {attempt+1}/{MAX_RETRIES} for menu_items fanout exchange: {e}", file=sys.stderr)
                    categorizer_query2_items_exchange = None
                    time.sleep(RETRY_DELAY)
            else:
                print(f"[gateway_protocol] Failed to connect to menu_items fanout exchange after {MAX_RETRIES} retries", file=sys.stderr)
                return -1
        elif csv_type == CSV_TYPES_REVERSE["users"]:  # users
            
            q4_rows = []
            for row in rows:
                row_items = row.split(",")
                q4_rows.append(",".join([row_items[0], row_items[2]]))  # user_id, birthday
            message, _ = build_message(client_id, csv_type, is_last, q4_rows) # Override message for q4

            for attempt in range(MAX_RETRIES):
                try:
                    if birth_dic_data_responses_queue is None:
                        birth_dic_data_responses_queue = MessageMiddlewareQueue(RABBITMQ_HOST, BIRTH_DIC_DATA_RESPONSES_QUEUE)
                    birth_dic_data_responses_queue.send(message)
                    break
                except Exception as e:
                    print(f"[gateway_protocol] Retry {attempt+1}/{MAX_RETRIES} for users queue: {e}", file=sys.stderr)
                    birth_dic_data_responses_queue = None
                    time.sleep(RETRY_DELAY)
            else:
                print(f"[gateway_protocol] Failed to connect to users queue after {MAX_RETRIES} retries", file=sys.stderr)
                return -1
        # Otros tipos no se envían por ahora
    except Exception as e:
        print(f"[gateway_protocol] Error sending message to queue: {e}", file=sys.stderr)
        return -1
    return 0

def send_end_messages_to_filter_by_year():
    """
    Envía mensajes END (rows vacías con is_last=True) a todos los workers de filter_by_year
    tanto para transactions como para transaction_items.
    """
    global filter_by_year_transactions_exchange, filter_by_year_transaction_items_exchange
    MAX_RETRIES = 10
    RETRY_DELAY = 3
    
    print(f"[gateway_protocol] Sending END messages to {NUMBER_OF_YEAR_WORKERS} filter_by_year workers", flush=True)
    
    # Send END messages for transactions
    for worker_index in range(NUMBER_OF_YEAR_WORKERS):
        end_message, _ = build_message(0, CSV_TYPES_REVERSE["transactions"], 1, [])  # client_id=0, is_last=1, empty rows
        routing_key = f"year.{worker_index}"
        
        for attempt in range(MAX_RETRIES):
            try:
                if filter_by_year_transactions_exchange is None:
                    filter_by_year_transactions_exchange = MessageMiddlewareExchange(
                        RABBITMQ_HOST, 
                        FILTER_BY_YEAR_TRANSACTIONS_EXCHANGE, 
                        'topic',
                        ""
                    )
                
                filter_by_year_transactions_exchange.send(end_message, routing_key=routing_key)
                print(f"[gateway_protocol] Sent transactions END message to worker {worker_index} with routing key {routing_key}", flush=True)
                break
            except Exception as e:
                print(f"[gateway_protocol] Retry {attempt+1}/{MAX_RETRIES} for transactions END message to worker {worker_index}: {e}", file=sys.stderr)
                filter_by_year_transactions_exchange = None
                time.sleep(RETRY_DELAY)
        else:
            print(f"[gateway_protocol] Failed to send transactions END message to worker {worker_index} after {MAX_RETRIES} retries", file=sys.stderr)
    
    # Send END messages for transaction_items
    for worker_index in range(NUMBER_OF_YEAR_WORKERS):
        end_message, _ = build_message(0, CSV_TYPES_REVERSE["transaction_items"], 1, [])  # client_id=0, is_last=1, empty rows
        routing_key = f"year.{worker_index}"
        
        for attempt in range(MAX_RETRIES):
            try:
                if filter_by_year_transaction_items_exchange is None:
                    filter_by_year_transaction_items_exchange = MessageMiddlewareExchange(
                        RABBITMQ_HOST, 
                        FILTER_BY_YEAR_TRANSACTION_ITEMS_EXCHANGE, 
                        'topic',
                        ""
                    )
                
                filter_by_year_transaction_items_exchange.send(end_message, routing_key=routing_key)
                print(f"[gateway_protocol] Sent transaction_items END message to worker {worker_index} with routing key {routing_key}", flush=True)
                break
            except Exception as e:
                print(f"[gateway_protocol] Retry {attempt+1}/{MAX_RETRIES} for transaction_items END message to worker {worker_index}: {e}", file=sys.stderr)
                filter_by_year_transaction_items_exchange = None
                time.sleep(RETRY_DELAY)
        else:
            print(f"[gateway_protocol] Failed to send transaction_items END message to worker {worker_index} after {MAX_RETRIES} retries", file=sys.stderr)
    
    print(f"[gateway_protocol] Finished sending END messages to all {NUMBER_OF_YEAR_WORKERS} filter_by_year workers", flush=True)

def filename_to_type(filename: str):
    if "menu_items" in filename:
        return CSV_TYPES_REVERSE['menu_items']
    elif "users" in filename:
        return CSV_TYPES_REVERSE['users']
    elif "transaction_items" in filename:
        return CSV_TYPES_REVERSE['transaction_items']
    elif "transactions" in filename:
        return CSV_TYPES_REVERSE['transactions']
    else:
        return -1