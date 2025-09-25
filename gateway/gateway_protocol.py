from socket import socket
import sys
import os
import time
from common.protocol_utils import *
from common.protocol import build_message, parse_message, CSV_TYPES_REVERSE
from common.middleware import MessageMiddlewareQueue

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
QUEUE_FILTER_BY_YEAR_T = 'filter_by_year_transactions_queue'
QUEUE_FILTER_BY_YEAR_T_ITEMS = 'filter_by_year_transaction_items_queue'


# Instancias de las colas (inicialización perezosa)
filter_by_year_t_queue = None
filter_by_year_t_items_queue = None


def recv_lines_batch(skt: socket):
    data_type = recv_int(skt)
    length = recv_int(skt)
    is_last = recv_bool(skt)
    lines_batch = []
    for _ in range(0, length):
        lines_batch.append(recv_h_str(skt))
    return data_type, lines_batch, is_last

def send_response(skt: socket, response_type, response = ""):
    send_int(skt, response_type)
    send_h_str(skt, response)
    return response_type, response

def handle_and_forward_chunk(client_id: int, csv_type: int, is_last: int, chunk: bytes) -> int:
    """
    Construye el mensaje usando el protocolo y lo envía a la cola correspondiente por RabbitMQ.
    """
    global filter_by_year_t_queue, filter_by_year_t_items_queue
    rows = chunk.decode("utf-8").splitlines()
    message, _ = build_message(client_id, csv_type, is_last, rows)
    MAX_RETRIES = 10
    RETRY_DELAY = 3
    try:
        if csv_type == 2:  # transaction_items
            for attempt in range(MAX_RETRIES):
                try:
                    if filter_by_year_t_items_queue is None:
                        filter_by_year_t_items_queue = MessageMiddlewareQueue(RABBITMQ_HOST, QUEUE_FILTER_BY_YEAR_T_ITEMS)
                    filter_by_year_t_items_queue.send(message)
                    break
                except Exception as e:
                    print(f"[gateway_protocol] Retry {attempt+1}/{MAX_RETRIES} for transaction_items queue: {e}", file=sys.stderr)
                    filter_by_year_t_items_queue = None
                    time.sleep(RETRY_DELAY)
            else:
                print(f"[gateway_protocol] Failed to connect to transaction_items queue after {MAX_RETRIES} retries", file=sys.stderr)
                return -1
        elif csv_type == 3:  # transactions
            for attempt in range(MAX_RETRIES):
                try:
                    if filter_by_year_t_queue is None:
                        filter_by_year_t_queue = MessageMiddlewareQueue(RABBITMQ_HOST, QUEUE_FILTER_BY_YEAR_T)
                    filter_by_year_t_queue.send(message)
                    break
                except Exception as e:
                    print(f"[gateway_protocol] Retry {attempt+1}/{MAX_RETRIES} for transactions queue: {e}", file=sys.stderr)
                    filter_by_year_t_queue = None
                    time.sleep(RETRY_DELAY)
            else:
                print(f"[gateway_protocol] Failed to connect to transactions queue after {MAX_RETRIES} retries", file=sys.stderr)
                return -1
        # Otros tipos no se envían por ahora
    except Exception as e:
        print(f"[gateway_protocol] Error sending message to queue: {e}", file=sys.stderr)
        return -1
    return 0

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