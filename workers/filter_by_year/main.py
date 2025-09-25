import os
import sys
import threading
from time import sleep
from datetime import datetime
from common.protocol import parse_message, row_to_dict, build_message
from common.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

# Configurable parameters (could be set via env vars or args)
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
QUEUE_T_ITEMS = os.environ.get('QUEUE_T_ITEMS', 'filter_by_year_transaction_items_queue')
QUEUE_T = os.environ.get('QUEUE_T', 'filter_by_year_transactions_queue')
HOUR_FILTER_QUEUE = os.environ.get('HOUR_FILTER_QUEUE', 'filter_by_hour_queue')
ITEM_CATEGORIZER_QUEUE = os.environ.get('ITEM_CATEGORIZER_QUEUE', 'filter_by_item_categorizer_queue')
STORE_USER_CATEGORIZER_QUEUE = os.environ.get('STORE_USER_CATEGORIZER_QUEUE', 'store_user_categorizer_queue')
FILTER_YEARS = [
    int(y.strip()) for y in os.environ.get('FILTER_YEAR', '2024,2025').split(',')
]

def on_message_callback_transactions(message: bytes, hour_filter_queue, store_user_categorizer_queue):
    try:
        # Acá va el parseo del mensaje para transactions
        parsed_message = parse_message(message)
        type_of_message = parsed_message['csv_type']  
        client_id = parsed_message['client_id']
        is_last = parsed_message['is_last']
        new_rows = []
        for row in parsed_message['rows']:
            print(f"[transactions] Procesando row: {row}")  # Mensaje agregado
            dic_fields_row = row_to_dict(row, type_of_message)
            # Parse year from created_at field
            try:
                created_at = dic_fields_row['created_at']
                msg_year = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").year
                if msg_year in FILTER_YEARS:
                    new_rows.append(row)
            except Exception as e:
                print(f"[transactions] Error parsing created_at: {created_at} ({e})", file=sys.stderr)

        new_message, _ = build_message(client_id, type_of_message, is_last, new_rows)
        # Naza: Acá va el recorte del mensaje para la Q1 y Q3
        hour_filter_queue.send(new_message)
        # Naza: Acá va el recorte del mensaje para la Q4
        store_user_categorizer_queue.send(new_message)
    except Exception as e:
        print(f"[transactions] Error decoding message: {e}", file=sys.stderr)

def on_message_callback_t_items(message: bytes, item_categorizer_queue):
    try:
        # Acá va el parseo del mensaje para transaction items
        parsed_message = parse_message(message)
        type_of_message = parsed_message['csv_type']  
        client_id = parsed_message['client_id']
        is_last = parsed_message['is_last']
        new_rows = []
        for row in parsed_message['rows']:
            dic_fields_row = row_to_dict(row, type_of_message)
            # Parse year from created_at field
            try:
                created_at = dic_fields_row['created_at']
                msg_year = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").year
                if msg_year in FILTER_YEARS:
                    new_rows.append(row)
            except Exception as e:
                print(f"[t_items] Error parsing created_at: {created_at} ({e})", file=sys.stderr)

        new_message, _ = build_message(client_id, type_of_message, is_last, new_rows)

        # Naza: Acá va el recorte del mensaje para transaction items (con solo la data necesaria para el categorizer de items (Q2))
        item_categorizer_queue.send(new_message)
    except Exception as e:
        print(f"[t_items] Error decoding message: {e}", file=sys.stderr)

def consume_queue_transactions(queue, callback, hour_filter_queue, store_user_categorizer_queue):
    def wrapper(message):
        callback(message, hour_filter_queue, store_user_categorizer_queue)
    try:
        queue.start_consuming(wrapper)
    except MessageMiddlewareDisconnectedError:
        print("[worker] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[worker] Message error in middleware.", file=sys.stderr)

def consume_queue_t_items(queue, callback, item_categorizer_queue):
    def wrapper(message):
        callback(message, item_categorizer_queue)
    try:
        queue.start_consuming(wrapper)
    except MessageMiddlewareDisconnectedError:
        print("[worker] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[worker] Message error in middleware.", file=sys.stderr)

def main():
    sleep(60)  # Esperar a que RabbitMQ esté listo
    print(f"[worker] Connecting to RabbitMQ at {RABBITMQ_HOST}, queues: {QUEUE_T}, {QUEUE_T_ITEMS}, filter years: {FILTER_YEARS}")
    queue_t = MessageMiddlewareQueue(RABBITMQ_HOST, QUEUE_T)
    queue_t_items = MessageMiddlewareQueue(RABBITMQ_HOST, QUEUE_T_ITEMS)
    hour_filter_queue = MessageMiddlewareQueue(RABBITMQ_HOST, HOUR_FILTER_QUEUE)
    store_user_categorizer_queue = MessageMiddlewareQueue(RABBITMQ_HOST, STORE_USER_CATEGORIZER_QUEUE)
    item_categorizer_queue = MessageMiddlewareQueue(RABBITMQ_HOST, ITEM_CATEGORIZER_QUEUE)

    t1 = threading.Thread(target=consume_queue_transactions, args=(queue_t, on_message_callback_transactions, hour_filter_queue, store_user_categorizer_queue))
    t2 = threading.Thread(target=consume_queue_t_items, args=(queue_t_items, on_message_callback_t_items, item_categorizer_queue))
    t1.start()
    t2.start()
    try:
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        print("[worker] Stopping...")
        queue_t.stop_consuming()
        queue_t_items.stop_consuming()
        queue_t.close()
        queue_t_items.close()
        hour_filter_queue.close()
        store_user_categorizer_queue.close()
        item_categorizer_queue.close()

if __name__ == "__main__":
    main()
