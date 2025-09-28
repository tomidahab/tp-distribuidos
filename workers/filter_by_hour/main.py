from datetime import datetime
import os
from time import sleep
import sys
from common.protocol import parse_message, row_to_dict, build_message, CSV_TYPES_REVERSE
from common.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError
import common.config as config

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
RECEIVER_QUEUE = os.environ.get('RECEIVER_QUEUE', 'filter_by_hour_queue')
START_HOUR = int(os.environ.get('START_HOUR', 6))
END_HOUR = int(os.environ.get('END_HOUR', 11))
QUEUE_FILTER_AMOUNT = os.environ.get('QUEUE_FILTER_AMOUNT', 'filter_by_amount_queue')
QUEUE_CATEGORIZER_STORES_SEMESTER = os.environ.get('QUEUE_CATEGORIZER_STORES_SEMESTER', 'store_semester_categorizer_queue')

def filter_message_by_hour(parsed_message, start_hour: int, end_hour: int) -> bool:
    try:
        type_of_message = parsed_message['csv_type']

        print(f"[transactions] Procesando mensaje con {len(parsed_message['rows'])} rows")  # Mens

        new_rows = []
        for row in parsed_message['rows']:
            dic_fields_row = row_to_dict(row, type_of_message)
            try:
                created_at = dic_fields_row['created_at']
                msg_hour = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").hour
                if start_hour <= msg_hour <= end_hour:
                    new_rows.append(row)
            except Exception as e:
                print(f"[transactions] Error parsing created_at: {created_at} ({e})", file=sys.stderr)
        if new_rows != []:
            return new_rows
    except Exception as e:
        print(f"[filter] Error parsing message: {e}", file=sys.stderr)
        return False

def on_message_callback(message: bytes, queue_filter_amount, queue_categorizer_stores_semester):
    print("[worker] Received a message!", flush=True)
    parsed_message = parse_message(message)
    type_of_message = parsed_message['csv_type']  
    client_id = parsed_message['client_id']
    is_last = parsed_message['is_last']
    filtered_rows = filter_message_by_hour(parsed_message, START_HOUR, END_HOUR)
    if filtered_rows:
        print(f"[worker] Number of rows on the message passed hour filter: {len(filtered_rows)}")

        new_message, _ = build_message(client_id, type_of_message, is_last, filtered_rows)
        # Naza: Aca se trimmea el mensaje para la Q1
        if type_of_message == CSV_TYPES_REVERSE['transactions']:  # transactions CHECK!
            queue_filter_amount.send(new_message)
            queue_categorizer_stores_semester.send(new_message)
        # Naza: Aca se trimmea el mensaje para la Q3
        if type_of_message == CSV_TYPES_REVERSE['transaction_items']:  # transactions CHECK!
            print(f"[worker] Received a transaction_items message, that should never happen!", flush=True)
    else:
        print(f"[worker] Whole Message filtered out by hour.")

def make_on_message_callback(queue_filter_amount, queue_categorizer_stores_semester):
    def wrapper(message: bytes):
        on_message_callback(message, queue_filter_amount, queue_categorizer_stores_semester)
    return wrapper

def main():
    sleep(config.MIDDLEWARE_UP_TIME)  # Esperar a que RabbitMQ esté listo
    print(f"[worker] Connecting to RabbitMQ at {RABBITMQ_HOST}, queue: {RECEIVER_QUEUE}, filter hours: {START_HOUR}-{END_HOUR}")
    queue = MessageMiddlewareQueue(RABBITMQ_HOST, RECEIVER_QUEUE)
    queue_filter_amount = MessageMiddlewareQueue(RABBITMQ_HOST, QUEUE_FILTER_AMOUNT)
    queue_categorizer_stores_semester = MessageMiddlewareQueue(RABBITMQ_HOST, QUEUE_CATEGORIZER_STORES_SEMESTER)
    try:
        callback = make_on_message_callback(queue_filter_amount, queue_categorizer_stores_semester)
        queue.start_consuming(callback)
    except MessageMiddlewareDisconnectedError:
        print("[worker] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[worker] Message error in middleware.", file=sys.stderr)
    except KeyboardInterrupt:
        print("[worker] Stopping...")
        queue.stop_consuming()
        queue.close()
        queue_filter_amount.close()
        queue_categorizer_stores_semester.close()

if __name__ == "__main__":
    main()