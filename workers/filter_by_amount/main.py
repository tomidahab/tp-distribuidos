import os
import sys
from time import sleep
from common.protocol import parse_message, row_to_dict, build_message
from common.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

RABBITMQ_HOST = os.environ.get('rabbitmq_server_HOST', 'rabbitmq_server')
QUEUE_NAME = os.environ.get('QUEUE_NAME', 'filter_by_amount_queue')
MIN_AMOUNT = float(os.environ.get('MIN_AMOUNT', 15.0))
RESULT_QUEUE = os.environ.get('RESULT_QUEUE', 'query1_result_receiver_queue')

def filter_message_by_amount(parsed_message, min_amount: float) -> bool:
    try:
        type_of_message = parsed_message['csv_type']
        new_rows = []
        for row in parsed_message['rows']:
            dic_fields_row = row_to_dict(row, type_of_message)
            try:
                amount = float(dic_fields_row['final_amount'])
                if amount >= min_amount:
                    new_rows.append(row)
            except Exception as e:
                print(f"[amount] Error parsing amount: {dic_fields_row.get('amount')} ({e})", file=sys.stderr)
        if new_rows != []:
            return new_rows
    except Exception as e:
        print(f"[filter] Error parsing message: {e}", file=sys.stderr)
        return False
    return False

def on_message_callback(message: bytes, receiver_queue, queue_result):
    parsed_message = parse_message(message)
    type_of_message = parsed_message['csv_type']  
    client_id = parsed_message['client_id']
    is_last = parsed_message['is_last']
    filtered_rows = filter_message_by_amount(parsed_message, MIN_AMOUNT)
    if filtered_rows:
        new_message, _ = build_message(client_id, type_of_message, is_last, filtered_rows)
        queue_result.send(new_message)
        print(f"[worker] Message passed amount filter: {len(filtered_rows)}, sending to RESULT_QUEUE")
    else:
        print(f"[worker] Message filtered out by amount.")

def make_on_message_callback(receiver_queue, queue_result):
    def wrapper(message: bytes):
        on_message_callback(message, receiver_queue, queue_result)
    return wrapper


def main():
    sleep(60)  # Esperar a que RabbitMQ esté listo
    print(f"[worker] Connecting to RabbitMQ at {RABBITMQ_HOST}, queue: {QUEUE_NAME}, filter by min amount: {MIN_AMOUNT}")
    queue = MessageMiddlewareQueue(RABBITMQ_HOST, QUEUE_NAME)
    queue_result = MessageMiddlewareQueue(RABBITMQ_HOST, RESULT_QUEUE)
    try:
        callback = make_on_message_callback(queue, queue_result)
        queue.start_consuming(callback)
    except MessageMiddlewareDisconnectedError:
        print("[worker] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[worker] Message error in middleware.", file=sys.stderr)
    except KeyboardInterrupt:
        print("[worker] Stopping...")
        queue.stop_consuming()
        queue.close()

if __name__ == "__main__":
    main()