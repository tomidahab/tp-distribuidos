import os
import sys
from common.protocol import protocol
from common.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
RECEIVER_QUEUE = os.environ.get('RECEIVER_QUEUE', 'filter_by_hour_queue')
START_HOUR = int(os.environ.get('START_HOUR', 6))
END_HOUR = int(os.environ.get('END_HOUR', 11))
QUEUE_FILTER_AMOUNT = os.environ.get('QUEUE_FILTER_AMOUNT', 'filter_by_amount_queue')
QUEUE_CATEGORIZER_STORES_SEMESTER = os.environ.get('QUEUE_CATEGORIZER_STORES_SEMESTER', 'store_semester_categorizer_queue')

def filter_message_by_hour(message: bytes, start_hour: int, end_hour: int) -> bool:
    try:
        decoded = protocol.decode_message(message)
        msg_hour = int(decoded['hour'])
        return start_hour <= msg_hour <= end_hour
    except Exception as e:
        print(f"[filter] Error parsing message: {e}", file=sys.stderr)
        return False

def on_message_callback(message: bytes, queue_filter_amount, queue_categorizer_stores_semester):
    if filter_message_by_hour(message, START_HOUR, END_HOUR):
        print(f"[worker] Message passed hour filter: {message}")
        # Naza: Aca se trimmea el mensaje para la Q1
        queue_filter_amount.send(message)
        # Naza: Aca se trimmea el mensaje para la Q3
        queue_categorizer_stores_semester.send(message)
    else:
        print(f"[worker] Message filtered out by hour.")

def make_on_message_callback(queue_filter_amount, queue_categorizer_stores_semester):
    def wrapper(message: bytes):
        on_message_callback(message, queue_filter_amount, queue_categorizer_stores_semester)
    return wrapper

def main():
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