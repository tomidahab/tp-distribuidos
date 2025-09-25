import os
import sys
from common.protocol import protocol
from common.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError
from workers.filter_by_hour.main import START_HOUR

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
QUEUE_NAME = os.environ.get('QUEUE_NAME', 'filter_by_amount_queue')
MIN_AMOUNT = float(os.environ.get('MIN_AMOUNT', 15.0))

def filter_message_by_amount(message: bytes, min_amount: float) -> bool:
    try:
        decoded = protocol.decode_message(message)
        msg_hour = float(decoded['amount'])
        return min_amount <= msg_hour
    except Exception as e:
        print(f"[filter] Error parsing message: {e}", file=sys.stderr)
        return False

def on_message_callback(message: bytes):
    if filter_message_by_amount(message, MIN_AMOUNT):
        print(f"[worker] Message passed amount filter: {message}")
    else:
        print(f"[worker] Message filtered out by amount.")

def main():
    print(f"[worker] Connecting to RabbitMQ at {RABBITMQ_HOST}, queue: {QUEUE_NAME}, filter by min amount: {MIN_AMOUNT}")
    queue = MessageMiddlewareQueue(RABBITMQ_HOST, QUEUE_NAME)
    try:
        queue.start_consuming(on_message_callback)
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