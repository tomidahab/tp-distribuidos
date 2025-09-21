import os
import sys
from common.protocol import protocol
from common.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
QUEUE_NAME = os.environ.get('QUEUE_NAME', 'filter_by_hour_queue')
START_HOUR = int(os.environ.get('START_HOUR', 6))
END_HOUR = int(os.environ.get('END_HOUR', 11))
NEXT_QUEUE = os.environ.get('NEXT_QUEUE', 'filter_by_amount_queue')

def filter_message_by_hour(message: bytes, start_hour: int, end_hour: int) -> bool:
    try:
        decoded = protocol.decode_message(message)
        msg_hour = int(decoded['hour'])
        return start_hour <= msg_hour <= end_hour
    except Exception as e:
        print(f"[filter] Error parsing message: {e}", file=sys.stderr)
        return False

def make_on_message_callback(next_queue):
    def on_message_callback(message: bytes):
        if filter_message_by_hour(message, START_HOUR, END_HOUR):
            print(f"[worker] Message passed hour filter: {message}")
            next_queue.send(message)
        else:
            print(f"[worker] Message filtered out by hour.")
    return on_message_callback

def main():
    print(f"[worker] Connecting to RabbitMQ at {RABBITMQ_HOST}, queue: {QUEUE_NAME}, filter hours: {START_HOUR}-{END_HOUR}")
    queue = MessageMiddlewareQueue(RABBITMQ_HOST, QUEUE_NAME)
    next_queue = MessageMiddlewareQueue(RABBITMQ_HOST, NEXT_QUEUE)
    on_message_callback = make_on_message_callback(next_queue)
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
        next_queue.close()

if __name__ == "__main__":
    main()