import os
import sys
from common.protocol import protocol
from common.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

# Configurable parameters (could be set via env vars or args)
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
QUEUE_NAME = os.environ.get('QUEUE_NAME', 'filter_by_year_queue')
NEXT_QUEUE = os.environ.get('NEXT_QUEUE', 'filter_by_hour_queue')
FILTER_YEARS = [
    int(y.strip()) for y in os.environ.get('FILTER_YEAR', '2024,2025').split(',')
]

def filter_message_by_year(message: bytes, years: list[int]) -> bool:
	try:
		decoded = protocol.decode_message(message)
		msg_year = int(decoded['year'])
		return msg_year in years
	except Exception as e:
		print(f"[filter] Error parsing message: {e}", file=sys.stderr)
		return False

def main():
    print(f"[worker] Connecting to RabbitMQ at {RABBITMQ_HOST}, queue: {QUEUE_NAME}, filter years: {FILTER_YEARS}")
    queue = MessageMiddlewareQueue(RABBITMQ_HOST, QUEUE_NAME)
    next_queue = MessageMiddlewareQueue(RABBITMQ_HOST, NEXT_QUEUE)
    def on_message_callback(message: bytes):
        if filter_message_by_year(message, FILTER_YEARS):
            next_queue.send(message)
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
