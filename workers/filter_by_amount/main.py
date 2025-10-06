import os
import signal
import sys
from collections import defaultdict
from time import sleep
from common.protocol import parse_message, row_to_dict, build_message
from common.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError
import common.config as config

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
RECEIVER_EXCHANGE = os.environ.get('RECEIVER_EXCHANGE', 'filter_by_amount_exchange')
WORKER_INDEX = int(os.environ.get('WORKER_INDEX', '0'))
MIN_AMOUNT = float(os.environ.get('MIN_AMOUNT', 15.0))
RESULT_QUEUE = os.environ.get('RESULT_QUEUE', 'query1_result_receiver_queue')
NUMBER_OF_HOUR_WORKERS = int(os.environ.get('NUMBER_OF_HOUR_WORKERS', '3'))

# Global counters for debugging
rows_received = 0
rows_sent = 0
# Track END messages per client: {client_id: count}
client_end_messages = defaultdict(int)
completed_clients = set()

queue_result = None
topic_middleware = None

def _close_queue(queue):
    if queue:
        queue.close()

def _sigterm_handler(signum, _):
    _close_queue(queue_result)
    _close_queue(topic_middleware)

def filter_message_by_amount(parsed_message, min_amount: float) -> list:
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
        return new_rows
    except Exception as e:
        print(f"[filter] Error parsing message: {e}", file=sys.stderr)
        return []

def on_message_callback(message: bytes, topic_middleware, should_stop):
    global rows_received, rows_sent, client_end_messages, completed_clients
    
    if should_stop.is_set():  # Don't process if we're stopping
        return
        
    # print(f"[filter_by_amount] Worker {WORKER_INDEX} received a message!", flush=True)
    parsed_message = parse_message(message)
    type_of_message = parsed_message['csv_type']  
    client_id = parsed_message['client_id']
    is_last = parsed_message['is_last']
    
    # Skip if client already completed
    if client_id in completed_clients:
        return
    
    # Count incoming rows
    incoming_rows = len(parsed_message['rows'])
    rows_received += incoming_rows
    # print(f"[filter_by_amount] Worker {WORKER_INDEX} received {incoming_rows} rows (total received: {rows_received})", flush=True)
    
    if is_last:
        client_end_messages[client_id] += 1
        print(f"[filter_by_amount] Worker {WORKER_INDEX} received END message {client_end_messages[client_id]}/{NUMBER_OF_HOUR_WORKERS} for client {client_id}", flush=True)
    
    filtered_rows = filter_message_by_amount(parsed_message, MIN_AMOUNT)
    
    # Check if this client has completed (received all END messages)
    client_completed = client_end_messages[client_id] >= NUMBER_OF_HOUR_WORKERS
    
    if filtered_rows or (is_last and client_completed):
        global queue_result
        # Create result queue for each message to avoid connection conflicts
        queue_result = MessageMiddlewareQueue(RABBITMQ_HOST, RESULT_QUEUE)
        
        # Only send is_last=1 if this client has received all END messages
        final_is_last = 1 if (is_last and client_completed) else 0
        new_message, _ = build_message(client_id, type_of_message, final_is_last, filtered_rows)
        queue_result.send(new_message)
        queue_result.close()
        
        # Count outgoing rows
        rows_sent += len(filtered_rows)
        print(f"[filter_by_amount] Worker {WORKER_INDEX} sent {len(filtered_rows)} filtered rows for client {client_id}, final_is_last={final_is_last}", flush=True)


def make_on_message_callback(topic_middleware, should_stop):
    def wrapper(message: bytes):
        on_message_callback(message, topic_middleware, should_stop)
    return wrapper


def main():
    import threading
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)
    # Global counters for debugging
    global rows_received, rows_sent, end_messages_received
    rows_received = 0
    rows_sent = 0
    end_messages_received = 0
    
    sleep(config.MIDDLEWARE_UP_TIME)  # Esperar a que RabbitMQ esté listo
    print(f"[filter_by_amount] Worker {WORKER_INDEX} connecting to RabbitMQ at {RABBITMQ_HOST}, exchange: {RECEIVER_EXCHANGE}, filter by min amount: {MIN_AMOUNT}", flush=True)
    
    # Create topic exchange middleware for receiving transaction messages
    global topic_middleware
    topic_middleware = MessageMiddlewareExchange(
        host=RABBITMQ_HOST,
        exchange_name=RECEIVER_EXCHANGE,
        exchange_type='topic',
        queue_name=f"filter_by_amount_worker_{WORKER_INDEX}_queue",
        routing_keys=[f'transaction.{WORKER_INDEX}']  # Each worker listens to specific routing key
    )
    
    # Create result queue
    # queue_result = MessageMiddlewareQueue(RABBITMQ_HOST, RESULT_QUEUE)
    
    print(f"[filter_by_amount] Worker {WORKER_INDEX} listening for transactions on exchange: {RECEIVER_EXCHANGE} with routing key: transaction.{WORKER_INDEX}", flush=True)
    
    # Flag to coordinate stopping
    should_stop = threading.Event()
    
    try:        
        # Start consuming from topic exchange (blocking)
        topic_callback = make_on_message_callback(topic_middleware, should_stop)
        topic_middleware.start_consuming(topic_callback)
        
        print(f"[filter_by_amount] Worker {WORKER_INDEX} topic consuming finished", flush=True)
        
        should_stop.set()
            
    except MessageMiddlewareDisconnectedError:
        print(f"[filter_by_amount] Worker {WORKER_INDEX} disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print(f"[filter_by_amount] Worker {WORKER_INDEX} message error in middleware.", file=sys.stderr)
    except KeyboardInterrupt:
        print(f"[filter_by_amount] Worker {WORKER_INDEX} stopping...")
        should_stop.set()
        topic_middleware.stop_consuming()

    finally:
        try:
            topic_middleware.close()
        except Exception as e:
            print(f"[filter_by_amount] Worker {WORKER_INDEX} error closing topic middleware: {e}", file=sys.stderr)
        # try:
        #     queue_result.close()
        # except Exception as e:
        #     print(f"[filter_by_amount] Worker {WORKER_INDEX} error closing result queue: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()