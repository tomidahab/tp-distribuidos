import os
import sys
from time import sleep
from common.protocol import parse_message, row_to_dict, build_message
from common.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError
import common.config as config

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
RECEIVER_EXCHANGE = os.environ.get('RECEIVER_EXCHANGE', 'filter_by_amount_exchange')
FANOUT_EXCHANGE = f"{RECEIVER_EXCHANGE}_fanout"
WORKER_INDEX = int(os.environ.get('WORKER_INDEX', '0'))
MIN_AMOUNT = float(os.environ.get('MIN_AMOUNT', 15.0))
RESULT_QUEUE = os.environ.get('RESULT_QUEUE', 'query1_result_receiver_queue')

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
    if should_stop.is_set():  # Don't process if we're stopping
        return
        
    print(f"[filter_by_amount] Worker {WORKER_INDEX} received a message!", flush=True)
    parsed_message = parse_message(message)
    type_of_message = parsed_message['csv_type']  
    client_id = parsed_message['client_id']
    is_last = parsed_message['is_last']
    
    filtered_rows = filter_message_by_amount(parsed_message, MIN_AMOUNT)
    
    if filtered_rows or is_last:
        # Create result queue for each message to avoid connection conflicts
        queue_result = MessageMiddlewareQueue(RABBITMQ_HOST, RESULT_QUEUE)
        new_message, _ = build_message(client_id, type_of_message, is_last, filtered_rows)
        queue_result.send(new_message)
        queue_result.close()
        print(f"[filter_by_amount] Worker {WORKER_INDEX} message passed amount filter: {len(filtered_rows)}, is_last={is_last}, sending to RESULT_QUEUE", flush=True)
    else:
        print(f"[filter_by_amount] Worker {WORKER_INDEX} message filtered out by amount.", flush=True)
    
    if is_last:
        print(f"[filter_by_amount] Worker {WORKER_INDEX} received end message, stopping.", flush=True)
        should_stop.set()
        topic_middleware.stop_consuming()

def on_end_message_callback(message: bytes, fanout_middleware, topic_middleware, should_stop):
    print(f"[filter_by_amount] Worker {WORKER_INDEX} received END message from fanout!", flush=True)
    try:
        parsed_message = parse_message(message)
        is_last = parsed_message['is_last']
        client_id = parsed_message['client_id']
        type_of_message = parsed_message['csv_type']
        
        if is_last and not should_stop.is_set():  # Only process if not already stopping
            print(f"[filter_by_amount] Worker {WORKER_INDEX} processing END message, sending empty result with is_last=1", flush=True)
            # Send empty result with is_last=1 to mark completion for this worker
            queue_result = MessageMiddlewareQueue(RABBITMQ_HOST, RESULT_QUEUE)
            end_message, _ = build_message(client_id, type_of_message, 1, [])
            queue_result.send(end_message)
            queue_result.close()
            
            print(f"[filter_by_amount] Worker {WORKER_INDEX} received END message from fanout, stopping.", flush=True)
            should_stop.set()
            topic_middleware.stop_consuming()
            fanout_middleware.stop_consuming()
    except Exception as e:
        print(f"[filter_by_amount] Worker {WORKER_INDEX} error processing END message: {e}", file=sys.stderr)

def make_on_message_callback(topic_middleware, should_stop):
    def wrapper(message: bytes):
        on_message_callback(message, topic_middleware, should_stop)
    return wrapper

def make_on_end_message_callback(fanout_middleware, topic_middleware, queue_result, should_stop):
    def wrapper(message: bytes):
        on_end_message_callback(message, fanout_middleware, topic_middleware, should_stop)
    return wrapper


def main():
    import threading
    
    sleep(config.MIDDLEWARE_UP_TIME)  # Esperar a que RabbitMQ esté listo
    print(f"[filter_by_amount] Worker {WORKER_INDEX} connecting to RabbitMQ at {RABBITMQ_HOST}, exchange: {RECEIVER_EXCHANGE}, filter by min amount: {MIN_AMOUNT}", flush=True)
    
    # Create topic exchange middleware for receiving transaction messages
    topic_middleware = MessageMiddlewareExchange(
        host=RABBITMQ_HOST,
        exchange_name=RECEIVER_EXCHANGE,
        exchange_type='topic',
        queue_name=f"filter_by_amount_worker_{WORKER_INDEX}_queue",
        routing_keys=[f'transaction.{WORKER_INDEX}']  # Each worker listens to specific routing key
    )
    
    # Create fanout exchange middleware for receiving END messages
    fanout_middleware = MessageMiddlewareExchange(
        host=RABBITMQ_HOST,
        exchange_name=FANOUT_EXCHANGE,
        exchange_type='fanout',
        queue_name=f"filter_by_amount_worker_{WORKER_INDEX}_fanout_queue",  # Different queue for fanout
        routing_keys=[]  # Fanout doesn't use routing keys
    )
    
    # Create result queue
    queue_result = MessageMiddlewareQueue(RABBITMQ_HOST, RESULT_QUEUE)
    
    print(f"[filter_by_amount] Worker {WORKER_INDEX} listening for transactions on exchange: {RECEIVER_EXCHANGE} with routing key: transaction.{WORKER_INDEX}", flush=True)
    print(f"[filter_by_amount] Worker {WORKER_INDEX} listening for END messages on fanout exchange: {FANOUT_EXCHANGE}", flush=True)
    
    # Flag to coordinate stopping
    should_stop = threading.Event()
    
    def fanout_consumer():
        try:
            fanout_callback = make_on_end_message_callback(fanout_middleware, topic_middleware, queue_result, should_stop)
            fanout_middleware.start_consuming(fanout_callback)
        except Exception as e:
            print(f"[filter_by_amount] Worker {WORKER_INDEX} fanout thread error: {e}", file=sys.stderr, flush=True)
    
    try:
        # Start consuming from fanout exchange in a separate thread for END messages
        fanout_thread = threading.Thread(target=fanout_consumer)
        fanout_thread.daemon = True
        fanout_thread.start()
        
        # Start consuming from topic exchange (blocking)
        topic_callback = make_on_message_callback(topic_middleware, should_stop)
        topic_middleware.start_consuming(topic_callback)
        
        print(f"[filter_by_amount] Worker {WORKER_INDEX} topic consuming finished", flush=True)
        
        # Signal stop and wait for fanout thread
        should_stop.set()
        if fanout_thread.is_alive():
            fanout_thread.join(timeout=5)
            
    except MessageMiddlewareDisconnectedError:
        print(f"[filter_by_amount] Worker {WORKER_INDEX} disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print(f"[filter_by_amount] Worker {WORKER_INDEX} message error in middleware.", file=sys.stderr)
    except KeyboardInterrupt:
        print(f"[filter_by_amount] Worker {WORKER_INDEX} stopping...")
        should_stop.set()
        topic_middleware.stop_consuming()
        fanout_middleware.stop_consuming()
    finally:
        try:
            topic_middleware.close()
        except Exception as e:
            print(f"[filter_by_amount] Worker {WORKER_INDEX} error closing topic middleware: {e}", file=sys.stderr)
        try:
            fanout_middleware.close()
        except Exception as e:
            print(f"[filter_by_amount] Worker {WORKER_INDEX} error closing fanout middleware: {e}", file=sys.stderr)
        try:
            queue_result.close()
        except Exception as e:
            print(f"[filter_by_amount] Worker {WORKER_INDEX} error closing result queue: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()