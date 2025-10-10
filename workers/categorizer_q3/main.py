import os
import signal
import sys
from collections import defaultdict
import time
from datetime import datetime
from common import config
# Debug startup
print("[categorizer_q3] STARTING UP - Basic imports done", flush=True)

# Debug imports
try:
    print("[categorizer_q3] Attempting to import common modules...", flush=True)
    from common.protocol import parse_message, row_to_dict, build_message, CSV_TYPES_REVERSE,create_response_message
    from common.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError
    print("[categorizer_q3] Successfully imported common modules", flush=True)
except ImportError as e:
    print(f"[categorizer_q3] IMPORT ERROR: {e}", flush=True)
    sys.exit(1)
except Exception as e:
    print(f"[categorizer_q3] UNEXPECTED IMPORT ERROR: {e}", flush=True)
    sys.exit(1)

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
RECEIVER_EXCHANGE = os.environ.get('RECEIVER_EXCHANGE', 'categorizer_q3_exchange')
FANOUT_EXCHANGE = os.environ.get('FANOUT_EXCHANGE', 'categorizer_q3_fanout_exchange')
WORKER_INDEX = int(os.environ.get('WORKER_INDEX', '0'))
GATEWAY_QUEUE = os.environ.get('GATEWAY_QUEUE', 'query3_result_receiver_queue')
NUMBER_OF_HOUR_WORKERS = int(os.environ.get('NUMBER_OF_HOUR_WORKERS', '3'))

# Define semester mapping for worker distribution
SEMESTER_MAPPING = {
    0: ['semester.2023-2', 'semester.2024-2'],  # Worker 0: second semesters
    1: ['semester.2024-1', 'semester.2025-1']   # Worker 1: first semesters
}

topic_middleware = None
gateway_result_queue = None

def _close_queue(queue):
    if queue:
        queue.close()

def _sigterm_handler(signum, _):
    _close_queue(topic_middleware)
    _close_queue(gateway_result_queue)

def get_semester(month):
    return 1 if 1 <= month <= 6 else 2

def listen_for_transactions():
    # Agregación por cliente: {client_id: {(year, semester, store_id): total_payment}}
    client_semester_store_stats = defaultdict(lambda: defaultdict(float))
    # Track END messages per client: {client_id: count}
    client_end_messages = defaultdict(int)
    processed_rows = 0
    completed_clients = set()
    
    # Get routing keys for this worker
    worker_routing_keys = SEMESTER_MAPPING.get(WORKER_INDEX, [])
    if not worker_routing_keys:
        print(f"[categorizer_q3] No routing keys for worker {WORKER_INDEX}", file=sys.stderr)
        return client_semester_store_stats
    
    print(f"[categorizer_q3] Worker {WORKER_INDEX} will handle routing keys: {worker_routing_keys}", flush=True)
    print(f"[categorizer_q3] About to create MessageMiddlewareExchange with exchange: {RECEIVER_EXCHANGE}", flush=True)
    
    try:
        print(f"[categorizer_q3] Creating topic MessageMiddlewareExchange for data messages...", flush=True)
        global topic_middleware
        topic_middleware = MessageMiddlewareExchange(
            host=RABBITMQ_HOST, 
            exchange_name=RECEIVER_EXCHANGE, 
            exchange_type='topic',
            queue_name=f"categorizer_q3_worker_{WORKER_INDEX}_queue",
            routing_keys=worker_routing_keys
        )
        print(f"[categorizer_q3] Successfully created topic MessageMiddlewareExchange", flush=True)

        print(f"[categorizer_q3] Creating fanout MessageMiddlewareExchange for END messages using SAME queue...", flush=True)
        fanout_middleware = MessageMiddlewareExchange(
            host=RABBITMQ_HOST, 
            exchange_name=FANOUT_EXCHANGE, 
            exchange_type='fanout',
            queue_name=f"categorizer_q3_worker_{WORKER_INDEX}_queue",  # SAME queue as topic exchange
            routing_keys=[]  # Fanout doesn't use routing keys
        )
        print(f"[categorizer_q3] Successfully created fanout MessageMiddlewareExchange", flush=True)

    except Exception as e:
        print(f"[categorizer_q3] Failed to connect to RabbitMQ: {e}", file=sys.stderr)
        return client_semester_store_stats
    
    print(f"[categorizer_q3] Worker {WORKER_INDEX} listening for transactions on exchange: {RECEIVER_EXCHANGE} with routing keys: {worker_routing_keys}")

    def on_message_callback(message: bytes):
        nonlocal processed_rows, completed_clients
        try:
            parsed_message = parse_message(message)
            type_of_message = parsed_message['csv_type']  
            client_id = parsed_message['client_id']
            is_last = parsed_message['is_last']
            
            # Skip if client already completed - check BEFORE any processing
            if client_id in completed_clients:
                print(f"[categorizer_q3] Worker {WORKER_INDEX} ignoring message from already completed client {client_id}")
                return
            
            print(f"[categorizer_q3] Worker {WORKER_INDEX} processing message for client {client_id}, is_last: {is_last}, rows: {len(parsed_message['rows'])}", flush=True)

            # Process regular rows
            for row in parsed_message['rows']:
                try:
                    dic_fields_row = row_to_dict(row, type_of_message)
                    
                    # Extract transaction data
                    created_at = dic_fields_row['created_at']
                    datetime_obj = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                    year = datetime_obj.year
                    month = datetime_obj.month
                    semester = get_semester(month)
                    store_id = str(dic_fields_row['store_id'])
                    payment_value = float(dic_fields_row.get('final_amount', 0.0))
                    
                    if payment_value > 0:
                        key = (year, semester, store_id)
                        client_semester_store_stats[client_id][key] += payment_value
                        processed_rows += 1
                        
                        # Log progress for large datasets
                        if processed_rows % 10000 == 0:
                            print(f"[categorizer_q3] Worker {WORKER_INDEX} processed {processed_rows} rows so far", flush=True)
                    elif payment_value == 0.0:
                        print(f"[categorizer_q3] Warning: Payment value is 0.0 for row: {row}", file=sys.stderr)
                        
                except Exception as row_error:
                    print(f"[categorizer_q3] Error processing row: {row_error}", file=sys.stderr)
                    continue
                
            # Handle END messages per client
            if is_last:
                client_end_messages[client_id] += 1
                print(f"[categorizer_q3] Worker {WORKER_INDEX} received END message {client_end_messages[client_id]}/{NUMBER_OF_HOUR_WORKERS} for client {client_id}", flush=True)
                
                # Check if this client has received all END messages
                if client_end_messages[client_id] >= NUMBER_OF_HOUR_WORKERS:
                    print(f"[categorizer_q3] Worker {WORKER_INDEX} completed all data for client {client_id}, sending results", flush=True)
                    completed_clients.add(client_id)
                    
                    # Send results for this client
                    send_client_results(client_id, client_semester_store_stats[client_id])
                    
                    # Don't delete client data yet - keep it for potential debugging
                    # but mark as completed
                    print(f"[categorizer_q3] Client {client_id} processing completed")
        except Exception as e:
            print(f"[categorizer_q3] Error processing transaction message: {e}", file=sys.stderr)

    try:
        print("[categorizer_q3] Starting to consume messages...", flush=True)
        topic_middleware.start_consuming(on_message_callback)
        print("[categorizer_q3] start_consuming finished unexpectedly", flush=True)
    except MessageMiddlewareDisconnectedError:
        print("[categorizer_q3] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[categorizer_q3] Message error in middleware.", file=sys.stderr)
    except Exception as e:
        print(f"[categorizer_q3] Unexpected error while consuming: {e}", file=sys.stderr)
    finally:
        topic_middleware.close()
        # fanout_middleware.close()
    return client_semester_store_stats

def send_client_results(client_id, semester_store_stats):
    """Send results for specific client to gateway"""
    try:
        global gateway_result_queue
        if gateway_result_queue is None:
            gateway_result_queue = MessageMiddlewareQueue(RABBITMQ_HOST, GATEWAY_QUEUE)
        
        # Convert to CSV format like Q1
        csv_lines = []
        for (year, semester, store_id), total_payment in semester_store_stats.items():
            csv_line = f"{year},{semester},{store_id},{total_payment}"
            csv_lines.append(csv_line)
            
        # Send as Q3-style message with client_id
        message, _ = build_message(client_id, 3, 1, csv_lines)  # csv_type=3 for Q3, is_last=1 (final message)
        gateway_result_queue.send(message)
        print(f"[categorizer_q3] Worker {WORKER_INDEX} sent {len(csv_lines)} results for client {client_id} to gateway")
            
    except Exception as e:
        print(f"[categorizer_q3] ERROR sending results for client {client_id}: {e}", file=sys.stderr)
        # Don't raise to avoid stopping other clients

def send_results_to_gateway(semester_store_stats):
    try:
        global gateway_result_queue
        gateway_result_queue = MessageMiddlewareQueue(RABBITMQ_HOST, GATEWAY_QUEUE)
        
        # Convert to CSV format like Q1
        csv_lines = []
        for (year, semester, store_id), total_payment in semester_store_stats.items():
            csv_line = f"{year},{semester},{store_id},{total_payment}"
            csv_lines.append(csv_line)
            
        # Send as Q1-style message with rows and is_last=1
        message, _ = build_message(0, 3, 1, csv_lines)  # client_id=0, csv_type=3, is_last=1 (final message)
        gateway_result_queue.send(message)
        print(f"[categorizer_q3] Sent {len(csv_lines)} results to gateway in batch")
            
        gateway_result_queue.close()
    except Exception as e:
        print(f"[categorizer_q3] ERROR in send_results_to_gateway: {e}")
        raise e

def main():
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)
    print("[categorizer_q3] MAIN FUNCTION STARTED", flush=True)
    try:
        print("[categorizer_q3] Waiting for RabbitMQ to be ready...", flush=True)
        time.sleep(config.MIDDLEWARE_UP_TIME)  # Wait for RabbitMQ to be ready
        print("[categorizer_q3] Starting worker...", flush=True)
        print("[categorizer_q3] About to call listen_for_transactions()...", flush=True)
        
        client_semester_store_stats = listen_for_transactions()
        print("[categorizer_q3] listen_for_transactions() completed - all clients processed", flush=True)
        print("[categorizer_q3] Worker completed successfully.", flush=True)
        
    except Exception as e:
        print(f"[categorizer_q3] Error in main: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

if __name__ == "__main__":
    print("[categorizer_q3] Script starting - __name__ == '__main__'", flush=True)
    main()