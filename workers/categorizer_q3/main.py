import os
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

def get_semester(month):
    return 1 if 1 <= month <= 6 else 2

def listen_for_transactions():
    # Agregación: {(year, semester, store_id): total_payment}
    semester_store_stats = defaultdict(float)
    end_messages_received = 0
    
    # Get routing keys for this worker
    worker_routing_keys = SEMESTER_MAPPING.get(WORKER_INDEX, [])
    if not worker_routing_keys:
        print(f"[categorizer_q3] No routing keys for worker {WORKER_INDEX}", file=sys.stderr)
        return semester_store_stats
    
    print(f"[categorizer_q3] Worker {WORKER_INDEX} will handle routing keys: {worker_routing_keys}", flush=True)
    print(f"[categorizer_q3] About to create MessageMiddlewareExchange with exchange: {RECEIVER_EXCHANGE}", flush=True)
    
    try:
        print(f"[categorizer_q3] Creating topic MessageMiddlewareExchange for data messages...", flush=True)
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
        return semester_store_stats
    
    print(f"[categorizer_q3] Worker {WORKER_INDEX} listening for transactions on exchange: {RECEIVER_EXCHANGE} with routing keys: {worker_routing_keys}")

    def on_message_callback(message: bytes):
        nonlocal end_messages_received  # Allow modification of the outer variable
        # print(f"[categorizer_q3] Worker {WORKER_INDEX} received a message!", flush=True)
        try:
            parsed_message = parse_message(message)
            type_of_message = parsed_message['csv_type']  
            client_id = parsed_message['client_id']
            is_last = parsed_message['is_last']
            
            # print(f"[categorizer_q3] Message details - is_last: {is_last}, rows: {len(parsed_message['rows'])}", flush=True)

            for row in parsed_message['rows']:
                # print(f"[categorizer_q3] Processing row: {row}, is_last={is_last}", flush=True)
                dic_fields_row = row_to_dict(row, type_of_message)
                
                # Extract transaction data
                created_at = dic_fields_row['created_at']
                datetime_obj = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                year = datetime_obj.year
                month = datetime_obj.month
                semester = get_semester(month)
                store_id = str(dic_fields_row['store_id'])
                payment_value = float(dic_fields_row.get('final_amount', 0.0))
                key = (year, semester, store_id)
                semester_store_stats[key] += payment_value
                
            if is_last:
                end_messages_received += 1
                print(f"[categorizer_q3] Worker {WORKER_INDEX} received END message {end_messages_received}/{NUMBER_OF_HOUR_WORKERS}")
                if end_messages_received >= NUMBER_OF_HOUR_WORKERS:
                    print(f"[categorizer_q3] Worker {WORKER_INDEX} received all END messages, stopping transaction collection.")
                    topic_middleware.stop_consuming()
        except Exception as e:
            print(f"[categorizer_q3] Error processing transaction message: {e}", file=sys.stderr)

    try:
        print("[categorizer_q3] Starting to consume messages...", flush=True)
        topic_middleware.start_consuming(on_message_callback)
    except MessageMiddlewareDisconnectedError:
        print("[categorizer_q3] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[categorizer_q3] Message error in middleware.", file=sys.stderr)
    except Exception as e:
        print(f"[categorizer_q3] Unexpected error while consuming: {e}", file=sys.stderr)
    finally:
        topic_middleware.close()
    return semester_store_stats

def send_results_to_gateway(semester_store_stats):
    try:
        queue = MessageMiddlewareQueue(RABBITMQ_HOST, GATEWAY_QUEUE)
        
        csv_lines = []
        for (year, semester, store_id), total_payment in semester_store_stats.items():
            csv_line = f"{year},{semester},{store_id},{total_payment}"
            csv_lines.append(csv_line)
            
        message, _ = build_message(0, 3, 1, csv_lines)
        queue.send(message)
        print(f"[categorizer_q3] Sent {len(csv_lines)} results to gateway in batch")
            
        queue.close()
    except Exception as e:
        print(f"[categorizer_q3] ERROR in send_results_to_gateway: {e}")
        raise e

def main():
    print("[categorizer_q3] MAIN FUNCTION STARTED", flush=True)
    try:
        print("[categorizer_q3] Waiting for RabbitMQ to be ready...", flush=True)
        time.sleep(config.MIDDLEWARE_UP_TIME)  # Wait for RabbitMQ to be ready
        print("[categorizer_q3] Starting worker...", flush=True)
        print("[categorizer_q3] About to call listen_for_transactions()...", flush=True)
        
        semester_store_stats = listen_for_transactions()
        print("[categorizer_q3] listen_for_transactions() completed", flush=True)
        print("[categorizer_q3] Final semester-store stats:")
        for key, total in semester_store_stats.items():
            year, semester, store_id = key
            print(f"Year: {year}, Semester: {semester}, Store: {store_id}, Total Payment: {total}")
            
        # if not semester_store_stats:
        #     print("[categorizer_q3] No transaction data received, exiting.")
        #     return
        # Even if empty, we send results to notify worker completion!
        send_results_to_gateway(semester_store_stats)
        print("[categorizer_q3] Worker completed successfully.", flush=True)
        
    except Exception as e:
        print(f"[categorizer_q3] Error in main: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

if __name__ == "__main__":
    print("[categorizer_q3] Script starting - __name__ == '__main__'", flush=True)
    main()