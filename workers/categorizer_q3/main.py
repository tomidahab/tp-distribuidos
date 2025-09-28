import os
import sys
from collections import defaultdict
import time
from datetime import datetime

# Debug startup
print("[categorizer_q3] STARTING UP - Basic imports done", flush=True)

# Debug imports
try:
    print("[categorizer_q3] Attempting to import common modules...", flush=True)
    from common.protocol import parse_message, row_to_dict, build_message, CSV_TYPES_REVERSE,create_response_message
    from common.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError
    print("[categorizer_q3] Successfully imported common modules", flush=True)
except ImportError as e:
    print(f"[categorizer_q3] IMPORT ERROR: {e}", flush=True)
    sys.exit(1)
except Exception as e:
    print(f"[categorizer_q3] UNEXPECTED IMPORT ERROR: {e}", flush=True)
    sys.exit(1)

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
RECEIVER_QUEUE = os.environ.get('RECEIVER_QUEUE', 'store_semester_categorizer_queue')
GATEWAY_QUEUE = os.environ.get('GATEWAY_QUEUE', 'query3_result_receiver_queue')

def get_semester(month):
    return 1 if 1 <= month <= 6 else 2

def listen_for_transactions():
    # Agregación: {(year, semester, store_id): total_payment}
    semester_store_stats = defaultdict(float)
    try:
        queue = MessageMiddlewareQueue(RABBITMQ_HOST, RECEIVER_QUEUE)
    except Exception as e:
        print(f"[categorizer_q3] Failed to connect to RabbitMQ: {e}", file=sys.stderr)
        return semester_store_stats
    
    print(f"[categorizer_q3] Listening for transactions on queue: {RECEIVER_QUEUE}")

    def on_message_callback(message: bytes):
        try:
            parsed_message = parse_message(message)
            type_of_message = parsed_message['csv_type']  
            client_id = parsed_message['client_id']
            is_last = parsed_message['is_last']
            
            for row in parsed_message['rows']:
                dic_fields_row = row_to_dict(row, type_of_message)
                
                # Extract transaction data
                created_at = dic_fields_row['created_at']
                datetime_obj = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                year = datetime_obj.year
                month = datetime_obj.month
                semester = get_semester(month)
                store_id = str(dic_fields_row['store_id'])
                payment_value = float(dic_fields_row.get('final_amount', 0.0))
                if payment_value == 0.0:
                    print(f"[categorizer_q3] Warning: Payment value is 0.0 for row: {row}", file=sys.stderr)
                
                key = (year, semester, store_id)
                semester_store_stats[key] += payment_value
                
            if is_last:
                print("[categorizer_q3] Received end message, stopping transaction collection.")
                queue.stop_consuming()
        except Exception as e:
            print(f"[categorizer_q3] Error processing transaction message: {e}", file=sys.stderr)

    try:
        print("[categorizer_q3] Starting to consume messages...")
        queue.start_consuming(on_message_callback)
    except MessageMiddlewareDisconnectedError:
        print("[categorizer_q3] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[categorizer_q3] Message error in middleware.", file=sys.stderr)
    except Exception as e:
        print(f"[categorizer_q3] Unexpected error while consuming: {e}", file=sys.stderr)
    finally:
        queue.close()
    return semester_store_stats

def send_results_to_gateway(semester_store_stats):
    try:
        queue = MessageMiddlewareQueue(RABBITMQ_HOST, GATEWAY_QUEUE)
        
        for (year, semester, store_id), total_payment in semester_store_stats.items():
            result = {
                'year': year,
                'semester': semester,
                'store_id': store_id,
                'total_payment': total_payment
            }

            message = create_response_message(3, str(result))
            queue.send(message)
            print(f"[categorizer_q3] Sent result to gateway: {result}")
            
        queue.close()
    except Exception as e:
        print(f"[categorizer_q3] ERROR in send_results_to_gateway: {e}")
        raise e

def main():
    print("[categorizer_q3] MAIN FUNCTION STARTED", flush=True)
    try:
        print("[categorizer_q3] Waiting for RabbitMQ to be ready...", flush=True)
        time.sleep(30)  # Wait for RabbitMQ to be ready
        print("[categorizer_q3] Starting worker...", flush=True)
        
        semester_store_stats = listen_for_transactions()
        print("[categorizer_q3] Final semester-store stats:")
        for key, total in semester_store_stats.items():
            year, semester, store_id = key
            print(f"Year: {year}, Semester: {semester}, Store: {store_id}, Total Payment: {total}")
            
        if not semester_store_stats:
            print("[categorizer_q3] No transaction data received, exiting.")
            return
            
        send_results_to_gateway(semester_store_stats)
        print("[categorizer_q3] Worker completed successfully.", flush=True)
        
    except Exception as e:
        print(f"[categorizer_q3] Error in main: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

if __name__ == "__main__":
    print("[categorizer_q3] Script starting - __name__ == '__main__'", flush=True)
    main()