from datetime import datetime
import os
import time
import sys
from collections import defaultdict

from common.protocol import parse_message, row_to_dict, build_message, CSV_TYPES_REVERSE
from common.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
RECEIVER_QUEUE = os.environ.get('RECEIVER_QUEUE', 'filter_by_hour_queue')
START_HOUR = int(os.environ.get('START_HOUR', '6'))
END_HOUR = int(os.environ.get('END_HOUR', '11'))
QUEUE_FILTER_AMOUNT = os.environ.get('QUEUE_FILTER_AMOUNT', 'filter_by_amount_queue')
CATEGORIZER_Q3_EXCHANGE = os.environ.get('CATEGORIZER_Q3_EXCHANGE', 'categorizer_q3_exchange')
CATEGORIZER_Q3_FANOUT_EXCHANGE = f"{CATEGORIZER_Q3_EXCHANGE}_fanout"

def get_semester_key(year, month):
    """Generate semester routing key based on year and month"""
    semester = 1 if 1 <= month <= 6 else 2
    return f"semester.{year}-{semester}"

def filter_message_by_hour(parsed_message, start_hour: int, end_hour: int) -> list:
    try:
        type_of_message = parsed_message['csv_type']

        print(f"[transactions] Procesando mensaje con {len(parsed_message['rows'])} rows")  # Mens

        new_rows = []
        for row in parsed_message['rows']:
            dic_fields_row = row_to_dict(row, type_of_message)
            try:
                created_at = dic_fields_row['created_at']
                msg_hour = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").hour
                if start_hour <= msg_hour <= end_hour:
                    new_rows.append(row)
            except Exception as e:
                print(f"[transactions] Error parsing created_at: {created_at} ({e})", file=sys.stderr)
        
        return new_rows
    except Exception as e:
        print(f"[filter] Error parsing message: {e}", file=sys.stderr)
        return []

def on_message_callback(message: bytes, queue_filter_amount, categorizer_q3_topic_exchange, categorizer_q3_fanout_exchange):
    print("[worker] Received a message!", flush=True)
    parsed_message = parse_message(message)
    type_of_message = parsed_message['csv_type']  
    client_id = parsed_message['client_id']
    is_last = int(parsed_message['is_last'])
    filtered_rows = filter_message_by_hour(parsed_message, START_HOUR, END_HOUR)

    print(f"[worker] Received message of {len(filtered_rows)} rows, is_last={is_last}", flush=True)
    print(f"[worker] DEBUG: filtered_rows length: {len(filtered_rows)}, is_last value: {is_last}, is_last type: {type(is_last)}", flush=True)
    print(f"[worker] DEBUG: filtered_rows or is_last == 1: {filtered_rows or is_last == 1}", flush=True)

    if (len(filtered_rows) != 0) or (is_last == 1):
        print(f"[worker] INSIDE IF CONDITION", flush=True)
        print(f"[worker] Number of rows on the message passed hour filter: {len(filtered_rows)}, is_last={is_last}", flush=True)
        print(f"[worker] AFTER PRINT STATEMENT", flush=True)

        # For Q1 - send to filter_by_amount
        if type_of_message == CSV_TYPES_REVERSE['transactions']:  # transactions
            new_message, _ = build_message(client_id, type_of_message, is_last, filtered_rows)
            queue_filter_amount.send(new_message)
            
            # For Q3 - group by semester and send to topic exchange
            if filtered_rows:  # Only process if there are rows
                rows_by_semester = defaultdict(list)
                for row in filtered_rows:
                    dic_fields_row = row_to_dict(row, type_of_message)
                    try:
                        created_at = dic_fields_row['created_at']
                        datetime_obj = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                        year = datetime_obj.year
                        month = datetime_obj.month
                        semester_key = get_semester_key(year, month)
                        rows_by_semester[semester_key].append(row)
                    except Exception as e:
                        print(f"[worker] Error parsing date for Q3 routing: {e}", file=sys.stderr)
                
                # Send grouped messages by semester
                for semester_key, semester_rows in rows_by_semester.items():
                    if semester_rows:
                        semester_message, _ = build_message(client_id, type_of_message, 0, semester_rows)
                        print(f"[worker] Sending {len(semester_rows)} rows for {semester_key} to categorizer_q3, is_last={is_last}")
                        categorizer_q3_topic_exchange.send(semester_message, routing_key=semester_key)
                
        elif type_of_message == CSV_TYPES_REVERSE['transaction_items']:  # transaction_items
            print(f"[worker] Received a transaction_items message, that should never happen!", flush=True)
        else:
            print(f"[worker] Unknown csv_type: {type_of_message}", file=sys.stderr)

        print(f"[worker] AFTER Finished processing message, is_last={is_last}", flush=True)
        # Send END message when last message is received (OUTSIDE the type_of_message check)
        if is_last == 1:
            print(f"[worker] About to send END message, is_last={is_last}", flush=True)
            try:
                end_message, _ = build_message(client_id, type_of_message, 1, [])
                print(f"[worker] Built end message successfully", flush=True)
                categorizer_q3_fanout_exchange.send(end_message)
                print("[worker] Sent END message to categorizer_q3 via fanout exchange", flush=True)
            except Exception as e:
                print(f"[worker] ERROR sending END message: {e}", flush=True)
                import traceback
                traceback.print_exc()
    else:
        print(f"[worker] Whole Message filtered out by hour.")

def make_on_message_callback(queue_filter_amount, categorizer_q3_topic_exchange, categorizer_q3_fanout_exchange):
    def wrapper(message: bytes):
        on_message_callback(message, queue_filter_amount, categorizer_q3_topic_exchange, categorizer_q3_fanout_exchange)
    return wrapper

def main():
    print("[filter_by_hour] STARTING UP - Basic imports done", flush=True)
    print(f"[filter_by_hour] Environment: RECEIVER_QUEUE={RECEIVER_QUEUE}, START_HOUR={START_HOUR}, END_HOUR={END_HOUR}", flush=True)
    print(f"[filter_by_hour] Environment: QUEUE_FILTER_AMOUNT={QUEUE_FILTER_AMOUNT}", flush=True)
    print(f"[filter_by_hour] Environment: CATEGORIZER_Q3_EXCHANGE={CATEGORIZER_Q3_EXCHANGE}", flush=True)
    print(f"[filter_by_hour] Environment: CATEGORIZER_Q3_FANOUT_EXCHANGE={CATEGORIZER_Q3_FANOUT_EXCHANGE}", flush=True)
    
    print("[filter_by_hour] Waiting for RabbitMQ to be ready...", flush=True)
    time.sleep(30)  # Wait for RabbitMQ to be ready
    print("[filter_by_hour] RabbitMQ should be ready now!", flush=True)
    
    try:
        print("[filter_by_hour] Creating queue connection...")
        queue = MessageMiddlewareQueue(
            RABBITMQ_HOST,
            RECEIVER_QUEUE
        )
        print(f"[filter_by_hour] Connected to queue: {RECEIVER_QUEUE}")
        
        print("[filter_by_hour] Creating filter_amount queue connection...")
        queue_filter_amount = MessageMiddlewareQueue(
            RABBITMQ_HOST,
            QUEUE_FILTER_AMOUNT
        )
        print(f"[filter_by_hour] Connected to queue: {QUEUE_FILTER_AMOUNT}")
        
        print("[filter_by_hour] Creating categorizer_q3 topic exchange connection...")
        # Connect to categorizer_q3 topic exchange
        categorizer_q3_topic_exchange = MessageMiddlewareExchange(
            RABBITMQ_HOST,
            exchange_name=CATEGORIZER_Q3_EXCHANGE,
            exchange_type='topic',
            queue_name=""  # Empty queue since we're only sending, not consuming
        )
        print(f"[filter_by_hour] Connected to categorizer_q3 topic exchange: {CATEGORIZER_Q3_EXCHANGE}")
        
        print("[filter_by_hour] Creating categorizer_q3 fanout exchange connection...")
        # Connect to categorizer_q3 fanout exchange for Q1
        categorizer_q3_fanout_exchange = MessageMiddlewareExchange(
            RABBITMQ_HOST,
            exchange_name=CATEGORIZER_Q3_FANOUT_EXCHANGE,
            exchange_type='fanout',
            queue_name=""  # Empty queue since we're only sending, not consuming
        )
        print(f"[filter_by_hour] Connected to categorizer_q3 fanout exchange: {CATEGORIZER_Q3_FANOUT_EXCHANGE}")
        
        print("[filter_by_hour] Creating callback and starting to consume...")
        callback = make_on_message_callback(queue_filter_amount, categorizer_q3_topic_exchange, categorizer_q3_fanout_exchange)
        print("[filter_by_hour] About to start consuming messages...")
        queue.start_consuming(callback)
    except MessageMiddlewareDisconnectedError:
        print("[filter_by_hour] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[filter_by_hour] Message error in middleware.", file=sys.stderr)
    except KeyboardInterrupt:
        print("[filter_by_hour] Stopping...")
        queue.stop_consuming()
        queue.close()

if __name__ == "__main__":
    print("[filter_by_hour] Script starting - __name__ == '__main__'", flush=True)
    try:
        main()
    except Exception as e:
        print(f"[filter_by_hour] EXCEPTION: {e}", flush=True)
        import traceback
        traceback.print_exc()