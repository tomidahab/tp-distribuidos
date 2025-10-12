from datetime import datetime
import os
import signal
import time
import sys
from collections import defaultdict

from common.protocol import parse_message, row_to_dict, build_message, CSV_TYPES_REVERSE
from common.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
RECEIVER_EXCHANGE = os.environ.get('RECEIVER_EXCHANGE', 'filter_by_hour_exchange')
WORKER_INDEX = int(os.environ.get('WORKER_INDEX', '0'))
START_HOUR = int(os.environ.get('START_HOUR', '6'))
END_HOUR = int(os.environ.get('END_HOUR', '11'))
FILTER_BY_AMOUNT_EXCHANGE = os.environ.get('FILTER_BY_AMOUNT_EXCHANGE', 'filter_by_amount_exchange')
CATEGORIZER_Q3_EXCHANGE = os.environ.get('CATEGORIZER_Q3_EXCHANGE', 'categorizer_q3_exchange')
CATEGORIZER_Q3_FANOUT_EXCHANGE = os.environ.get('CATEGORIZER_Q3_FANOUT_EXCHANGE', 'categorizer_q3_fanout_exchange')
NUMBER_OF_AMOUNT_WORKERS = int(os.environ.get('NUMBER_OF_AMOUNT_WORKERS', '3'))
NUMBER_OF_HOUR_WORKERS = int(os.environ.get('NUMBER_OF_HOUR_WORKERS', '3'))
NUMBER_OF_YEAR_WORKERS = int(os.environ.get('NUMBER_OF_YEAR_WORKERS', '3'))

SEMESTER_KEYS_FOR_FANOUT = ['semester.2023-1', 'semester.2024-1', 'semester.2024-2','semester.2025-1']

# Global counters for debugging
rows_received = 0
rows_sent_to_amount = 0
rows_sent_to_q3 = 0
end_messages_received = 0
# Track END messages per client: {client_id: count}
client_end_messages = defaultdict(int)
completed_clients = set()

# Track detailed stats per client
client_stats = defaultdict(lambda: {
    'messages_received': 0,
    'rows_received': 0,
    'rows_sent_to_amount': 0,
    'rows_sent_to_q3': 0,
    'end_messages_received': 0,
    'amount_worker_counter': 0  # Per-client counter for amount workers
})

topic_middleware = None
filter_by_amount_exchange = None
categorizer_q3_topic_exchange = None
categorizer_q3_fanout_exchange = None

def _close_queue(queue):
    if queue:
        queue.close()

def _sigterm_handler(signum, _):
    _close_queue(topic_middleware)
    _close_queue(filter_by_amount_exchange)
    _close_queue(categorizer_q3_topic_exchange)
    _close_queue(categorizer_q3_fanout_exchange)

def get_semester_key(year, month):
    """Generate semester routing key based on year and month"""
    semester = 1 if 1 <= month <= 6 else 2
    return f"semester.{year}-{semester}"

def filter_message_by_hour(parsed_message, start_hour: int, end_hour: int) -> list:
    try:
        type_of_message = parsed_message['csv_type']

        # print(f"[transactions] Procesando mensaje con {len(parsed_message['rows'])} rows")  # Mens

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

def on_message_callback(message: bytes, filter_by_amount_exchange, categorizer_q3_topic_exchange, categorizer_q3_fanout_exchange, should_stop):
    global rows_received, rows_sent_to_amount, rows_sent_to_q3, end_messages_received, client_end_messages, completed_clients, client_stats
    
    if should_stop.is_set():  # Don't process if we're stopping
        return
        
    # print(f"[filter_by_hour] Worker {WORKER_INDEX} received a message!", flush=True)
    parsed_message = parse_message(message)
    type_of_message = parsed_message['csv_type']  
    client_id = parsed_message['client_id']
    is_last = int(parsed_message['is_last'])
    
    # Update client stats
    client_stats[client_id]['messages_received'] += 1
    client_stats[client_id]['rows_received'] += len(parsed_message['rows'])
    
    # Handle END message FIRST
    if is_last == 1:
        client_stats[client_id]['end_messages_received'] += 1
        client_end_messages[client_id] += 1
        end_messages_received += 1  # Keep global counter for logging
        print(f"[filter_by_hour] Worker {WORKER_INDEX} received END message {client_end_messages[client_id]}/{NUMBER_OF_YEAR_WORKERS} for client {client_id} (total END messages: {end_messages_received})", flush=True)
    
    # Skip if client already completed - check AFTER processing END message
    if client_id in completed_clients:
        print(f"[filter_by_hour] Worker {WORKER_INDEX} SKIPPING message from completed client {client_id} with {len(parsed_message['rows'])} rows, is_last={is_last}")
        return
    
    #print(f"[filter_by_hour] Worker {WORKER_INDEX} received message from client {client_id} with {len(parsed_message['rows'])} rows, is_last={is_last} (total msgs: {client_stats[client_id]['messages_received']}, total rows: {client_stats[client_id]['rows_received']})")
    
    # Count incoming rows
    incoming_rows = len(parsed_message['rows'])
    rows_received += incoming_rows
    # print(f"[filter_by_hour] Worker {WORKER_INDEX} received {incoming_rows} rows (total received: {rows_received})", flush=True)
    
    filtered_rows = filter_message_by_hour(parsed_message, START_HOUR, END_HOUR)

    #print(f"[filter_by_hour] Worker {WORKER_INDEX} client {client_id}: {len(filtered_rows)} rows passed hour filter (from {len(parsed_message['rows'])} input rows)")

    if (len(filtered_rows) != 0) or (is_last == 1):
        # For Q1 - send to filter_by_amount exchange
        if type_of_message == CSV_TYPES_REVERSE['transactions']:  # transactions
            # Route by transaction_id for load balancing
            if filtered_rows:
                client_stats[client_id]['rows_sent_to_amount'] += len(filtered_rows)
                
                # Group rows by worker to send in batches using per-client counter
                rows_by_worker = defaultdict(list)
                for i, row in enumerate(filtered_rows):
                    # Use per-client counter for deterministic distribution
                    worker_index = (client_stats[client_id]['amount_worker_counter'] + i) % NUMBER_OF_AMOUNT_WORKERS
                    routing_key = f"transaction.{worker_index}"
                    rows_by_worker[routing_key].append(row)
                
                # Update per-client counter (DON'T mod here, just increment)
                client_stats[client_id]['amount_worker_counter'] += len(filtered_rows)
                
                # Send batched messages to each worker
                for routing_key, worker_rows in rows_by_worker.items():
                    if worker_rows:
                        new_message, _ = build_message(client_id, type_of_message, 0, worker_rows)
                        filter_by_amount_exchange.send(new_message, routing_key=routing_key)
                        rows_sent_to_amount += len(worker_rows)
                        #print(f"[filter_by_hour] Worker {WORKER_INDEX} client {client_id}: Sent {len(worker_rows)} rows to filter_by_amount {routing_key} (total sent to amount: {client_stats[client_id]['rows_sent_to_amount']}, counter at: {client_stats[client_id]['amount_worker_counter']})")
                        # print(f"[filter_by_hour] Worker {WORKER_INDEX} sent {len(worker_rows)} rows to {routing_key} (total sent to amount: {rows_sent_to_amount})", flush=True)
            
            # For Q3 - group by semester and send to topic exchange
            if filtered_rows:  # Only process if there are rows
                client_stats[client_id]['rows_sent_to_q3'] += len(filtered_rows)
                
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
                        categorizer_q3_topic_exchange.send(semester_message, routing_key=semester_key)
                        rows_sent_to_q3 += len(semester_rows)
                        #print(f"[filter_by_hour] Worker {WORKER_INDEX} client {client_id}: Sent {len(semester_rows)} rows for {semester_key} to categorizer_q3 (total sent to q3: {client_stats[client_id]['rows_sent_to_q3']})")
                
        #elif type_of_message == CSV_TYPES_REVERSE['transaction_items']:  # transaction_items
            #print(f"[filter_by_hour] Worker {WORKER_INDEX} received a transaction_items message, that should never happen!", flush=True)
        #else:
            #print(f"[filter_by_hour] Worker {WORKER_INDEX} unknown csv_type: {type_of_message}", file=sys.stderr)

    # Check if this client has received all END messages and complete processing
    if is_last == 1 and client_end_messages[client_id] >= NUMBER_OF_YEAR_WORKERS:
        if client_id not in completed_clients:
            print(f"[filter_by_hour] Worker {WORKER_INDEX} client {client_id} received all END messages from filter_by_year workers. Sending END messages...", flush=True)
            completed_clients.add(client_id)
            
            # Print summary stats for this client
            stats = client_stats[client_id]
            print(f"[filter_by_hour] Worker {WORKER_INDEX} SUMMARY for client {client_id}: messages_received={stats['messages_received']}, rows_received={stats['rows_received']}, rows_sent_to_amount={stats['rows_sent_to_amount']}, rows_sent_to_q3={stats['rows_sent_to_q3']}")
            
            try:
                # Send END to filter_by_amount (to all workers) for this specific client
                end_message, _ = build_message(client_id, type_of_message, 1, [])
                for i in range(NUMBER_OF_AMOUNT_WORKERS):
                    routing_key = f"transaction.{i}"
                    filter_by_amount_exchange.send(end_message, routing_key=routing_key)
                    print(f"[filter_by_hour] Worker {WORKER_INDEX} sent END message for client {client_id} to filter_by_amount worker {i} via topic exchange", flush=True)
                
                # Send END to categorizer_q3 via fanout exchange for this specific client
                end_message, _ = build_message(client_id, type_of_message, 1, [])
                categorizer_q3_fanout_exchange.send(end_message)
                print(f"[filter_by_hour] Worker {WORKER_INDEX} sent END message for client {client_id} to categorizer_q3 via fanout exchange", flush=True)
            except Exception as e:
                print(f"[filter_by_hour] Worker {WORKER_INDEX} ERROR sending END message for client {client_id}: {e}", flush=True)
                import traceback
                traceback.print_exc()


def make_on_message_callback(filter_by_amount_exchange, categorizer_q3_topic_exchange, categorizer_q3_fanout_exchange, should_stop):
    def wrapper(message: bytes):
        on_message_callback(message, filter_by_amount_exchange, categorizer_q3_topic_exchange, categorizer_q3_fanout_exchange, should_stop)
    return wrapper

def main():
    import threading
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)
    # Global counters for debugging
    global rows_received, rows_sent_to_amount, rows_sent_to_q3
    rows_received = 0
    rows_sent_to_amount = 0
    rows_sent_to_q3 = 0
    
    print(f"[filter_by_hour] Worker {WORKER_INDEX} STARTING UP - Basic imports done", flush=True)
    print(f"[filter_by_hour] Worker {WORKER_INDEX} Environment: RECEIVER_EXCHANGE={RECEIVER_EXCHANGE}, START_HOUR={START_HOUR}, END_HOUR={END_HOUR}", flush=True)
    print(f"[filter_by_hour] Worker {WORKER_INDEX} Environment: FILTER_BY_AMOUNT_EXCHANGE={FILTER_BY_AMOUNT_EXCHANGE}", flush=True)
    print(f"[filter_by_hour] Worker {WORKER_INDEX} Environment: CATEGORIZER_Q3_EXCHANGE={CATEGORIZER_Q3_EXCHANGE}", flush=True)
    
    print(f"[filter_by_hour] Worker {WORKER_INDEX} waiting for RabbitMQ to be ready...", flush=True)
    time.sleep(30)  # Wait for RabbitMQ to be ready
    print(f"[filter_by_hour] Worker {WORKER_INDEX} RabbitMQ should be ready now!", flush=True)
    
    # Create topic exchange middleware for receiving messages
    global topic_callback
    topic_middleware = MessageMiddlewareExchange(
        host=RABBITMQ_HOST,
        exchange_name=RECEIVER_EXCHANGE,
        exchange_type='topic',
        queue_name=f"filter_by_hour_worker_{WORKER_INDEX}_queue",
        routing_keys=[f'hour.{WORKER_INDEX}']  # Each worker listens to specific routing key
    )
    
    print(f"[filter_by_hour] Worker {WORKER_INDEX} connecting to exchanges...", flush=True)
    
    try:
        print(f"[filter_by_hour] Worker {WORKER_INDEX} creating filter_by_amount topic exchange connection...")
        global filter_by_amount_exchange
        filter_by_amount_exchange = MessageMiddlewareExchange(
            RABBITMQ_HOST,
            exchange_name=FILTER_BY_AMOUNT_EXCHANGE,
            exchange_type='topic',
            queue_name=""  # Empty queue since we're only sending, not consuming
        )
        print(f"[filter_by_hour] Worker {WORKER_INDEX} connected to filter_by_amount topic exchange: {FILTER_BY_AMOUNT_EXCHANGE}")
        
        print(f"[filter_by_hour] Worker {WORKER_INDEX} creating categorizer_q3 topic exchange connection...")
        # Connect to categorizer_q3 topic exchange
        global categorizer_q3_topic_exchange
        categorizer_q3_topic_exchange = MessageMiddlewareExchange(
            RABBITMQ_HOST,
            exchange_name=CATEGORIZER_Q3_EXCHANGE,
            exchange_type='topic',
            queue_name=""  # Empty queue since we're only sending, not consuming
        )
        print(f"[filter_by_hour] Worker {WORKER_INDEX} connected to categorizer_q3 topic exchange: {CATEGORIZER_Q3_EXCHANGE}")
        
        print(f"[filter_by_hour] Worker {WORKER_INDEX} creating categorizer_q3 fanout exchange connection...")
        # Connect to categorizer_q3 fanout exchange for END messages
        global categorizer_q3_fanout_exchange
        categorizer_q3_fanout_exchange = MessageMiddlewareExchange(
            RABBITMQ_HOST,
            exchange_name=CATEGORIZER_Q3_FANOUT_EXCHANGE,
            exchange_type='fanout',
            queue_name=""  # Empty queue since we're only sending, not consuming
        )
        print(f"[filter_by_hour] Worker {WORKER_INDEX} connected to categorizer_q3 fanout exchange: {CATEGORIZER_Q3_FANOUT_EXCHANGE}")
        
        print(f"[filter_by_hour] Worker {WORKER_INDEX} listening for transactions on exchange: {RECEIVER_EXCHANGE} with routing key: hour.{WORKER_INDEX}", flush=True)
        
        # Flag to coordinate stopping
        should_stop = threading.Event()
        
        # Start consuming from topic exchange (blocking)
        topic_callback = make_on_message_callback(filter_by_amount_exchange, categorizer_q3_topic_exchange, categorizer_q3_fanout_exchange, should_stop)
        topic_middleware.start_consuming(topic_callback)
        
        print(f"[filter_by_hour] Worker {WORKER_INDEX} topic consuming finished", flush=True)
        
        should_stop.set()
            
    except MessageMiddlewareDisconnectedError:
        print(f"[filter_by_hour] Worker {WORKER_INDEX} disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print(f"[filter_by_hour] Worker {WORKER_INDEX} message error in middleware.", file=sys.stderr)
    except KeyboardInterrupt:
        print(f"[filter_by_hour] Worker {WORKER_INDEX} stopping...")
        should_stop.set()
        topic_middleware.stop_consuming()

    finally:
        try:
            topic_middleware.close()
        except Exception as e:
            print(f"[filter_by_hour] Worker {WORKER_INDEX} error closing topic middleware: {e}", file=sys.stderr)
        try:
            filter_by_amount_exchange.close()
        except Exception as e:
            print(f"[filter_by_hour] Worker {WORKER_INDEX} error closing filter_by_amount_exchange: {e}", file=sys.stderr)
        try:
            categorizer_q3_topic_exchange.close()
        except Exception as e:
            print(f"[filter_by_hour] Worker {WORKER_INDEX} error closing categorizer_q3_topic_exchange: {e}", file=sys.stderr)
        try:
            categorizer_q3_fanout_exchange.close()
        except Exception as e:
            print(f"[filter_by_hour] Worker {WORKER_INDEX} error closing categorizer_q3_fanout_exchange: {e}", file=sys.stderr)

if __name__ == "__main__":
    print(f"[filter_by_hour] Worker {WORKER_INDEX} script starting - __name__ == '__main__'", flush=True)
    try:
        main()
    except Exception as e:
        print(f"[filter_by_hour] Worker {WORKER_INDEX} EXCEPTION: {e}", flush=True)
        import traceback
        traceback.print_exc()
        traceback.print_exc()