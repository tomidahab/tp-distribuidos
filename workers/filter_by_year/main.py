import os
import signal
import sys
import threading
from time import sleep
from datetime import datetime
from common.protocol import parse_message, row_to_dict, build_message
from common.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError
import common.config as config
from collections import defaultdict

# Configurable parameters (could be set via env vars or args)
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', os.environ.get('rabbitmq_server_HOST', 'rabbitmq_server'))
RECEIVER_EXCHANGE_T_ITEMS = os.environ.get('RECEIVER_EXCHANGE_T_ITEMS', 'filter_by_year_transaction_items_exchange')
RECEIVER_EXCHANGE_T = os.environ.get('RECEIVER_EXCHANGE_T', 'filter_by_year_transactions_exchange')
WORKER_INDEX = int(os.environ.get('WORKER_INDEX', '0'))
HOUR_FILTER_EXCHANGE = os.environ.get('HOUR_FILTER_EXCHANGE', 'filter_by_hour_exchange')
NUMBER_OF_HOUR_WORKERS = int(os.environ.get('NUMBER_OF_HOUR_WORKERS', '3'))
NUMBER_OF_YEAR_WORKERS = int(os.environ.get('NUMBER_OF_YEAR_WORKERS', '3'))
NUMBER_OF_Q2_CATEGORIZER_WORKERS = int(os.environ.get('NUMBER_OF_Q2_CATEGORIZER_WORKERS', '3'))
ITEM_CATEGORIZER_QUEUE = os.environ.get('ITEM_CATEGORIZER_QUEUE', 'categorizer_q2_receiver_queue')
FILTER_YEARS = [
    int(y.strip()) for y in os.environ.get('FILTER_YEAR', '2024,2025').split(',')
]
TOPIC_EXCHANGE = os.environ.get('TOPIC_EXCHANGE', 'categorizer_q2_topic_exchange')

# Add these environment variables
CATEGORIZER_Q4_TOPIC_EXCHANGE = os.environ.get('CATEGORIZER_Q4_TOPIC_EXCHANGE', 'categorizer_q4_topic_exchange')
CATEGORIZER_Q4_FANOUT_EXCHANGE = os.environ.get('CATEGORIZER_Q4_FANOUT_EXCHANGE', 'categorizer_q4_fanout_exchange')
CATEGORIZER_Q4_WORKERS = int(os.environ.get('CATEGORIZER_Q4_WORKERS', 3))

receiver_exchange_t = None
receiver_exchange_t_items = None
hour_filter_exchange = None
item_categorizer_exchange = None
item_categorizer_fanout_exchange = None
categorizer_q3_topic_exchange = None
categorizer_q4_topic_exchange = None
categorizer_q4_fanout_exchange = None

# Global counters for debugging
client_stats = defaultdict(lambda: {
    'transactions_received': 0,
    'transaction_items_received': 0,
    'transactions_rows_received': 0,
    'transaction_items_rows_received': 0,
    'transactions_rows_sent_to_hour': 0,
    'transaction_items_rows_sent_to_q2': 0,
    'transactions_rows_sent_to_q4': 0,
    'transactions_end_received': 0,
    'transaction_items_end_received': 0,
    'hour_worker_counter': 0,  # Per-client counter for hour workers
    'q4_message_counter': 0   # Per-client counter for q4 distribution
})

def _close_queue(queue):
    if queue:
        queue.close()

def _sigterm_handler(signum, _):
    _close_queue(receiver_exchange_t)
    _close_queue(receiver_exchange_t_items)
    _close_queue(hour_filter_exchange)
    _close_queue(item_categorizer_exchange)
    _close_queue(item_categorizer_fanout_exchange)
    _close_queue(categorizer_q3_topic_exchange)
    _close_queue(categorizer_q4_topic_exchange)
    _close_queue(categorizer_q4_fanout_exchange)

print(f"[filter_by_year] Worker {WORKER_INDEX} starting with FILTER_YEARS: {FILTER_YEARS}")


def on_message_callback_transactions(message: bytes, hour_filter_exchange, categorizer_q4_topic_exchange, categorizer_q4_fanout_exchange):
    global client_stats
    try:
        parsed_message = parse_message(message)
        type_of_message = parsed_message['csv_type']  
        client_id = parsed_message['client_id']
        is_last = parsed_message['is_last']
        new_rows = []

        # Update stats
        client_stats[client_id]['transactions_received'] += 1
        client_stats[client_id]['transactions_rows_received'] += len(parsed_message['rows'])
        
        print(f"[filter_by_year] Worker {WORKER_INDEX} received transactions message from client {client_id} with {len(parsed_message['rows'])} rows, is_last={is_last} (total msgs: {client_stats[client_id]['transactions_received']}, total rows: {client_stats[client_id]['transactions_rows_received']})")

        for row in parsed_message['rows']:
            dic_fields_row = row_to_dict(row, type_of_message)
            try:
                created_at = dic_fields_row['created_at']
                msg_year = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").year
                if msg_year in FILTER_YEARS:
                    new_rows.append(row)
            except Exception as e:
                print(f"[transactions] Worker {WORKER_INDEX} Error parsing created_at: {created_at} ({e})", file=sys.stderr)

        if new_rows or is_last: 
            if is_last:
                print(f"[filter_by_year] Worker {WORKER_INDEX} client {client_id}: {len(new_rows)} rows passed year filter, is_last={is_last}")
            
            # Send to filter_by_hour workers using per-client round robin
            if new_rows:
                # Group rows by worker to send in batches using per-client counter
                rows_by_worker = defaultdict(list)
                for i, row in enumerate(new_rows):
                    # Use per-client counter for deterministic distribution
                    worker_index = (client_stats[client_id]['hour_worker_counter'] + i) % NUMBER_OF_HOUR_WORKERS
                    routing_key = f"hour.{worker_index}"
                    rows_by_worker[routing_key].append(row)
                
                # Update per-client counter (DON'T mod here, just increment)
                client_stats[client_id]['hour_worker_counter'] += len(new_rows)
                
                # Update stats
                client_stats[client_id]['transactions_rows_sent_to_hour'] += len(new_rows)
                
                # Send batched messages to each worker
                for routing_key, worker_rows in rows_by_worker.items():
                    if worker_rows:
                        new_message, _ = build_message(client_id, type_of_message, 0, worker_rows)
                        hour_filter_exchange.send(new_message, routing_key=routing_key)
                        print(f"[filter_by_year] Worker {WORKER_INDEX} client {client_id}: Sent {len(worker_rows)} rows to filter_by_hour {routing_key} (total sent to hour: {client_stats[client_id]['transactions_rows_sent_to_hour']}, counter at: {client_stats[client_id]['hour_worker_counter']})")

            # Send to categorizer_q4 workers  
            batches = defaultdict(list)
            if new_rows:
                client_stats[client_id]['transactions_rows_sent_to_q4'] += len(new_rows)
            for row in new_rows:
                dic_fields_row = row_to_dict(row, type_of_message)
                store_id = int(dic_fields_row['store_id'])
                routing_key = f"store.{store_id % CATEGORIZER_Q4_WORKERS}"
                batches[routing_key].append(row)

            for routing_key, batch_rows in batches.items():
                if batch_rows:
                    q4_message, _ = build_message(client_id, type_of_message, 0, batch_rows)
                    print(f"[filter_by_year] Worker {WORKER_INDEX} client {client_id}: Sending {len(batch_rows)} rows to categorizer_q4 with routing key {routing_key}")
                    categorizer_q4_topic_exchange.send(q4_message, routing_key=routing_key)

        if is_last:
            client_stats[client_id]['transactions_end_received'] += 1
            print(f"[filter_by_year] Worker {WORKER_INDEX} received transactions END message from client {client_id} (total END msgs: {client_stats[client_id]['transactions_end_received']})")
            
            # Send END message to all filter_by_hour workers
            end_message, _ = build_message(client_id, type_of_message, 1, [])
            for i in range(NUMBER_OF_HOUR_WORKERS):
                routing_key = f"hour.{i}"
                hour_filter_exchange.send(end_message, routing_key=routing_key)
                print(f"[filter_by_year] Worker {WORKER_INDEX} Sent END message for client {client_id} to filter_by_hour worker {i} via topic exchange", flush=True)
            
            # Send messages for Q4 workers
            for store_id in range(CATEGORIZER_Q4_WORKERS):
                routing_key = f"store.{store_id % CATEGORIZER_Q4_WORKERS}"
                q4_message, _ = build_message(client_id, type_of_message, 1, [])
                categorizer_q4_topic_exchange.send(q4_message, routing_key=routing_key)
                print(f"[filter_by_year] Worker {WORKER_INDEX} client {client_id}: Sent END message to categorizer_q4 with routing_key={routing_key}")
                
            # Print summary stats for this client
            stats = client_stats[client_id]
            print(f"[filter_by_year] Worker {WORKER_INDEX} SUMMARY for client {client_id}: transactions_received={stats['transactions_received']}, transactions_rows_received={stats['transactions_rows_received']}, transactions_rows_sent_to_hour={stats['transactions_rows_sent_to_hour']}, transactions_rows_sent_to_q4={stats['transactions_rows_sent_to_q4']}")

            # Using topic instead of fanout so do not use the next code
            # end_message, _ = build_message(client_id, type_of_message, 1, [])
            # hour_filter_queue.send(end_message)
            # print("[filter_by_year] Sent END message to categorizer_q4 via fanout exchange.")
            # categorizer_q4_fanout_exchange.send(end_message)
            # print("[filter_by_year] Sent END message to hour filter queue.")

    except Exception as e:
        print(f"[transactions] Worker {WORKER_INDEX} Error decoding message: {e}", file=sys.stderr)

def on_message_callback_t_items(message: bytes, item_categorizer_exchange, item_categorizer_fanout_exchange):
    global client_stats
    try:
        parsed_message = parse_message(message)
        type_of_message = parsed_message['csv_type']  
        client_id = parsed_message['client_id']
        is_last = parsed_message['is_last']

        # Update stats
        client_stats[client_id]['transaction_items_received'] += 1
        client_stats[client_id]['transaction_items_rows_received'] += len(parsed_message['rows'])

        print(f"[filter_by_year] Worker {WORKER_INDEX} received transaction_items message from client {client_id} with {len(parsed_message['rows'])} rows, is_last={is_last} (total msgs: {client_stats[client_id]['transaction_items_received']}, total rows: {client_stats[client_id]['transaction_items_rows_received']})")

        rows_by_month = defaultdict(list)
        for row in parsed_message['rows']:
            dic_fields_row = row_to_dict(row, type_of_message)
            try:
                created_at = dic_fields_row['created_at']
                msg_year = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").year
                month = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").month
                if msg_year in FILTER_YEARS:
                    rows_by_month[month].append(row)
            except Exception as e:
                print(f"[t_items] Worker {WORKER_INDEX} Error parsing created_at: {dic_fields_row.get('created_at', '')} ({e})", file=sys.stderr)

        for month, rows in rows_by_month.items():
            if rows != []:
                client_stats[client_id]['transaction_items_rows_sent_to_q2'] += len(rows)
                new_message, _ = build_message(client_id, type_of_message, 0, rows)
                routing_key = f"month.{month}"
                print(f"[filter_by_year] Worker {WORKER_INDEX} client {client_id}: Sending {len(rows)} rows for month {month} to categorizer_q2 with routing key {routing_key} (total sent to q2: {client_stats[client_id]['transaction_items_rows_sent_to_q2']})")
                item_categorizer_exchange.send(new_message, routing_key=routing_key)

        if is_last == 1:
            client_stats[client_id]['transaction_items_end_received'] += 1
            print(f"[filter_by_year] Worker {WORKER_INDEX} received transaction_items END message from client {client_id} (total END msgs: {client_stats[client_id]['transaction_items_end_received']})")
            
            end_message, _ = build_message(client_id, type_of_message, is_last, [])
            # Send END message to all months (1-12) to ensure all categorizer_q2 workers receive it
            # Sending this with (1, 13) range will make a categorizer receive the 3 messages that need to pass
            for month in range(1, 13, 13 // NUMBER_OF_Q2_CATEGORIZER_WORKERS):
                routing_key = f"month.{month}"
                item_categorizer_exchange.send(end_message, routing_key=routing_key)
                print(f"[filter_by_year] Worker {WORKER_INDEX} client {client_id}: Sent END message to categorizer_q2 with routing key {routing_key}")
                
            # Print summary stats for this client
            stats = client_stats[client_id]
            print(f"[filter_by_year] Worker {WORKER_INDEX} T_ITEMS SUMMARY for client {client_id}: transaction_items_received={stats['transaction_items_received']}, transaction_items_rows_received={stats['transaction_items_rows_received']}, transaction_items_rows_sent_to_q2={stats['transaction_items_rows_sent_to_q2']}")
    except Exception as e:
        print(f"[t_items] Worker {WORKER_INDEX} Error decoding message: {e}", file=sys.stderr)

def main():
    global receiver_exchange_t
    global receiver_exchange_t_items
    global receiver_exchange_t
    global receiver_exchange_t_items
    global hour_filter_exchange
    global item_categorizer_exchange
    global item_categorizer_fanout_exchange
    global categorizer_q4_topic_exchange
    global categorizer_q4_fanout_exchange

    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)
    
    print(f"[filter_by_year] Worker {WORKER_INDEX} starting...")
    sleep(config.MIDDLEWARE_UP_TIME)  
    print(f"[filter_by_year] Worker {WORKER_INDEX} Connecting to RabbitMQ at {RABBITMQ_HOST}, exchanges: {RECEIVER_EXCHANGE_T}, {RECEIVER_EXCHANGE_T_ITEMS}, filter years: {FILTER_YEARS}")
    
    # Track END messages for both streams
    transactions_end_received = 0
    transaction_items_end_received = 0
    
    try:
        # Create receiver exchange for transactions
        receiver_exchange_t = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=RECEIVER_EXCHANGE_T,
            exchange_type='topic',
            queue_name=f"filter_by_year_transactions_worker_{WORKER_INDEX}_queue",
            routing_keys=[f"year.{WORKER_INDEX}"]
        )
        print(f"[filter_by_year] Worker {WORKER_INDEX} Connected to transactions exchange: {RECEIVER_EXCHANGE_T}")
        
        # Create receiver exchange for transaction_items
        receiver_exchange_t_items = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=RECEIVER_EXCHANGE_T_ITEMS,
            exchange_type='topic',
            queue_name=f"filter_by_year_transaction_items_worker_{WORKER_INDEX}_queue",
            routing_keys=[f"year.{WORKER_INDEX}"]
        )
        print(f"[filter_by_year] Worker {WORKER_INDEX} Connected to transaction_items exchange: {RECEIVER_EXCHANGE_T_ITEMS}")
        
        # Create sender exchanges
        hour_filter_exchange = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=HOUR_FILTER_EXCHANGE,
            exchange_type='topic',
            queue_name="",  # Empty since we're only sending
        )
        print(f"[filter_by_year] Worker {WORKER_INDEX} Connected to hour filter exchange: {HOUR_FILTER_EXCHANGE}")

        item_categorizer_exchange = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=TOPIC_EXCHANGE,
            exchange_type='topic',
            queue_name="",  # Empty since we're only sending
        )
        print(f"[filter_by_year] Worker {WORKER_INDEX} Connected to item categorizer topic exchange: {TOPIC_EXCHANGE}")
        
        FANOUT_EXCHANGE = os.environ.get('FANOUT_EXCHANGE', 'categorizer_q2_fanout_exchange')
        item_categorizer_fanout_exchange = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=FANOUT_EXCHANGE,
            exchange_type='fanout',
            queue_name="",  # Empty since we're only sending
        )
        print(f"[filter_by_year] Worker {WORKER_INDEX} Connected to item categorizer fanout exchange: {FANOUT_EXCHANGE}")
        
        categorizer_q4_topic_exchange = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=CATEGORIZER_Q4_TOPIC_EXCHANGE,
            exchange_type='topic',
            queue_name='', 
        )
        print(f"[filter_by_year] Worker {WORKER_INDEX} Connected to categorizer_q4 topic exchange: {CATEGORIZER_Q4_TOPIC_EXCHANGE}")
        
        categorizer_q4_fanout_exchange = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=CATEGORIZER_Q4_FANOUT_EXCHANGE,
            exchange_type='fanout',
            queue_name='', 
        )
        print(f"[filter_by_year] Worker {WORKER_INDEX} Connected to categorizer_q4 fanout exchange: {CATEGORIZER_Q4_FANOUT_EXCHANGE}")
        
    except Exception as e:
        print(f"[filter_by_year] Worker {WORKER_INDEX} Error connecting to RabbitMQ: {e}", file=sys.stderr)
        return

    print(f"[filter_by_year] Worker {WORKER_INDEX} Starting consumer threads...")
    
    # Transaction processing thread
    def consume_transactions():
        nonlocal transactions_end_received
        print(f"[filter_by_year] Worker {WORKER_INDEX} Starting transactions consumer...")
        
        def on_transactions_message(message: bytes):
            nonlocal transactions_end_received
            try:
                parsed_message = parse_message(message)
                is_last = parsed_message['is_last']
                
                if is_last:
                    transactions_end_received += 1
                    parsed_msg = parse_message(message)
                    client_id = parsed_msg['client_id']
                    print(f"[filter_by_year] Worker {WORKER_INDEX} received transactions END message {transactions_end_received} from client {client_id}")
                    # Don't stop consuming - keep processing messages from multiple clients
                
                on_message_callback_transactions(message, hour_filter_exchange, categorizer_q4_topic_exchange, categorizer_q4_fanout_exchange)
            except Exception as e:
                print(f"[filter_by_year] Worker {WORKER_INDEX} Error in transactions callback: {e}", file=sys.stderr)
        
        try:
            receiver_exchange_t.start_consuming(on_transactions_message)
        except Exception as e:
            print(f"[filter_by_year] Worker {WORKER_INDEX} Error consuming transactions: {e}", file=sys.stderr)
    
    # Transaction items processing thread
    def consume_transaction_items():
        nonlocal transaction_items_end_received
        print(f"[filter_by_year] Worker {WORKER_INDEX} Starting transaction_items consumer...")
        
        def on_transaction_items_message(message: bytes):
            nonlocal transaction_items_end_received
            try:
                parsed_message = parse_message(message)
                is_last = parsed_message['is_last']
                
                # Always process the message first
                on_message_callback_t_items(message, item_categorizer_exchange, item_categorizer_fanout_exchange)
                
                if is_last:
                    transaction_items_end_received += 1
                    client_id = parsed_message['client_id']
                    print(f"[filter_by_year] Worker {WORKER_INDEX} received transaction_items END message {transaction_items_end_received} from client {client_id}")
                    # Don't stop consuming - keep processing messages from multiple clients
                
            except Exception as e:
                print(f"[filter_by_year] Worker {WORKER_INDEX} Error in transaction_items callback: {e}", file=sys.stderr)
        
        try:
            receiver_exchange_t_items.start_consuming(on_transaction_items_message)
        except Exception as e:
            print(f"[filter_by_year] Worker {WORKER_INDEX} Error consuming transaction_items: {e}", file=sys.stderr)
    
    # Start both consumer threads
    t1 = threading.Thread(target=consume_transactions)
    t2 = threading.Thread(target=consume_transaction_items)
    
    print(f"[filter_by_year] Worker {WORKER_INDEX} Starting transactions thread...")
    t1.start()
    print(f"[filter_by_year] Worker {WORKER_INDEX} Starting transaction_items thread...")
    t2.start()
    
    print(f"[filter_by_year] Worker {WORKER_INDEX} All threads started, waiting for messages...")
    try:
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        print(f"[filter_by_year] Worker {WORKER_INDEX} Stopping...")
        receiver_exchange_t.stop_consuming()
        receiver_exchange_t_items.stop_consuming()
        receiver_exchange_t.close()
        receiver_exchange_t_items.close()
        hour_filter_exchange.close()
        item_categorizer_exchange.close()
        item_categorizer_fanout_exchange.close()
        categorizer_q4_topic_exchange.close()
        categorizer_q4_fanout_exchange.close()

if __name__ == "__main__":
    main()
