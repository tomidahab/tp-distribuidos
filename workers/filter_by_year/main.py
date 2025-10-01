import os
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
ITEM_CATEGORIZER_QUEUE = os.environ.get('ITEM_CATEGORIZER_QUEUE', 'categorizer_q2_receiver_queue')
FILTER_YEARS = [
    int(y.strip()) for y in os.environ.get('FILTER_YEAR', '2024,2025').split(',')
]
TOPIC_EXCHANGE = os.environ.get('TOPIC_EXCHANGE', 'categorizer_q2_topic_exchange')

# Add these environment variables
CATEGORIZER_Q4_TOPIC_EXCHANGE = os.environ.get('CATEGORIZER_Q4_TOPIC_EXCHANGE', 'categorizer_q4_topic_exchange')
CATEGORIZER_Q4_FANOUT_EXCHANGE = os.environ.get('CATEGORIZER_Q4_FANOUT_EXCHANGE', 'categorizer_q4_fanout_exchange')
CATEGORIZER_Q4_WORKERS = int(os.environ.get('CATEGORIZER_Q4_WORKERS', 3))

print(f"[filter_by_year] Worker {WORKER_INDEX} starting with FILTER_YEARS: {FILTER_YEARS}")


def on_message_callback_transactions(message: bytes, hour_filter_exchange, categorizer_q4_topic_exchange, categorizer_q4_fanout_exchange):
    try:
        parsed_message = parse_message(message)
        type_of_message = parsed_message['csv_type']  
        client_id = parsed_message['client_id']
        is_last = parsed_message['is_last']
        new_rows = []
        
        print(f"[filter_by_year] Worker {WORKER_INDEX} received transactions message with {len(parsed_message['rows'])} rows, is_last={is_last}")
        
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
                print(f"[filter_by_year] Worker {WORKER_INDEX} Number of rows on the message passed year filter: {len(new_rows)}, is_last={is_last}")
            
            # Send to filter_by_hour workers using round robin
            if new_rows:
                # Group rows by worker to send in batches instead of one by one
                rows_by_worker = defaultdict(list)
                for i, row in enumerate(new_rows):
                    worker_index = i % NUMBER_OF_HOUR_WORKERS  # Simple round robin by row index
                    routing_key = f"hour.{worker_index}"
                    rows_by_worker[routing_key].append(row)
                
                # Send batched messages to each worker
                for routing_key, worker_rows in rows_by_worker.items():
                    if worker_rows:
                        new_message, _ = build_message(client_id, type_of_message, 0, worker_rows)
                        hour_filter_exchange.send(new_message, routing_key=routing_key)
                        print(f"[filter_by_year] Worker {WORKER_INDEX} Sent {len(worker_rows)} rows to filter_by_hour {routing_key}", flush=True)

            # Send to categorizer_q4 workers  
            batches = defaultdict(list)
            for row in new_rows:
                dic_fields_row = row_to_dict(row, type_of_message)
                store_id = int(dic_fields_row['store_id'])
                routing_key = f"store.{store_id % CATEGORIZER_Q4_WORKERS}"
                batches[routing_key].append(row)

            for routing_key, batch_rows in batches.items():
                if batch_rows:
                    q4_message, _ = build_message(client_id, type_of_message, 0, batch_rows)
                    print(f"[filter_by_year] Worker {WORKER_INDEX} Sending {len(batch_rows)} rows to categorizer_q4 with routing key {routing_key}")
                    categorizer_q4_topic_exchange.send(q4_message, routing_key=routing_key)

        if is_last:
            # Send END message to all filter_by_hour workers
            end_message, _ = build_message(client_id, type_of_message, 1, [])
            for i in range(NUMBER_OF_HOUR_WORKERS):
                routing_key = f"hour.{i}"
                hour_filter_exchange.send(end_message, routing_key=routing_key)
                print(f"[filter_by_year] Worker {WORKER_INDEX} Sent END message to filter_by_hour worker {i} via topic exchange", flush=True)
            
            # Send END message to categorizer_q4 via fanout exchange
            categorizer_q4_fanout_exchange.send(end_message)
            print(f"[filter_by_year] Worker {WORKER_INDEX} Sent END message to categorizer_q4 via fanout exchange.")

    except Exception as e:
        print(f"[transactions] Worker {WORKER_INDEX} Error decoding message: {e}", file=sys.stderr)

def on_message_callback_t_items(message: bytes, item_categorizer_exchange, item_categorizer_fanout_exchange):
    try:
        parsed_message = parse_message(message)
        type_of_message = parsed_message['csv_type']  
        client_id = parsed_message['client_id']
        is_last = parsed_message['is_last']

        print(f"[filter_by_year] Worker {WORKER_INDEX} received transaction_items message with {len(parsed_message['rows'])} rows, is_last={is_last}")

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
                new_message, _ = build_message(client_id, type_of_message, 0, rows)
                routing_key = f"month.{month}"
                print(f"[filter_by_year] Worker {WORKER_INDEX} Sending {len(rows)} rows for month {month} to categorizer_q2 with routing key {routing_key}")
                item_categorizer_exchange.send(new_message, routing_key=routing_key)

        if is_last == 1:
            end_message, _ = build_message(client_id, type_of_message, is_last, [])
            item_categorizer_fanout_exchange.send(end_message)
            print(f"[filter_by_year] Worker {WORKER_INDEX} Sent END message to categorizer_q2 via fanout exchange.")
    except Exception as e:
        print(f"[t_items] Worker {WORKER_INDEX} Error decoding message: {e}", file=sys.stderr)

def main():
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
                    print(f"[filter_by_year] Worker {WORKER_INDEX} received transactions END message {transactions_end_received}")
                    if transactions_end_received >= 1:  # Expecting 1 END message from gateway
                        print(f"[filter_by_year] Worker {WORKER_INDEX} stopping transactions consumer")
                        receiver_exchange_t.stop_consuming()
                
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
                
                if is_last:
                    transaction_items_end_received += 1
                    print(f"[filter_by_year] Worker {WORKER_INDEX} received transaction_items END message {transaction_items_end_received}")
                    if transaction_items_end_received >= 1:  # Expecting 1 END message from gateway
                        print(f"[filter_by_year] Worker {WORKER_INDEX} stopping transaction_items consumer")
                        receiver_exchange_t_items.stop_consuming()
                        return
                
                on_message_callback_t_items(message, item_categorizer_exchange, item_categorizer_fanout_exchange)
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
