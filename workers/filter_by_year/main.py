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
QUEUE_T_ITEMS = os.environ.get('QUEUE_T_ITEMS', 'filter_by_year_transaction_items_queue')
QUEUE_T = os.environ.get('QUEUE_T', 'filter_by_year_transactions_queue')
HOUR_FILTER_QUEUE = os.environ.get('HOUR_FILTER_QUEUE', 'filter_by_hour_queue')
ITEM_CATEGORIZER_QUEUE = os.environ.get('ITEM_CATEGORIZER_QUEUE', 'categorizer_q2_receiver_queue')
FILTER_YEARS = [
    int(y.strip()) for y in os.environ.get('FILTER_YEAR', '2024,2025').split(',')
]
TOPIC_EXCHANGE = os.environ.get('TOPIC_EXCHANGE', 'categorizer_q2_topic_exchange')

# Add these environment variables
CATEGORIZER_Q4_TOPIC_EXCHANGE = os.environ.get('CATEGORIZER_Q4_TOPIC_EXCHANGE', 'categorizer_q4_topic_exchange')
CATEGORIZER_Q4_FANOUT_EXCHANGE = os.environ.get('CATEGORIZER_Q4_FANOUT_EXCHANGE', 'categorizer_q4_fanout_exchange')
CATEGORIZER_Q4_WORKERS = int(os.environ.get('CATEGORIZER_Q4_WORKERS', 3))

print(f"[filter_by_year] Starting with FILTER_YEARS: {FILTER_YEARS}")


def on_message_callback_transactions(message: bytes, hour_filter_queue, categorizer_q4_topic_exchange, categorizer_q4_fanout_exchange):
    try:
        parsed_message = parse_message(message)
        type_of_message = parsed_message['csv_type']  
        client_id = parsed_message['client_id']
        is_last = parsed_message['is_last']
        new_rows = []
        for row in parsed_message['rows']:
            dic_fields_row = row_to_dict(row, type_of_message)
            try:
                created_at = dic_fields_row['created_at']
                msg_year = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").year
                if msg_year in FILTER_YEARS:
                    new_rows.append(row)
            except Exception as e:
                print(f"[transactions] Error parsing created_at: {created_at} ({e})", file=sys.stderr)

        if new_rows or is_last: 
            if is_last:
                print(f"[worker] Number of rows on the message passed year filter: {len(new_rows)}, is_last={is_last}")
            new_message, _ = build_message(client_id, type_of_message, is_last, new_rows)
            hour_filter_queue.send(new_message)
            #for row in new_rows:
                #dic_fields_row = row_to_dict(row, type_of_message)
                #store_id = int(dic_fields_row['store_id'])
                #routing_key = f"store.{store_id % CATEGORIZER_Q4_WORKERS}"
                #q4_message, _ = build_message(client_id, type_of_message, 0, [row])
                #categorizer_q4_topic_exchange.send(q4_message, routing_key=routing_key)

            batches = defaultdict(list)
            for row in new_rows:
                dic_fields_row = row_to_dict(row, type_of_message)
                store_id = int(dic_fields_row['store_id'])
                routing_key = f"store.{store_id % CATEGORIZER_Q4_WORKERS}"
                batches[routing_key].append(row)

            for routing_key, batch_rows in batches.items():
                if batch_rows:
                    q4_message, _ = build_message(client_id, type_of_message, 0, batch_rows)
                    print(f"[filter_by_year] Sending {len(batch_rows)} rows to categorizer_q4 with routing key {routing_key}")
                    categorizer_q4_topic_exchange.send(q4_message, routing_key=routing_key)

        #if is_last:
            #end_message, _ = build_message(client_id, type_of_message, 1, [])
            #hour_filter_queue.send(end_message)
            #print("[filter_by_year] Sent END message to hour filter queue.")
            #categorizer_q4_fanout_exchange.send(end_message)
            #print("[filter_by_year] Sent END message to categorizer_q4 via fanout exchange.")

    except Exception as e:
        print(f"[transactions] Error decoding message: {e}", file=sys.stderr)

def on_message_callback_t_items(message: bytes, item_categorizer_exchange, item_categorizer_fanout_exchange):
    try:
        parsed_message = parse_message(message)
        type_of_message = parsed_message['csv_type']  
        client_id = parsed_message['client_id']
        is_last = parsed_message['is_last']

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
                print(f"[t_items] Error parsing created_at: {dic_fields_row.get('created_at', '')} ({e})", file=sys.stderr)

        for month, rows in rows_by_month.items():
            if rows != []:
                new_message, _ = build_message(client_id, type_of_message, 0, rows)
                routing_key = f"month.{month}"
                print(f"[filter_by_year] Sending {len(rows)} rows for month {month} to categorizer_q2 with routing key {routing_key}")
                item_categorizer_exchange.send(new_message, routing_key=routing_key)

        if is_last == 1:
            end_message, _ = build_message(client_id, type_of_message, is_last, [])
            item_categorizer_fanout_exchange.send(end_message)
            print("[filter_by_year] Sent END message to categorizer_q2 via fanout exchange.")
    except Exception as e:
        print(f"[t_items] Error decoding message: {e}", file=sys.stderr)

def consume_queue_transactions(queue, callback, hour_filter_queue, categorizer_q4_topic_exchange, categorizer_q4_fanout_exchange):
    print("[filter_by_year] Starting to consume transactions queue...")
    def wrapper(message):
        callback(message, hour_filter_queue, categorizer_q4_topic_exchange, categorizer_q4_fanout_exchange)
    try:
        queue.start_consuming(wrapper)
    except MessageMiddlewareDisconnectedError:
        print("[filter_by_year] Disconnected from middleware (transactions).", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[filter_by_year] Message error in middleware (transactions).", file=sys.stderr)
    except Exception as e:
        print(f"[filter_by_year] Unexpected error in transactions consumer: {e}", file=sys.stderr)

def consume_queue_t_items(queue, callback, item_categorizer_exchange, item_categorizer_fanout_exchange):
    print("[filter_by_year] Starting to consume transaction_items queue...")
    def wrapper(message):
        callback(message, item_categorizer_exchange, item_categorizer_fanout_exchange)
    try:
        queue.start_consuming(wrapper)
    except MessageMiddlewareDisconnectedError:
        print("[filter_by_year] Disconnected from middleware (transaction_items).", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[filter_by_year] Message error in middleware (transaction_items).", file=sys.stderr)
    except Exception as e:
        print(f"[filter_by_year] Unexpected error in transaction_items consumer: {e}", file=sys.stderr)

def main():
    print("[filter_by_year] Worker starting...")
    sleep(config.MIDDLEWARE_UP_TIME)  
    print(f"[filter_by_year] Connecting to RabbitMQ at {RABBITMQ_HOST}, queues: {QUEUE_T}, {QUEUE_T_ITEMS}, filter years: {FILTER_YEARS}")
    
    try:
        queue_t = MessageMiddlewareQueue(RABBITMQ_HOST, QUEUE_T)
        print(f"[filter_by_year] Connected to transactions queue: {QUEUE_T}")
        
        queue_t_items = MessageMiddlewareQueue(RABBITMQ_HOST, QUEUE_T_ITEMS)
        print(f"[filter_by_year] Connected to transaction_items queue: {QUEUE_T_ITEMS}")
        
        hour_filter_queue = MessageMiddlewareQueue(RABBITMQ_HOST, HOUR_FILTER_QUEUE)
        print(f"[filter_by_year] Connected to hour filter queue: {HOUR_FILTER_QUEUE}")

        item_categorizer_exchange = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=TOPIC_EXCHANGE,
            exchange_type='topic',
            queue_name=ITEM_CATEGORIZER_QUEUE,
        )
        print(f"[filter_by_year] Connected to item categorizer topic exchange: {TOPIC_EXCHANGE}")
        
        FANOUT_EXCHANGE = os.environ.get('FANOUT_EXCHANGE', 'categorizer_q2_fanout_exchange')
        item_categorizer_fanout_exchange = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=FANOUT_EXCHANGE,
            exchange_type='fanout',
            queue_name=ITEM_CATEGORIZER_QUEUE,  
        )
        print(f"[filter_by_year] Connected to item categorizer fanout exchange: {FANOUT_EXCHANGE}")
        
        categorizer_q4_topic_exchange = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=CATEGORIZER_Q4_TOPIC_EXCHANGE,
            exchange_type='topic',
            queue_name='', 
        )
        print(f"[filter_by_year] Connected to categorizer_q4 topic exchange: {CATEGORIZER_Q4_TOPIC_EXCHANGE}")
        
        # categorizer_q4_fanout_exchange = MessageMiddlewareExchange(
        #     host=RABBITMQ_HOST,
        #     exchange_name=CATEGORIZER_Q4_FANOUT_EXCHANGE,
        #     exchange_type='fanout',
        #     queue_name='', 
        # )
        # print(f"[filter_by_year] Connected to categorizer_q4 fanout exchange: {CATEGORIZER_Q4_FANOUT_EXCHANGE}")
        
    except Exception as e:
        print(f"[filter_by_year] Error connecting to RabbitMQ: {e}", file=sys.stderr)
        return

    print("[filter_by_year] Starting consumer threads...")
    categorizer_q4_fanout_exchange=""
    categorizer_q4_topic_exchange =""
    t1 = threading.Thread(target=consume_queue_transactions, args=(queue_t, on_message_callback_transactions, hour_filter_queue, categorizer_q4_topic_exchange, categorizer_q4_fanout_exchange))
    t2 = threading.Thread(
        target=consume_queue_t_items,
        args=(queue_t_items, on_message_callback_t_items, item_categorizer_exchange, item_categorizer_fanout_exchange)
    )
    
    print("[filter_by_year] Starting transactions thread...")
    t1.start()
    print("[filter_by_year] Starting transaction_items thread...")
    t2.start()
    
    print("[filter_by_year] All threads started, waiting for messages...")
    try:
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        print("[filter_by_year] Stopping...")
        queue_t.stop_consuming()
        queue_t_items.stop_consuming()
        queue_t.close()
        queue_t_items.close()
        hour_filter_queue.close()

if __name__ == "__main__":
    main()
