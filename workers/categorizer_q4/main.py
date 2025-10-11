import os
import signal
import sys
from collections import defaultdict, Counter
import time
from common.protocol import build_message, parse_message, row_to_dict
from common.middleware import MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
RECEIVER_QUEUE = os.environ.get('RECEIVER_QUEUE', 'store_user_categorizer_queue')
TOPIC_EXCHANGE = os.environ.get('TOPIC_EXCHANGE', 'categorizer_q4_topic_exchange')
FANOUT_EXCHANGE = os.environ.get('FANOUT_EXCHANGE', 'categorizer_q4_fanout_exchange')
WORKER_INDEX = int(os.environ.get('WORKER_INDEX', 0))
BIRTHDAY_DICT_QUEUE = os.environ.get('BIRTHDAY_DICT_QUEUE', 'birthday_dictionary_queue')
AMOUNT_OF_WORKERS = int(os.environ.get('AMOUNT_OF_WORKERS', 1))
NUMBER_OF_YEAR_WORKERS = int(os.environ.get('NUMBER_OF_YEAR_WORKERS', '3'))

receiver_queue = None
birthday_dict_queue = None

def _close_queue(queue):
    if queue:
        queue.close()

def _sigterm_handler(signum, _):
    _close_queue(receiver_queue)
    _close_queue(birthday_dict_queue)

def listen_for_transactions():
    # Per-client store-user tracking: {client_id: {store_id: Counter(user_id: count)}}
    client_store_user_counter = defaultdict(lambda: defaultdict(Counter))
    # Track END messages per client: {client_id: count}
    client_end_messages = defaultdict(int)
    completed_clients = set()

    topic_routing_key = f"store.{WORKER_INDEX}"
    print(f"[categorizer_q4] Worker index: {WORKER_INDEX}, routing key: {topic_routing_key}")
    # Bind to both topic and fanout exchanges
    global receiver_queue
    receiver_queue = MessageMiddlewareExchange(
        host=RABBITMQ_HOST,
        exchange_name=TOPIC_EXCHANGE,
        exchange_type='topic',
        queue_name=RECEIVER_QUEUE,
        routing_keys=[topic_routing_key]
    )
    fanout_queue = MessageMiddlewareExchange(
        host=RABBITMQ_HOST,
        exchange_name=FANOUT_EXCHANGE,
        exchange_type='fanout',
        queue_name=RECEIVER_QUEUE
    )

    print(f"[categorizer_q4] Listening for transactions on queue: {RECEIVER_QUEUE} (topic key: {topic_routing_key})")

    def on_message_callback(message: bytes):
        nonlocal completed_clients
        try:
            parsed_message = parse_message(message)
            type_of_message = parsed_message['csv_type']
            client_id = parsed_message['client_id']
            is_last = parsed_message['is_last']
            
            # Skip if client already completed
            if client_id in completed_clients:
                return
                
            # print(f"[categorizer_q4] Worker {WORKER_INDEX} processing message for client {client_id}, is_last={is_last}, rows: {len(parsed_message['rows'])}")
            
            for row in parsed_message['rows']:
                dic_fields_row = row_to_dict(row, type_of_message)
                store_id = dic_fields_row.get('store_id')
                user_id = dic_fields_row.get('user_id')
                if None not in (store_id, user_id) and store_id != '' and user_id != '':
                    client_store_user_counter[client_id][store_id][int(float(user_id))] += 1
            
            if is_last:
                client_end_messages[client_id] += 1
                print(f"[categorizer_q4] Worker {WORKER_INDEX} received END message {client_end_messages[client_id]}/{NUMBER_OF_YEAR_WORKERS} for client {client_id}")
                
                if client_end_messages[client_id] >= NUMBER_OF_YEAR_WORKERS:
                    print(f"[categorizer_q4] Client {client_id} received all END messages, processing results")
                    completed_clients.add(client_id)
                    
                    # Process and send results for this client
                    send_client_q4_results(client_id, client_store_user_counter[client_id])
                    
                    # Don't delete client data yet - keep it for potential debugging
                    # but mark as completed
                    print(f"[categorizer_q4] Client {client_id} processing completed")
                        
        except Exception as e:
            print(f"[categorizer_q4] Error processing message: {e}", file=sys.stderr)

    try:
        receiver_queue.start_consuming(on_message_callback)
    except MessageMiddlewareDisconnectedError:
        print("[categorizer_q4] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[categorizer_q4] Message error in middleware.", file=sys.stderr)
    finally:
        receiver_queue.close()
        # fanout_queue.close()
    return client_store_user_counter

def get_top_users_per_store(store_user_counter, top_n=3):
    # Returns {store_id: [(user_id, purchase_count), ...]}
    top_users = {}
    for store_id, user_counter in store_user_counter.items():
        top_users[store_id] = user_counter.most_common(top_n)
    return top_users

def send_client_q4_results(client_id, store_user_counter):
    """Send Q4 results for specific client to birthday_dictionary"""
    try:
        global birthday_dict_queue
        if birthday_dict_queue is None:
            birthday_dict_queue = MessageMiddlewareQueue(RABBITMQ_HOST, BIRTHDAY_DICT_QUEUE)
        
        # Get top users per store for this client
        top_users = get_top_users_per_store(store_user_counter, top_n=3)
        
        # Send one message per store with its top 3 users
        for store_id, users in top_users.items():
            rows = []
            for user_id, count in users:
                rows.append(f"{store_id},{user_id},{count}")
            # Send with client_id
            message, _ = build_message(client_id, 4, 0, rows)
            birthday_dict_queue.send(message)
            print(f"[categorizer_q4] Worker {WORKER_INDEX} sent top users for store {store_id} (client {client_id}) to Birthday_Dictionary: {rows}")
        
        # Send END message for this client
        end_message, _ = build_message(client_id, 4, 1, [])
        birthday_dict_queue.send(end_message)
        print(f"[categorizer_q4] Worker {WORKER_INDEX} sent END message for client {client_id} to Birthday_Dictionary")
        
    except Exception as e:
        print(f"[categorizer_q4] ERROR sending results for client {client_id}: {e}", file=sys.stderr)
        # Don't raise to avoid stopping other clients

def send_results_to_birthday_dict(top_users):
    global birthday_dict_queue
    birthday_dict_queue = MessageMiddlewareQueue(RABBITMQ_HOST, BIRTHDAY_DICT_QUEUE)
    # Send one message per store with its top 3 users
    for store_id, users in top_users.items():
        rows = []
        for user_id, count in users:
            rows.append(f"{store_id},{user_id},{count}")
        # is_last=0 for regular messages
        message, _ = build_message(0, 4, 0, rows)
        birthday_dict_queue.send(message)
        print(f"[categorizer_q4] Sent top users for store {store_id} to Birthday_Dictionary: {rows}")
    # After all stores, send the end message
    end_message, _ = build_message(0, 4, 1, [])
    birthday_dict_queue.send(end_message)
    print("[categorizer_q4] Sent END message to Birthday_Dictionary.")
    birthday_dict_queue.close()

def main():
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)
    time.sleep(30)
    client_store_user_counter = listen_for_transactions()
    print("[categorizer_q4] All clients processed successfully.")

if __name__ == "__main__":
    main()