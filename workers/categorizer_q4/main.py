import os
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

def listen_for_transactions():
    store_user_counter = defaultdict(Counter)
    end_messages_received = 0

    topic_routing_key = f"store.{WORKER_INDEX}"
    print(f"[categorizer_q4] Worker index: {WORKER_INDEX}, routing key: {topic_routing_key}")
    # Bind to both topic and fanout exchanges
    queue = MessageMiddlewareExchange(
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
        nonlocal end_messages_received
        parsed_message = parse_message(message)
        type_of_message = parsed_message['csv_type']
        is_last = parsed_message['is_last']
        print(f"[categorizer_q4] Received message with {len(parsed_message['rows'])} rows, is_last={is_last}")
        for row in parsed_message['rows']:
            dic_fields_row = row_to_dict(row, type_of_message)
            store_id = dic_fields_row.get('store_id')
            user_id = dic_fields_row.get('user_id')
            if None not in (store_id, user_id) and store_id != '' and user_id != '':
                store_user_counter[store_id][int(float(user_id))] += 1
        
        if is_last:
            end_messages_received += 1
            print(f"[categorizer_q4] Received END message {end_messages_received}/{NUMBER_OF_YEAR_WORKERS} from filter_by_year workers")
            if end_messages_received >= NUMBER_OF_YEAR_WORKERS:
                print(f"[categorizer_q4] Received all END messages from {NUMBER_OF_YEAR_WORKERS} filter_by_year workers, stopping transaction collection.")
                queue.stop_consuming()

    try:
        queue.start_consuming(on_message_callback)
    except MessageMiddlewareDisconnectedError:
        print("[categorizer_q4] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[categorizer_q4] Message error in middleware.", file=sys.stderr)
    finally:
        queue.close()
        # fanout_queue.close()
    return store_user_counter

def get_top_users_per_store(store_user_counter, top_n=3):
    # Returns {store_id: [(user_id, purchase_count), ...]}
    top_users = {}
    for store_id, user_counter in store_user_counter.items():
        top_users[store_id] = user_counter.most_common(top_n)
    return top_users

def send_results_to_birthday_dict(top_users):
    queue = MessageMiddlewareQueue(RABBITMQ_HOST, BIRTHDAY_DICT_QUEUE)
    # Send one message per store with its top 3 users
    for store_id, users in top_users.items():
        rows = []
        for user_id, count in users:
            rows.append(f"{store_id},{user_id},{count}")
        # is_last=0 for regular messages
        message, _ = build_message(0, 4, 0, rows)
        queue.send(message)
        print(f"[categorizer_q4] Sent top users for store {store_id} to Birthday_Dictionary: {rows}")
    # After all stores, send the end message
    end_message, _ = build_message(0, 4, 1, [])
    queue.send(end_message)
    print("[categorizer_q4] Sent END message to Birthday_Dictionary.")
    queue.close()

def main():
    time.sleep(30)
    store_user_counter = listen_for_transactions()
    print("[categorizer_q4] Final store-user stats:")
    top_users = get_top_users_per_store(store_user_counter, top_n=3)
    send_results_to_birthday_dict(top_users)
    print("[categorizer_q4] Worker completed successfully.")

if __name__ == "__main__":
    main()