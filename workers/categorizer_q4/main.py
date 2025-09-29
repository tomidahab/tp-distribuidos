import os
import sys
from collections import defaultdict, Counter
from common.protocol import protocol, build_message, parse_message
from common.middleware import MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
RECEIVER_QUEUE = os.environ.get('RECEIVER_QUEUE', 'store_user_categorizer_queue')
TOPIC_EXCHANGE = os.environ.get('TOPIC_EXCHANGE', 'categorizer_q4_topic_exchange')
FANOUT_EXCHANGE = os.environ.get('FANOUT_EXCHANGE', 'categorizer_q4_fanout_exchange')
WORKER_INDEX = int(os.environ.get('WORKER_INDEX', 0))
BIRTHDAY_DICT_QUEUE = os.environ.get('BIRTHDAY_DICT_QUEUE', 'birthday_dictionary_queue')
AMOUNT_OF_WORKERS = int(os.environ.get('AMOUNT_OF_WORKERS', 1))

def listen_for_transactions():
    store_user_counter = defaultdict(Counter)

    topic_routing_key = f"store.{WORKER_INDEX}"
    queue = MessageMiddlewareExchange(
        host=RABBITMQ_HOST,
        exchange_name=TOPIC_EXCHANGE,
        exchange_type='topic',
        queue_name=RECEIVER_QUEUE,
        routing_keys=[topic_routing_key]
    )
    _fanout_queue = MessageMiddlewareExchange(
        host=RABBITMQ_HOST,
        exchange_name=FANOUT_EXCHANGE,
        exchange_type='fanout',
        queue_name=RECEIVER_QUEUE
    )

    print(f"[categorizer_q4] Listening for transactions on queue: {RECEIVER_QUEUE} (topic key: {topic_routing_key})")

    def on_message_callback(message: bytes):
        message_decoded = protocol.decode_transaction_message(message)
        store_id = message_decoded.get('store_id')
        user_id = message_decoded.get('user_id')
        msg_type = message_decoded.get('type')

        if msg_type == 'end':
            print("[categorizer_q4] Received end message, stopping transaction collection.")
            queue.stop_consuming()
            return

        if None not in (store_id, user_id):
            store_user_counter[store_id][user_id] += 1

    try:
        queue.start_consuming(on_message_callback)
    except MessageMiddlewareDisconnectedError:
        print("[categorizer_q4] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[categorizer_q4] Message error in middleware.", file=sys.stderr)
    finally:
        queue.close()
    return store_user_counter

def get_top_users_per_store(store_user_counter, top_n=3):
    # Devuelve {store_id: [(user_id, purchase_count), ...]}
    top_users = {}
    for store_id, user_counter in store_user_counter.items():
        top_users[store_id] = user_counter.most_common(top_n)
    return top_users

def send_results_to_birthday_dict(top_users):
    queue = MessageMiddlewareQueue(RABBITMQ_HOST, BIRTHDAY_DICT_QUEUE)

    rows = []
    for store_id, users in top_users.items():
        for user_id, count in users:
            rows.append(f"{store_id},{user_id},{count}")
    message, _ = build_message(0, 4, 1, rows)  
    queue.send(message)
    print(f"[categorizer_q4] Sent result to Birthday_Dictionary: {rows}")
    queue.close()

def main():
    store_user_counter = listen_for_transactions()
    print("[categorizer_q4] Final store-user stats:")
    for store_id, user_counter in store_user_counter.items():
        print(f"Store: {store_id}, Users: {user_counter}")
    top_users = get_top_users_per_store(store_user_counter, top_n=3)
    send_results_to_birthday_dict(top_users)

if __name__ == "__main__":
    main()