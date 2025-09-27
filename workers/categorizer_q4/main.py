import os
import sys
from collections import defaultdict, Counter
from common.protocol import protocol
from common.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
RECEIVER_QUEUE = os.environ.get('RECEIVER_QUEUE', 'store_user_categorizer_queue')
BIRTHDAY_DICT_QUEUE = os.environ.get('BIRTHDAY_DICT_QUEUE', 'birthday_dictionary_queue')

def listen_for_transactions():

    store_user_counter = defaultdict(Counter)
    queue = MessageMiddlewareQueue(RABBITMQ_HOST, RECEIVER_QUEUE)
    print(f"[categorizer_q4] Listening for transactions on queue: {RECEIVER_QUEUE}")

    def on_message_callback(message: bytes):

        # Usar el protocolo para desencodear el mensaje
        message_decoded = protocol.decode_transaction_message(message)

        store_id = message_decoded.get('store_id')
        user_id = message_decoded.get('user_id')

        if None not in (store_id, user_id):
            store_user_counter[store_id][user_id] += 1
        # Naza: Hay que definir el mensaje de fin
        if message_decoded.get('type') == 'end':
            print("[categorizer_q4] Received end message, stopping transaction collection.")
            queue.stop_consuming()

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
    for store_id, users in top_users.items():
        result = {
            'store_id': store_id,
            'top_users': [{'user_id': user_id, 'purchase_count': count} for user_id, count in users]
        }
        # Naza: Usar el protocolo para encodear el mensaje
        message = protocol.encode_result_message(result)
        queue.send(message)
        print(f"[categorizer_q4] Sent result to Birthday_Dictionary: {result}")
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