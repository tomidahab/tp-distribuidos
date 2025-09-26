import os
import sys
from collections import defaultdict
from common.protocol import protocol
from common.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
RECEIVER_QUEUE = os.environ.get('RECEIVER_QUEUE', 'store_semester_categorizer_queue')
GATEWAY_QUEUE = os.environ.get('GATEWAY_QUEUE', 'categorizer_q3_gateway_queue')

def get_semester(month):
    return 1 if 1 <= month <= 6 else 2

def listen_for_transactions():
    # Agregación: {(year, semester, store_id): total_payment}
    semester_store_stats = defaultdict(float)
    queue = MessageMiddlewareQueue(RABBITMQ_HOST, RECEIVER_QUEUE)
    print(f"[categorizer_q3] Listening for transactions on queue: {RECEIVER_QUEUE}")

    def on_message_callback(message: bytes):
        message_decoded = protocol.decode_result_message(message)
        for row in message_decoded.get('rows', []):
            # Naza: Hay que parsear el mensaje para obtener year, month, store_id, payment_value
            year = message_decoded.get('year')
            month = message_decoded.get('month')
            store_id = message_decoded.get('store_id')
            payment_value = message_decoded.get('payment_value')
            if None not in (year, month, store_id, payment_value):
                semester = get_semester(month)
                key = (year, semester, store_id)
                semester_store_stats[key] += payment_value
        # Naza: Hay que definir el mensaje de fin
        if message_decoded.get('type') == 'end':
            print("[categorizer_q3] Received end message, stopping transaction collection.")
            queue.stop_consuming()

    try:
        queue.start_consuming(on_message_callback)
    except MessageMiddlewareDisconnectedError:
        print("[categorizer_q3] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[categorizer_q3] Message error in middleware.", file=sys.stderr)
    finally:
        queue.close()
    return semester_store_stats

def send_results_to_gateway(semester_store_stats):
    queue = MessageMiddlewareQueue(RABBITMQ_HOST, GATEWAY_QUEUE)
    for (year, semester, store_id), total_payment in semester_store_stats.items():
        result = {
            'year': year,
            'semester': semester,
            'store_id': store_id,
            'total_payment': total_payment
        }
        message = protocol.encode_result_message(result)  # encodear con el protocolo
        queue.send(message)
        # Naza: Esto lo hice para que mande de a uno cada combinacion de semestre y sucursal, no se si lo quieren cambiar a que lo mande todo de una
        print(f"[categorizer_q3] Sent result to gateway: {result}")
    queue.close()

def main():
    semester_store_stats = listen_for_transactions()
    print("[categorizer_q3] Final semester-store stats:")
    for key, total in semester_store_stats.items():
        year, semester, store_id = key
        print(f"Year: {year}, Semester: {semester}, Store: {store_id}, Total Payment: {total}")
    send_results_to_gateway(semester_store_stats)

if __name__ == "__main__":
    main()