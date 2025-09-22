import os
import sys
from collections import defaultdict
from common.protocol import protocol
from common.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
ITEMS_QUEUE = os.environ.get('ITEMS_QUEUE', 'categorizer_q2_items_queue')
RECEIVER_QUEUE = os.environ.get('RECEIVER_QUEUE', 'categorizer_q2_receiver_queue')
GATEWAY_QUEUE = os.environ.get('GATEWAY_QUEUE', 'categorizer_q2_gateway_queue')

def listen_for_items():
    items = []
    queue = MessageMiddlewareQueue(RABBITMQ_HOST, ITEMS_QUEUE)
    print(f"[categorizer_q2] Listening for items on queue: {ITEMS_QUEUE}")

    def on_message_callback(message: bytes):
        # Naza: Acá va el parseo del mensaje para los items de la Q2 (Lo del mensaje de end es un ejemplo nomas)
        decoded = protocol.decode_item_message(message)
        if decoded.get('type') == 'end':
            print("[categorizer_q2] Received end message, stopping item collection.")
            queue.stop_consuming()
        else:
            items.append({'item_id': decoded['item_id'], 'name': decoded['name']})

    try:
        queue.start_consuming(on_message_callback)
    except MessageMiddlewareDisconnectedError:
        print("[categorizer_q2] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[categorizer_q2] Message error in middleware.", file=sys.stderr)
    finally:
        queue.close()
    return items

def listen_for_sales(items):
    # Dictionary: {(item_id, month): {'count': int, 'sum': float}}
    sales_stats = defaultdict(lambda: {'count': 0, 'sum': 0.0})
    queue = MessageMiddlewareQueue(RABBITMQ_HOST, RECEIVER_QUEUE)
    print(f"[categorizer_q2] Listening for sales on queue: {RECEIVER_QUEUE}")

    def on_message_callback(message: bytes):
        # Naza: Acá va el parseo del mensaje para las transacciones de la Q2 (Incluido el mensaje de finalización de la transmisión)
        decoded = protocol.decode_sale_message(message)
        item_id = decoded['item_id']
        month = decoded['month']
        price = float(decoded['price'])
        if item_id in [item['item_id'] for item in items]:
            key = (item_id, month)
            sales_stats[key]['count'] += 1
            sales_stats[key]['sum'] += price
            print(f"[categorizer_q2] Updated stats for {key}: {sales_stats[key]}")
        # el end esta como ejemplo nomas
        if decoded.get('type') == 'end':
            print("[categorizer_q2] Received end message, stopping sales collection.")
            queue.stop_consuming()

    try:
        queue.start_consuming(on_message_callback)
    except MessageMiddlewareDisconnectedError:
        print("[categorizer_q2] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[categorizer_q2] Message error in middleware.", file=sys.stderr)
    finally:
        queue.close()
    return sales_stats

def get_top_products_per_month(sales_stats, items):
    
    id_to_name = {item['item_id']: item['name'] for item in items}
    month_stats = defaultdict(list)
    for (item_id, month), stats in sales_stats.items():
        month_stats[month].append({'item_id': item_id, 'count': stats['count'], 'sum': stats['sum']})

    top_by_count = {}
    top_by_sum = {}
    for month, stats_list in month_stats.items():
        top_count = max(stats_list, key=lambda x: x['count'])
        top_sum = max(stats_list, key=lambda x: x['sum'])
        top_by_count[month] = {
            'item_id': top_count['item_id'],
            'name': id_to_name.get(top_count['item_id'], 'Unknown'),
            'count': top_count['count']
        }
        top_by_sum[month] = {
            'item_id': top_sum['item_id'],
            'name': id_to_name.get(top_sum['item_id'], 'Unknown'),
            'sum': top_sum['sum']
        }
    return top_by_count, top_by_sum

def send_results_to_gateway(top_by_count, top_by_sum):
    queue = MessageMiddlewareQueue(RABBITMQ_HOST, GATEWAY_QUEUE)
    for month in set(top_by_count.keys()).union(top_by_sum.keys()):
        result = {
            'month': month,
            'top_count_item': {
                'name': top_by_count[month]['name'],
                'item_id': top_by_count[month]['item_id'],
                'count': top_by_count[month]['count']
            },
            'top_sum_item': {
                'name': top_by_sum[month]['name'],
                'item_id': top_by_sum[month]['item_id'],
                'sum': top_by_sum[month]['sum']
            }
        }
        # Aca se usa el protocolo para codificar el mensaje de resultado
        message = protocol.encode_result_message(result)
        queue.send(message)
        print(f"[categorizer_q2] Sent result for month {month} to gateway: {result}")
    queue.close()

def main():
    items = listen_for_items()
    print(f"[categorizer_q2] Collected items: {items}")
    sales_stats = listen_for_sales(items)
    print("[categorizer_q2] Final sales stats:")
    for (item_id, month), stats in sales_stats.items():
        print(f"Item: {item_id}, Month: {month}, Count: {stats['count']}, Sum: {stats['sum']}")
    top_by_count, top_by_sum = get_top_products_per_month(sales_stats, items)
    send_results_to_gateway(top_by_count, top_by_sum)

if __name__ == "__main__":
    main()