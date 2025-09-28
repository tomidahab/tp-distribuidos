import os
import sys
from collections import defaultdict
import time
from datetime import datetime

# Debug imports
try:
    from common.protocol import parse_message, row_to_dict, build_message, CSV_TYPES_REVERSE
    from common.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError
except ImportError as e:
    print(f"[categorizer_q2] IMPORT ERROR: {e}", flush=True)
    sys.exit(1)
except Exception as e:
    print(f"[categorizer_q2] UNEXPECTED IMPORT ERROR: {e}", flush=True)
    sys.exit(1)

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
ITEMS_QUEUE = os.environ.get('ITEMS_QUEUE', 'categorizer_q2_items_queue')
RECEIVER_QUEUE = os.environ.get('RECEIVER_QUEUE', 'categorizer_q2_receiver_queue')
GATEWAY_QUEUE = os.environ.get('GATEWAY_QUEUE', 'categorizer_q2_gateway_queue')

def listen_for_items():
    items = []
    try:
        queue = MessageMiddlewareQueue(RABBITMQ_HOST, ITEMS_QUEUE)
    except Exception as e:
        print(f"[categorizer_q2] Failed to connect to RabbitMQ: {e}", file=sys.stderr)
        return items

    def on_message_callback(message: bytes):
        try:
            # Naza: Acá va el parseo del mensaje para los items de la Q2 (Lo del mensaje de end es un ejemplo nomas)
            parsed_message = parse_message(message)
            type_of_message = parsed_message['csv_type']  
            client_id = parsed_message['client_id']
            is_last = parsed_message['is_last']
            for row in parsed_message['rows']:
                dic_fields_row = row_to_dict(row, type_of_message)
                items.append({'item_id': dic_fields_row['item_id'], 'item_name': dic_fields_row['item_name']})

            if is_last:
                print("[categorizer_q2] Received end message, stopping item collection.")
                print("[categorizer_q2] items = ", items)
                queue.stop_consuming()
        except Exception as e:
            print(f"[categorizer_q2] Error processing item message: {e}", file=sys.stderr)

    try:
        # Agregar timeout para evitar quedarse colgado indefinidamente
        print("[categorizer_q2] Starting to consume messages...")
        queue.start_consuming(on_message_callback)
    except MessageMiddlewareDisconnectedError:
        print("[categorizer_q2] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[categorizer_q2] Message error in middleware.", file=sys.stderr)
    except Exception as e:
        print(f"[categorizer_q2] Unexpected error while consuming: {e}", file=sys.stderr)
    finally:
        queue.close()
    return items

def listen_for_sales(items):
    # Dictionary: {(item_id, month): {'count': int, 'sum': float}}
    sales_stats = defaultdict(lambda: {'count': 0, 'sum': 0.0})
    try:
        print(f"[categorizer_q2] Attempting to connect to RabbitMQ at {RABBITMQ_HOST}")
        queue = MessageMiddlewareQueue(RABBITMQ_HOST, RECEIVER_QUEUE)
        print(f"[categorizer_q2] Connected successfully. Listening for sales on queue: {RECEIVER_QUEUE}")
    except Exception as e:
        print(f"[categorizer_q2] Failed to connect to RabbitMQ: {e}", file=sys.stderr)
        return sales_stats

    def on_message_callback(message: bytes):
        try:
            # Naza: Acá va el parseo del mensaje para las transacciones de la Q2 (Incluido el mensaje de finalización de la transmisión)
            parsed_message = parse_message(message)
            type_of_message = parsed_message['csv_type']  
            client_id = parsed_message['client_id']
            is_last = parsed_message['is_last']
            # print(f"[categorizer_q2] Received transaction message, csv_type: {type_of_message}, is_last: {is_last}")  # Too verbose
            
            for row in parsed_message['rows']:
                dic_fields_row = row_to_dict(row, type_of_message)
                # print(f"[categorizer_q2] Processing row: {dic_fields_row}")  # Too verbose for large files
                item_id = dic_fields_row['item_id']
                created_at = dic_fields_row['created_at']
                datetime_obj = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                month = datetime_obj.month
                price = float(dic_fields_row['subtotal'])
                if item_id in [item['item_id'] for item in items]:
                    key = (item_id, month)
                    sales_stats[key]['count'] += 1
                    sales_stats[key]['sum'] += price
                    # print(f"[categorizer_q2] Updated stats for {key}: {sales_stats[key]}")  # Too verbose
            if is_last:
                print("[categorizer_q2] Received end message, stopping sales collection.")
                queue.stop_consuming()
        except Exception as e:
            print(f"[categorizer_q2] Error processing sales message: {e}", file=sys.stderr)

    try:
        print("[categorizer_q2] Starting to consume sales messages...")
        queue.start_consuming(on_message_callback)
    except MessageMiddlewareDisconnectedError:
        print("[categorizer_q2] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[categorizer_q2] Message error in middleware.", file=sys.stderr)
    except Exception as e:
        print(f"[categorizer_q2] Unexpected error while consuming sales: {e}", file=sys.stderr)
    finally:
        queue.close()
    return sales_stats

def get_top_products_per_month(sales_stats, items):
    
    id_to_name = {item['item_id']: item['item_name'] for item in items}
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
        # TODO: Implementar encode_result_message en common.protocol
        message = build_message(result, 'result', 1, False)  # CHANGE THIS!!!
        queue.send(message)
        print(f"[categorizer_q2] Sent result for month {month} to gateway: {result}")
    queue.close()

def main():
    try:
        print("[categorizer_q2] Waiting for RabbitMQ to be ready...")
        time.sleep(30)  # Esperar a que RabbitMQ esté listo
        print("[categorizer_q2] Starting worker...")
        
        items = listen_for_items()
        print(f"[categorizer_q2] Collected {len(items)} items: {items}")
        
        if not items:
            print("[categorizer_q2] No items received, exiting.")
            return
            
        sales_stats = listen_for_sales(items)
        print("[categorizer_q2] Final sales stats:")
        for (item_id, month), stats in sales_stats.items():
            print(f"Item: {item_id}, Month: {month}, Count: {stats['count']}, Sum: {stats['sum']}")
            
        if not sales_stats:
            print("[categorizer_q2] No sales data received, exiting.")
            return
            
        top_by_count, top_by_sum = get_top_products_per_month(sales_stats, items)
        send_results_to_gateway(top_by_count, top_by_sum)
        print("[categorizer_q2] Worker completed successfully.")
        
    except Exception as e:
        print(f"[categorizer_q2] Error in main: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()