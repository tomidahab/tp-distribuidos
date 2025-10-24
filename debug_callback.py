# Version del callback con debugging detallado
def on_message_callback_debug(message: bytes, topic_middleware, should_stop):
    global rows_received, rows_sent, client_end_messages, completed_clients, client_stats
    
    print(f"[filter_by_amount] Worker {WORKER_INDEX} DEBUG: Received message of {len(message)} bytes", flush=True)
    
    if should_stop.is_set():  # Don't process if we're stopping
        print(f"[filter_by_amount] Worker {WORKER_INDEX} DEBUG: should_stop is set, returning", flush=True)
        return
    
    try:
        print(f"[filter_by_amount] Worker {WORKER_INDEX} DEBUG: About to parse message", flush=True)
        parsed_message = parse_message(message)
        print(f"[filter_by_amount] Worker {WORKER_INDEX} DEBUG: Message parsed successfully", flush=True)
        
        type_of_message = parsed_message['csv_type']  
        client_id = parsed_message['client_id']
        is_last = parsed_message['is_last']
        sender = parsed_message.get('sender')  # Optional field
        
        print(f"[filter_by_amount] Worker {WORKER_INDEX} DEBUG: client_id={client_id}, sender={sender}, is_last={is_last}, rows={len(parsed_message['rows'])}", flush=True)
        
        # Update client stats
        client_stats[client_id]['messages_received'] += 1
        client_stats[client_id]['rows_received'] += len(parsed_message['rows'])
        
        # Count incoming rows
        incoming_rows = len(parsed_message['rows'])
        rows_received += incoming_rows
        
        print(f"[filter_by_amount] Worker {WORKER_INDEX} DEBUG: Updated stats, processing...", flush=True)
        
        # ... rest of processing would go here ...
        
        print(f"[filter_by_amount] Worker {WORKER_INDEX} DEBUG: Message processed successfully", flush=True)
        
    except Exception as e:
        print(f"[filter_by_amount] Worker {WORKER_INDEX} ERROR in callback: {e}", flush=True)
        import traceback
        traceback.print_exc()
        # Re-raise to trigger middleware error handling
        raise