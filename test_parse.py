#!/usr/bin/env python3

import struct

def parse_message_new(message: bytes):
    """Test new format parsing"""
    print(f"Message length: {len(message)}")
    print(f"Message hex: {message[:50].hex()}...")
    
    # Read client_id_len (2 bytes)
    client_id_len = struct.unpack(">H", message[:2])[0]
    print(f"client_id_len: {client_id_len}")
    
    # Read client_id (client_id_len bytes)
    client_id_bytes = message[2:2+client_id_len]
    client_id = client_id_bytes.decode("utf-8")
    print(f"client_id: {client_id}")
    
    # Read csv_type, is_last
    offset = 2 + client_id_len
    csv_type, is_last = struct.unpack(">BB", message[offset:offset+2])
    print(f"csv_type: {csv_type}, is_last: {is_last}")
    offset += 2
    
    # NEW FORMAT: always expect 32-byte sender field
    SENDER_SIZE = 32
    if len(message) >= offset + SENDER_SIZE + 4:  # Check if enough bytes
        sender_bytes = message[offset:offset+SENDER_SIZE]
        sender_bytes = sender_bytes.rstrip(b'\x00')  # Remove null padding
        sender = sender_bytes.decode("utf-8") if sender_bytes else None
        offset += SENDER_SIZE
        
        # Read payload_len (4 bytes)
        payload_len = struct.unpack(">I", message[offset:offset+4])[0]
        offset += 4
        
        print(f"NEW FORMAT: sender='{sender}', payload_len={payload_len}")
    else:
        print("NOT ENOUGH BYTES for new format - this is OLD FORMAT")
        # This is old format
        payload_len = struct.unpack(">I", message[offset:offset+4])[0]
        offset += 4
        sender = None
        print(f"OLD FORMAT: payload_len={payload_len}")
    
    # Read payload
    payload = message[offset:offset+payload_len]
    payload_text = payload.decode("utf-8")
    rows = payload_text.split("\n") if payload_text else []
    
    return {
        "client_id": client_id,
        "csv_type": csv_type,
        "is_last": is_last,
        "sender": sender,
        "rows": rows
    }

if __name__ == "__main__":
    from common.protocol import build_message
    
    print("=== Testing message formats ===")
    
    # Test old format (build_message with sender=None should still create new format)
    print("\n1. Testing build_message with sender=None:")
    rows = ["row1,data1", "row2,data2"]
    msg_old, _ = build_message("client_1", 3, 0, rows, sender=None)
    result_old = parse_message_new(msg_old)
    print(f"Result: {result_old}")
    
    print("\n2. Testing build_message with sender:")
    msg_new, _ = build_message("client_1", 3, 0, rows, sender="filter_by_hour_0")
    result_new = parse_message_new(msg_new)
    print(f"Result: {result_new}")