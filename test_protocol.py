#!/usr/bin/env python3

from common.protocol import build_message, parse_message

def test_protocol():
    print("=== Testing Protocol ===")
    
    # Test old format (no sender)
    print("\n1. Testing OLD format (no sender):")
    rows = ["row1", "row2", "row3"]
    msg_old, len_old = build_message("client_1", 3, 0, rows, sender=None)
    print(f"Old message length: {len_old}")
    print(f"Old message hex: {msg_old[:50].hex()}...")
    
    try:
        parsed_old = parse_message(msg_old)
        print(f"Old parsed: client_id={parsed_old['client_id']}, csv_type={parsed_old['csv_type']}, is_last={parsed_old['is_last']}, sender={parsed_old.get('sender')}, rows={len(parsed_old['rows'])}")
    except Exception as e:
        print(f"ERROR parsing old format: {e}")
        import traceback
        traceback.print_exc()
    
    # Test new format (with sender)
    print("\n2. Testing NEW format (with sender):")
    msg_new, len_new = build_message("client_1", 3, 0, rows, sender="filter_by_hour_0")
    print(f"New message length: {len_new}")
    print(f"New message hex: {msg_new[:50].hex()}...")
    
    try:
        parsed_new = parse_message(msg_new)
        print(f"New parsed: client_id={parsed_new['client_id']}, csv_type={parsed_new['csv_type']}, is_last={parsed_new['is_last']}, sender={parsed_new.get('sender')}, rows={len(parsed_new['rows'])}")
    except Exception as e:
        print(f"ERROR parsing new format: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_protocol()