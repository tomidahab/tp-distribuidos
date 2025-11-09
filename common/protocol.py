import struct
import uuid
from typing import List, Tuple, Dict

CSV_TYPES = {
    1: "menu_items",
    2: "transaction_items",
    3: "transactions",
    4: "users",
    5: "client_request"  # Added type 5 for birthday dictionary client requests
}

CSV_TYPES_REVERSE = {v: k for k, v in CSV_TYPES.items()}

def row_to_dict(row: str, csv_type: int) -> Dict:
    """
    Convierte un row (string) y el tipo de CSV en un diccionario con los campos correspondientes.
    """
    HEADERS = {
        1: ["item_id", "item_name", "category", "price", "is_seasonal", "available_from", "available_to"],
        2: ["transaction_id", "item_id", "quantity", "unit_price", "subtotal", "created_at"],
        3: ["transaction_id", "store_id", "payment_method_id", "voucher_id", "user_id", "original_amount", "discount_applied", "final_amount", "created_at"],
        4: ["user_id", "gender", "birthdate", "registered_at"],
        5: ["client_id"]  # For client request, adjust as needed
    }
    fields = row.split(",")
    header = HEADERS.get(csv_type)
    if not header:
        raise ValueError(f"csv_type {csv_type} no reconocido")
    # Si hay menos fields que headers, completa con None
    fields += [None] * (len(header) - len(fields))
    return dict(zip(header, fields))

def create_response_message(response_query: int, response: str) -> bytes:
    """
    Crea un mensaje de respuesta con el protocolo definido.
    Header: 1 byte response_type, 4 bytes response_length
    Payload: response en texto plano UTF-8
    """
    payload_bytes = response.encode("utf-8")
    payload_len = len(payload_bytes)
    header = struct.pack(">BI", response_query, payload_len)
    message = header + payload_bytes
    return message

def unpack_response_message(message: bytes) -> Tuple[int, str]:
    """
    Desempaqueta un mensaje de respuesta y devuelve el response_type y el response en texto plano.
    """
    response_type, payload_len = struct.unpack(">BI", message[:5])
    payload = message[5:5+payload_len]
    response = payload.decode("utf-8")
    return response_type, response

def build_message(client_id, csv_type: int, is_last: int, rows: List[str], message_id: str = None, sender: str = None) -> Tuple[bytes, int]:
    """
    Arma un mensaje binario con el protocolo definido.
    Header: 2 bytes client_id_len, client_id bytes, 1 byte message_id_len, message_id bytes, 1 byte sender_len, sender bytes, 1 byte csv_type, 1 byte is_last, 4 bytes payload_len
    Payload: rows en texto plano UTF-8 separados por \n, encodeado a bytes
    """
    assert 1 <= csv_type <= 5, "csv_type debe estar entre 1 y 5"
    assert is_last in (0, 1), "is_last debe ser 0 o 1"
    
    # Generate message_id if not provided
    if message_id is None:
        message_id = str(uuid.uuid4())
    
    # Default sender if not provided
    if sender is None:
        sender = "unknown"
    
    # Convert client_id to string if it's not already
    client_id_str = str(client_id)
    client_id_bytes = client_id_str.encode("utf-8")
    client_id_len = len(client_id_bytes)
    
    # Convert message_id to bytes
    message_id_bytes = message_id.encode("utf-8")
    message_id_len = len(message_id_bytes)
    
    # Convert sender to bytes
    sender_bytes = sender.encode("utf-8")
    sender_len = len(sender_bytes)
    
    payload_text = "\n".join(rows)
    payload_bytes = payload_text.encode("utf-8")
    payload_len = len(payload_bytes)
    
    # Header: 2 bytes client_id_len + client_id + 1 byte message_id_len + message_id + 1 byte sender_len + sender + 1 byte csv_type + 1 byte is_last + 4 bytes payload_len
    header = (struct.pack(">H", client_id_len) + client_id_bytes + 
              struct.pack(">B", message_id_len) + message_id_bytes +
              struct.pack(">B", sender_len) + sender_bytes +
              struct.pack(">BBI", csv_type, is_last, payload_len))
    message = header + payload_bytes
    return message, len(message)

def message_to_text(message: bytes) -> str:
    """
    Convierte el mensaje binario a texto plano (solo payload).
    """
    # Read client_id_len and client_id
    client_id_len = struct.unpack(">H", message[:2])[0]
    offset = 2 + client_id_len
    
    # Read message_id_len and message_id
    message_id_len = struct.unpack(">B", message[offset:offset+1])[0]
    offset += 1 + message_id_len
    
    # Read sender_len and sender
    sender_len = struct.unpack(">B", message[offset:offset+1])[0]
    offset += 1 + sender_len
    
    # Read payload_len
    payload_len = struct.unpack(">I", message[offset+2:offset+6])[0]
    payload_offset = offset + 6
    payload = message[payload_offset:payload_offset+payload_len]
    return payload.decode("utf-8")

def text_to_message(text: str, client_id, csv_type: int, is_last: int, message_id: str = None, sender: str = None) -> Tuple[bytes, int]:
    """
    Convierte texto plano (rows separados por \n) a mensaje binario.
    """
    rows = text.strip().split("\n")
    return build_message(client_id, csv_type, is_last, rows, message_id, sender)

def parse_message(message: bytes) -> Dict:
    """
    Parsea el mensaje binario y devuelve un dict con los campos y las rows.
    Compatible con protocolo viejo (sin sender) y nuevo (con sender).
    """
    # Read client_id_len (2 bytes)
    client_id_len = struct.unpack(">H", message[:2])[0]
    
    # Read client_id (client_id_len bytes)
    client_id_bytes = message[2:2+client_id_len]
    client_id = client_id_bytes.decode("utf-8")
    
    # Read message_id_len (1 byte)
    offset = 2 + client_id_len
    message_id_len = struct.unpack(">B", message[offset:offset+1])[0]
    
    # Read message_id (message_id_len bytes)
    offset += 1
    message_id_bytes = message[offset:offset+message_id_len]
    message_id = message_id_bytes.decode("utf-8")
    
    # Try to determine if this message has sender field (new protocol) or not (old protocol)
    offset += message_id_len
    
    # We'll try to read what should be sender_len, but if the value is too high (>127)
    # it's probably csv_type from old protocol
    potential_sender_len = struct.unpack(">B", message[offset:offset+1])[0]
    
    if potential_sender_len <= 127 and offset + 1 + potential_sender_len < len(message):
        # This looks like a valid sender_len, try new protocol
        try:
            # Read sender (sender_len bytes)
            offset += 1
            sender_bytes = message[offset:offset+potential_sender_len]
            sender = sender_bytes.decode("utf-8")
            
            # Read csv_type, is_last, payload_len
            offset += potential_sender_len
            csv_type, is_last, payload_len = struct.unpack(">BBI", message[offset:offset+6])
            
            # Validate that this makes sense
            payload_offset = offset + 6
            if payload_offset + payload_len <= len(message):
                # This looks valid - new protocol
                payload = message[payload_offset:payload_offset+payload_len]
                payload_text = payload.decode("utf-8")
                rows = payload_text.split("\n") if payload_text else []
                
                return {
                    "client_id": client_id,
                    "message_id": message_id,
                    "sender": sender,
                    "csv_type": csv_type,
                    "csv_type_name": CSV_TYPES.get(csv_type, "unknown"),
                    "is_last": is_last,
                    "rows": rows
                }
        except (struct.error, UnicodeDecodeError):
            pass  # Fall through to old protocol
    
    # Fall back to old protocol (no sender field)
    # potential_sender_len is actually csv_type
    csv_type = potential_sender_len
    is_last, payload_len = struct.unpack(">BI", message[offset+1:offset+6])
    
    # Read payload
    payload_offset = offset + 6
    payload = message[payload_offset:payload_offset+payload_len]
    payload_text = payload.decode("utf-8")
    rows = payload_text.split("\n") if payload_text else []
    
    return {
        "client_id": client_id,
        "message_id": message_id,
        "sender": "unknown",  # Default for old protocol
        "csv_type": csv_type,
        "csv_type_name": CSV_TYPES.get(csv_type, "unknown"),
        "is_last": is_last,
        "rows": rows
    }

def get_client_id(message: bytes) -> str:
    """Get client_id from message header"""
    client_id_len = struct.unpack(">H", message[:2])[0]
    client_id_bytes = message[2:2+client_id_len]
    return client_id_bytes.decode("utf-8")

def get_message_id(message: bytes) -> str:
    """Get message_id from message header"""
    client_id_len = struct.unpack(">H", message[:2])[0]
    offset = 2 + client_id_len
    message_id_len = struct.unpack(">B", message[offset:offset+1])[0]
    offset += 1
    message_id_bytes = message[offset:offset+message_id_len]
    return message_id_bytes.decode("utf-8")

def get_sender(message: bytes) -> str:
    """Get sender from message header"""
    client_id_len = struct.unpack(">H", message[:2])[0]
    offset = 2 + client_id_len
    message_id_len = struct.unpack(">B", message[offset:offset+1])[0]
    offset += 1 + message_id_len
    sender_len = struct.unpack(">B", message[offset:offset+1])[0]
    offset += 1
    sender_bytes = message[offset:offset+sender_len]
    return sender_bytes.decode("utf-8")

def get_csv_type(message: bytes) -> int:
    """Get csv_type from message header"""
    client_id_len = struct.unpack(">H", message[:2])[0]
    offset = 2 + client_id_len
    message_id_len = struct.unpack(">B", message[offset:offset+1])[0]
    offset += 1 + message_id_len
    sender_len = struct.unpack(">B", message[offset:offset+1])[0]
    offset += 1 + sender_len
    return struct.unpack(">B", message[offset:offset+1])[0]

def get_is_last(message: bytes) -> int:
    """Get is_last from message header"""
    client_id_len = struct.unpack(">H", message[:2])[0]
    offset = 2 + client_id_len
    message_id_len = struct.unpack(">B", message[offset:offset+1])[0]
    offset += 1 + message_id_len
    sender_len = struct.unpack(">B", message[offset:offset+1])[0]
    offset += 1 + sender_len + 1
    return struct.unpack(">B", message[offset:offset+1])[0]

def get_rows(message: bytes) -> List[str]:
    """Get rows from message payload"""
    client_id_len = struct.unpack(">H", message[:2])[0]
    offset = 2 + client_id_len
    message_id_len = struct.unpack(">B", message[offset:offset+1])[0]
    offset += 1 + message_id_len
    sender_len = struct.unpack(">B", message[offset:offset+1])[0]
    offset += 1 + sender_len
    csv_type, is_last, payload_len = struct.unpack(">BBI", message[offset:offset+6])
    payload_offset = offset + 6
    payload = message[payload_offset:payload_offset+payload_len]
    payload_text = payload.decode("utf-8")
    return payload_text.split("\n") if payload_text else []


# Ejemplo de uso
"""
def main():
    rows = ["1,female,1970-04-22,2023-07-01 08:13:07", "2,female,1991-12-08,2023-07-01 09:53:48"]
    msg, length = build_message(1234, 4, 1, rows)
    print(msg)
    parsed = parse_message(msg)
    print(parsed)

    print(row_to_dict(rows[0],4))

main()
"""