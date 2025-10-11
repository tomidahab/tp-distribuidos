import struct
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

def build_message(client_id, csv_type: int, is_last: int, rows: List[str]) -> Tuple[bytes, int]:
    """
    Arma un mensaje binario con el protocolo definido.
    Header: 2 bytes client_id_len, client_id bytes, 1 byte csv_type, 1 byte is_last, 4 bytes payload_len
    Payload: rows en texto plano UTF-8 separados por \n, encodeado a bytes
    """
    assert 1 <= csv_type <= 5, "csv_type debe estar entre 1 y 5"
    assert is_last in (0, 1), "is_last debe ser 0 o 1"
    
    # Convert client_id to string if it's not already
    client_id_str = str(client_id)
    client_id_bytes = client_id_str.encode("utf-8")
    client_id_len = len(client_id_bytes)
    
    payload_text = "\n".join(rows)
    payload_bytes = payload_text.encode("utf-8")
    payload_len = len(payload_bytes)
    
    # Header: 2 bytes client_id_len + client_id + 1 byte csv_type + 1 byte is_last + 4 bytes payload_len
    header = struct.pack(">H", client_id_len) + client_id_bytes + struct.pack(">BBI", csv_type, is_last, payload_len)
    message = header + payload_bytes
    return message, len(message)

def message_to_text(message: bytes) -> str:
    """
    Convierte el mensaje binario a texto plano (solo payload).
    """
    payload_len = struct.unpack(">I", message[6:10])[0]
    payload = message[10:10+payload_len]
    return payload.decode("utf-8")

def text_to_message(text: str, client_id, csv_type: int, is_last: int) -> Tuple[bytes, int]:
    """
    Convierte texto plano (rows separados por \n) a mensaje binario.
    """
    rows = text.strip().split("\n")
    return build_message(client_id, csv_type, is_last, rows)

def parse_message(message: bytes) -> Dict:
    """
    Parsea el mensaje binario y devuelve un dict con los campos y las rows.
    """
    # Read client_id_len (2 bytes)
    client_id_len = struct.unpack(">H", message[:2])[0]
    
    # Read client_id (client_id_len bytes)
    client_id_bytes = message[2:2+client_id_len]
    client_id = client_id_bytes.decode("utf-8")
    
    # Read csv_type, is_last, payload_len
    offset = 2 + client_id_len
    csv_type, is_last, payload_len = struct.unpack(">BBI", message[offset:offset+6])
    
    # Read payload
    payload_offset = offset + 6
    payload = message[payload_offset:payload_offset+payload_len]
    payload_text = payload.decode("utf-8")
    rows = payload_text.split("\n") if payload_text else []
    
    return {
        "client_id": client_id,
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

def get_csv_type(message: bytes) -> int:
    """Get csv_type from message header"""
    client_id_len = struct.unpack(">H", message[:2])[0]
    offset = 2 + client_id_len
    return struct.unpack(">B", message[offset:offset+1])[0]

def get_is_last(message: bytes) -> int:
    """Get is_last from message header"""
    client_id_len = struct.unpack(">H", message[:2])[0]
    offset = 2 + client_id_len + 1
    return struct.unpack(">B", message[offset:offset+1])[0]

def get_rows(message: bytes) -> List[str]:
    """Get rows from message payload"""
    client_id_len = struct.unpack(">H", message[:2])[0]
    offset = 2 + client_id_len
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