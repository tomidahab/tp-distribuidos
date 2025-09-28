import struct
from typing import List, Tuple, Dict

CSV_TYPES = {
    1: "menu_items",
    2: "transaction_items",
    3: "transactions",
    4: "users"
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
        4: ["user_id", "gender", "birthdate", "registered_at"]
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

def build_message(client_id: int, csv_type: int, is_last: int, rows: List[str]) -> Tuple[bytes, int]:
    """
    Arma un mensaje binario con el protocolo definido.
    Header: 4 bytes client_id, 1 byte csv_type, 1 byte is_last
    Payload: rows en texto plano UTF-8 separados por \n, encodeado a bytes
    """
    assert 1 <= csv_type <= 4, "csv_type debe estar entre 1 y 4"
    assert is_last in (0, 1), "is_last debe ser 0 o 1"
    payload_text = "\n".join(rows)
    payload_bytes = payload_text.encode("utf-8")
    # Encode payload length as 4 bytes (big endian)
    payload_len = len(payload_bytes)
    header = struct.pack(">IBBI", client_id, csv_type, is_last, payload_len)
    message = header + payload_bytes
    return message, len(message)

def message_to_text(message: bytes) -> str:
    """
    Convierte el mensaje binario a texto plano (solo payload).
    """
    payload_len = struct.unpack(">I", message[6:10])[0]
    payload = message[10:10+payload_len]
    return payload.decode("utf-8")

def text_to_message(text: str, client_id: int, csv_type: int, is_last: int) -> Tuple[bytes, int]:
    """
    Convierte texto plano (rows separados por \n) a mensaje binario.
    """
    rows = text.strip().split("\n")
    return build_message(client_id, csv_type, is_last, rows)

def parse_message(message: bytes) -> Dict:
    """
    Parsea el mensaje binario y devuelve un dict con los campos y las rows.
    """
    client_id, csv_type, is_last, payload_len = struct.unpack(">IBBI", message[:10])
    payload = message[10:10+payload_len]
    payload_text = payload.decode("utf-8")
    rows = payload_text.split("\n") if payload_text else []
    return {
        "client_id": client_id,
        "csv_type": csv_type,
        "csv_type_name": CSV_TYPES.get(csv_type, "unknown"),
        "is_last": is_last,
        "rows": rows
    }

def get_client_id(message: bytes) -> int:
    return struct.unpack(">I", message[:4])[0]

def get_csv_type(message: bytes) -> int:
    return struct.unpack(">B", message[4:5])[0]

def get_is_last(message: bytes) -> int:
    return struct.unpack(">B", message[5:6])[0]

def get_rows(message: bytes) -> List[str]:
    payload_len = struct.unpack(">I", message[6:10])[0]
    payload = message[10:10+payload_len]
    return payload.decode("utf-8").split("\n") if payload else []


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