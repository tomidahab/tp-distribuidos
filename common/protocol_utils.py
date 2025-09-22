from common.socket_utils import *
import ctypes

INT_SIZE = 4
LONG_SIZE = 8
FLOAT_SIZE = 8
BOOL_SIZE = 1

def send_int(skt, value):
    """
    Sends an **int** `value` through `skt`.
    """
    send_all(skt, int(value).to_bytes(INT_SIZE, byteorder='big', signed=False))

def recv_int(skt):
    """
    Receives an **int** value from `skt`.
    """
    data = recv_all(skt, INT_SIZE)
    return int.from_bytes(data, byteorder='big', signed=False)

def send_long(skt, value):
    """
    Sends an **int long** `value` through `skt`.
    """
    send_all(skt, int(value).to_bytes(LONG_SIZE, byteorder='big', signed=False))

def recv_long(skt):
    """
    Receives an **int long** value from `skt`.
    """
    data = recv_all(skt, LONG_SIZE)
    return int.from_bytes(data, byteorder='big', signed=False)

def send_float(skt, value):
    """
    Sends a **float** `value` through `skt`.
    """
    c_double = ctypes.c_double(value)
    data = bytearray(ctypes.string_at(ctypes.byref(c_double), FLOAT_SIZE))
    send_all(skt, data)

def recv_float(skt):
    """
    Receives a **float** value from `skt`.
    """
    data = recv_all(skt, FLOAT_SIZE)
    return ctypes.c_double.from_buffer_copy(data).value

def send_bool(skt, value):
    """
    Sends a **bool** `value` through `skt`.
    """
    data = bytes([1 if value else 0])
    send_all(skt, data)

def recv_bool(skt):
    """
    Receives a **bool** value from `skt`.
    """
    data = recv_all(skt, BOOL_SIZE)
    return bool(data[0])

def send_h_bytes(skt, buffer):
    """
    Sends a bytes `buffer` through `skt`, preceded by a header indicating its size.
    """
    send_int(skt, len(buffer))
    send_all(skt, buffer)

def recv_h_bytes(skt):
    """
    Receives a bytes buffer from `skt`, preceded by a header indicating its size.
    """
    length = recv_int(skt)
    data = recv_all(skt, length)
    return data

def send_h_str(skt, value):
    """
    Sends a string `value` through `skt`, preceded by a header indicating its length.
    """
    send_int(skt, len(value))
    data = value.encode('utf-8')
    send_all(skt, data)

def recv_h_str(skt):
    """
    Receives a string value from `skt`, preceded by a header indicating its length.
    """
    length = recv_int(skt)
    data = recv_all(skt, length)
    return data.decode('utf-8')

def send_str(skt, value):
    """
    Sends a string `value` through `skt`.
    """
    data = value.encode('utf-8')
    send_all(skt, data)

def recv_str(skt, length):
    """
    Receives a string value of certain `length` through `skt`.
    """
    data = recv_all(skt, length)
    return data.decode('utf-8')