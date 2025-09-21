from socket_utils import *
import ctypes

INT_SIZE = 4
FLOAT_SIZE = 8
BOOL_SIZE = 1

def send_int(skt, value):
    send_all(skt, int(value).to_bytes(INT_SIZE, byteorder='big', signed=False))

def recv_int(skt):
    data = recv_all(skt, INT_SIZE)
    return int.from_bytes(data, byteorder='big', signed=False)

def send_float(skt, value):
    c_double = ctypes.c_double(value)
    data = bytearray(ctypes.string_at(ctypes.byref(c_double), FLOAT_SIZE))
    send_all(skt, data)

def recv_float(skt):
    data = recv_all(skt, FLOAT_SIZE)
    return ctypes.c_double.from_buffer_copy(data).value

def send_bool(skt, value):
    data = bytes([1 if value else 0])
    send_all(skt, data)

def recv_bool(skt):
    data = recv_all(skt, BOOL_SIZE)
    return bool(data[0])

def send_h_str(skt, value):
    send_all(skt, int(len(value)).to_bytes(INT_SIZE, byteorder='big', signed=False))
    data = value.encode('utf-8')
    send_all(skt, data)

def recv_h_str(skt):
    len_bytes = recv_all(INT_SIZE)
    length = int.from_bytes(len_bytes, byteorder='big', signed=False)
    data = recv_all(skt, length)
    return data.decode('utf-8')

def send_str(skt, value):
    data = value.encode('utf-8')
    send_all(skt, data)

def recv_str(skt, length):
    data = recv_all(skt, length)
    return data.decode('utf-8')