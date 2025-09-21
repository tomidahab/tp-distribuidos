from socket import socket
from common.protocol_utils import *

def recv_lines_batch(skt: socket):
    data_type = recv_int(skt)
    length = recv_int(skt)
    is_last = recv_bool(skt)
    lines_batch = []
    for _ in range(0, length):
        lines_batch.append(recv_h_str(skt))
    return data_type, lines_batch, is_last

def send_response(skt: socket, response_type, response = ""):
    send_int(skt, response_type)
    send_h_str(skt, response)
    return response_type, response