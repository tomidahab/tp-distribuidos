from socket import socket
from common.protocol_utils import *

def send_lines_batch(skt: socket, data_type, lines_batch, is_last):
    send_int(skt, data_type)
    send_int(skt, len(lines_batch))
    send_bool(skt, is_last)
    for line in lines_batch:
        send_h_str(skt, line)

def recv_response(skt: socket):
    type = recv_int(skt)
    response_type = recv_h_str(skt)
    return type, response_type