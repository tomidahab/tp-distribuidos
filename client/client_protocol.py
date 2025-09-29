from socket import socket
from common.protocol_utils import *

# def send_lines_batch(skt: socket, data_type, lines_batch, is_last):
#     send_int(skt, data_type)
#     send_int(skt, len(lines_batch))
#     send_bool(skt, is_last)
#     for line in lines_batch:
#         send_h_str(skt, line)

def recv_lines_batch(skt: socket):
    data_type = recv_int(skt)
    length = recv_int(skt)
    is_last = recv_bool(skt)
    lines_batch = []
    for _ in range(0, length):
        lines_batch.append(recv_h_str(skt))
    return data_type, lines_batch, is_last

def recv_response(skt: socket):
    type = recv_int(skt)
    response_type = recv_h_str(skt)
    return type, response_type