from socket import *

def recv_all(skt, n):
    """
    Receive exactly `n` bytes from the socket `skt`.

    If the peer closes the connection or an error occurs before `size` bytes are received, 
    a ConnectionError is raised.

    Avoids short reads.
    """
    data = b''
    while len(data) < n:
        pkt = skt.recv(n - len(data))
        if not pkt:
            raise ConnectionError("Socket connection closed")
        data += pkt
    return data

def send_all(skt, data):
    """
    Send all bytes in `data` to the socket `skt`.

    If the peer closes the connection or an error occurs before all data is sent, 
    a ConnectionError is raised.

    Avoids short writes.
    """
    total_sent = 0
    while total_sent < len(data):
        sent = skt.send(data[total_sent:])
        if sent == 0:
            raise ConnectionError("Socket connection closed")
        total_sent += sent