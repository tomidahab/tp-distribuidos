from socket import socket
from common.protocol_utils import *
from common.data import *

def send_transaction_batch(skt: socket, t_batch, is_last):
    send_int(skt, len(t_batch))
    send_bool(skt, is_last)
    for t in t_batch:
        _send_transaction(skt, t)

def _send_transaction(skt: socket, t: Transaction):
    send_str(skt, t.transaction_id)
    send_int(skt, t.store_id)
    send_int(skt, t.payment_method_id)
    send_int(skt, t.voucher_id)
    send_int(skt, t.user_id)
    send_float(skt, t.original_amount)
    send_float(skt, t.discount_applied)
    send_float(skt, t.final_amount)
    send_str(skt, t.created_at)

def send_transaction_items_batch(skt: socket, t_batch, is_last):
    send_int(skt, len(t_batch))
    send_bool(skt, is_last)
    for t in t_batch:
        _send_transaction_item(skt, t)

def _send_transaction_item(skt: socket, t: TransactionItem):
    send_str(skt, t.transaction_id)
    send_int(skt, t.item_id)
    send_int(skt, t.quantity)
    send_float(skt, t.unit_price)
    send_float(skt, t.subtotal)
    send_str(skt, t.created_at)
    