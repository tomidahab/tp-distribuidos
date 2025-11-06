import os
import signal
import sys
from collections import defaultdict
import time
import threading
import socket
from queue import Queue # Thread safe queue
import struct

from common.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareExchange

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
WORKER_INDEX = int(os.environ.get('WORKER_INDEX', 0))
AMOUNT_OF_WORKERS = int(os.environ.get('AMOUNT_OF_WORKERS', 1))

LEADER_COM_EXCHANGE = os.environ.get('LEADER_COM_EXCHANGE', 'leader_com_exchange')
LEADER_COM_QUEUE = os.environ.get('LEADER_COM_QUEUE', f'leader_com_queue_{WORKER_INDEX}')

CLUSTER_RECEIVER_EXCHANGE = os.environ.get('CLUSTER_RECEIVER_EXCHANGE', 'cluster_receciver_exchange')
CLUSTER_RECEIVER_QUEUE = os.environ.get('CLUSTER_RECEIVER_QUEUE', f'cluster_receciver_queue_{WORKER_INDEX}')

MSG_ELECTION = 0
MSG_LEADER_ACK = 1
MSG_NODE_KEEP_ALIVE = 2
MSG_NEW_LEADER = 3
MSG_FIND_LEADER = 4
MSG_LEADER_FOUND = 5

election_participant_cond = threading.Condition()
participant = False
leader_id = 0

leader_res_cond = threading.Condition()
leader_alive_response = False

leader_def_cond = threading.Condition()

leader_t = None

ELECION_RESPONSE_TIME_OUT = 1

# TODO: All ring sendings with attempts and timeouts to try to send to the subnext node in case of no response  

def cprint(*args, **kwargs):
    print("[health_checker]", *args, flush=True, **kwargs)

def decode_message(data):
    msg_id, from_id, value = struct.unpack("!III", data)
    return msg_id, from_id, value

def encode_message(msg_id, value):
    data = struct.pack("!III", msg_id, WORKER_INDEX, value)
    return data

def is_leader_defined():
    return leader_id != -1

def get_queue():
    queue = MessageMiddlewareExchange(
        host=RABBITMQ_HOST,
        exchange_name=CLUSTER_RECEIVER_EXCHANGE,
        exchange_type='topic',
        queue_name=CLUSTER_RECEIVER_QUEUE,
        routing_keys=[f"health_checker.{WORKER_INDEX}"]
    )
    return queue
        
class ClusterReceiver(threading.Thread):
    """
    Election process
    """
    def __init__(self):
        super().__init__(daemon=True)

    def run(self):
        # TODO: 
        # Receive messages and send it to next in cluster 

        receiver_queue = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=CLUSTER_RECEIVER_EXCHANGE,
            exchange_type='topic',
            queue_name=CLUSTER_RECEIVER_QUEUE,
            # queue_name="", # Empty since we're only sending
            routing_keys=[f"health_checker.{WORKER_INDEX}"]
        )
        cprint(f"Listening cluster messages on queue: {CLUSTER_RECEIVER_QUEUE}")


        def on_message_callback(data: bytes):
            global leader_id, participant
            
            # Process the message
            msg_id, from_id, value = decode_message(data)
            # client_id = parsed_message['client_id']
            if msg_id == MSG_ELECTION: # election
                # Send election message to next node
                if value == WORKER_INDEX:
                    # So this node is the new leader
                    receiver_queue.send(encode_message(MSG_NEW_LEADER, WORKER_INDEX), routing_key=f"client.{(WORKER_INDEX + 1) % AMOUNT_OF_WORKERS}")                    
                else:
                    receiver_queue.send(encode_message(MSG_ELECTION, max(value, WORKER_INDEX)), routing_key=f"client.{(WORKER_INDEX + 1)  % AMOUNT_OF_WORKERS}")
                pass
            elif msg_id == MSG_LEADER_ACK: # leader_ack
                # Received ack ok
                global leader_alive_response
                with leader_res_cond:
                    leader_alive_response = True
                    leader_res_cond.notify_all()   
                pass
            elif msg_id == MSG_NODE_KEEP_ALIVE: # node_keep_alive
                #
                receiver_queue.send(encode_message(MSG_LEADER_ACK, 0), routing_key=f"client.{from_id}")                                
                pass
            elif msg_id == MSG_NEW_LEADER: # elected_leader
                with election_participant_cond:
                    leader_id = value
                    participant = False
                    election_participant_cond.notify_all()
                # Propagation to next nodes (if the cycle is not end)
                if value == WORKER_INDEX:
                    pass
                else:
                    receiver_queue.send(encode_message(MSG_NEW_LEADER, value), routing_key=f"client.{(WORKER_INDEX + 1)  % AMOUNT_OF_WORKERS}")                                            
                pass
            elif msg_id == MSG_FIND_LEADER:
                # value = node who ask
                if WORKER_INDEX == leader_id:
                    receiver_queue.send(encode_message(MSG_LEADER_FOUND, WORKER_INDEX), routing_key=f"client.{value}")
                else:
                    if value == WORKER_INDEX:
                        # ring complete without leader found, start election
                        receiver_queue.send(encode_message(MSG_ELECTION, WORKER_INDEX), routing_key=f"client.{(WORKER_INDEX + 1)  % AMOUNT_OF_WORKERS}")
                        with election_participant_cond:
                            leader_id = -1
                            participant = True
                    else:
                        # pass to next
                        receiver_queue.send(encode_message(MSG_FIND_LEADER, value), routing_key=f"client.{(WORKER_INDEX + 1)  % AMOUNT_OF_WORKERS}")
                                                            
                pass
            elif msg_id == MSG_LEADER_FOUND:
                with election_participant_cond:
                    leader_id = value
                pass
        try:
            receiver_queue.start_consuming(on_message_callback)
        except MessageMiddlewareDisconnectedError:
            cprint("Disconnected from middleware.", file=sys.stderr)
        except MessageMiddlewareMessageError:
            cprint("Message error in middleware.", file=sys.stderr)
        finally:
            receiver_queue.close()

class LeaderHealthChecker(threading.Thread):
    """
    As node check the leader health by messages
    """
    def __init__(self):
        super().__init__(daemon=True)

    def run(self):
        
        with leader_def_cond:
            leader_def_cond.wait_for(lambda: is_leader_defined() and leader_id != WORKER_INDEX)
        
        sender_queue = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=CLUSTER_RECEIVER_EXCHANGE,
            exchange_type='topic',
            # queue_name=CLUSTER_RECEIVER_QUEUE,
            queue_name="", # Empty since we're only sending
            routing_keys=[f"health_checker.{WORKER_INDEX}"]
        )
        
        # TODO: 
        while True:
            time.sleep(1)                          

            # TODO: Send alive message
            sender_queue.send(encode_message(MSG_NODE_KEEP_ALIVE, 0), routing_key=f"client.{leader_id}")
  
            global leader_alive_response
            leader_alive_response = False
            with leader_res_cond:
                result = leader_res_cond.wait_for(lambda: leader_alive_response, timeout=ELECION_RESPONSE_TIME_OUT)

                if result:
                    # all ok
                    pass
                else:
                    # Wait response
                    # > If Timeout, Start election and wait for new leader      
                    sender_queue.send(encode_message(MSG_ELECTION, WORKER_INDEX), routing_key=f"client.{(WORKER_INDEX + 1)  % AMOUNT_OF_WORKERS}")
                    # Wait until get new leader
                    global leader_id, participant
                    with election_participant_cond:
                        leader_id = -1
                        participant = True
                        election_participant_cond.wait_for(lambda: not participant)
                        # At this point, the election has been done and a new leader is set
                    pass

class SystemHealthChecker(threading.Thread):
    """
    As Leader check all nodes health of the system by messages
    """
    def __init__(self):
        super().__init__(daemon=True)

    def run(self):
        with leader_def_cond:
            leader_def_cond.wait_for(lambda: is_leader_defined() and leader_id == WORKER_INDEX)
            
        cprint("This node is the leader, starting check system health")
        # TODO: 
        # Loop
        # Wait Delay
        # Connect to all nodes by socket udp or consider using rabbit
        # Send alive messages
        # Wait responses
        # > If Timeout in node, Reactive it (Docker)
        pass

def _sigterm_handler(signum, _):
    pass

def main():
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)
    
    cluster_com = ClusterReceiver()
    cluster_com.start()
    
    leader_health_check = LeaderHealthChecker()
    leader_health_check.start()
    
    system_checker = SystemHealthChecker()
    system_checker.start()
    
    # Find leader
    queue = get_queue()

    global leader_find_response, leader_id
    leader_found = False
    next_node = WORKER_INDEX
    while not leader_found:
        next_node = (next_node + 1)  % AMOUNT_OF_WORKERS
        if next_node == WORKER_INDEX:
            # I am the leader
            # TODO: Deterministic?
            with leader_def_cond:
                leader_id = WORKER_INDEX
            break
        queue.send(encode_message(MSG_FIND_LEADER, 0), routing_key=f"client.{next_node}")
        with leader_def_cond:
            leader_found = leader_def_cond.wait_for(lambda: is_leader_defined(), 3)
            next_node += 1
    
    cluster_com.join()
    leader_health_check.join()
    system_checker.join()
    
if __name__ == "__main__":
    main()