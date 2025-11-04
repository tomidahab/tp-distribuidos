import os
import signal
import sys
from collections import defaultdict
import time
import threading
from queue import Queue # Thread safe queue

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
WORKER_INDEX = int(os.environ.get('WORKER_INDEX', 0))
AMOUNT_OF_WORKERS = int(os.environ.get('AMOUNT_OF_WORKERS', 1))


class LeaderHealthChecker(threading.Thread):
    """
    Check the leader health by messages
    """
    def __init__(self):
        super().__init__(daemon=True)

    def run(self):
        # TODO: 
        # Loop
        # Wait Delay
        # Connect to leader by socket udp
        # Send alive message
        # Wait response
        # > If Timeout, Start election
        pass

class SystemHealthChecker(threading.Thread):
    """
    Check all nodes health of the system by messages
    """
    def __init__(self):
        super().__init__(daemon=True)

    def run(self):
        # TODO: 
        # Loop
        # Wait Delay
        # Connect to all nodes by socket udp
        # Send alive messages
        # Wait responses
        # > If Timeout in node, Reactive it (Docker)
        pass

def _sigterm_handler(signum, _):
    pass

def main():
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)
    # time.sleep(30)

if __name__ == "__main__":
    main()