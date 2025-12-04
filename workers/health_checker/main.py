import os
import signal
import subprocess
import time
import threading
from socket import socket, AF_INET, SOCK_STREAM
from common.health_check_receiver import HealthCheckReceiver
from common.protocol_utils import *

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')

WORKER_INDEX = int(os.environ.get('WORKER_INDEX', 0))
AMOUNT_OF_WORKERS = int(os.environ.get('AMOUNT_OF_WORKERS', 1))

HEALTH_ADDRESS_TARGETS = os.environ.get('HEALTH_ADDRESS_TARGETS', f'').split(",")

def cprint(*args, **kwargs):
    print("[health_checker]", *args, flush=True, **kwargs)

def _sigterm_handler(signum, _):
    pass

class HealthCheckerHandler(threading.Thread):
    def __init__(self, addr):
        super().__init__(daemon=True)
        self.host, self.port = addr
        self.skt = None

    def send_health_checks(self):
        while True:
            time.sleep(1)
            send_int(self.skt, 1)
            ## cprint(f"Alive message sent to {self.host}:{self.port}")
            recv_int(self.skt)
            ## cprint(f"Alive response ok from {self.host}:{self.port}")            
            
    def run(self):
        while True:
            try:
                time.sleep(2) # Wait some time to let the node be ready
                self.skt = socket(AF_INET, SOCK_STREAM)
                self.skt.connect((self.host, self.port))
                ## cprint(f"Connected to {self.host}:{self.port}")

                self.send_health_checks()
            except Exception:  # Catch any connection error
                cprint(f"Cannot connect to {self.host}:{self.port}, starting container...")
                
                containder_name = self.host
                # Start the container
                result = subprocess.run(['docker', 'start', containder_name], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                cprint(f'Container start result: {result.returncode}')
                time.sleep(10)  # Wait for container to start

def main():
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)
    cprint(f"Booting Health checker node {WORKER_INDEX}")
    checkers = []
    for addr in HEALTH_ADDRESS_TARGETS:
        host, port = addr.split(":")
        checker = HealthCheckerHandler((host, int(port)))
        checker.start()
        checkers.append(checker)

    health_check_receiver = HealthCheckReceiver()
    health_check_receiver.start()

    # This will block the flow since this worker is not suppose to end
    signal.pause()
    
if __name__ == "__main__":
    main()