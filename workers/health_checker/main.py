import os
import signal
import subprocess
import sys
from collections import defaultdict
import time
import threading
from socket import socket, AF_INET, SOCK_STREAM
from queue import Queue # Thread safe queue
from common.protocol_utils import *

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')

WORKER_INDEX = int(os.environ.get('WORKER_INDEX', 0))
AMOUNT_OF_WORKERS = int(os.environ.get('AMOUNT_OF_WORKERS', 1))

HEALTH_ADDRESS_TARGETS = os.environ.get('HEALTH_ADDRESS_TARGETS', f'').split(",")

def cprint(*args, **kwargs):
    print("[health_checker]", *args, flush=True, **kwargs)

def _sigterm_handler(signum, _):
    pass

class HealthCheckReceiver(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.skt.bind(('', 2000))
        self.skt.listen(1)

    def handle_checker(self, checker_skt):
        cprint("Health checker connected, handling messages")        
        while True:
            alive_msg = recv_int(checker_skt)
            send_int(checker_skt, 1)
    
    def run(self):            
        cprint("Start handling health connections")
        while True:
            try:
                c, addr = self.skt.accept()
                self.handle_checker(c)
            except ConnectionError:
                cprint(f"Checker connection lost, reconnection expected...")

class HealthCheckerHandler(threading.Thread):
    def __init__(self, addr):
        self.host, self.port = addr
        self.skt = None
        super().__init__(daemon=True)

    def send_health_checks(self):
        while True:
            time.sleep(1)
            send_int(self.skt, 1)
            cprint(f"Alive message sent to {self.host}:{self.port}")
            recv_int(self.skt)
            cprint(f"Alive response ok from {self.host}:{self.port}")            
            
    def run(self):
        while True:
            try:
                time.sleep(2) # Wait some time to let the node be ready
                self.skt = socket(AF_INET, SOCK_STREAM)
                self.skt.connect((self.host, self.port))
                cprint(f"Connected to {self.host}:{self.port}")

                self.send_health_checks()
            except ConnectionError:
                cprint(f"Disconnected from {self.host}:{self.port}, booting up the node...")
                
                containder_name = self.host
                # Stop the process in case for some reason still active
                result = subprocess.run(['docker', 'stop', containder_name], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                cprint('Command executed. Result={}. Output={}. Error={}'.format(result.returncode, result.stdout, result.stderr))
                # Start the process again               
                result = subprocess.run(['docker', 'start', containder_name], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                cprint('Command executed. Result={}. Output={}. Error={}'.format(result.returncode, result.stdout, result.stderr))
                cprint(f"The node has been booted up")

def main():
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)

    checkers = []
    for addr in HEALTH_ADDRESS_TARGETS:
        host, port = addr.split(":")
        checkers.append(HealthCheckerHandler((host, int(port))))

    for c in checkers:
        c.join() # This will block the flow since this worker is not suppose to end
    
if __name__ == "__main__":
    main()