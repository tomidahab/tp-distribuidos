import threading
from socket import AF_INET, SOCK_STREAM, socket
from common.protocol_utils import recv_int, send_int

class HealthCheckReceiver(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.skt = socket(AF_INET, SOCK_STREAM)
        self.skt.bind(('', 2000))
        self.skt.listen(1)

    def handle_checker(self, checker_skt):
        ## print("Health checker connected, handling messages", flush=True)        
        while True:
            alive_msg = recv_int(checker_skt)
            send_int(checker_skt, 1)
    
    def run(self):            
        ## print("Start handling health connections", flush=True)
        while True:
            try:
                c, addr = self.skt.accept()
                self.handle_checker(c)
            except ConnectionError:
                print(f"Checker connection lost, reconnection expected...", flush=True)