from socket import socket, AF_INET, SOCK_STREAM
import os
import signal
from common.protocol_utils import *
from client.client_protocol import *
import logging
import common.response_types as response_types
from client.display import DisplayTable

SERVER_HOST = "gateway"  # local test, cambiar a "gateway" en docker
SERVER_PORT = 5000
CHUNK_TARGET = 4096

class Client:
    def __init__(self):
        self.skt = None
        self.current_file = None
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        signal.signal(signal.SIGINT, self._sigterm_handler)

    def _sigterm_handler(self, signum, _):
        if self.skt:
            self.skt.close()
            self.skt = None
        if self.current_file:
            self.current_file.close()
            self.current_file = None
        logging.info('closing file descriptors and shutdown [sigterm]')
    
    def _send_file(self, file_path, last_file, last_dataset):
        filename = os.path.basename(file_path)
        filesize = os.path.getsize(file_path)

        self.current_file = open(file_path, "rb")
        header_line = self.current_file.readline() # Read and skip header

        send_h_str(self.skt, filename)
        send_long(self.skt, filesize - len(header_line))
        send_bool(self.skt, last_file)
        send_bool(self.skt, last_dataset)

        buffer = b""
        for line in self.current_file:            
            if len(buffer) + len(line) > CHUNK_TARGET and buffer:
                send_h_bytes(self.skt, buffer)
                buffer = b""
            buffer += line
        if buffer:
            send_h_bytes(self.skt, buffer)
            
        self.current_file.close()
        logging.info(f"Archivo {filename} enviado ({filesize} bytes).")

    def recv_query_result(self, query, display, max_results=float('inf')):
        is_last = False
        print(f"\n{query} Results:\n", flush=True)
        display.display_header()
        while not is_last:
            data_type, lines_batch, is_last = recv_lines_batch(self.skt)

            for line in lines_batch:
                if max_results > 0:
                    display.display_row(line.strip().split(','))
                    max_results -= 1

    def recv_q2_results(self):
        is_last = False
        print(f"Q2 Results:\n", flush=True)
        # 2025,1,5,Flat White,154512,8,Matcha Latte,3071810.0
        # year,month,top_count_item_id,top_count_item_name,top_count,top_sum_item_id,top_sum_item_name,top_sum
        top_selling_results = {}
        top_profit_results = {}

        while not is_last:
            data_type, lines_batch, is_last = recv_lines_batch(self.skt)
            for line in lines_batch:
                fields = line.strip().split(',')
                year = int(fields[0])
                month = int(fields[1])

                if year not in top_selling_results:
                    top_selling_results[year] = []
                if year not in top_profit_results:
                    top_profit_results[year] = []

                top_count_item_name = fields[3]
                top_count = float(fields[4])
                top_selling_results[year].append((month, top_count_item_name, top_count))

                top_sum_item_name = fields[6]
                top_sum = float(fields[7])
                top_profit_results[year].append((month, top_sum_item_name, top_sum))
        
        print("- Top Selling Items:", flush=True)
        top_sellings_display = DisplayTable([("Year-Month", 10), ("Item name", 10), ("Sold count", 10)])
        top_sellings_display.display_header()
        for year in sorted(top_selling_results.keys()):
            month, top_count_item_name, top_count = sorted(top_selling_results[year], key=lambda x: x[2], reverse=True)[0]
            top_sellings_display.display_row([f"{year}-{month}", top_count_item_name, top_count])
        
        print("- Top Profit Items:", flush=True)
        top_profit_display = DisplayTable([("Year-Month", 10), ("Item name", 10), ("Profit sum", 10)])
        top_profit_display.display_header()
        for year in sorted(top_profit_results.keys()):
            month, top_count_item_name, top_sum = sorted(top_profit_results[year], key=lambda x: x[2], reverse=True)[0]
            top_profit_display.display_row([f"{year}-{month}", top_count_item_name, top_count])

    def get_q1_display(self) -> DisplayTable:
        return DisplayTable([("Transaction ID", 16), ("Store ID", 10), ("PM ID", 10), ("Voucher ID", 10), ("User ID", 10), ("Original #", 10), ("Discount", 10), ("Final #", 10), ("Created at", 19)])

    def get_q3_display(self) -> DisplayTable:
        return DisplayTable([("Year", 10), ("Month", 10), ("Store ID", 10), ("TPV", 15)])

    def get_q4_display(self) -> DisplayTable:
        return DisplayTable([("Store ID", 10), ("User ID", 10), ("Purchase #", 10), ("Birthday", 19)])

    def run(self, data_sets, q4_dataset):        
        try:
            self.skt = socket(AF_INET, SOCK_STREAM)
            self.skt.connect((SERVER_HOST, SERVER_PORT))
            logging.info(f"Conectado a {SERVER_HOST}:{SERVER_PORT}")
            for data_set_files in data_sets[:-1]:
                for file in data_set_files[:-1]:
                    self._send_file(file, False, False)
                self._send_file(data_set_files[-1], True, False)

            for file in data_sets[-1][:-1]:
                self._send_file(file, False, True)
            self._send_file(data_sets[-1][-1], True, True)

            logging.info("Todos los archivos fueron enviados correctamente.")

            # Now we expect to receive the q4 signal to send the extra data 
            if q4_dataset:
                logging.info("Esperando recibir request de data para la q4.")
                type, _ = recv_response(self.skt)
                if type != response_types.Q4_STEP:
                    logging.error(f"Tipo de respuesta inesperada: {type}")
                    return
                logging.info("Request para q4 recibido, enviando data.")
                # Send extra data for Q4
                for file in q4_dataset[:-1]:
                    self._send_file(file, False, True)
                self._send_file(q4_dataset[-1], True, True)
                logging.info("La data para la q4 fue enviada correctamente.")

            print("Esperando resultados de queries...", flush=True)
                
            # Wait for results
            self.recv_query_result("Q1", self.get_q1_display(), 5)
            self.recv_q2_results()
            self.recv_query_result("Q3", self.get_q3_display(), 5)

            if q4_dataset:
                self.recv_query_result("Q4", self.get_q4_display())
                pass

            print("Los resultados fueron recibidos correctamete.", flush=True)
        except OSError as e:
            logging.error(f"Error: {e}")
        finally:
            if self.skt:
                self.skt.close()
            if self.current_file:
                self.current_file.close()
