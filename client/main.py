import os
import sys
import re
import logging
import time
from client.client import Client
from time import sleep
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.protocol_utils import *

# Configuración del logger
logging.basicConfig(
    level=logging.INFO,
    format="[CLIENT] %(asctime)s - %(levelname)s - %(message)s"
)

PATTERNS = {
    "transactions": re.compile(r"^transactions_\d{4}\d{2}\.csv$"),
    "transaction_items": re.compile(r"^transaction_items_\d{4}\d{2}\.csv$"),
    "users": re.compile(r"^users_\d{4}\d{2}\.csv$"),
    "menu_items": re.compile(r"^menu_items\.csv$")
}

def collect_files(folder):
    files = os.listdir(folder)
    menu_items = [f for f in files if PATTERNS["menu_items"].match(f)]
    transactions = sorted([f for f in files if PATTERNS["transactions"].match(f)])
    transaction_items = sorted([f for f in files if PATTERNS["transaction_items"].match(f)])
    users = sorted([f for f in files if PATTERNS["users"].match(f)])
    # ordered = menu_items + transactions + transaction_items + users
    # return [os.path.join(folder, f) for f in ordered]
    datasets_list = []
    def add_files(files):
        if files:
            datasets_list.append([os.path.join(folder, f) for f in files])
    add_files(menu_items)
    add_files(transactions)
    add_files(transaction_items)
    add_files(users)
    return datasets_list

def main(folder):
    client = Client()
    time.sleep(60)
    file_list = collect_files(folder)
    if not file_list:
        logging.warning("No se encontraron archivos para enviar.")
        return
    
    if len(file_list) < 4:
        logging.warning("No se encontraron todos los archivos necesarios para las consultas.")
        client.run(file_list, None)
    else:
        client.run(file_list[:-1], file_list[3])

if __name__ == "__main__":
    folder = os.path.join("data", "input") if len(sys.argv) < 2 else sys.argv[1]
    main(folder)
