import os
import sys
import re
import logging
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
    transactions = sorted([f for f in files if PATTERNS["transactions"].match(f)])
    transaction_items = sorted([f for f in files if PATTERNS["transaction_items"].match(f)])
    users = sorted([f for f in files if PATTERNS["users"].match(f)])
    menu_items = [f for f in files if PATTERNS["menu_items"].match(f)]
    ordered = menu_items + transactions + transaction_items + users
    return [os.path.join(folder, f) for f in ordered]

def main(folder):
    client = Client()
    file_list = collect_files(folder)
    if not file_list:
        logging.warning("No se encontraron archivos para enviar.")
        return
    
    client.run(file_list)

if __name__ == "__main__":
    folder = os.path.join("data", "input") if len(sys.argv) < 2 else sys.argv[1]
    main(folder)
