import os
import sys
import logging
from gateway import *

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configuración del logger
logging.basicConfig(
    level=logging.INFO,
    format="[GATEWAY] %(asctime)s - %(levelname)s - %(message)s"
)

def main():
    # Start result queue listener thread
    gateway = Gateway()
    gateway.run()

if __name__ == "__main__":
    main()
