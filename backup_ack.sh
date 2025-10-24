#!/bin/bash

echo "Creando backup del sistema ACK..."

# Backup current files
cp workers/filter_by_hour/main.py workers/filter_by_hour/main.py.with_ack
cp workers/filter_by_amount/main.py workers/filter_by_amount/main.py.with_ack
cp common/protocol.py common/protocol.py.with_ack

echo "Files backed up with .with_ack extension"
echo "Now you can revert to non-ACK versions to fix the basic system first"