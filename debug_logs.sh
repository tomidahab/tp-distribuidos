#!/bin/bash

echo "=== Checking filter_by_amount logs ==="
echo "Worker 0:"
docker logs filter_by_amount_worker_0 2>&1 | head -20

echo
echo "Worker 1:"  
docker logs filter_by_amount_worker_1 2>&1 | head -20

echo
echo "=== Checking filter_by_hour logs ==="
echo "Worker 0:"
docker logs filter_by_hour_worker_0 2>&1 | head -20

echo
echo "Worker 1:"
docker logs filter_by_hour_worker_1 2>&1 | head -20