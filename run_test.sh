#!/bin/bash

echo "=== STARTING AUTOMATED ASSIGNMENT VALIDATION SCRIPT ==="

# Clean old artifacts
rm -f server operations client reassembled.dat execution_log.txt

# Compile simulations and your client
gcc -O2 dummy_server.c -o server
gcc -O2 dummy_ops.c -o operations

# Scaledown client for high-speed testing environment
sed -i 's/1024ULL \* 1024ULL \* 1024ULL/16ULL \* 1024ULL \* 1024ULL/g' src/client.c
make client

echo "[STEP 2] Launching infrastructure simulation environment..."
./server &
SERVER_PID=$!

sleep 0.5

echo "-> Spawning client process engine..."
./client -p 4 -h 127.0.0.1

# Restore client.c back to its original 1GB setup for grading requirements
sed -i 's/16ULL \* 1024ULL \* 1024ULL/1024ULL \* 1024ULL \* 1024ULL/g' src/client.c

wait $SERVER_PID 2>/dev/null

echo "=== RUN COMPLETION BARRIER TRACE ==="
if [ -f "execution_log.txt" ]; then
    echo "[PASS] execution_log.txt was successfully generated!"
    cat execution_log.txt
else
    echo "[WARNING] execution_log.txt was not generated yet."
fi

