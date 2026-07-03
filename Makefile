CC = gcc
CFLAGS = -Wall -Wextra -pthread -O2

all: client operations

client: src/client.c
	$(CC) $(CFLAGS) src/client.c -o client

operations: src/operations.c
	$(CC) $(CFLAGS) src/operations.c -o operations

clean:
	rm -f client operations reassembled.dat execution_log.txt result_min.txt result_max.txt result_sorted.dat

