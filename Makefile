CC = gcc
CFLAGS = -Wall -Wextra -pthread -O2

all: client

client: src/client.c
	$(CC) $(CFLAGS) src/client.c -o client

clean:
	rm -f client reassembled.dat execution_log.txt