#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>

/*
   PROJECT DOCUMENTATION FOR TEAMMATE:
   - Shared Memory: "/my_shm" (1GB)
   - Connection: Port 8080, IP 127.0.0.1
   - Protocol: Header (long offset, int size) followed by data.
*/

// Define a simple structure for the header
struct PacketHeader
{
    long offset;    // Where in the 1GB file this chunk goes
    int chunk_size; // How big the chunk is
};

int main()
{
    printf("I am the main boss! Setting up Shared Memory...\n");

    // 1. Setup the 1GB Shared Memory "Bucket"
    size_t file_size = 1024 * 1024 * 1024; // 1GB
    int fd = shm_open("/my_shm", O_CREAT | O_RDWR, 0666);

    if (fd == -1)
    {
        perror("shm_open failed");
        return 1;
    }

    ftruncate(fd, file_size);

    char *shared_mem = mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

    if (shared_mem == MAP_FAILED)
    {
        perror("mmap failed");
        return 1;
    }

    printf("Shared memory created and mapped successfully.\n");

    // 2. Fork the child processes
    for (int i = 0; i < 4; i++)
    {
        pid_t pid = fork();

        if (pid == 0)
        {
            // --- INSIDE CHILD PROCESS ---
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            struct sockaddr_in serv_addr;
            serv_addr.sin_family = AF_INET;
            serv_addr.sin_port = htons(8080);
            inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

            if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
            {
                printf("Child %d: Server not found yet (this is normal!)\n", i);
            }
            else
            {
                printf("Child %d: Connected! Starting to receive...\n", i);

                // Receiver Loop
                while (1)
                {
                    struct PacketHeader header;
                    // Receive the header (8 bytes usually)
                    int bytes_received = recv(sock, &header, sizeof(header), MSG_WAITALL);
                    if (bytes_received <= 0)
                        break; // Server closed connection

                    // Receive the data and write it directly to the shared memory offset
                    recv(sock, shared_mem + header.offset, header.chunk_size, MSG_WAITALL);

                    printf("Child %d: Received chunk of size %d at offset %ld\n", i, header.chunk_size, header.offset);
                }
            }
            close(sock);
            return 0; // The child stops here
        }
    }

    // 3. Wait for all children to finish
    for (int i = 0; i < 4; i++)
    {
        wait(NULL);
    }

    printf("All children finished. Cleaning up.\n");
    munmap(shared_mem, file_size);
    shm_unlink("/my_shm");

    return 0;
}