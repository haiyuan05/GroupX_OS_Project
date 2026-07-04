#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdint.h>
#include <arpa/inet.h>

#define SERVER_PORT 9090

struct ChunkHeader
{
    uint32_t seq_num;      // 4 bytes: Network byte order
    uint32_t payload_size; // 4 bytes: Network byte order
};

int main(int argc, char *argv[])
{
    if (argc != 2)
    {
        fprintf(stderr, "Usage: %s <input_file>\n", argv[0]);
        return 1;
    }

    // 1. Get file size using stat
    struct stat st;
    if (stat(argv[1], &st) < 0)
    {
        perror("Failed to get file stat");
        return 1;
    }
    size_t file_size = st.st_size;

    // 2. Set up Master TCP Socket
    int server_fd = socket(AF_INET, SOCK_STREAM, 0), opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in address = {.sin_family = AF_INET, .sin_port = htons(SERVER_PORT), .sin_addr.s_addr = INADDR_ANY};
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0 || listen(server_fd, 64) < 0)
    {
        perror("Bind or Listen failed");
        return 1;
    }
    printf("Server listening on port %d...\n", SERVER_PORT);

    // 3. Accept connections sequentially (matching H.Y.'s client loop)
    // Assuming a default test execution framework value of N = 4 chunks
    int num_processes = 4;
    int client_sockets[128];
    socklen_t addr_len = sizeof(address);

    for (int i = 0; i < num_processes; i++)
    {
        client_sockets[i] = accept(server_fd, (struct sockaddr *)&address, &addr_len);
        if (client_sockets[i] < 0)
        {
            perror("Accept failed");
            return 1;
        }
    }
    printf("Collected all N = %d client connection mappings successfully.\n", num_processes);

    // 4. Calculate Chunk Sizing Rules
    size_t base_chunk_size = (file_size + num_processes - 1) / num_processes;

    // 5. Fork worker processes to handle chunks
    for (int i = 0; i < num_processes; i++)
    {
        uint32_t seq_num = i + 1; // 1-based index
        int worker_socket = client_sockets[i];

        pid_t pid = fork();
        if (pid < 0)
        {
            perror("Fork failed");
            return 1;
        }

        if (pid == 0)
        {                     // --- CHILD PROCESS WORKER ---
            close(server_fd); // Child doesn't use the listener

            int child_file_fd = open(argv[1], O_RDONLY);
            if (child_file_fd < 0)
            {
                perror("Child failed to open file");
                exit(1);
            }

            // Determine explicit byte offset boundaries
            size_t offset = (size_t)(seq_num - 1) * base_chunk_size;
            size_t payload_size = base_chunk_size;

            if (offset >= file_size)
                payload_size = 0;
            else if (offset + payload_size > file_size)
                payload_size = file_size - offset;

            // Pack Header structural framing context rules
            struct ChunkHeader header = {.seq_num = htonl(seq_num), .payload_size = htonl(payload_size)};
            send(worker_socket, &header, sizeof(header), 0);

            // Move pointer directly to the assigned file chunk block offset
            lseek(child_file_fd, offset, SEEK_SET);

            // Basic block streaming loop pattern
            char buffer[8192];
            size_t total_sent = 0;
            while (total_sent < payload_size)
            {
                size_t to_read = payload_size - total_sent;
                if (to_read > sizeof(buffer))
                    to_read = sizeof(buffer);

                ssize_t bytes_read = read(child_file_fd, buffer, to_read);
                if (bytes_read <= 0)
                    break;

                ssize_t bytes_sent = 0;
                while (bytes_sent < bytes_read)
                {
                    ssize_t sent = send(worker_socket, buffer + bytes_sent, bytes_read - bytes_sent, 0);
                    if (sent <= 0)
                        break;
                    bytes_sent += sent;
                }
                total_sent += bytes_sent;
            }

            close(child_file_fd);
            close(worker_socket);
            exit(0); // Safely terminate child
        }
        else
        {
            close(worker_socket); // Parent closes descriptor copy
        }
    }

    // 6. Harvest/reap dead process states
    for (int i = 0; i < num_processes; i++)
        wait(NULL);

    close(server_fd);
    printf("Server execution completed successfully.\n");
    return 0;
}
