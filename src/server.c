#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdint.h>
#include <arpa/inet.h>

#define SERVER_PORT 9090

// Mandatory 8-byte Header Protocol Structure (Section 3.2)
struct ChunkHeader
{
    uint32_t seq_num;      // 4 bytes: Network byte order
    uint32_t payload_size; // 4 bytes: Network byte order
};

void print_usage(const char *prog)
{
    fprintf(stderr, "Usage: %s <input_file>\n", prog);
}

int main(int argc, char *argv[])
{
    if (argc != 2)
    {
        print_usage(argv[0]);
        return 1;
    }

    const char *input_file_path = argv[1];

    // 1. Open the large input binary file and inspect its true system size
    int file_fd = open(input_file_path, O_RDONLY);
    if (file_fd < 0)
    {
        perror("Failed to open input file");
        return 1;
    }

    struct stat st;
    if (fstat(file_fd, &st) < 0)
    {
        perror("Failed to get file status size");
        close(file_fd);
        return 1;
    }
    size_t file_size = st.st_size;
    close(file_fd); // Main process closes it; children will open independently to seek safely

    // 2. Set up the Master TCP Listening Socket
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0)
    {
        perror("Socket creation failed");
        return 1;
    }

    // Allow immediate reuse of local port mapping to avoid "Address already in use" errors
    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
    {
        perror("setsockopt failed");
        close(server_fd);
        return 1;
    }

    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(SERVER_PORT);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0)
    {
        perror("Bind failed");
        close(server_fd);
        return 1;
    }

    // Backlog size set generously to handle instant consecutive client connection bursts
    if (listen(server_fd, 64) < 0)
    {
        perror("Listen failed");
        close(server_fd);
        return 1;
    }

    printf("Server listening on port %d... Waiting for incoming group connection sequence.\n", SERVER_PORT);

    // 3. Keep track of incoming connections to dynamically infer total chunks (N)
    // Because H.Y.'s client does not explicitly tell us 'N', we accept the incoming
    // connections dynamically.
    int client_sockets[128];
    int num_processes = 0;

    struct sockaddr_in client_addr;
    socklen_t addr_len = sizeof(client_addr);

    // Read the first socket connection to kick off processing
    int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &addr_len);
    if (client_fd < 0)
    {
        perror("Accept failed");
        close(server_fd);
        return 1;
    }
    client_sockets[num_processes++] = client_fd;

    // Use a short timeout mechanism via select() to discover how many concurrent
    // connections are being spawned simultaneously by the client program execution loop.
    while (num_processes < 128)
    {
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(server_fd, &read_fds);

        struct timeval timeout;
        timeout.tv_sec = 0;
        timeout.tv_usec = 200000; // 200ms window barrier

        int activity = select(server_fd + 1, &read_fds, NULL, NULL, &timeout);
        if (activity > 0 && FD_ISSET(server_fd, &read_fds))
        {
            client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &addr_len);
            if (client_fd >= 0)
            {
                client_sockets[num_processes++] = client_fd;
            }
        }
        else
        {
            // Timeout reached with no more incoming connections; group sequence complete
            break;
        }
    }

    printf("Detected N = %d total connection requests from client framework.\n", num_processes);

    // 4. Implement exact chunk calculation logic matching client side tracking math rules
    size_t base_chunk_size = (file_size + num_processes - 1) / num_processes;

    // 5. Fork workers for each sequence mapped chunk connection instance
    for (int i = 0; i < num_processes; i++)
    {
        uint32_t seq_num = i + 1; // 1-based index matching specifications (Section 3.1)
        int worker_socket = client_sockets[i];

        pid_t pid = fork();
        if (pid < 0)
        {
            perror("Fork failed");
            return 1;
        }

        if (pid == 0)
        {                     // --- SERVER CHILD PROCESS EXECUTION SCOPE ---
            close(server_fd); // Child does not need the listener socket

            // Re-open input file safely to prevent multi-process shared file offset conflicts
            int child_file_fd = open(input_file_path, O_RDONLY);
            if (child_file_fd < 0)
            {
                perror("Child failed to open input file");
                close(worker_socket);
                exit(1);
            }

            // Precision calculation targeting individual byte window range bounds
            size_t offset = (size_t)(seq_num - 1) * base_chunk_size;
            size_t payload_size = base_chunk_size;

            // Boundary validation ensuring trailing edge handling for uneven data divisions
            if (offset >= file_size)
            {
                payload_size = 0;
            }
            else if (offset + payload_size > file_size)
            {
                payload_size = file_size - offset;
            }

            // Prepare Header strictly adhering to Network Byte Order guidelines
            struct ChunkHeader header;
            header.seq_num = htonl(seq_num);
            header.payload_size = htonl((uint32_t)payload_size);

            // Transmit the 8-byte control structural context frame block completely
            if (send(worker_socket, &header, sizeof(header), 0) != sizeof(header))
            {
                perror("Child failed to send complete chunk header context");
                close(child_file_fd);
                close(worker_socket);
                exit(1);
            }

            // Seek directly to assigned file chunk segment block
            if (lseek(child_file_fd, offset, SEEK_SET) < 0)
            {
                perror("Child file lseek failed");
                close(child_file_fd);
                close(worker_socket);
                exit(1);
            }

            // Stream transmission sequence processing loop block
            char buffer[8192];
            size_t total_sent = 0;
            while (total_sent < payload_size)
            {
                size_t to_read = payload_size - total_sent;
                if (to_read > sizeof(buffer))
                {
                    to_read = sizeof(buffer);
                }

                ssize_t bytes_read = read(child_file_fd, buffer, to_read);
                if (bytes_read <= 0)
                {
                    if (bytes_read < 0)
                        perror("File read error in child");
                    break;
                }

                ssize_t bytes_sent = 0;
                while (bytes_sent < bytes_read)
                {
                    ssize_t sent = send(worker_socket, buffer + bytes_sent, bytes_read - bytes_sent, 0);
                    if (sent <= 0)
                    {
                        perror("Socket send error in child");
                        break;
                    }
                    bytes_sent += sent;
                }
                total_sent += bytes_sent;
            }

            close(child_file_fd);
            close(worker_socket);
            exit(0); // Exit smoothly after successful chunk delivery handler flow completes
        }
        else
        {
            // Parent closes this client socket descriptor instance to prevent descriptor resource leaks
            close(worker_socket);
        }
    }

    // 6. Master Server waiting loop harvesting finished worker instances safely
    for (int i = 0; i < num_processes; i++)
    {
        wait(NULL);
    }

    close(server_fd);
    printf("Server execution task completed successfully for all client connections.\n");
    return 0;
}
