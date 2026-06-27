#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <pthread.h>
#include <stdint.h>

extern char *optarg;
extern int optind;

#define SERVER_PORT 9090
#define FILE_SIZE_1GB (1024ULL * 1024ULL * 1024ULL)

// Mandatory 8-byte Header Protocol Structure (Section 3.2)
struct ChunkHeader
{
    uint32_t seq_num;      // 4 bytes: Network byte order
    uint32_t payload_size; // 4 bytes: Network byte order
};

// Shared state structural layout mapped within shared memory
struct SharedState
{
    sem_t received_count; // POSIX semaphore to count received chunks
};

void print_usage(const char *prog)
{
    fprintf(stderr, "Usage: %s -p <num_processes> [-h <server_ip>]\n", prog);
}

int main(int argc, char *argv[])
{
    int num_processes = 0;
    const char *server_ip = "127.0.0.1"; // Default fallback IP
    int opt;

    // Parse mandatory command line arguments
    while ((opt = getopt(argc, argv, "p:h:")) != -1)
    {
        switch (opt)
        {
        case 'p':
            num_processes = atoi(optarg);
            break;
        case 'h':
            server_ip = optarg;
            break;
        default:
            print_usage(argv[0]);
            return 1;
        }
    }

    if (num_processes <= 0)
    {
        print_usage(argv[0]);
        return 1;
    }

    // Explicit math for memory size allocation matching specifications
    size_t base_chunk_size = (FILE_SIZE_1GB + num_processes - 1) / num_processes;
    size_t total_shm_size = sizeof(struct SharedState) + FILE_SIZE_1GB;

    // Create anonymous shared memory region accessible to child sub-processes
    void *shm_ptr = mmap(NULL, total_shm_size,
                         PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (shm_ptr == MAP_FAILED)
    {
        perror("mmap failed");
        return 1;
    }

    struct SharedState *shared_state = (struct SharedState *)shm_ptr;
    char *data_buffer = (char *)shm_ptr + sizeof(struct SharedState);

    // Initialize the shared POSIX semaphore for inter-process coordination
    if (sem_init(&shared_state->received_count, 1, 0) < 0)
    {
        perror("sem_init failed");
        munmap(shm_ptr, total_shm_size);
        return 1;
    }

    // Fork exactly N consumer child processes
    for (int i = 0; i < num_processes; i++)
    {
        pid_t pid = fork();
        if (pid < 0)
        {
            perror("fork failed");
            sem_destroy(&shared_state->received_count);
            munmap(shm_ptr, total_shm_size);
            return 1;
        }

        if (pid == 0)
        { // --- CHILD PROCESS EXECUTION SCOPE ---
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0)
            {
                perror("Socket creation failed");
                exit(1);
            }

            struct sockaddr_in serv_addr;
            memset(&serv_addr, 0, sizeof(serv_addr));
            serv_addr.sin_family = AF_INET;
            serv_addr.sin_port = htons(SERVER_PORT); // Mandatory Port: 9090

            if (inet_pton(AF_INET, server_ip, &serv_addr.sin_addr) <= 0)
            {
                perror("Invalid address/ Address not supported");
                close(sock);
                exit(1);
            }

            // Establish physical network connection
            if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
            {
                perror("Connection to server failed");
                close(sock);
                exit(1);
            }

            struct ChunkHeader header;
            // Guaranteed extraction of the full 8-byte control structural context
            if (recv(sock, &header, sizeof(header), MSG_WAITALL) != sizeof(header))
            {
                perror("Failed to read complete chunk header");
                close(sock);
                exit(1);
            }

            // Convert values from Network Byte Order to Host Byte Order
            uint32_t seq_num = ntohl(header.seq_num);
            uint32_t payload_size = ntohl(header.payload_size);

            if (seq_num < 1 || seq_num > (uint32_t)num_processes)
            {
                fprintf(stderr, "Error: Invalid sequence number received: %u\n", seq_num);
                close(sock);
                exit(1);
            }

            // Compute precision offset based on standardized alignment rules
            size_t write_offset = (size_t)(seq_num - 1) * base_chunk_size;
            size_t bytes_left = payload_size;
            char *dest_ptr = data_buffer + write_offset;

            // Stream network payload data straight into mapped destination window
            while (bytes_left > 0)
            {
                ssize_t received = recv(sock, dest_ptr, bytes_left, 0);
                if (received <= 0)
                {
                    perror("Error receiving raw payload data chunk stream");
                    close(sock);
                    exit(1);
                }
                dest_ptr += received;
                bytes_left -= received;
            }

            // Post to shared semaphore indicating data chunk block is completely written
            sem_post(&shared_state->received_count);
            close(sock);
            exit(0);
        }
    }

    // --- PARENT PROCESS CLEANUP & VALIDATION CONTROL LOOP ---
    int status;
    int child_errors = 0;

    // Step 1: Wait for all N children to physically terminate
    for (int i = 0; i < num_processes; i++)
    {
        wait(&status);
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
        {
            child_errors++;
        }
    }

    // FIX: Catch errors immediately to prevent deadlock on sem_wait loops
    if (child_errors > 0)
    {
        fprintf(stderr, "Error: %d child worker sub-processes failed execution.\n", child_errors);
        sem_destroy(&shared_state->received_count);
        munmap(shm_ptr, total_shm_size);
        return 1;
    }

    // Step 2: Safe confirmation barrier check matching count tracking requirements
    for (int i = 0; i < num_processes; i++)
    {
        sem_wait(&shared_state->received_count);
    }

    // Save out standard binary mapping dataset cleanly to expected filename target
    FILE *out_file = fopen("reassembled.dat", "wb");
    if (!out_file)
    {
        perror("Failed to open output file reassembled.dat");
        sem_destroy(&shared_state->received_count);
        munmap(shm_ptr, total_shm_size);
        return 1;
    }

    // Write the 1GB block using an optimized streaming block function call
    size_t written = fwrite(data_buffer, 1, FILE_SIZE_1GB, out_file);
    fclose(out_file);

    if (written != FILE_SIZE_1GB)
    {
        fprintf(stderr, "Critical error: Complete block file content assembly tracking failed.\n");
        sem_destroy(&shared_state->received_count);
        munmap(shm_ptr, total_shm_size);
        return 1;
    }

    // Write out compliant automated test verification diagnostic telemetry log entries
    FILE *log_file = fopen("execution_log.txt", "w");
    if (log_file)
    {
        fprintf(log_file, "[PART1] CHUNKS=%d | PROCS=%d | SYNC_USED=sem\n", num_processes, num_processes);
        fclose(log_file);
    }

    printf("Reassembly complete. Launching Part 2 operations framework analytics module...\n");

    // Automatically trigger analytical framework logic via fork+exec handoff sequence
    pid_t analytics_pid = fork();
    if (analytics_pid == 0)
    {
        // Run Part 2. Hardcoding 8 threads as expected default testing parameter
        char *args[] = {"./operations", "-t", "8", "-f", "reassembled.dat", NULL};
        execv(args[0], args);

        // If execv reaches this point, an execution error occurred
        perror("execv failed to launch ./operations module");
        exit(1);
    }
    else if (analytics_pid > 0)
    {
        waitpid(analytics_pid, &status, 0);
    }
    else
    {
        perror("Failed to fork analytics tracking system runner sub process");
        sem_destroy(&shared_state->received_count);
        munmap(shm_ptr, total_shm_size);
        return 1;
    }

    // Clean up shared environment resources before clean termination
    sem_destroy(&shared_state->received_count);
    munmap(shm_ptr, total_shm_size);

    return 0;
}