#include "com.h"

#include <fcntl.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#define COM_SHM_NAME_LEN 128

/* Central mailbox used by the router process.
 * User processes place one send request here, and the server forwards
 * only metadata into the destination inbox.
 */
typedef struct {
    sem_t empty;                /* Server request slot is free. */
    sem_t full;                 /* Server request slot contains a message. */
    sem_t delivered;            /* Server routed message into destination inbox. */
    int src_rank;               /* Sender rank. */
    int dest_rank;              /* Destination rank, -1 means server shutdown. */
    int msg_id;                 /* unique id of one logical message. */
    size_t size;                /* Message size in bytes. */
    char shm_name[COM_SHM_NAME_LEN]; /* Sender shm segment name. */
} ServerMailbox;

/* One process-owned slot in shared memory.
 *
 * The same structure serves three purposes:
 * 1. inbox metadata for one incoming message,
 * 2. direct acknowledgement slot for messages sent by this process,
 * 3. information about the persistent shm segment owned by this process.
 */
typedef struct {
    pid_t pid;                  /* Child PID of the user process. */
    sem_t empty;                /* Inbox is free and can accept metadata. */
    sem_t full;                 /* Inbox contains metadata for one message. */
    sem_t ack_empty;            /* CHANGED: sender-side ack slot is free. */
    sem_t ack_full;             /* CHANGED: sender-side ack slot contains one ack. */
    int src_rank;               /* Sender rank filled by the server. */
    int msg_id;                 /* CHANGED: msg_id of the inbox message. */
    int ack_from_rank;          /* CHANGED: rank of the receiver that produced the ack. */
    int ack_msg_id;             /* CHANGED: msg_id acknowledged for this sender. */
    size_t size;                /* Current message size in bytes. */
    size_t capacity;            /* Persistent shm capacity of this process segment. */
    char shm_name[COM_SHM_NAME_LEN]; /* Persistent shm segment owned by this process. */
} ProcessSlot;

/* Global shared state inherited by all children after fork(). */
typedef struct {
    int nr_proc;                /* Number of user processes. */
    pid_t server_pid;           /* pid of the router process. */
    int finalized_count;        /* Number of user processes that already finalized. */
    sem_t ready_sem;            /* Child reports that its shm segment is ready. */
    sem_t start_sem;            /* Parent releases all children after setup. */
    sem_t finalize_lock;        /* Protects finalized_count / last-process shutdown. */
    ServerMailbox server_box;   /* Central server mailbox. */
    ProcessSlot slots[];        /* One inbox + one ack slot + shm owner record per user process. */
} SharedState;

static int g_rank = -1;
static int g_next_msg_id = 1;   /* Local counter used to distinguish multiple sends from one process. */
static SharedState *g_shared = NULL;

static size_t shared_region_size(int nr_proc)
{
    return sizeof(SharedState) + (size_t)nr_proc * sizeof(ProcessSlot);
}

static void sem_wait_checked(sem_t *sem)
{
    if (sem_wait(sem) == -1) {
        perror("sem_wait");
        exit(1);
    }
}

static void sem_post_checked(sem_t *sem)
{
    if (sem_post(sem) == -1) {
        perror("sem_post");
        exit(1);
    }
}

static SharedState *create_shared_region(int nr_proc)
{
    int i;
    size_t size = shared_region_size(nr_proc);
    SharedState *shared = mmap(NULL, size, PROT_READ | PROT_WRITE,
                               MAP_SHARED | MAP_ANONYMOUS, -1, 0);

    if (shared == MAP_FAILED) {
        perror("mmap");
        exit(1);
    }

    memset(shared, 0, size);
    shared->nr_proc = nr_proc;
    shared->server_pid = -1;
    shared->finalized_count = 0;

    if (sem_init(&shared->ready_sem, 1, 0) == -1) {
        perror("sem_init(ready_sem)");
        exit(1);
    }

    if (sem_init(&shared->start_sem, 1, 0) == -1) {
        perror("sem_init(start_sem)");
        exit(1);
    }

    if (sem_init(&shared->finalize_lock, 1, 1) == -1) {
        perror("sem_init(finalize_lock)");
        exit(1);
    }

    shared->server_box.src_rank = -1;
    shared->server_box.dest_rank = -1;
    shared->server_box.msg_id = 0;
    shared->server_box.size = 0;
    shared->server_box.shm_name[0] = '\0';

    if (sem_init(&shared->server_box.empty, 1, 1) == -1) {
        perror("sem_init(server_box.empty)");
        exit(1);
    }
    if (sem_init(&shared->server_box.full, 1, 0) == -1) {
        perror("sem_init(server_box.full)");
        exit(1);
    }
    if (sem_init(&shared->server_box.delivered, 1, 0) == -1) {
        perror("sem_init(server_box.delivered)");
        exit(1);
    }

    for (i = 0; i < nr_proc; i++) {
        shared->slots[i].pid = -1;
        shared->slots[i].src_rank = -1;
        shared->slots[i].msg_id = 0;
        shared->slots[i].ack_from_rank = -1;
        shared->slots[i].ack_msg_id = 0;
        shared->slots[i].size = 0;
        shared->slots[i].capacity = 0;
        shared->slots[i].shm_name[0] = '\0';

        if (sem_init(&shared->slots[i].empty, 1, 1) == -1) {
            perror("sem_init(empty)");
            exit(1);
        }
        if (sem_init(&shared->slots[i].full, 1, 0) == -1) {
            perror("sem_init(full)");
            exit(1);
        }
        if (sem_init(&shared->slots[i].ack_empty, 1, 1) == -1) {
            perror("sem_init(ack_empty)");
            exit(1);
        }
        if (sem_init(&shared->slots[i].ack_full, 1, 0) == -1) {
            perror("sem_init(ack_full)");
            exit(1);
        }
    }

    return shared;
}

static void destroy_shared_region(void)
{
    int i;
    int nr_proc = g_shared->nr_proc;
    size_t size = shared_region_size(nr_proc);

    for (i = 0; i < nr_proc; i++) {
        if (g_shared->slots[i].shm_name[0] != '\0') {
            if (shm_unlink(g_shared->slots[i].shm_name) == -1) {
                perror("shm_unlink");
                exit(1);
            }
        }
    }

    for (i = 0; i < nr_proc; i++) {
        if (sem_destroy(&g_shared->slots[i].empty) == -1) {
            perror("sem_destroy(empty)");
            exit(1);
        }
        if (sem_destroy(&g_shared->slots[i].full) == -1) {
            perror("sem_destroy(full)");
            exit(1);
        }
        if (sem_destroy(&g_shared->slots[i].ack_empty) == -1) {
            perror("sem_destroy(ack_empty)");
            exit(1);
        }
        if (sem_destroy(&g_shared->slots[i].ack_full) == -1) {
            perror("sem_destroy(ack_full)");
            exit(1);
        }
    }

    if (sem_destroy(&g_shared->server_box.empty) == -1) {
        perror("sem_destroy(server_box.empty)");
        exit(1);
    }
    if (sem_destroy(&g_shared->server_box.full) == -1) {
        perror("sem_destroy(server_box.full)");
        exit(1);
    }
    if (sem_destroy(&g_shared->server_box.delivered) == -1) {
        perror("sem_destroy(server_box.delivered)");
        exit(1);
    }
    if (sem_destroy(&g_shared->finalize_lock) == -1) {
        perror("sem_destroy(finalize_lock)");
        exit(1);
    }
    if (sem_destroy(&g_shared->ready_sem) == -1) {
        perror("sem_destroy(ready_sem)");
        exit(1);
    }
    if (sem_destroy(&g_shared->start_sem) == -1) {
        perror("sem_destroy(start_sem)");
        exit(1);
    }

    if (munmap(g_shared, size) == -1) {
        perror("munmap");
        exit(1);
    }

    g_shared = NULL;
}

static void create_process_segment(int rank)
{
    int fd;
    int written;
    ProcessSlot *slot = &g_shared->slots[rank];

    slot->pid = getpid();
    slot->capacity = 0;

    written = snprintf(slot->shm_name, COM_SHM_NAME_LEN, "/%ld", (long)slot->pid);
    if (written < 0 || written >= COM_SHM_NAME_LEN) {
        fprintf(stderr, "segment name too long\n");
        exit(1);
    }

    fd = shm_open(slot->shm_name, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd == -1) {
        perror("shm_open(create own segment)");
        exit(1);
    }

    if (close(fd) == -1) {
        perror("close(create own segment)");
        exit(1);
    }
}

/* The shm segment is reused between sends.
 * We only grow it when a new message does not fit into the previous capacity.
 */
static void write_message_to_own_segment(int rank, const void *message, size_t size)
{
    int fd;
    void *mapping;
    ProcessSlot *slot = &g_shared->slots[rank];

    fd = shm_open(slot->shm_name, O_RDWR, 0600);
    if (fd == -1) {
        perror("shm_open(write own segment)");
        exit(1);
    }

    if (size > slot->capacity) {
        if (ftruncate(fd, (off_t)size) == -1) {
            perror("ftruncate(write own segment)");
            exit(1);
        }
        slot->capacity = size;
    }

    if (size > 0) {
        mapping = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (mapping == MAP_FAILED) {
            perror("mmap(write own segment)");
            exit(1);
        }

        memcpy(mapping, message, size);

        if (munmap(mapping, size) == -1) {
            perror("munmap(write own segment)");
            exit(1);
        }
    }

    if (close(fd) == -1) {
        perror("close(write own segment)");
        exit(1);
    }
}

static void read_message_from_segment(const char *name, size_t size, void **message_out)
{
    int fd;
    void *mapping;
    void *copy;

    fd = shm_open(name, O_RDONLY, 0);
    if (fd == -1) {
        perror("shm_open(read)");
        exit(1);
    }

    if (size == 0) {
        copy = malloc(1);
        if (copy == NULL) {
            fprintf(stderr, "malloc failed\n");
            exit(1);
        }

        *message_out = copy;

        if (close(fd) == -1) {
            perror("close(read empty)");
            exit(1);
        }
        return;
    }

    mapping = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
    if (mapping == MAP_FAILED) {
        perror("mmap(read)");
        exit(1);
    }

    copy = malloc(size);
    if (copy == NULL) {
        fprintf(stderr, "malloc failed\n");
        exit(1);
    }

    memcpy(copy, mapping, size);

    if (munmap(mapping, size) == -1) {
        perror("munmap(read)");
        exit(1);
    }

    if (close(fd) == -1) {
        perror("close(read)");
        exit(1);
    }

    *message_out = copy;
}

/* Router process.
 * It copies only metadata from the central mailbox into the destination inbox.
 * The router does not wait until the receiver reads the message; that acknowledgement
 * is sent directly from receiver back to sender.
 */
static void server_loop(void)
{
    ServerMailbox *server_box = &g_shared->server_box;

    for (;;) {
        int dest_rank;
        ProcessSlot *dest_box;

        sem_wait_checked(&server_box->full);

        dest_rank = server_box->dest_rank;
        if (dest_rank == -1) {
            break;
        }

        if (dest_rank < 0 || dest_rank >= g_shared->nr_proc) {
            fprintf(stderr, "server_loop: invalid destination rank\n");
            exit(1);
        }

        dest_box = &g_shared->slots[dest_rank];
        sem_wait_checked(&dest_box->empty);

        dest_box->src_rank = server_box->src_rank;
        dest_box->msg_id = server_box->msg_id;
        dest_box->size = server_box->size;
        memcpy(dest_box->shm_name, server_box->shm_name, COM_SHM_NAME_LEN);

        sem_post_checked(&dest_box->full);
        sem_post_checked(&server_box->delivered);
    }

    _exit(0);
}

void com_initialize(int nr_proc, int *rank)
{
    int i;
    pid_t pid;

    g_shared = create_shared_region(nr_proc);

    /* Start one dedicated router process before forking user processes. */
    pid = fork();
    if (pid == -1) {
        perror("fork(server)");
        exit(1);
    }
    if (pid == 0) {
        server_loop();
    }
    g_shared->server_pid = pid;

    for (i = 0; i < nr_proc; i++) {
        pid = fork();

        if (pid == -1) {
            perror("fork");
            exit(1);
        }

        if (pid == 0) {
            g_rank = i;
            create_process_segment(i);
            sem_post_checked(&g_shared->ready_sem);
            sem_wait_checked(&g_shared->start_sem);
            *rank = i;
            return;
        }

        g_shared->slots[i].pid = pid;
    }

    for (i = 0; i < nr_proc; i++) {
        sem_wait_checked(&g_shared->ready_sem);
    }

    for (i = 0; i < nr_proc; i++) {
        sem_post_checked(&g_shared->start_sem);
    }

    for (i = 0; i < nr_proc; i++) {
        if (waitpid(g_shared->slots[i].pid, NULL, 0) == -1) {
            perror("waitpid(user)");
            exit(1);
        }
    }

    /* The last finishing user process sends a shutdown sentinel to the router in com_finalize(). */
    if (waitpid(g_shared->server_pid, NULL, 0) == -1) {
        perror("waitpid(server)");
        exit(1);
    }

    destroy_shared_region();
    _exit(0);
}

/* Send protocol:
 * 1. write payload into the sender-owned shm segment,
 * 2. place metadata into the central router mailbox,
 * 3. wait until the router confirms delivery into the receiver inbox,
 * 4. wait for a direct acknowledgement from the receiver.
 */
void com_send(int rank, void *msg, size_t size)
{
    ServerMailbox *server_box = &g_shared->server_box;
    ProcessSlot *my_slot = &g_shared->slots[g_rank];
    int my_msg_id = g_next_msg_id++;

    write_message_to_own_segment(g_rank, msg, size);

    sem_wait_checked(&server_box->empty);

    server_box->src_rank = g_rank;
    server_box->dest_rank = rank;
    server_box->msg_id = my_msg_id;
    server_box->size = size;
    memcpy(server_box->shm_name, my_slot->shm_name, COM_SHM_NAME_LEN);

    sem_post_checked(&server_box->full);
    sem_wait_checked(&server_box->delivered);
    sem_post_checked(&server_box->empty);

    /* The sender waits on its own ack slot, so the router stays free for other traffic. */
    for (;;) {
        sem_wait_checked(&my_slot->ack_full);

        if (my_slot->ack_msg_id == my_msg_id && my_slot->ack_from_rank == rank) {
            my_slot->ack_msg_id = 0;
            my_slot->ack_from_rank = -1;
            sem_post_checked(&my_slot->ack_empty);
            break;
        }

        /* With the current benchmark each process has at most one outstanding send,
         * so this branch should normally not be hit.
         * The loop is kept for defensive correctness.
         */
        sem_post_checked(&my_slot->ack_empty);
    }
}

void com_recv(void **msg, size_t *size)
{
    ProcessSlot *slot = &g_shared->slots[g_rank];
    int sender_rank;
    int sender_msg_id;
    ProcessSlot *sender_slot;

    sem_wait_checked(&slot->full);

    sender_rank = slot->src_rank;
    sender_msg_id = slot->msg_id;
    *size = slot->size;
    read_message_from_segment(slot->shm_name, slot->size, msg);

    slot->src_rank = -1;
    slot->msg_id = 0;
    slot->size = 0;
    sem_post_checked(&slot->empty);

    /* After copying the message locally, the receiver acknowledges exactly which message was read. */
    sender_slot = &g_shared->slots[sender_rank];
    sem_wait_checked(&sender_slot->ack_empty);
    sender_slot->ack_from_rank = g_rank;
    sender_slot->ack_msg_id = sender_msg_id;
    sem_post_checked(&sender_slot->ack_full);
}

void com_mcast(void *msg, size_t size)
{
    int i;

    for (i = 0; i < g_shared->nr_proc; i++) {
        if (i != g_rank) {
            com_send(i, msg, size);
        }
    }
}

void com_finalize(void)
{
    int is_last = 0;
    size_t size;

    /* The last finishing user process sends a shutdown sentinel to stop the router. */
    sem_wait_checked(&g_shared->finalize_lock);
    g_shared->finalized_count++;
    if (g_shared->finalized_count == g_shared->nr_proc) {
        is_last = 1;
    }
    sem_post_checked(&g_shared->finalize_lock);

    if (is_last) {
        sem_wait_checked(&g_shared->server_box.empty);
        g_shared->server_box.src_rank = -1;
        g_shared->server_box.dest_rank = -1;
        g_shared->server_box.msg_id = 0;
        g_shared->server_box.size = 0;
        g_shared->server_box.shm_name[0] = '\0';
        sem_post_checked(&g_shared->server_box.full);
    }

    size = shared_region_size(g_shared->nr_proc);
    if (munmap(g_shared, size) == -1) {
        perror("munmap");
        exit(1);
    }

    g_shared = NULL;
}