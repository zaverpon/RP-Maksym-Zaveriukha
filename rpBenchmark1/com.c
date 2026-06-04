#define _GNU_SOURCE
#define _DEFAULT_SOURCE
#define _POSIX_C_SOURCE 200809L

#ifdef __APPLE__
#define _DARWIN_C_SOURCE
#endif

#include "com.h"
#include <errno.h>
#include <fcntl.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>
#include <dirent.h>
#include <pthread.h>
#include <stdarg.h>

#ifndef MAP_ANONYMOUS
#ifdef MAP_ANON
#define MAP_ANONYMOUS MAP_ANON
#endif
#endif

#define COM_SHM_NAME_LEN 128
#define COM_SHM_PREFIX "com_ipc_"

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

    sem_t ack_empty;            /* Kept for compatibility, not needed for broadcast ack counting. */
    sem_t ack_full;             /* Counting semaphore: one post means one receiver freed the segment. */

    int src_rank;               /* Sender rank filled by the server. */
    int msg_id;                 /* msg_id of the inbox message. */

    int ack_from_rank;          /* Kept for debugging / compatibility. */
    int ack_msg_id;             /* Kept for debugging / compatibility. */

    int pending_acks;           /* Number of receivers that still use this send segment. */

    size_t size;                /* Current message size in bytes. */
    size_t capacity;            /* Persistent shm capacity of this process segment. */
    size_t prepared_size;       /* Information of current size of prepared shm */
    char own_shm_name[COM_SHM_NAME_LEN]; /* Persistent shm segment owned by this process. */
    char inbox_shm_name[COM_SHM_NAME_LEN];
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
static void *g_send_mapping = NULL; /* local mapping shm of curr proces*/
static size_t g_send_mapping_capacity = 0;
static pthread_mutex_t g_send_buffer_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_send_buffer_active = 0;


static void checked_output(FILE *stream, const char *fmt, va_list ap)
{
    if (vfprintf(stream, fmt, ap) < 0) {
        perror("vfprintf");
        exit(1);
    }
}

static void checked_print_to(FILE *stream, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    checked_output(stream, fmt, ap);
    va_end(ap);
}

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

static void send_buffer_lock_checked(void)
{
    int rc = pthread_mutex_lock(&g_send_buffer_lock);
    if (rc != 0) {
        checked_print_to(stderr, "pthread_mutex_lock(g_send_buffer_lock) failed: %d\n", rc);
        exit(1);
    }
    if (g_send_buffer_active) {
        checked_print_to(stderr, "send buffer is already active\n");
        exit(1);
    }
    g_send_buffer_active = 1;
}

static void send_buffer_unlock_checked(void)
{
    int rc;
    if (!g_send_buffer_active) {
        checked_print_to(stderr, "send buffer is not active\n");
        exit(1);
    }

    g_send_buffer_active = 0;
    rc = pthread_mutex_unlock(&g_send_buffer_lock);
    if (rc != 0) {
        checked_print_to(stderr, "pthread_mutex_unlock(g_send_buffer_lock) failed: %d\n", rc);
        exit(1);
    }
}

/*
 * Wait until all previous messages sent by this process were copied
 * by their receivers.
 *
 * For broadcast, pending_acks can be greater than 1.
 * ack_full works as a counting semaphore: every receiver posts it once
 * after copying the message from this process-owned shm segment.
 *
 * This moves blocking from com_send() to com_prepare_send_buffer().
 */
static void wait_for_pending_acks(ProcessSlot *slot)
{
    while (slot->pending_acks > 0) {
        sem_wait_checked(&slot->ack_full);
        slot->pending_acks--;
    }

    slot->ack_msg_id = 0;
    slot->ack_from_rank = -1;
    slot->prepared_size = 0;
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
        shared->slots[i].pending_acks = 0;

        shared->slots[i].size = 0;
        shared->slots[i].capacity = 0;
        shared->slots[i].prepared_size = 0;
        shared->slots[i].own_shm_name[0] = '\0';
        shared->slots[i].inbox_shm_name[0] = '\0';

        if (sem_init(&shared->slots[i].empty, 1, 1) == -1) {
            perror("sem_init(empty)");
            exit(1);
        }
        if (sem_init(&shared->slots[i].full, 1, 0) == -1) {
            perror("sem_init(full)");
            exit(1);
        }

        /*
         * ack_empty is no longer used as a strict one-ack slot lock,
         * because broadcast needs multiple receivers to acknowledge
         * without waiting for the sender to consume each ack immediately.
         */
        if (sem_init(&shared->slots[i].ack_empty, 1, 1) == -1) {
            perror("sem_init(ack_empty)");
            exit(1);
        }

        /*
         * ack_full is now a counting semaphore.
         * One sem_post means one receiver copied the message and freed
         * this sender-owned segment for that receiver.
         */
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
        if (g_shared->slots[i].own_shm_name[0] != '\0') {
            /*checked_print_to(stderr, "[DEBUG] unlink slot=%d pid=%ld name=%s\n",
                    i,
                    (long)g_shared->slots[i].pid,
                    g_shared->slots[i].own_shm_name);*/

            if (shm_unlink(g_shared->slots[i].own_shm_name) == -1) {
                perror("shm_unlink");
                checked_print_to(stderr, "[DEBUG] errno=%d\n", errno);
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
    
    written = snprintf(slot->own_shm_name, COM_SHM_NAME_LEN, "/" COM_SHM_PREFIX "%ld", (long)slot->pid);
    if (written < 0 || written >= COM_SHM_NAME_LEN) {
        checked_print_to(stderr, "segment name too long\n");
        exit(1);
    }
    shm_unlink(slot->own_shm_name);
    fd = shm_open(slot->own_shm_name, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd == -1) {
        perror("shm_open(create own segment)");
        exit(1);
    }

    if (close(fd) == -1) {
        perror("close(create own segment)");
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
            checked_print_to(stderr, "malloc failed\n");
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
        checked_print_to(stderr, "malloc failed\n");
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
            checked_print_to(stderr, "server_loop: invalid destination rank\n");
            exit(1);
        }

        dest_box = &g_shared->slots[dest_rank];
        sem_wait_checked(&dest_box->empty);

        dest_box->src_rank = server_box->src_rank;
        dest_box->msg_id = server_box->msg_id;
        dest_box->size = server_box->size;
        memcpy(dest_box->inbox_shm_name, server_box->shm_name, COM_SHM_NAME_LEN);

        sem_post_checked(&dest_box->full);

        /* This confirms only metadata delivery, not payload reading. */
        sem_post_checked(&server_box->delivered);
    }

    _exit(0);
}

static void cleanup_old_segments_with_prefix(void)
{
    DIR *dir;
    struct dirent *entry;

    dir = opendir("/dev/shm");
    if (dir == NULL) {
        return;
    }

    while ((entry = readdir(dir)) != NULL) {
        char shm_name[COM_SHM_NAME_LEN];

        if (strncmp(entry->d_name, COM_SHM_PREFIX, strlen(COM_SHM_PREFIX)) == 0) {
            int written = snprintf(shm_name,
                                   sizeof(shm_name),
                                   "/%s",
                                   entry->d_name);

            if (written > 0 && written < (int)sizeof(shm_name)) {
                if (shm_unlink(shm_name) == -1 && errno != ENOENT) {
                    perror("shm_unlink(cleanup)");
                }
            }
        }
    }

    closedir(dir);
}

void com_initialize(int nr_proc, int *rank)
{
    int i;
    pid_t pid;
    cleanup_old_segments_with_prefix();
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

void *com_prepare_send_buffer(size_t size)
{
    int fd;
    ProcessSlot *slot = &g_shared->slots[g_rank];

    /*Only one thread in this process may use the process-owned
     * send segment at a time.*/
    send_buffer_lock_checked();

    /*
     * Block here before reusing the send buffer.
     * For broadcast, this waits until all receivers have copied
     * the previous message from this sender-owned segment.
     */
    wait_for_pending_acks(slot);

    fd = shm_open(slot->own_shm_name, O_RDWR, 0600);
    if (fd == -1) {
        perror("shm_open(prepare)");
        exit(1);
    }

    if (size > slot->capacity) {
        if (ftruncate(fd, (off_t)size) == -1) {
            perror("ftruncate(prepare)");
            exit(1);
        }
        slot->capacity = size;

        if (g_send_mapping != NULL) {
            if (munmap(g_send_mapping, g_send_mapping_capacity) == -1) {
                perror("munmap(prepare)");
                exit(1);
            }
            g_send_mapping = NULL;
            g_send_mapping_capacity = 0;
        }
    }

    if (size == 0) {
        slot->prepared_size = 0;
        if (close(fd) == -1) {
            perror("close(prepare)");
            exit(1);
        }
        return NULL;
    }

    if (g_send_mapping == NULL || g_send_mapping_capacity < size) {
        g_send_mapping = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (g_send_mapping == MAP_FAILED) {
            perror("mmap(prepare)");
            exit(1);
        }
        g_send_mapping_capacity = size;
    }

    slot->prepared_size = size;

    if (close(fd) == -1) {
        perror("close(prepare)");
        exit(1);
    }

    return g_send_mapping;
}

/* Send protocol:
 * 1. the caller prepares payload in the sender-owned shm segment,
 * 2. com_send places only metadata into the central router mailbox,
 * 3. wait until the router confirms delivery into the receiver inbox.
 *
 * com_send does not wait until the receiver reads the payload.
 * That wait happens later in com_prepare_send_buffer().
 */
void com_send(int rank, size_t size)
{
    ServerMailbox *server_box = &g_shared->server_box;
    ProcessSlot *my_slot = &g_shared->slots[g_rank];
    int my_msg_id = g_next_msg_id++;

    if (rank < 0 || rank >= g_shared->nr_proc) {
        checked_print_to(stderr, "com_send: invalid destination rank\n");
        exit(1);
    }

    if (my_slot->prepared_size != size) {
        checked_print_to(stderr, "com_send: size mismatch\n");
        exit(1);
    }

    sem_wait_checked(&server_box->empty);

    server_box->src_rank = g_rank;
    server_box->dest_rank = rank;
    server_box->msg_id = my_msg_id;
    server_box->size = size;
    memcpy(server_box->shm_name, my_slot->own_shm_name, COM_SHM_NAME_LEN);

    sem_post_checked(&server_box->full);

    /* Wait only until metadata was placed into receiver inbox. */
    sem_wait_checked(&server_box->delivered);
    sem_post_checked(&server_box->empty);

    /*
     * One more receiver now uses this sender-owned shm segment.
     * For broadcast, this counter grows once per destination.
     */
    my_slot->pending_acks++;
}

void com_finish_send_buffer(void)
{
    ProcessSlot *slot = &g_shared->slots[g_rank];
    slot->prepared_size = 0;
    send_buffer_unlock_checked();
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

    read_message_from_segment(slot->inbox_shm_name, slot->size, msg);

    slot->src_rank = -1;
    slot->msg_id = 0;
    slot->size = 0;
    slot->inbox_shm_name[0] = '\0';

    sem_post_checked(&slot->empty);

    /*
     * After copying the message locally, the receiver frees sender's segment
     * for this one message.
     *
     * For broadcast, several receivers can do this independently.
     * Therefore we do not wait on ack_empty here.
     * ack_full is used as a counting semaphore.
     */
    sender_slot = &g_shared->slots[sender_rank];

    sender_slot->ack_from_rank = g_rank;
    sender_slot->ack_msg_id = sender_msg_id;

    sem_post_checked(&sender_slot->ack_full);
}

void com_mcast(void *msg, size_t size)
{
    int i;
    void *buf = com_prepare_send_buffer(size);

    if (size > 0) {
        memcpy(buf, msg, size);
    }

    /*
     * Broadcast/multicast uses the same sender-owned shm segment
     * for all destinations.
     *
     * Each com_send() increases pending_acks by one.
     * The segment can be reused only after all receivers acknowledge.
     */
    for (i = 0; i < g_shared->nr_proc; i++) {
        if (i != g_rank) {
            com_send(i, size);
        }
    }
    com_finish_send_buffer();
}

void com_finalize(void)
{
    int is_last = 0;
    size_t size;
    ProcessSlot *slot = &g_shared->slots[g_rank];

    if (g_send_buffer_active) {
        com_finish_send_buffer();
    }

    /*
     * Before finalizing, wait until all receivers copied messages
     * sent by this process.
     *
     * This is also necessary after broadcast, because one send segment
     * can still be used by several receivers.
     */
    wait_for_pending_acks(slot);

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

    if (g_send_mapping != NULL) {
        if (munmap(g_send_mapping, g_send_mapping_capacity) == -1) {
            perror("munmap(g_send_mapping)");
            exit(1);
        }
        g_send_mapping = NULL;
        g_send_mapping_capacity = 0;
    }

    size = shared_region_size(g_shared->nr_proc);
    if (munmap(g_shared, size) == -1) {
        perror("munmap");
        exit(1);
    }

    g_shared = NULL;
}