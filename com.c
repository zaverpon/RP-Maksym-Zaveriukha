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

/* Per-process mailbox stored in shared anonymous memory. */
typedef struct {
    pid_t pid;                  /*  process id, also used for shm name. */
    sem_t empty;                /* Slot is free and can accept a new message. */
    sem_t full;                 /* Slot contains a message ready to read. */
    sem_t consumed;             /* Receiver finished reading the message. */
    size_t size;                /* Current message size in bytes. */
    char shm_name[COM_SHM_NAME_LEN]; /* Name of this process's persistent shm segment. */
} ProcessSlot;

/* Global shared state inherited by all children after fork(). */
typedef struct {
    int nr_proc;                /* Number of user processes. */
    sem_t ready_sem;            /* Child reports that its shm segment is ready. */
    sem_t start_sem;            /* Server releases all children after setup. */
    ProcessSlot slots[];        /* One mailbox slot per user process. */
} SharedState;

/* Rank of the current user process. */
static int g_rank = -1;
/* Pointer to the shared anonymous region. */
static SharedState *g_shared = NULL;


static size_t shared_region_size(int nr_proc)
{
    return sizeof(SharedState) + (size_t)nr_proc * sizeof(ProcessSlot);
}

/* Wrapper for sem_wait */
static void sem_wait_checked(sem_t *sem)
{
    if (sem_wait(sem) == -1) {
        perror("sem_wait");
        exit(1);
    }
}

/* Wrapper for sem_post */
static void sem_post_checked(sem_t *sem)
{
    if (sem_post(sem) == -1) {
        perror("sem_post");
        exit(1);
    }
}

/* Create and initialize the shared anonymous region used by all processes. */
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

    if (sem_init(&shared->ready_sem, 1, 0) == -1) {
        perror("sem_init(ready_sem)");
        exit(1);
    }

    if (sem_init(&shared->start_sem, 1, 0) == -1) {
        perror("sem_init(start_sem)");
        exit(1);
    }

    for (i = 0; i < nr_proc; i++) {
        shared->slots[i].pid = -1;
        shared->slots[i].size = 0;
        shared->slots[i].shm_name[0] = '\0';

        if (sem_init(&shared->slots[i].empty, 1, 1) == -1) {
            perror("sem_init(empty)");
            exit(1);
        }
        if (sem_init(&shared->slots[i].full, 1, 0) == -1) {
            perror("sem_init(full)");
            exit(1);
        }
        if (sem_init(&shared->slots[i].consumed, 1, 0) == -1) {
            perror("sem_init(consumed)");
            exit(1);
        }
    }

    return shared;
}

/* Destroy all shared synchronization objects and remove all shm segments. */
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
        if (sem_destroy(&g_shared->slots[i].consumed) == -1) {
            perror("sem_destroy(consumed)");
            exit(1);
        }
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

/* Create the persistent shm segment owned by one user process. */
static void create_process_segment(int rank)
{
    int fd;
    int written;
    ProcessSlot *slot = &g_shared->slots[rank];

    slot->pid = getpid();

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

/* Open receiver's shm segment, resize it, and copy the payload into it. */
static void write_message_to_segment(const char *name, const void *message, size_t size)
{
    int fd;
    void *mapping;

    fd = shm_open(name, O_RDWR, 0600);
    if (fd == -1) {
        perror("shm_open(write)");
        exit(1);
    }

    if (ftruncate(fd, (off_t)size) == -1) {
        perror("ftruncate(write)");
        exit(1);
    }

    if (size > 0) {
        mapping = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (mapping == MAP_FAILED) {
            perror("mmap(write)");
            exit(1);
        }

        memcpy(mapping, message, size);

        if (munmap(mapping, size) == -1) {
            perror("munmap(write)");
            exit(1);
        }
    }

    if (close(fd) == -1) {
        perror("close(write)");
        exit(1);
    }
}

/* Read payload from the current process's shm segment into local heap memory. */
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
        return;†
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

/* Server process creates all children and waits until all are ready to run. */
void com_initialize(int nr_proc, int *rank)
{
    int i;

    g_shared = create_shared_region(nr_proc);

    for (i = 0; i < nr_proc; i++) {
        pid_t pid = fork();

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
            perror("waitpid");
            exit(1);
        }
    }

    destroy_shared_region();
    _exit(0);
}

/* Send one message to the target process using its dedicated shm segment. */
void com_send(int rank, void *msg, size_t size)
{
    ProcessSlot *slot = &g_shared->slots[rank];

    sem_wait_checked(&slot->empty);

    write_message_to_segment(slot->shm_name, msg, size);

    slot->size = size;

    sem_post_checked(&slot->full);
    sem_wait_checked(&slot->consumed);
    sem_post_checked(&slot->empty);
}

/* Receive one message from the current process's dedicated slot. */
void com_recv(void **msg, size_t *size)
{
    ProcessSlot *slot = &g_shared->slots[g_rank];

    sem_wait_checked(&slot->full);

    *size = slot->size;
    read_message_from_segment(slot->shm_name, slot->size, msg);

    slot->size = 0;

    sem_post_checked(&slot->consumed);
}

/* Multicast is implemented as repeated point-to-point sends. */
void com_mcast(void *msg, size_t size)
{
    int i;

    for (i = 0; i < g_shared->nr_proc; i++) {
        if (i != g_rank) {
            com_send(i, msg, size);
        }
    }
}

/* Detach the current process from the shared anonymous region. */
void com_finalize(void)
{
    size_t size = shared_region_size(g_shared->nr_proc);

    if (munmap(g_shared, size) == -1) {
        perror("munmap");
        exit(1);
    }

    g_shared = NULL;
}
