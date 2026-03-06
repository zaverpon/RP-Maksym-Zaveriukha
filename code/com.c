
#include "com.h"

#include <errno.h>
#include <fcntl.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define USER_COUNT 2
#define COM_SHM_NAME_LEN 128

typedef struct {
    sem_t finalized_user;
} Control;

typedef struct {
    sem_t empty;
    sem_t full;
    sem_t delivered;
    int src_rank;
    int dest_rank;
    size_t size;
    char shm_name[COM_SHM_NAME_LEN];
} ServerMailbox;

typedef struct {
    sem_t empty;
    sem_t full;
    sem_t consumed;
    int src_rank;
    size_t size;
    char shm_name[COM_SHM_NAME_LEN];
} ClientMailbox;

typedef struct {
    Control ctl;
    ServerMailbox server_box;
    ClientMailbox inbox[USER_COUNT];
} SharedState;

static int g_rank = -2;
static pid_t g_server_pid = -1;
static pid_t g_user1_pid = -1;
static unsigned long g_msg_counter = 0;
static SharedState *g_shared = NULL;

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

static void *create_shared_region(size_t size)
{
    void *ptr = mmap(NULL, size, PROT_READ | PROT_WRITE,
                     MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (ptr == MAP_FAILED) {
        perror("mmap");
        exit(1);
    }

    memset(ptr, 0, size);
    return ptr;
}

static void init_mailbox(ClientMailbox *box)
{
    if (sem_init(&box->empty, 1, 1) == -1) {
        perror("sem_init(client mailbox empty)");
        exit(1);
    }
    if (sem_init(&box->full, 1, 0) == -1) {
        perror("sem_init(client mailbox full)");
        exit(1);
    }
    if (sem_init(&box->consumed, 1, 0) == -1) {
        perror("sem_init(client mailbox consumed)");
        exit(1);
    }
}

static void destroy_mailbox(ClientMailbox *box)
{
    if (sem_destroy(&box->empty) == -1) {
        perror("sem_destroy(client mailbox empty)");
        exit(1);
    }
    if (sem_destroy(&box->full) == -1) {
        perror("sem_destroy(client mailbox full)");
        exit(1);
    }
    if (sem_destroy(&box->consumed) == -1) {
        perror("sem_destroy(client mailbox consumed)");
        exit(1);
    }
}

static void create_message_shm(const void *message, size_t size, char name_out[COM_SHM_NAME_LEN])
{
    int fd;
    void *mapping;
    struct timespec ts;
    int written;

    if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
        perror("clock_gettime");
        exit(1);
    }

    written = snprintf(name_out, COM_SHM_NAME_LEN,
                       "/com_msg_%ld_%lu_%ld",
                       (long)getpid(),
                       g_msg_counter++,
                       (long)ts.tv_nsec);
    if (written < 0 || written >= COM_SHM_NAME_LEN) {
        fprintf(stderr, "create_message_shm: name too long\n");
        exit(1);
    }

    fd = shm_open(name_out, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd == -1) {
        perror("shm_open(create)");
        exit(1);
    }

    if (ftruncate(fd, (off_t)size) == -1) {
        int saved_errno = errno;
        close(fd);
        shm_unlink(name_out);
        errno = saved_errno;
        perror("ftruncate");
        exit(1);
    }

    if (size > 0) {
        mapping = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (mapping == MAP_FAILED) {
            int saved_errno = errno;
            close(fd);
            shm_unlink(name_out);
            errno = saved_errno;
            perror("mmap(message create)");
            exit(1);
        }

        memcpy(mapping, message, size);

        if (munmap(mapping, size) == -1) {
            int saved_errno = errno;
            close(fd);
            shm_unlink(name_out);
            errno = saved_errno;
            perror("munmap(message create)");
            exit(1);
        }
    }

    if (close(fd) == -1) {
        int saved_errno = errno;
        shm_unlink(name_out);
        errno = saved_errno;
        perror("close(message create)");
        exit(1);
    }
}

static void read_message_shm(const char *name, size_t size, void **message_out)
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
            close(fd);
            fprintf(stderr, "malloc failed\n");
            exit(1);
        }

        *message_out = copy;

        if (close(fd) == -1) {
            perror("close(message read empty)");
            exit(1);
        }
        return;
    }

    mapping = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
    if (mapping == MAP_FAILED) {
        int saved_errno = errno;
        close(fd);
        errno = saved_errno;
        perror("mmap(message read)");
        exit(1);
    }

    copy = malloc(size);
    if (copy == NULL) {
        munmap(mapping, size);
        close(fd);
        fprintf(stderr, "malloc failed\n");
        exit(1);
    }

    memcpy(copy, mapping, size);

    if (munmap(mapping, size) == -1) {
        int saved_errno = errno;
        free(copy);
        close(fd);
        errno = saved_errno;
        perror("munmap(message read)");
        exit(1);
    }

    if (close(fd) == -1) {
        int saved_errno = errno;
        free(copy);
        errno = saved_errno;
        perror("close(message read)");
        exit(1);
    }

    *message_out = copy;
}

static void server_loop(void)
{
    ServerMailbox *server_box = &g_shared->server_box;

    for (;;) {
        int dest_rank;
        ClientMailbox *dest_box;

        sem_wait_checked(&server_box->full);

        dest_rank = server_box->dest_rank;
        if (dest_rank == -1) {
            break;
        }

        if (dest_rank < 0 || dest_rank >= USER_COUNT) {
            fprintf(stderr, "server_loop: invalid destination rank\n");
            exit(1);
        }

        dest_box = &g_shared->inbox[dest_rank];
        sem_wait_checked(&dest_box->empty);

        dest_box->src_rank = server_box->src_rank;
        dest_box->size = server_box->size;
        memcpy(dest_box->shm_name, server_box->shm_name, COM_SHM_NAME_LEN);

        sem_post_checked(&dest_box->full);
        sem_wait_checked(&dest_box->consumed);
        sem_post_checked(&server_box->delivered);
    }

    _exit(0);
}

void com_initialize(int nr_proc, int *rank)
{
    if (rank == NULL) {
        fprintf(stderr, "com_initialize: rank is NULL\n");
        exit(1);
    }

    if (nr_proc != USER_COUNT) {
        fprintf(stderr, "com_initialize: this minimal version supports exactly 2 user processes\n");
        exit(1);
    }

    g_shared = create_shared_region(sizeof(SharedState));

    if (sem_init(&g_shared->ctl.finalized_user, 1, 0) == -1) {
        perror("sem_init(finalized_user)");
        exit(1);
    }

    if (sem_init(&g_shared->server_box.empty, 1, 1) == -1) {
        perror("sem_init(server mailbox empty)");
        exit(1);
    }
    if (sem_init(&g_shared->server_box.full, 1, 0) == -1) {
        perror("sem_init(server mailbox full)");
        exit(1);
    }
    if (sem_init(&g_shared->server_box.delivered, 1, 0) == -1) {
        perror("sem_init(server mailbox delivered)");
        exit(1);
    }

    init_mailbox(&g_shared->inbox[0]);
    init_mailbox(&g_shared->inbox[1]);

    g_server_pid = fork();
    if (g_server_pid == -1) {
        perror("fork(server)");
        exit(1);
    }
    if (g_server_pid == 0) {
        g_rank = -1;
        server_loop();
    }

    g_user1_pid = fork();
    if (g_user1_pid == -1) {
        perror("fork(user1)");
        exit(1);
    }
    if (g_user1_pid == 0) {
        g_rank = 1;
        *rank = 1;
        return;
    }

    g_rank = 0;
    *rank = 0;
}

void com_send(int rank, void *msg, size_t size)
{
    char shm_name[COM_SHM_NAME_LEN];
    ServerMailbox *server_box;

    if (g_rank < 0) {
        fprintf(stderr, "com_send: server cannot use user API\n");
        exit(1);
    }
    if (rank < 0 || rank >= USER_COUNT) {
        fprintf(stderr, "com_send: invalid destination rank\n");
        exit(1);
    }
    if (rank == g_rank) {
        fprintf(stderr, "com_send: sending to self is not supported\n");
        exit(1);
    }
    if (size > 0 && msg == NULL) {
        fprintf(stderr, "com_send: msg is NULL but size > 0\n");
        exit(1);
    }

    create_message_shm(msg, size, shm_name);

    server_box = &g_shared->server_box;
    sem_wait_checked(&server_box->empty);

    server_box->src_rank = g_rank;
    server_box->dest_rank = rank;
    server_box->size = size;
    memcpy(server_box->shm_name, shm_name, COM_SHM_NAME_LEN);

    sem_post_checked(&server_box->full);
    sem_wait_checked(&server_box->delivered);
    sem_post_checked(&server_box->empty);

    if (shm_unlink(shm_name) == -1) {
        perror("shm_unlink(message)");
        exit(1);
    }
}

void com_recv(void **msg, size_t *size)
{
    ClientMailbox *box;

    if (g_rank < 0) {
        fprintf(stderr, "com_recv: server cannot use user API\n");
        exit(1);
    }
    if (msg == NULL || size == NULL) {
        fprintf(stderr, "com_recv: invalid arguments\n");
        exit(1);
    }

    box = &g_shared->inbox[g_rank];
    sem_wait_checked(&box->full);

    *size = box->size;
    read_message_shm(box->shm_name, box->size, msg);

    box->src_rank = -1;
    box->size = 0;
    box->shm_name[0] = '\0';

    sem_post_checked(&box->consumed);
    sem_post_checked(&box->empty);
}

void com_mcast(void *msg, size_t size)
{
    if (g_rank < 0) {
        fprintf(stderr, "com_mcast: server cannot use user API\n");
        exit(1);
    }

    com_send(1 - g_rank, msg, size);
}

void com_finalize(void)
{
    ServerMailbox *server_box;

    if (g_rank < 0) {
        fprintf(stderr, "com_finalize: server cannot use user API\n");
        exit(1);
    }

    if (g_rank == 1) {
        sem_post_checked(&g_shared->ctl.finalized_user);

        if (munmap(g_shared, sizeof(SharedState)) == -1) {
            perror("munmap(shared child)");
            exit(1);
        }

        g_shared = NULL;
        return;
    }

    sem_wait_checked(&g_shared->ctl.finalized_user);

    server_box = &g_shared->server_box;
    sem_wait_checked(&server_box->empty);
    server_box->src_rank = -1;
    server_box->dest_rank = -1;
    server_box->size = 0;
    server_box->shm_name[0] = '\0';
    sem_post_checked(&server_box->full);

    if (waitpid(g_server_pid, NULL, 0) == -1) {
        perror("waitpid(server)");
        exit(1);
    }

    if (waitpid(g_user1_pid, NULL, 0) == -1) {
        perror("waitpid(user1)");
        exit(1);
    }

    if (sem_destroy(&g_shared->ctl.finalized_user) == -1) {
        perror("sem_destroy(finalized_user)");
        exit(1);
    }

    if (sem_destroy(&g_shared->server_box.empty) == -1) {
        perror("sem_destroy(server mailbox empty)");
        exit(1);
    }
    if (sem_destroy(&g_shared->server_box.full) == -1) {
        perror("sem_destroy(server mailbox full)");
        exit(1);
    }
    if (sem_destroy(&g_shared->server_box.delivered) == -1) {
        perror("sem_destroy(server mailbox delivered)");
        exit(1);
    }

    destroy_mailbox(&g_shared->inbox[0]);
    destroy_mailbox(&g_shared->inbox[1]);

    if (munmap(g_shared, sizeof(SharedState)) == -1) {
        perror("munmap(shared parent)");
        exit(1);
    }

    g_shared = NULL;
}