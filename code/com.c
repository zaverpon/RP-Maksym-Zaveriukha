/****************************************************************************
*                com.c —  shared-memory communication
*****************************************************************************/

#include "com.h"
#include <stdlib.h>
#include <sys/mman.h>
#include <stdio.h>
#include <string.h>
#include <semaphore.h>
#include <unistd.h>
#include <time.h>

/*****************************************************************************
* Shared structures
******************************************************************************/

typedef struct {
    int nr_proc;       /* number of client processes */
    int next_rank;     /* next client rank to assign */
    int live_count;    /* number of alive processes */
    int ready_flag;    /* sync flag after fork */
    sem_t reg_sem;     /* protects next_rank and live_count */
    sem_t ready_sem;   /* used for initial barrier during startup */
} Control;

/* Message slot structure */
typedef struct {
    volatile int full;   /* 0 = empty, 1 = message available */
    int sender_rank;
    size_t size;
    void *addr;          /* mmap address of message */
} MessageSlot;

/*****************************************************************************
* Globals
******************************************************************************/

static Control *ctl = NULL;       /* shared control block */
static MessageSlot *msg = NULL;  /* shared message slot */

static int server_rank = -1;
static int g_rank = -1;

/*****************************************************************************
* Shared memory allocator
******************************************************************************/

static void *create_shared_region(size_t size)
{
    void *region = mmap(NULL, size,
                        PROT_READ | PROT_WRITE,
                        MAP_SHARED | MAP_ANONYMOUS,
                        -1, 0);
    if (region == MAP_FAILED) {
        perror("mmap");
        exit(1);
    }
    return region;
}

/*****************************************************************************
* com_initialize
******************************************************************************/

void com_initialize(int nr_proc, int *rank)
{
    if (nr_proc <= 0 || !rank) {
        fprintf(stderr, "Invalid arguments\n");
        exit(1);
    }

    /* Allocate and initialize control block */
    ctl = create_shared_region(sizeof(Control));
    memset(ctl, 0, sizeof(Control));

    /* Allocate and initialize message slot */
    msg = create_shared_region(sizeof(MessageSlot));
    memset(msg, 0, sizeof(MessageSlot));

    ctl->nr_proc = nr_proc;
    ctl->next_rank = 0;
    ctl->live_count = 1; /* server */
    ctl->ready_flag = 0;

    if (sem_init(&ctl->reg_sem, 1, 1) != 0) {
        perror("sem_init reg_sem");
        exit(1);
    }
    if (sem_init(&ctl->ready_sem, 1, 0) != 0) {
        perror("sem_init ready_sem");
        exit(1);
    }

    *rank = server_rank;
    g_rank = server_rank;

    printf("Server initialized: rank=%d, pid=%d\n",
           g_rank, (int)getpid());

    /* Fork clients */
    for (int i = 0; i < nr_proc; i++) {
        pid_t pid = fork();

        if (pid < 0) {
            perror("fork");
            exit(1);
        }
        else if (pid == 0) {
            /* CHILD */
            sem_wait(&ctl->reg_sem);
            int r = ctl->next_rank++;
            ctl->live_count++;
            sem_post(&ctl->reg_sem);

            g_rank = r;
            *rank = r;

            printf("Client started: rank=%d, pid=%d\n",
                   r, (int)getpid());

            sem_post(&ctl->ready_sem);

            while (!ctl->ready_flag)
                sleep(1);

            return;
        }
    }

    /* Server waits for all clients */
    for (int i = 0; i < nr_proc; i++)
        sem_wait(&ctl->ready_sem);

    ctl->ready_flag = 1;

    printf("All %d client processes initialized. Total processes=%d\n",
           nr_proc, nr_proc + 1);
}

/*****************************************************************************
* com_finalize
******************************************************************************/

void com_finalize(void)
{
    if (!ctl)
        return;

    if (g_rank != -1) {
        /* client */
        sem_wait(&ctl->reg_sem);
        ctl->live_count--;
        int alive = ctl->live_count;
        sem_post(&ctl->reg_sem);

        printf("Client (rank %d) finalized. Alive=%d\n",
               g_rank, alive);
        return;
    }

    /* server */
    while (1) {
        sem_wait(&ctl->reg_sem);
        int alive = ctl->live_count;
        sem_post(&ctl->reg_sem);

        if (alive == 1)
            break;

        sleep(1);
    }

    printf("All clients terminated. Cleaning up...\n");

    sem_destroy(&ctl->reg_sem);
    sem_destroy(&ctl->ready_sem);

    munmap(msg, sizeof(MessageSlot));
    munmap(ctl, sizeof(Control));

    printf("Server finalized.\n");
}

/*****************************************************************************
* com_send
******************************************************************************/

void com_send(int rank, void *message, size_t size)
{
    (void)rank; /* not used in this simple version */

    /* wait until message slot is free */
    while (msg->full) {
        usleep(1000); /* 1 ms */
    }

    void *buf = mmap(NULL, size,
                     PROT_READ | PROT_WRITE,
                     MAP_SHARED | MAP_ANONYMOUS,
                     -1, 0);
    if (buf == MAP_FAILED) {
        perror("mmap message");
        exit(1);
    }

    memcpy(buf, message, size);

    msg->addr = buf;
    msg->size = size;
    msg->sender_rank = g_rank;

    /* publish LAST */
    msg->full = 1;
}

/*****************************************************************************
* com_recv
******************************************************************************/

void com_recv(void **message, size_t *size)
{
    /* wait for message */
    while (!msg->full) {
        usleep(1000); /* 1 ms */
    }

    *size = msg->size;

    *message = malloc(msg->size);
    if (!*message) {
        perror("malloc");
        exit(1);
    }

    memcpy(*message, msg->addr, msg->size);

    munmap(msg->addr, msg->size);

    msg->full = 0;
}

/*****************************************************************************
* com_mcast
******************************************************************************/

void com_mcast(void *msg_buf, size_t size)
{
    
}