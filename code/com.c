/****************************************************************************
*                com.c — multi-slot shared-memory IPC (fixed)
*****************************************************************************/

#include "com.h"
#include <stdlib.h>
#include <sys/mman.h>
#include <stdio.h>
#include <string.h>
#include <semaphore.h>
#include <unistd.h>
#include <pthread.h>

/*****************************************************************************
* Constants
******************************************************************************/

#define COM_MAX_MSG_SIZE 4096

/*****************************************************************************
* Shared structures
******************************************************************************/

/* Global control block (shared by all processes) */
typedef struct {
    int nr_proc;        /* total number of processes (including server rank 0) */
    int next_rank;      /* next rank to assign after fork */
    int live_count;     /* how many processes are still alive */
    volatile int ready; /* barrier flag to release all clients */

    sem_t reg_sem;      /* protects next_rank + live_count */
    sem_t ready_sem;    /* startup barrier for children */
    sem_t msg_sem;      /* counts messages waiting to be routed */
} Control;

/*
 * Each message slot is used either:
 *  - in server_in[rank]:  client -> server
 *  - in client_in[rank]:  server -> client
 *
 * sem_empty: 1 when slot is free, 0 when occupied.
 * sem_full : used only for client_in[*] to wake receivers.
 */
typedef struct {
    int full;                    /* 1 = has message, 0 = empty */
    int src_rank;
    int dest_rank;
    size_t size;
    char data[COM_MAX_MSG_SIZE];

    sem_t sem_empty;             /* "slot free" semaphore */
    sem_t sem_full;              /* "message ready" semaphore (used by receivers) */
} MessageSlot;

/*****************************************************************************
* Globals
******************************************************************************/

static int g_rank = -1;
static Control *ctl = NULL;

/* server_in[rank]: slot where client with given rank sends to the server */
static MessageSlot *server_in = NULL;

/* client_in[rank]: inbox for messages delivered to this rank */
static MessageSlot *client_in = NULL;

/*****************************************************************************
* Shared memory helper
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
* Server routing thread
******************************************************************************/

 void* server_thread(void* arg)
{
    int i;

    while (1) {
        /* Wait until at least one client has posted a message */
        sem_wait(&ctl->msg_sem);

        /* Find one full server_in slot and route it */
        for (i = 0; i < ctl->nr_proc; i++) {
            MessageSlot *in = &server_in[i];

            if (in->full == 0)
                continue;

            /* We found a message from rank i */
            int dest = in->dest_rank;
            if (dest < 0 || dest >= ctl->nr_proc) {
                fprintf(stderr, "Invalid destination rank %d\n", dest);
                /* Mark this slot free to avoid deadlock */
                in->full = 0;
                sem_post(&in->sem_empty);
                break;
            }

            MessageSlot *out = &client_in[dest];

            /* Wait until destination inbox is empty */
            sem_wait(&out->sem_empty);

            /* Copy payload to receiver's inbox */
            out->src_rank  = in->src_rank;
            out->dest_rank = dest;
            out->size      = in->size;
            memcpy(out->data, in->data, in->size);
            out->full = 1;

            /* Wake up destination client */
            sem_post(&out->sem_full);

            /* Mark sender slot as empty again */
            in->full = 0;
            sem_post(&in->sem_empty);

            break; /* handle exactly one message per msg_sem token */
        }
    }
    return NULL;
}

/*****************************************************************************
* com_initialize
******************************************************************************/

void com_initialize(int nr_proc, int *rank)
{
    int i;

    if (!rank || nr_proc <= 0) {
        fprintf(stderr, "com_initialize: invalid arguments\n");
        exit(1);
    }

    /* Allocate and initialize control block */
    ctl = create_shared_region(sizeof(Control));
    memset(ctl, 0, sizeof(Control));

    ctl->nr_proc    = nr_proc;
    ctl->next_rank  = 1;   /* rank 0 is server, others are clients */
    ctl->live_count = 1;   /* server itself */
    ctl->ready      = 0;

    sem_init(&ctl->reg_sem,   1, 1);
    sem_init(&ctl->ready_sem, 1, 0);
    sem_init(&ctl->msg_sem,   1, 0);

    /* Allocate per-rank slots */
    server_in = create_shared_region(sizeof(MessageSlot) * nr_proc);
    client_in = create_shared_region(sizeof(MessageSlot) * nr_proc);

    for (i = 0; i < nr_proc; i++) {
        memset(&server_in[i], 0, sizeof(MessageSlot));
        memset(&client_in[i], 0, sizeof(MessageSlot));

        /* server_in: only sem_empty is used, sem_full unused */
        sem_init(&server_in[i].sem_empty, 1, 1); /* initially empty */
        sem_init(&server_in[i].sem_full,  1, 0); /* unused; keep for symmetry */

        /* client_in: both sem_empty and sem_full are used */
        sem_init(&client_in[i].sem_empty, 1, 1); /* inbox initially empty */
        sem_init(&client_in[i].sem_full,  1, 0); /* no message yet */
    }

    /* This process becomes the server (rank 0) */
    g_rank = 0;
    *rank  = 0;
    printf("Server initialized: rank=0\n");

    /* Start routing thread BEFORE fork() so only server has it */
    {
        pthread_t tid;
        pthread_create(&tid, NULL, server_thread, NULL);
        pthread_detach(tid);
    }

    /* Fork client processes */
    for (i = 1; i < nr_proc; i++) {
        pid_t pid = fork();

        if (pid < 0) {
            perror("fork");
            exit(1);
        }
        else if (pid == 0) {
            /* CHILD process: becomes a client */
            int r;

            sem_wait(&ctl->reg_sem);
            r = ctl->next_rank++;
            ctl->live_count++;
            sem_post(&ctl->reg_sem);

            g_rank = r;
            *rank  = r;

            printf("Client started: rank=%d\n", r);

            /* Notify server we are created */
            sem_post(&ctl->ready_sem);

            /* Wait until server releases all clients */
            while (!ctl->ready) {
                /* small pause to avoid hot spinning */
                usleep(1000);
            }

            return; /* return to main() in this client */
        }
    }

    /* Server waits until all clients have started */
    for (i = 1; i < nr_proc; i++)
        sem_wait(&ctl->ready_sem);

    /* Release clients from startup barrier */
    ctl->ready = 1;
    printf("All processes initialized\n");
}

/*****************************************************************************
* com_send
******************************************************************************/

void com_send(int rank, void *message, size_t size)
{
    MessageSlot *s;

    if (g_rank < 0) {
        fprintf(stderr, "com_send called before com_initialize\n");
        exit(1);
    }

    if (rank < 0 || rank >= ctl->nr_proc) {
        fprintf(stderr, "com_send: invalid destination rank %d\n", rank);
        exit(1);
    }

    if (size > COM_MAX_MSG_SIZE) {
        fprintf(stderr, "com_send: message too large (%zu > %d)\n",
                (size_t)size, COM_MAX_MSG_SIZE);
        exit(1);
    }

    /* Each rank uses its own server_in[rank] slot */
    s = &server_in[g_rank];

    /* Wait until this slot is free (no previous message pending) */
    sem_wait(&s->sem_empty);

    s->src_rank  = g_rank;
    s->dest_rank = rank;
    s->size      = size;
    memcpy(s->data, message, size);
    s->full = 1;

    /* Notify routing thread that one more message is pending */
    sem_post(&ctl->msg_sem);
}

/*****************************************************************************
* com_recv
******************************************************************************/

void com_recv(void **msg, size_t *size)
{
    MessageSlot *s;

    if (g_rank < 0) {
        fprintf(stderr, "com_recv called before com_initialize\n");
        exit(1);
    }

    s = &client_in[g_rank];

    /* Wait until server delivers a message for this rank */
    sem_wait(&s->sem_full);

    *size = s->size;
    *msg  = malloc(*size);
    if (!*msg) {
        perror("malloc");
        exit(1);
    }
    memcpy(*msg, s->data, *size);

    s->full = 0;

    /* Mark inbox free again */
    sem_post(&s->sem_empty);
}

/*****************************************************************************
* com_finalize
******************************************************************************/

void com_finalize(void)
{
    if (!ctl) {
        fprintf(stderr, "com_finalize called before com_initialize\n");
        exit(1);
    }

    /* Client path */
    if (g_rank != 0) {
        sem_wait(&ctl->reg_sem);
        ctl->live_count--;
        sem_post(&ctl->reg_sem);
        return;
    }

    /* Server path: wait until only server remains */
    while (1) {
        int alive;

        sem_wait(&ctl->reg_sem);
        alive = ctl->live_count;
        sem_post(&ctl->reg_sem);

        if (alive == 1)
            break;

        usleep(1000);
    }

    printf("Server finalized\n");
}

/*****************************************************************************
* Optional broadcast stub
******************************************************************************/

void com_mcast(void *msg, size_t size)
{
  
}
