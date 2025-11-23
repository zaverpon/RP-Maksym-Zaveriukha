/****************************************************************************
*                com.c — fixed shared-memory communication
*****************************************************************************/

#define _XOPEN_SOURCE 700

#include "com.h"
#include <stdlib.h>
#include <sys/mman.h>
#include <stdio.h>
#include <string.h>
#include <semaphore.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>

/*****************************************************************************
* Constants
******************************************************************************/

#define COM_MAX_MSG_SIZE 4096

/*****************************************************************************
* Shared structures
******************************************************************************/

typedef struct {
    int nr_proc;
    int next_rank;
    int live_count;

    volatile int all_ready;

    sem_t reg_sem;
    sem_t ready_sem;
} Control;

typedef struct {
    int full;
    int src_rank;
    int dest_rank;
    size_t size;
    char data[COM_MAX_MSG_SIZE];
    sem_t sem;
} MessageSlot;

/*****************************************************************************
* Globals
******************************************************************************/

static int g_rank = -1;
static Control *ctl = NULL;
static MessageSlot *server_slot = NULL;
static MessageSlot *slots = NULL;

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

void* server_thread(void* arg);

/*****************************************************************************
* com_initialize
******************************************************************************/

void com_initialize(int nr_proc, int *rank)
{
    if (nr_proc <= 0 || !rank) {
        fprintf(stderr, "Invalid arguments\n");
        exit(1);
    }

    /* Control block */
    ctl = create_shared_region(sizeof(Control));
    memset(ctl, 0, sizeof(Control));

    ctl->nr_proc = nr_proc;
    ctl->next_rank = 1;
    ctl->live_count = 1;
    ctl->all_ready = 0;

    sem_init(&ctl->reg_sem,   1, 1);
    sem_init(&ctl->ready_sem, 1, 0);

    /* Server slot */
    server_slot = create_shared_region(sizeof(MessageSlot));
    memset(server_slot, 0, sizeof(MessageSlot));
    sem_init(&server_slot->sem, 1, 0);

    /* Per-client slots */
    slots = create_shared_region(sizeof(MessageSlot) * nr_proc);
    int i = 0;
    for (i = 0; i < nr_proc; i++) {
        memset(&slots[i], 0, sizeof(MessageSlot));
        sem_init(&slots[i].sem, 1, 0);
    }

    /* Server */
    g_rank = 0;
    *rank = 0;
    printf("Server initialized: rank=0, pid=%d\n", getpid());

    /* Start router thread BEFORE fork() */
    pthread_t tid;
    pthread_create(&tid, NULL, server_thread, NULL);
    pthread_detach(tid);

    /* Fork clients */
    for (i = 1; i < nr_proc; i++) {
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

            printf("Client started: rank=%d, pid=%d\n", r, getpid());

            sem_post(&ctl->ready_sem);

            while (!ctl->all_ready)
                sleep_ms(1);

            return;
        }
    }

    /* Server waits for all clients */
    for (i = 1; i < nr_proc; i++)
        sem_wait(&ctl->ready_sem);

    printf("All %d processes registered.\n", nr_proc);

    ctl->all_ready = 1;

    printf("All %d processes initialized.\n", nr_proc);
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

    if (g_rank != 0) {
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

    munmap(ctl, sizeof(Control));

    printf("Server finalized.\n");
}

/*****************************************************************************
* com_recv
******************************************************************************/

void com_recv(void **message, size_t *size)
{
    MessageSlot *slot = &slots[g_rank];

    sem_wait(&slot->sem);

    *size = slot->size;
    void *tmp = malloc(*size);
    memcpy(tmp, slot->data, *size);
    *message = tmp;

    slot->full = 0;
    slot->size = 0;
    slot->src_rank = -1;
    slot->dest_rank = -1;
}

/*****************************************************************************
* com_send
******************************************************************************/

void com_send(int rank, void *message, size_t size)
{
    if (size > COM_MAX_MSG_SIZE) {
        fprintf(stderr, "Message too large (%zu > %d)\n",
                size, COM_MAX_MSG_SIZE);
        exit(1);
    }

    MessageSlot *slot = server_slot;

    while (__sync_lock_test_and_set(&slot->full, 1) == 1)
        sleep(1);

    slot->src_rank  = g_rank;
    slot->dest_rank = rank;
    slot->size      = size;
    memcpy(slot->data, message, size);

    sem_post(&slot->sem);
}

/*****************************************************************************
* SERVER THREAD — router
******************************************************************************/

void* server_thread(void* arg)
{
    while (1) {
        sem_wait(&server_slot->sem);

        sem_wait(&ctl->reg_sem);
        int alive = ctl->live_count;
        sem_post(&ctl->reg_sem);

        if (alive <= 1)
            break;

        int dest = server_slot->dest_rank;
        MessageSlot *client_slot = &slots[dest];

        while (__sync_lock_test_and_set(&client_slot->full, 1) == 1)
            sleep(1);

        client_slot->src_rank  = server_slot->src_rank;
        client_slot->dest_rank = dest;
        client_slot->size      = server_slot->size;
        memcpy(client_slot->data, server_slot->data, server_slot->size);

        sem_post(&client_slot->sem);

        server_slot->full = 0;
        server_slot->size = 0;
    }

    return NULL;
}
