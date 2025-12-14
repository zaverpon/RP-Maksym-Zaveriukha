/****************************************************************************
* com.c — shared-memory communication 
*****************************************************************************/

#include "com.h"
#include <stdlib.h>
#include <sys/mman.h>
#include <stdio.h>
#include <string.h>
#include <semaphore.h>
#include <unistd.h>

#define COM_MAX_MSG 1024

typedef struct {
    int nr_proc;
    int total;       /* nr_proc + 1 (including server) */
    int next_rank;
    int live_count;

    sem_t reg_sem;
    sem_t ready_sem;   /* clients -> server */
    sem_t start_sem;   /* server -> clients */
} Control;

typedef struct {
    sem_t empty;   /* 1 if empty */
    sem_t full;    /* 1 if full  */

    int sender_rank;
    size_t size;
    unsigned char data[COM_MAX_MSG];
} InboxSlot;

static Control *ctl = NULL;
static InboxSlot *slots = NULL;
static int g_rank = -1;

static void *create_shared_region(size_t size)
{
    void *region;

    region = mmap(NULL, size,
                  PROT_READ | PROT_WRITE,
                  MAP_SHARED | MAP_ANONYMOUS,
                  -1, 0);
    if (region == MAP_FAILED) {
        perror("mmap");
        exit(1);
    }
    return region;
}


void com_initialize(int nr_proc, int *rank)
{
    int i;
    int r;
    pid_t pid;

    if (nr_proc <= 0 || !rank) {
        fprintf(stderr, "Invalid arguments\n");
        exit(1);
    }

    ctl = (Control *)create_shared_region(sizeof(Control));
    memset(ctl, 0, sizeof(Control));

    ctl->nr_proc = nr_proc;
    ctl->total = nr_proc + 1;
    ctl->next_rank = 0;
    ctl->live_count = 1; /* server */

    /* allocate inbox slots for ALL processes: clients + server */
    slots = (InboxSlot *)create_shared_region(sizeof(InboxSlot) * (size_t)ctl->total);
    memset(slots, 0, sizeof(InboxSlot) * (size_t)ctl->total);

    if (sem_init(&ctl->reg_sem, 1, 1) != 0) { perror("sem_init reg_sem"); exit(1); }
    if (sem_init(&ctl->ready_sem, 1, 0) != 0) { perror("sem_init ready_sem"); exit(1); }
    if (sem_init(&ctl->start_sem, 1, 0) != 0) { perror("sem_init start_sem"); exit(1); }

    for (i = 0; i < ctl->total; i++) {
        if (sem_init(&slots[i].empty, 1, 1) != 0) { perror("sem_init inbox empty"); exit(1); }
        if (sem_init(&slots[i].full,  1, 0) != 0) { perror("sem_init inbox full"); exit(1); }
        slots[i].sender_rank = -2;
        slots[i].size = 0;
    }

    /* server rank */
    g_rank = -1;
    *rank = -1;
    printf("Server initialized: rank=%d, pid=%d\n", g_rank, (int)getpid());

    /* Fork clients */
    for (i = 0; i < nr_proc; i++) {
        pid = fork();
        if (pid < 0) {
            perror("fork");
            exit(1);
        }

        if (pid == 0) {
            /* CHILD: assign rank and mark alive */
            sem_wait(&ctl->reg_sem);
            r = ctl->next_rank++;
            ctl->live_count++;
            sem_post(&ctl->reg_sem);

            g_rank = r;
            *rank = r;

            printf("Client started: rank=%d, pid=%d\n", r, (int)getpid());

            sem_post(&ctl->ready_sem);
            sem_wait(&ctl->start_sem);

            return; /* child continues in main */
        }
    }

    /* server waits until all clients ready */
    for (i = 0; i < nr_proc; i++) sem_wait(&ctl->ready_sem);

    /* release clients */
    for (i = 0; i < nr_proc; i++) sem_post(&ctl->start_sem);

    printf("All %d client processes initialized. Total processes=%d\n",
           nr_proc, nr_proc + 1);
}

void com_finalize(void)
{
    int alive;
    int i;

    if (!ctl) return;

    if (g_rank != -1) {
        sem_wait(&ctl->reg_sem);
        ctl->live_count--;
        alive = ctl->live_count;
        sem_post(&ctl->reg_sem);

        printf("Client (rank %d) finalized. Alive=%d\n", g_rank, alive);
        return;
    }

    /* server waits all clients exit */
    for (;;) {
        sem_wait(&ctl->reg_sem);
        alive = ctl->live_count;
        sem_post(&ctl->reg_sem);

        if (alive == 1) break;
        sleep(1);
    }

    printf("All clients terminated. Cleaning up...\n");

    for (i = 0; i < ctl->total; i++) {
        sem_destroy(&slots[i].empty);
        sem_destroy(&slots[i].full);
    }

    sem_destroy(&ctl->reg_sem);
    sem_destroy(&ctl->ready_sem);
    sem_destroy(&ctl->start_sem);

    munmap(slots, sizeof(InboxSlot) * (size_t)ctl->total);
    munmap(ctl, sizeof(Control));

    slots = NULL;
    ctl = NULL;

    printf("Server finalized.\n");
}

void com_send(int rank, void *message, size_t size)
{
    int dest_idx;
    InboxSlot *s;

    if (!message || size == 0) {
        fprintf(stderr, "com_send: invalid message\n");
        exit(1);
    }
    if (size > (size_t)COM_MAX_MSG) {
        fprintf(stderr, "com_send: message too large\n");
        exit(1);
    }

    if (rank == -1) dest_idx = ctl->nr_proc;
    else dest_idx = rank;   
    
    if (dest_idx < 0) {
        fprintf(stderr, "com_send: invalid destination rank\n");
        exit(1);
    }

    s = &slots[dest_idx];

    /* wait until receiver's inbox is empty */
    sem_wait(&s->empty);

    memcpy(s->data, message, size);
    s->size = size;
    s->sender_rank = g_rank;

    /* mark inbox as full (receiver can read) */
    sem_post(&s->full);
}

void com_recv(void **message, size_t *size)
{
  
}

void com_mcast(void *msg_buf, size_t size)
{

}
