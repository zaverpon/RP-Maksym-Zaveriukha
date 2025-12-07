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
    int nr_proc;       /* number of clients of processes */
    int next_rank;     /* next client rank to assign */
    int live_count;    /* number of alive processes */
    int ready_flag;    /* sync flag after fork */ 
    sem_t reg_sem;     /* protects next_rank and live_count */
    sem_t ready_sem;   /* used for initial barrier during startup */
} Control;

/*****************************************************************************
* Globals
******************************************************************************/

static Control *ctl = NULL; /*pointer to shared memory*/
static int server_rank = -1;
static int g_rank = -1;     /*rank of current proccess*/

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

    /* Allocate and initialize control block in shared memory */
    ctl = create_shared_region(sizeof(Control));
    memset(ctl, 0, sizeof(Control));

    ctl->nr_proc = nr_proc;
    ctl->next_rank = 0;     /* first client will get rank 0 */  
    ctl->live_count = 1;    /* server itself */
    ctl->ready_flag = 0;

    /* Initialize semaphores */
    if (sem_init(&ctl->reg_sem, 1, 1) != 0) {
        perror("sem_init reg_sem");
        exit(1);
    }
    if (sem_init(&ctl->ready_sem, 1, 0) != 0) {
        perror("sem_init ready_sem");
        exit(1);
    }
   
    /* This process is the server (rank -1) before fork */
    *rank = server_rank;
    g_rank = server_rank;
    printf("Server initialized: rank=%d, pid=%d\n", g_rank, (int)getpid());


    /* Fork clients */
    int i;
    for (i = 0; i < nr_proc; i++) {
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
            g_rank = r;
            *rank = r;
            sem_post(&ctl->reg_sem);

            printf("Client started: rank=%d, pid=%d\n", r, getpid());

            /* Notify server that this client is ready */
            sem_post(&ctl->ready_sem);

            /* Wait until server releases all clients */
            while (!ctl->ready_flag)
                sleep(1);

            return;
        }
        /* Parent continues loop to fork more clients */
    }

    /* Server waits until all clients report ready */
    for (i = 0; i < nr_proc; i++)
        sem_wait(&ctl->ready_sem);

    /* Release clients */
    ctl->ready_flag = 1;

    printf("All %d client processes initialized. Total processes=%d\n", nr_proc, nr_proc + 1);
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
    munmap(ctl, sizeof(Control));

    printf("Server finalized.\n");
}

/*****************************************************************************
* com_recv
******************************************************************************/

void com_recv(void **message, size_t *size)
{

}

/*****************************************************************************
* com_send
******************************************************************************/

void com_send(int rank, void *message, size_t size)
{

}

/*****************************************************************************
* com_mcast
******************************************************************************/
void com_mcast(void *msg, size_t size)
{

}


