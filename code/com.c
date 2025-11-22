/****************************************************************************
*                com.c
*****************************************************************************/

#include "com.h"
#include <stdlib.h>
#include <sys/mman.h>
#include <stdio.h>
#include <string.h>     
#include <semaphore.h>
#include <unistd.h>

/*****************************************************************************
* Local typedefs
******************************************************************************/

typedef struct {
    int nr_proc;        /* total number of processes (including server)*/
    int next_rank;      /* counter for assigning ranks*/
    int live_count;     /* number of active processes*/
    sem_t reg_sem;      /* protects registration (next_rank)*/
    sem_t ready_sem;    /* used to signal clients that rank is assigned*/
} Control;

typedef struct {
    int full;
    int src_rank;
    int dest_rank;
    size_t size;
    void *shm_ptr;
    sem_t sem;
} MessageSlot;


/*****************************************************************************
* Local variables
******************************************************************************/
static int g_rank = -1;
static Control *ctl = NULL;    /* shared control structure*/
static MessageSlot *server_slot = NULL;
static MessageSlot *slots = NULL;

/* --------------------------------------------------------------------------
   Support function for creating shared memory
   -------------------------------------------------------------------------- */

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
*
* FUNCTION
*
*   com_initialize
*
* INPUT
*
*   int nr_proc - number of processes
*
* OUTPUT
*
*   int *rank - pointer to the rank of a process (0... N-1)
*
* RETURNS
*
*   void
*
* DESCRIPTION
*
*   Forks processes, sets up shared memory and synchronization objects.
*   Server initializes shared control block and semaphores.
*   Clients inherit mapping and register with the server to obtain rank.
*
******************************************************************************/

void com_initialize(int nr_proc, int *rank)
{
    if (nr_proc <= 0 || !rank) {
        fprintf(stderr, "Invalid arguments\n");
        exit(1);
    }

    /* Step 1: server creates shared control region */
    ctl = create_shared_region(sizeof(Control));
    memset(ctl, 0, sizeof(Control));

    ctl->nr_proc = nr_proc;
    ctl->next_rank = 1;      
    ctl->live_count = 1;   

    if (sem_init(&ctl->reg_sem, 1, 1) == -1) {
        perror("sem_init(reg_sem)");
        exit(1);
    }
    if (sem_init(&ctl->ready_sem, 1, 0) == -1) {
        perror("sem_init(ready_sem)");
        exit(1);
    }

    /* Server is rank 0 */
    *rank = 0;
    g_rank = 0;
    printf("Server initialized: rank=%d, pid=%d\n", *rank, getpid());
    int i = 0;
    /* Step 2: fork clients */
    for (i = 1; i < nr_proc; i++) {
        pid_t pid = fork();
        if (pid == -1) {
            perror("fork");
            exit(1);
        } else if (pid == 0) {
            /* --- Child (client) --- */
            
            sem_wait(&ctl->reg_sem);
            int my_rank = ctl->next_rank++;
            ctl->live_count++;
            sem_post(&ctl->reg_sem);

            *rank = my_rank;
            g_rank = my_rank;
            printf("Client started: rank=%d, pid=%d\n", *rank, getpid());

           
            sem_post(&ctl->ready_sem);
            return; 
        }
    }

    /* allocate slot for server inbox */
    server_slot = create_shared_region(sizeof(MessageSlot));
    memset(server_slot, 0, sizeof(MessageSlot));
    sem_init(&server_slot->sem, 1, 0);

    /* allocate slot for server inbox */
    slots = create_shared_region(sizeof(MessageSlot) * nr_proc);
    for (int i = 0; i < nr_proc; i++){
        slots[i].full = 0;
        slots[i].size = 0;
        slots[i].src_rank = -1;
        slots[i].dest_rank = -1;
        slots[i].shm_ptr = NULL;
        sem_init(&slots[i].sem, 1, 0);
    }



    /* --- Parent (server) continues --- */
    for (i = 1; i < nr_proc; i++) {
        sem_wait(&ctl->ready_sem);
    }

    printf("All %d processes initialized.\n", nr_proc);
}

/*****************************************************************************
*
* FUNCTION
*
*   com_finalize
*
* DESCRIPTION
*
*   Cleanly terminates processes
*
******************************************************************************/

void com_finalize(void)
{
    if (ctl == NULL){
        fprintf(stderr, "com_finalize called before com_initialize");
        exit(1);
    }
    if (g_rank != 0){
        sem_wait(&ctl->reg_sem);
        ctl->live_count--;
        int alive = ctl->live_count;
        sem_post(&ctl->reg_sem);

        printf("Client (rank %d, pid %d) finalized. Alive=%d\n",
               g_rank, getpid(), alive);
        return;
    }

    for (;;) {
        sem_wait(&ctl->reg_sem);
        int alive = ctl->live_count;
        sem_post(&ctl->reg_sem);

        if (alive == 1)
            break;

        usleep(1000); 
    }

    printf("All clients terminated. Cleaning up...\n");

    if (sem_destroy(&ctl->reg_sem) == -1) {
        perror("sem_destroy(reg_sem)");
    }

    if (sem_destroy(&ctl->ready_sem) == -1) {
        perror("sem_destroy(ready_sem)");
    }

    if (munmap(ctl, sizeof(Control)) == -1) {
        perror("munmap");
    }

    printf("Server finalized. Resources released.\n");

}

/*****************************************************************************
*
* FUNCTION
*
*   com_recv
*
******************************************************************************/

void com_recv(void **message, size_t *size)
{

}

/*****************************************************************************
*
* FUNCTION
*
*   com_send
*
******************************************************************************/

void com_send(int rank, void *message, size_t size)
{
    if (ctl == NULL) {
        printf(stderr, "com_send: library not initialized \n");
        exit(1);
    }

    if ( message == NULL || size == 0){
        fprintf(stderr, "com_send: Invalid message\n");
        exit(1);
    }

    if (rank < 0 || rank >= ctl->nr_proc){
        fprintf(stderr, "com_send: Invalid rank of proccess\n");
        exit(1);
    }

    MessageSlot *slot = server_slot;

    /* Wait until slot is free */
    while (__sync_lock_test_and_set(&slot->full, 1) == 1) {
        usleep(1000);
    }

    /* Allocate shared memory for payload */
    void *payload = create_shared_region(size);
    memcpy(payload, message, size);

    slot->size = size;
    slot->src_rank = g_rank;
    slot->dest_rank = rank;
    slot->shm_ptr = payload;

    /* Notify server there is a new message */
    sem_post(&slot->sem);
}

/*****************************************************************************
*
* FUNCTION
*
*   com_mcast
*
******************************************************************************/

void com_mcast(void *message, size_t size)
{
}