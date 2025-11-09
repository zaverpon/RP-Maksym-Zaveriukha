/****************************************************************************
*                com.c
*****************************************************************************/

#include "com.h"
#include <stdlib.h>
#include <sys/mman.h>
#include <stdio.h>
#include <semaphore.h>
#include <unistd.h>

/*****************************************************************************
* Local preprocessor defines
******************************************************************************/

/*****************************************************************************
* Local typedefs
******************************************************************************/

/*****************************************************************************
* Local variables
******************************************************************************/

int *shared_counter = NULL; //process counter for issuing rank
sem_t *shared_sem = NULL; //shared semaphore for synchronization

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
* Implementation of functions
******************************************************************************/

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
*   Forks processes given by input, prepares communication over shared memory
*
******************************************************************************/

void com_initialize(int nr_proc, int *rank)
{
    if (nr_proc <= 0 || !rank) {
        fprintf(stderr, "Invalid arguments");
        exit(1);
    }

    void* region = create_shared_region(sizeof(int) + sizeof(sem_t));
    shared_counter = (int*)region;
    shared_sem = (sem_t*)((char*)region + sizeof(int));

    if (sem_init(shared_sem, 1, 1) == -1) {
        perror("sem_init");
        exit(1);
    }
    *shared_counter = 0;

    for (int i = 0 ; i < nr_proc; i++){
        pid_t pid = fork();
        if (pid == -1){
            perror("fork");
            exit(1);
        } else if (pid == 0){
            sem_wait(shared_sem);
            *shared_counter += 1;
            *rank = *shared_counter;
            sem_post(shared_sem);
            printf("Client started: rank=%d, pid=%d\n", *rank, getpid());
            return;
        }
    }

}

/*****************************************************************************
*
* FUNCTION
*
*   com_finalize
*
* INPUT
*
* OUTPUT
*
* RETURNS
*
*   void
*
* DESCRIPTION
*
*   Cleanly terminates processes
*
******************************************************************************/

void com_finalize()
{
}

/*****************************************************************************
*
* FUNCTION
*
*   com_recv
*
* INPUT
*
* OUTPUT
*   void **message - pointer to the received message
*   size_t *size - pointer to the size of the received message
*
* RETURNS
*
*   void
*
* DESCRIPTION
*
*   Receives a message
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
* INPUT
*
*   int rank - rank of the recipient
*   void *message - the message being sent
*   size_t size - size of the message being sent
*
* OUTPUT
*
* RETURNS
*
*   void
*
* DESCRIPTION
*
*   Sends a message
*
******************************************************************************/

void com_send(int rank, void *message, size_t size)
{
}

/*****************************************************************************
*
* FUNCTION
*
*   com_mcast
*
* INPUT
*
* OUTPUT
*   void *message - the message being sent
*   size_t size - size of the message being sent
*
* RETURNS
*
*   void
*
* DESCRIPTION
*
*   Sends (multicasts) a message to all processes except to itself
*
******************************************************************************/

void com_mcast(void *message, size_t size)
{
}
