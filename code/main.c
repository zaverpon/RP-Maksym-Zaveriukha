#include "com.h"
#include <stdio.h>
#include <unistd.h>

int main(void)
{
    int rank;

    com_initialize(5, &rank);

    if (rank == 0) {
        printf("Server: waiting for clients to finish...\n");
        sleep(2); /* simulate some work */
        com_finalize();
        printf("Server: done.\n");
    } else {
        printf("Client %d: doing some work...\n", rank);
        sleep(1 + rank); /* simulate different work times */
        com_finalize();
    }

    return 0;
}