#include "com.h"
#include <stdio.h>
#include <unistd.h>

int main(void)
{
    int rank;

    com_initialize(4, &rank);

    printf("Proccess running: rank: %d , pid: %d\n", rank, getpid());

    sleep(rank + 1);

    printf("Proccess finishing: rank: %d, pid: %d\n", rank, getpid());

    com_finalize();

    return 0;
}