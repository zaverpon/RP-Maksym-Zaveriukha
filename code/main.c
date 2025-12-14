#include "com.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>

int main(void)
{
    int rank;
    int nr_proc = 3;

    com_initialize(nr_proc, &rank);

    /* ================= SERVER CODE ================= */
    if (rank == -1) {
        com_finalize();
        return 0;
    }    
}
