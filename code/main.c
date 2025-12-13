#include "com.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int main(void)
{
    int rank;
    int nr_proc = 3;  

    com_initialize(nr_proc, &rank);

    /* ================= CLIENT CODE ================= */
    if (rank >= 0) {
        char msg[128];

        snprintf(msg, sizeof(msg),
                 "Hello from client %d (pid=%d)",
                 rank, getpid());

        sleep(rank + 1);

        printf("Client %d sending message\n", rank);
        com_send(-1, msg, strlen(msg) + 1);

        com_finalize();
        return 0;
    }

    /* ================= SERVER CODE ================= */
    if (rank == -1) {
        for (int i = 0; i < nr_proc; i++) {
            void *buf;
            size_t size;

            com_recv(&buf, &size);

            printf("Server received: \"%s\"\n", (char *)buf);

            free(buf);
        }

        com_finalize();
    }

    return 0;
}