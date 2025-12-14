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
        int i;
        for (i = 0; i < nr_proc; i++) {
            void *buf = NULL;
            size_t size = 0;

            com_recv(&buf, &size);
            printf("Server received: \"%s\"\n", (char *)buf);

            free(buf);
        }

        com_finalize();
        return 0;
    }

    /* ================= CLIENT CODE ================= */
    
        char msg[128];

        snprintf(msg, sizeof(msg),
                 "Hello from client %d (pid=%d)",
                 rank, (int)getpid());

        sleep(rank + 1);

        printf("Client %d sending message\n", rank);
        com_send(-1, msg, strlen(msg) + 1);

        com_finalize();
        return 0;
    
}
