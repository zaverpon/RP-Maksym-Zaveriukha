#include "com.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(void)
{
    int rank;

    com_initialize(2, &rank);

    if (rank == 0) {
        const char *msg = "Hello from rank 0 hsjfdhfjkshgkhsdjgkhasdilghasklga";
        com_send(1, (void *)msg, strlen(msg) + 1);
        com_finalize();
        return 0;
    }

    if (rank == 1) {
        void *buffer = NULL;
        size_t size = 0;

        com_recv(&buffer, &size);
        printf("rank 1 received: %s\n", (char *)buffer);
        free(buffer);
        com_finalize();
        return 0;
    }

    return 1;
}
