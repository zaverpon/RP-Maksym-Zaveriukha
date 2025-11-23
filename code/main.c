#include "com.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

int main()
{
    int rank;
    com_initialize(3, &rank);

    if (rank == 0) {
        printf("Server: waiting for clients to finish...\n");
        com_finalize();
        printf("Server: done.\n");
        return 0;
    }

    if (rank == 1) {
        const char* msg = "Hello from client 1!\n";
        com_send(2, (void*)msg, strlen(msg) + 1);
        printf("Client 1: message sent.\n");
        sleep(1);
    }

    if (rank == 2) {
        void* buff;
        size_t size;

        com_recv(&buff, &size);
        printf("Client 2: received message:'%s'\n", (char*)buff);
        free(buff);
        sleep(1);
    }

    com_finalize();
    return 0;
}
