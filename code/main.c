#include "com.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/*
 Test scenario:
 - Server rank = 0
 - Clients = 1,2,3,4
 - Client 1 sends to client 2
 - Client 3 sends to client 4
 - Clients 2 and 4 receive messages
*/

int main()
{
    int rank;
    com_initialize(5, &rank);

    if (rank == 0) {
        printf("Server: waiting for clients to finish...\n");
        com_finalize();
        printf("Server: done.\n");
        return 0;
    }

    /* ------------------ CLIENT 1 ------------------ */
    if (rank == 1) {
        const char* msg = "Hello from client 1!";
        com_send(2, (void*)msg, strlen(msg) + 1);
        printf("[Client 1] Sent message to client 2\n");
        sleep(1);
        com_finalize();
        return 0;
    }

    /* ------------------ CLIENT 3 ------------------ */
    if (rank == 3) {
        const char* msg = "Greetings from client 3!";
        com_send(4, (void*)msg, strlen(msg) + 1);
        printf("[Client 3] Sent message to client 4\n");
        sleep(1);
        com_finalize();
        return 0;
    }

    /* ------------------ CLIENT 2 RECEIVER ------------------ */
    if (rank == 2) {
        void* buff;
        size_t size;
        com_recv(&buff, &size);
        printf("[Client 2] Received: '%s'\n", (char*)buff);
        free(buff);
        sleep(1);
        com_finalize();
        return 0;
    }

    /* ------------------ CLIENT 4 RECEIVER ------------------ */
    if (rank == 4) {
        void* buff;
        size_t size;
        com_recv(&buff, &size);
        printf("[Client 4] Received: '%s'\n", (char*)buff);
        free(buff);
        sleep(1);
        com_finalize();
        return 0;
    }

    return 0;
}
