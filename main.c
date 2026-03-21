#include "com.h"

#include <stdio.h>
#include <stdlib.h>

#define PROC_COUNT 4

static void fill_message(unsigned char *buf, size_t size, int src)
{
    size_t i;

    for (i = 0; i < size; i++) {
        buf[i] = (unsigned char)('A' + ((src + (int)i) % 26));
    }
}

static void print_message(const void *msg, size_t size)
{
    if (size > 0) {
        fwrite(msg, 1, size, stdout);
    }
}

int main(void)
{
    int rank;
    void *msg;
    size_t size;

    com_initialize(PROC_COUNT, &rank);

    if (rank == 0) {
        unsigned char *buffer;

        printf("Process 0: enter message size for process 1: ");
        fflush(stdout);

        if (scanf("%zu", &size) != 1) {
            fprintf(stderr, "Failed to read size\n");
            exit(1);
        }

        buffer = malloc(size);
        if (size > 0 && buffer == NULL) {
            fprintf(stderr, "malloc failed\n");
            exit(1);
        }

        fill_message(buffer, size, rank);

        printf("Process 0 sends to process 1 | size=%zu | message=", size);
        print_message(buffer, size);
        printf("\n");
        fflush(stdout);

        com_send(1, buffer, size);
        free(buffer);
    }
    else if (rank == 1) {
        unsigned char *buffer;

        com_recv(&msg, &size);

        printf("Process 1 received from process 0 | size=%zu | message=", size);
        print_message(msg, size);
        printf("\n");
        fflush(stdout);

        free(msg);

        printf("Process 1: enter message size for process 2: ");
        fflush(stdout);

        if (scanf("%zu", &size) != 1) {
            fprintf(stderr, "Failed to read size\n");
            exit(1);
        }

        buffer = malloc(size);
        if (size > 0 && buffer == NULL) {
            fprintf(stderr, "malloc failed\n");
            exit(1);
        }

        fill_message(buffer, size, rank);

        printf("Process 1 sends to process 2 | size=%zu | message=", size);
        print_message(buffer, size);
        printf("\n");
        fflush(stdout);

        com_send(2, buffer, size);
        free(buffer);
    }
    else if (rank == 2) {
        unsigned char *buffer;

        com_recv(&msg, &size);

        printf("Process 2 received from process 1 | size=%zu | message=", size);
        print_message(msg, size);
        printf("\n");
        fflush(stdout);

        free(msg);

        printf("Process 2: enter message size for process 3: ");
        fflush(stdout);

        if (scanf("%zu", &size) != 1) {
            fprintf(stderr, "Failed to read size\n");
            exit(1);
        }

        buffer = malloc(size);
        if (size > 0 && buffer == NULL) {
            fprintf(stderr, "malloc failed\n");
            exit(1);
        }

        fill_message(buffer, size, rank);

        printf("Process 2 sends to process 3 | size=%zu | message=", size);
        print_message(buffer, size);
        printf("\n");
        fflush(stdout);

        com_send(3, buffer, size);
        free(buffer);
    }
    else if (rank == 3) {
        com_recv(&msg, &size);

        printf("Process 3 received from process 2 | size=%zu | message=", size);
        print_message(msg, size);
        printf("\n");
        fflush(stdout);

        free(msg);
    }

    com_finalize();
    return 0;
}