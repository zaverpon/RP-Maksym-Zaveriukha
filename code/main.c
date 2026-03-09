#include "com.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

static void fill_message(unsigned char *buf, size_t size)
{
    size_t i = 0;
    for ( i = 0; i < size; i++) {
        buf[i] = (unsigned char)( '0' + (i % 10));
    }
}

static bool check_message(const unsigned char *buf, size_t size)
{
    size_t i = 0;
    for ( i = 0; i < size; i++) {
        if (buf[i] != (unsigned char)('0' + (i % 10))) {
            return false;
        }
    }
    return true;
}

int main(void)
{
    int rank;
    size_t tests[] = {
        0, 1, 2, 3, 7, 15, 16, 31, 32, 63, 64,
        127, 128, 255, 256, 511, 512,
        1023, 1024, 2048, 4096, 8192,
        16384, 65536, 262144, 1048576, 9999999
    };
    size_t test_count = sizeof(tests) / sizeof(tests[0]);

    com_initialize(2, &rank);

    if (rank == 0) {
        size_t t = 0;
        for ( t = 0; t < test_count; t++) {
            size_t size = tests[t];
            unsigned char *msg = NULL;

            if (size > 0) {
                msg = malloc(size);
                if (msg == NULL) {
                    fprintf(stderr, "malloc failed on sender\n");
                    exit(1);
                }
                fill_message(msg, size);
            }

            com_send(1, msg, size);

            free(msg);
            printf("SEND OK size=%zu\n", size);
            fflush(stdout);
        }
    } else {
        size_t t = 0;
        for ( t = 0; t < test_count; t++) {
            void *msg = NULL;
            size_t size = 0;

            com_recv(&msg, &size);

            if (size != tests[t]) {
                fprintf(stderr, "FAIL: expected size %zu, got %zu\n", tests[t], size);
                exit(1);
            }

            if (size > 0 && !check_message((unsigned char *)msg, size)) {
                fprintf(stderr, "FAIL: content mismatch for size %zu\n", size);
                free(msg);
                exit(1);
            }

            printf("Message content: ");

            if (size > 0 && size < 255) {
                fwrite(msg, 1, size, stdout);
            }

            free(msg);
            printf(" RECV OK size=%zu\n", size);
            fflush(stdout);
        }
    }

    com_finalize();
    return 0;
}