#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "com.h"

#ifdef ENABLE_DEBUG
#define DEBUG(arg) do { arg; } while (0)
#else
#define DEBUG(arg) do { } while (0)
#endif

#define NS_PER_SEC 1000000000L

/* Prints correct usage and exits */
static void die_usage(const char *prog) {
    fprintf(stderr, "Usage: %s <num_processes> <message_size> <repeat_count>\n", prog);
    exit(1);
}

/* Parses positive long value from string */
static long parse_positive_long(const char *text, const char *name) {
    char *end;
    long val = strtol(text, &end, 10);
    if (*end != '\0' || val <= 0) {
        fprintf(stderr, "Invalid %s: %s\n", name, text);
        exit(1);
    }
    return val;
}

/* Parses size_t value from string */
static size_t parse_size_value(const char *text, const char *name) {
    char *end;
    unsigned long long val = strtoull(text, &end, 10);
    if (*end != '\0') {
        fprintf(stderr, "Invalid %s: %s\n", name, text);
        exit(1);
    }
    return (size_t)val;
}

/* Computes elapsed time in seconds */
static double elapsed_seconds(const struct timespec *start,
                              const struct timespec *end) {
    long sec = end->tv_sec - start->tv_sec;
    long nsec = end->tv_nsec - start->tv_nsec;
    return (double)sec + (double)nsec / NS_PER_SEC;
}

/* Fills buffer with predictable test pattern */
static void fill_message(unsigned char *buf, size_t size) {
    for (size_t i = 0; i < size; i++) {
        buf[i] = 'A' + (i % 26);
    }
}

/* Prints raw message content (for debug) */
static void print_message_text(const void *msg, size_t size) {
    fwrite(msg, 1, size, stdout);
}

/* Prints benchmark summary (non-debug mode) */
static void print_benchmark_summary(const char *name,
                                    int proc,
                                    size_t size,
                                    long repeat,
                                    long total,
                                    double time) {
    printf("[%s] proc=%d size=%zu repeat=%ld total=%ld time=%.6f s\n",
           name, proc, size, repeat, total, time);
}

/* Scenario 1: process 0 sends message to all others */
static void run_broadcast_only(int rank,
                               int nr_proc,
                               const unsigned char *buf,
                               size_t size,
                               long repeat) {

    if (rank == 0) {
        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC, &start);

        for (long i = 0; i < repeat; i++) {
            for (int p = 1; p < nr_proc; p++) {
                DEBUG(printf("[DEBUG] 0 -> %d\n", p));
                com_send(p, (void *)buf, size);
            }
        }

        clock_gettime(CLOCK_MONOTONIC, &end);

        print_benchmark_summary("broadcast_only",
                               nr_proc,
                               size,
                               repeat,
                               repeat * (nr_proc - 1),
                               elapsed_seconds(&start, &end));
    } else {
        for (long i = 0; i < repeat; i++) {
            void *msg;
            size_t sz;
            com_recv(&msg, &sz);

            DEBUG(printf("[DEBUG] %d received: ", rank));
            DEBUG(print_message_text(msg, sz); printf("\n"));

            free(msg);
        }
    }
}

/* Scenario 2: process 0 sends and each process replies back */
static void run_broadcast_and_return(int rank,
                                      int nr_proc,
                                      const unsigned char *buf,
                                      size_t size,
                                      long repeat) {

    if (rank == 0) {
        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC, &start);

        for (long i = 0; i < repeat; i++) {
            for (int p = 1; p < nr_proc; p++) {
                /* send to one process */
                com_send(p, (void *)buf, size);

                /* immediately receive reply to avoid deadlock */
                void *reply;
                size_t rsz;
                com_recv(&reply, &rsz);

                DEBUG(printf("[DEBUG] reply from %d\n", p));

                free(reply);
            }
        }

        clock_gettime(CLOCK_MONOTONIC, &end);

        print_benchmark_summary("broadcast_and_return",
                               nr_proc,
                               size,
                               repeat,
                               repeat * (nr_proc - 1) * 2,
                               elapsed_seconds(&start, &end));
    } else {
        for (long i = 0; i < repeat; i++) {
            void *msg;
            size_t sz;

            /* receive message from process 0 */
            com_recv(&msg, &sz);

            DEBUG(printf("[DEBUG] %d got message, sending back\n", rank));

            /* send same message back */
            com_send(0, msg, sz);

            free(msg);
        }
    }
}

int main(int argc, char *argv[]) {

    /* validate arguments */
    if (argc != 4) {
        die_usage(argv[0]);
    }

    /* parse input values */
    long nr_proc_l = parse_positive_long(argv[1], "num_processes");
    size_t msg_size = parse_size_value(argv[2], "message_size");
    long repeat = parse_positive_long(argv[3], "repeat_count");

    if (nr_proc_l < 2) {
        fprintf(stderr, "Need at least 2 processes\n");
        return 1;
    }

    int nr_proc = (int)nr_proc_l;
    int rank;

    /* initialize communication system (forks processes) */
    com_initialize(nr_proc, &rank);

    /* allocate and prepare message buffer */
    unsigned char *buf = malloc(msg_size > 0 ? msg_size : 1);
    fill_message(buf, msg_size);

    DEBUG(printf("[DEBUG] rank=%d started\n", rank));

    /* run test scenarios */
    run_broadcast_only(rank, nr_proc, buf, msg_size, repeat);
    run_broadcast_and_return(rank, nr_proc, buf, msg_size, repeat);

    /* cleanup */
    free(buf);
    com_finalize();

    return 0;
}