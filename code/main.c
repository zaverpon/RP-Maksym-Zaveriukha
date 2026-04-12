#define _POSIX_C_SOURCE 200809L

#include <math.h>
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

typedef struct {
    long n;
    double mean;
    double M2;
} WelfordStats;

/* Prints correct usage and exits */
static void die_usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s <num_processes> <message_size> <repeat_count> <benchmark_runs>\n",
            prog);
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
    size_t i;
    for (i = 0; i < size; i++) {
        buf[i] = 'A' + (i % 26);
    }
}

/* Welford init */
static void welford_init(WelfordStats *s) {
    s->n = 0;
    s->mean = 0.0;
    s->M2 = 0.0;
}

/* Welford online update */
static void welford_update(WelfordStats *s, double x) {
    double delta;
    double delta2;

    s->n++;
    delta = x - s->mean;
    s->mean += delta / (double)s->n;
    delta2 = x - s->mean;
    s->M2 += delta * delta2;
}

static double welford_sample_variance(const WelfordStats *s) {
    if (s->n < 2) {
        return 0.0;
    }
    return s->M2 / (double)(s->n - 1);
}

static double welford_stddev(const WelfordStats *s) {
    return sqrt(welford_sample_variance(s));
}

/* prepare sender-owned shm buffer, fill it, then send */
static void send_prepared_pattern(int dest_rank, size_t size) {
    unsigned char *send_buf = com_prepare_send_buffer(size);

    if (size > 0 && send_buf == NULL) {
        fprintf(stderr, "com_prepare_send_buffer returned NULL for non-zero size\n");
        exit(1);
    }

    if (size > 0) {
        fill_message(send_buf, size);
    }

    com_send(dest_rank, size);
}

/* copy received message into prepared shm buffer, then send */
static void send_reply_copy(int dest_rank, const void *msg, size_t size) {
    void *send_buf = com_prepare_send_buffer(size);

    if (size > 0 && send_buf == NULL) {
        fprintf(stderr, "com_prepare_send_buffer returned NULL for non-zero size\n");
        exit(1);
    }

    if (size > 0) {
        memcpy(send_buf, msg, size);
    }

    com_send(dest_rank, size);
}

/* Single benchmark scenario:
 * rank 0 performs ping-pong round-trips with every other process.
 */
static void run_ping_pong_benchmark(int rank,
                                    int nr_proc,
                                    size_t msg_size,
                                    long repeat,
                                    long benchmark_runs) {
    long run;
    long i;
    int p;

    if (rank == 0) {
        WelfordStats stats;
        welford_init(&stats);

        for (run = 0; run < benchmark_runs; run++) {
            struct timespec start, end;
            double t;

            DEBUG(printf("[MAIN] rank 0 starting benchmark run %ld\n", run));

            clock_gettime(CLOCK_MONOTONIC, &start);

            for (i = 0; i < repeat; i++) {
                for (p = 1; p < nr_proc; p++) {
                    void *reply;
                    size_t reply_size;

                    DEBUG(printf("[MAIN] run=%ld iter=%ld start ping-pong with rank %d size=%lu\n",
                                 run, i, p, (unsigned long)msg_size));

                    send_prepared_pattern(p, msg_size);

                    DEBUG(printf("[MAIN] run=%ld iter=%ld waiting reply from rank %d\n",
                                 run, i, p));

                    com_recv(&reply, &reply_size);

                    DEBUG(printf("[MAIN] run=%ld iter=%ld got reply from rank %d size=%lu\n",
                                 run, i, p, (unsigned long)reply_size));

                    free(reply);

                    DEBUG(printf("[MAIN] run=%ld iter=%ld finished ping-pong with rank %d\n",
                                 run, i, p));
                }
            }

            clock_gettime(CLOCK_MONOTONIC, &end);

            t = elapsed_seconds(&start, &end);
            welford_update(&stats, t);

            DEBUG(printf("[MAIN] rank 0 finished benchmark run %ld time=%.6f s\n", run, t));
        }

        printf("[ping_pong] proc=%d size=%lu repeat=%ld runs=%ld total_per_run=%ld "
               "mean=%.6f s sample_variance=%.12f stddev=%.6f s\n",
               nr_proc,
               (unsigned long)msg_size,
               repeat,
               benchmark_runs,
               repeat * (nr_proc - 1) * 2,
               stats.mean,
               welford_sample_variance(&stats),
               welford_stddev(&stats));
    } else {
        for (run = 0; run < benchmark_runs; run++) {
            DEBUG(printf("[MAIN] rank %d starting benchmark run %ld\n", rank, run));

            for (i = 0; i < repeat; i++) {
                void *msg;
                size_t sz;

                DEBUG(printf("[MAIN] rank %d run=%ld iter=%ld waiting request\n",
                             rank, run, i));

                com_recv(&msg, &sz);

                DEBUG(printf("[MAIN] rank %d run=%ld iter=%ld got request size=%lu, sending back\n",
                             rank, run, i, (unsigned long)sz));

                send_reply_copy(0, msg, sz);
                free(msg);

                DEBUG(printf("[MAIN] rank %d run=%ld iter=%ld reply sent\n",
                             rank, run, i));
            }
        }
    }
}

int main(int argc, char *argv[]) {
    long nr_proc_l;
    size_t msg_size;
    long repeat;
    long benchmark_runs;
    int nr_proc;
    int rank;
    struct timespec total_start, total_end;
    double total_time;

    if (argc != 5) {
        die_usage(argv[0]);
    }

    nr_proc_l = parse_positive_long(argv[1], "num_processes");
    msg_size = parse_size_value(argv[2], "message_size");
    repeat = parse_positive_long(argv[3], "repeat_count");
    benchmark_runs = parse_positive_long(argv[4], "benchmark_runs");

    if (nr_proc_l < 2) {
        fprintf(stderr, "Need at least 2 processes\n");
        return 1;
    }

    nr_proc = (int)nr_proc_l;

    com_initialize(nr_proc, &rank);

    DEBUG(printf("[MAIN] rank=%d started\n", rank));

    clock_gettime(CLOCK_MONOTONIC, &total_start);

    run_ping_pong_benchmark(rank, nr_proc, msg_size, repeat, benchmark_runs);

    clock_gettime(CLOCK_MONOTONIC, &total_end);
    total_time = elapsed_seconds(&total_start, &total_end);

    if (rank == 0) {
        printf("[program_total] time=%.6f s\n", total_time);
    }

    DEBUG(printf("[MAIN] rank=%d calling com_finalize()\n", rank));
    com_finalize();
    return 0;
}