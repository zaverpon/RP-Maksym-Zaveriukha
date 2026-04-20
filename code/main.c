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
#define BENCHMARK_RUNS 10L

typedef struct {
    long n;
    double mean;
    double M2;
} WelfordStats;

/* Prints correct usage and exits */
static void die_usage(const char *prog) {
    fprintf(stderr, "Usage: %s <num_processes> <message_size> <repeat_count>\n", prog);
    fprintf(stderr, "This benchmark currently supports exactly 2 processes.\n");
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

/* Parses positive size_t value from string */
static size_t parse_size_value(const char *text, const char *name) {
    char *end;
    unsigned long long val = strtoull(text, &end, 10);
    if (*end != '\0' || val == 0) {
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

/* Benchmark scenario:
 * Exactly 2 processes.
 * Rank 0 performs roundtrips with rank 1:
 *   send to 1 -> receive reply from 1
 */
static void run_roundtrip_benchmark_2proc(int rank,
                                          size_t msg_size,
                                          long repeat) {
    if (rank == 0) {
        WelfordStats stats;
        long run;

        welford_init(&stats);

        for (run = 0; run < BENCHMARK_RUNS; run++) {
            struct timespec start, end;
            double t;
            long i;

            DEBUG(printf("[MAIN] rank 0 starting benchmark run %ld, size=%lu, repeat=%ld\n",
                         run, (unsigned long)msg_size, repeat));

            clock_gettime(CLOCK_MONOTONIC, &start);

            for (i = 0; i < repeat; i++) {
                void *reply;
                size_t reply_size;

                send_prepared_pattern(1, msg_size);
                com_recv(&reply, &reply_size);

                if (reply_size != msg_size) {
                    fprintf(stderr,
                            "Unexpected reply size: got %lu, expected %lu\n",
                            (unsigned long)reply_size,
                            (unsigned long)msg_size);
                    free(reply);
                    exit(1);
                }

                free(reply);
            }

            clock_gettime(CLOCK_MONOTONIC, &end);

            t = elapsed_seconds(&start, &end);
            welford_update(&stats, t);

            DEBUG(printf("[MAIN] rank 0 finished run %ld, time=%.6f s\n", run, t));
        }

        {
            double mean_time = stats.mean;
            double stddev_time = welford_stddev(&stats);
            double total_bytes = 2.0 * (double)msg_size * (double)repeat;
            double avg_roundtrip_us = (mean_time / (double)repeat) * 1e6;
            double throughput_mib_s = total_bytes / mean_time / (1024.0 * 1024.0);

            printf("[roundtrip_2proc] size=%lu repeat=%ld runs=%ld "
                   "mean=%.6f s stddev=%.6f s "
                   "avg_roundtrip_us=%.3f throughput_mib_s=%.3f\n",
                   (unsigned long)msg_size,
                   repeat,
                   BENCHMARK_RUNS,
                   mean_time,
                   stddev_time,
                   avg_roundtrip_us,
                   throughput_mib_s);

            /* Easy-to-parse line for graph generation:
             * 1=size
             * 2=repeat
             * 3=runs
             * 4=mean_time_sec
             * 5=stddev_time_sec
             * 6=avg_roundtrip_us
             * 7=throughput_mib_s
             */
            printf("[data] %lu %ld %ld %.6f %.6f %.3f %.3f\n",
                   (unsigned long)msg_size,
                   repeat,
                   BENCHMARK_RUNS,
                   mean_time,
                   stddev_time,
                   avg_roundtrip_us,
                   throughput_mib_s);
        }
    } else if (rank == 1) {
        long run;

        for (run = 0; run < BENCHMARK_RUNS; run++) {
            long i;

            for (i = 0; i < repeat; i++) {
                void *msg;
                size_t sz;

                com_recv(&msg, &sz);

                if (sz != msg_size) {
                    fprintf(stderr,
                            "Unexpected request size: got %lu, expected %lu\n",
                            (unsigned long)sz,
                            (unsigned long)msg_size);
                    free(msg);
                    exit(1);
                }

                send_reply_copy(0, msg, sz);
                free(msg);
            }
        }
    }
}

int main(int argc, char *argv[]) {
    long nr_proc_l;
    size_t msg_size;
    long repeat;
    int nr_proc;
    int rank;
    struct timespec total_start, total_end;
    double total_time;

    if (argc != 4) {
        die_usage(argv[0]);
    }

    nr_proc_l = parse_positive_long(argv[1], "num_processes");
    msg_size = parse_size_value(argv[2], "message_size");
    repeat = parse_positive_long(argv[3], "repeat_count");

    if (nr_proc_l != 2) {
        fprintf(stderr, "This benchmark currently supports exactly 2 processes\n");
        return 1;
    }

    nr_proc = (int)nr_proc_l;

    com_initialize(nr_proc, &rank);

    DEBUG(printf("[MAIN] rank=%d started\n", rank));

    clock_gettime(CLOCK_MONOTONIC, &total_start);

    run_roundtrip_benchmark_2proc(rank, msg_size, repeat);

    clock_gettime(CLOCK_MONOTONIC, &total_end);
    total_time = elapsed_seconds(&total_start, &total_end);

    if (rank == 0) {
        printf("[program_total] time=%.6f s\n", total_time);
    }

    DEBUG(printf("[MAIN] rank=%d calling com_finalize()\n", rank));
    com_finalize();
    return 0;
}