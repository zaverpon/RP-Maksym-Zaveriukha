#define _POSIX_C_SOURCE 200809L

#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdarg.h>

#include "com.h"

#ifdef ENABLE_DEBUG
#define DEBUG(arg) do { arg; } while (0)
#else
#define DEBUG(arg) do { } while (0)
#endif

#define NS_PER_SEC 1000000000L

typedef struct {
    int rank;
    int nr_proc;
    size_t msg_size;
    long repeat;
} ThreadArgs;

static void checked_output(FILE *stream, const char *fmt, va_list ap)
{
    if (vfprintf(stream, fmt, ap) < 0) {
        perror("vfprintf");
        exit(1);
    }
}

static void checked_print_to(FILE *stream, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    checked_output(stream, fmt, ap);
    va_end(ap);
}

static void checked_print(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    checked_output(stdout, fmt, ap);
    va_end(ap);
}

/* Prints correct usage and exits. */
static void die_usage(const char *prog)
{
    checked_print_to(stderr, "Usage: %s <num_processes> <message_size> <repeat_count>\n", prog);
    checked_print_to(stderr, "Example: %s 4 1024 10000\n", prog);
    exit(1);
}

/* Parses positive long value from string. */
static long parse_positive_long(const char *text, const char *name)
{
    char *end;
    long val = strtol(text, &end, 10);

    if (*end != '\0' || val <= 0) {
        checked_print_to(stderr, "Invalid %s: %s\n", name, text);
        exit(1);
    }

    return val;
}

/* Parses positive size_t value from string. */
static size_t parse_size_value(const char *text, const char *name)
{
    char *end;
    unsigned long long val = strtoull(text, &end, 10);

    if (*end != '\0' || val == 0) {
        checked_print_to(stderr, "Invalid %s: %s\n", name, text);
        exit(1);
    }

    return (size_t)val;
}

/* Computes elapsed time in seconds. */
static double elapsed_seconds(const struct timespec *start,
                              const struct timespec *end)
{
    long sec = end->tv_sec - start->tv_sec;
    long nsec = end->tv_nsec - start->tv_nsec;

    return (double)sec + (double)nsec / NS_PER_SEC;
}

/* Fills buffer with predictable test pattern. */
static void fill_message(unsigned char *buf, size_t size, int rank, long cycle)
{
    size_t i;
    unsigned char base = (unsigned char)('A' + (rank % 26));

    for (i = 0; i < size; i++) {
        buf[i] = (unsigned char)(base + ((i + (size_t)cycle) % 26));
    }
}

/*
 * Sender thread:
 *
 * In every cycle this process sends one message to every other process.
 *
 * One local cycle:
 *     nr_proc - 1 sends
 *
 * One global cycle:
 *     nr_proc * (nr_proc - 1) total messages
 */
static void *sender_thread_main(void *arg)
{
    ThreadArgs *args = (ThreadArgs *)arg;
    long cycle;

    for (cycle = 0; cycle < args->repeat; cycle++) {
        int dest;
        unsigned char *send_buf;

        send_buf = com_prepare_send_buffer(args->msg_size);

        if (args->msg_size > 0 && send_buf == NULL) {
            checked_print_to(stderr, "com_prepare_send_buffer returned NULL for non-zero size\n");
            exit(1);
        }

        if (args->msg_size > 0) {
            fill_message(send_buf, args->msg_size, args->rank, cycle);
        }

        for (dest = 0; dest < args->nr_proc; dest++) {
            if (dest != args->rank) {
                com_send(dest, args->msg_size);
            }
        }
        com_finish_send_buffer();

        DEBUG(checked_print("[rank %d] sender finished cycle %ld\n",
                     args->rank, cycle));
    }

    return NULL;
}

/*
 * Receiver thread:
 *
 * It receives all expected messages directly from com_recv().
 *
 * No queue is used in this version.
 * Each received message is checked and immediately freed.
 */
static void *receiver_thread_main(void *arg)
{
    ThreadArgs *args = (ThreadArgs *)arg;
    long total_receives = args->repeat * (long)(args->nr_proc - 1);
    long i;

    for (i = 0; i < total_receives; i++) {
        void *msg;
        size_t sz;

        com_recv(&msg, &sz);

        if (sz != args->msg_size) {
            checked_print_to(stderr,
                    "[rank %d] unexpected message size: got %lu, expected %lu\n",
                    args->rank,
                    (unsigned long)sz,
                    (unsigned long)args->msg_size);
            free(msg);
            exit(1);
        }

        free(msg);

        DEBUG(checked_print("[rank %d] receiver got message %ld/%ld\n",
                     args->rank, i + 1, total_receives));
    }

    return NULL;
}

/*
 * Runs one all-to-all benchmark run inside one user process.
 *
 * Every process starts:
 *     1 sender thread
 *     1 receiver thread
 *
 * The receiver thread does not store messages.
 * It receives and immediately frees them.
 */
static double run_all_to_all_once(int rank,
                                  int nr_proc,
                                  size_t msg_size,
                                  long repeat)
{
    pthread_t sender_thread;
    pthread_t receiver_thread;
    ThreadArgs args;
    struct timespec start;
    struct timespec end;

    args.rank = rank;
    args.nr_proc = nr_proc;
    args.msg_size = msg_size;
    args.repeat = repeat;

    clock_gettime(CLOCK_MONOTONIC, &start);

    if (pthread_create(&receiver_thread, NULL, receiver_thread_main, &args) != 0) {
        perror("pthread_create(receiver)");
        exit(1);
    }

    if (pthread_create(&sender_thread, NULL, sender_thread_main, &args) != 0) {
        perror("pthread_create(sender)");
        exit(1);
    }

    if (pthread_join(sender_thread, NULL) != 0) {
        perror("pthread_join(sender)");
        exit(1);
    }

    if (pthread_join(receiver_thread, NULL) != 0) {
        perror("pthread_join(receiver)");
        exit(1);
    }

    clock_gettime(CLOCK_MONOTONIC, &end);

    return elapsed_seconds(&start, &end);
}

static void run_all_to_all_benchmark(int rank,
                                     int nr_proc,
                                     size_t msg_size,
                                     long repeat)
{
    double measured_time;

    measured_time = run_all_to_all_once(rank, nr_proc, msg_size, repeat);

    if (rank == 0) {
        double messages_per_cycle = (double)nr_proc * (double)(nr_proc - 1);
        double total_messages = messages_per_cycle * (double)repeat;
        double total_bytes = total_messages * (double)msg_size;

        double avg_cycle_us = (measured_time / (double)repeat) * 1e6;
        double avg_message_us = (measured_time / total_messages) * 1e6;
        double throughput_mib_s = total_bytes / measured_time / (1024.0 * 1024.0);

        checked_print("[all_to_all_threads_no_queue] processes=%d size=%lu repeat=%ld "
               "time=%.6f s "
               "messages_per_cycle=%.0f total_messages=%.0f "
               "avg_cycle_us=%.3f avg_message_us=%.3f "
               "throughput_mib_s=%.3f\n",
               nr_proc,
               (unsigned long)msg_size,
               repeat,
               measured_time,
               messages_per_cycle,
               total_messages,
               avg_cycle_us,
               avg_message_us,
               throughput_mib_s);

        checked_print("[data] %d %lu %ld %.6f %.0f %.0f %.3f %.3f %.3f\n",
               nr_proc,
               (unsigned long)msg_size,
               repeat,
               measured_time,
               messages_per_cycle,
               total_messages,
               avg_cycle_us,
               avg_message_us,
               throughput_mib_s);
    }
}

int main(int argc, char *argv[])
{
    long nr_proc_l;
    size_t msg_size;
    long repeat;
    int nr_proc;
    int rank;
    struct timespec total_start;
    struct timespec total_end;
    double total_time;

    if (argc != 4) {
        die_usage(argv[0]);
    }

    nr_proc_l = parse_positive_long(argv[1], "num_processes");
    msg_size = parse_size_value(argv[2], "message_size");
    repeat = parse_positive_long(argv[3], "repeat_count");

    if (nr_proc_l < 2) {
        checked_print_to(stderr, "num_processes must be at least 2\n");
        return 1;
    }

    if (nr_proc_l > 1024) {
        checked_print_to(stderr, "num_processes is too large\n");
        return 1;
    }

    nr_proc = (int)nr_proc_l;

    com_initialize(nr_proc, &rank);

    DEBUG(checked_print("[MAIN] rank=%d started\n", rank));

    clock_gettime(CLOCK_MONOTONIC, &total_start);

    run_all_to_all_benchmark(rank, nr_proc, msg_size, repeat);

    clock_gettime(CLOCK_MONOTONIC, &total_end);
    total_time = elapsed_seconds(&total_start, &total_end);

    if (rank == 0) {
        checked_print("[program_total] time=%.6f s\n", total_time);
    }

    DEBUG(checked_print("[MAIN] rank=%d calling com_finalize()\n", rank));

    com_finalize();

    return 0;
}