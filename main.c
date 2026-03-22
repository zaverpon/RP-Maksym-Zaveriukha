#include "com.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef ENABLE_DEBUG
#define DEBUG(arg) do { arg; } while (0)
#else
#define DEBUG(arg) do { } while (0)
#endif

#define NS_PER_SEC 1000000000L

static void die_usage(const char *prog)
{
    fprintf(stderr,
            "Usage: %s <num_processes> <message_size> <repeat_count>\n",
            prog);
    exit(1);
}

static long parse_positive_long(const char *text, const char *name)
{
    char *end = NULL;
    long value;

    errno = 0;
    value = strtol(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || value <= 0) {
        fprintf(stderr, "Invalid %s: %s\n", name, text);
        exit(1);
    }

    return value;
}

static size_t parse_size_value(const char *text, const char *name)
{
    char *end = NULL;
    unsigned long long value;

    errno = 0;
    value = strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0') {
        fprintf(stderr, "Invalid %s: %s\n", name, text);
        exit(1);
    }

    return (size_t)value;
}

static double elapsed_seconds(const struct timespec *start,
                              const struct timespec *end)
{
    long sec = end->tv_sec - start->tv_sec;
    long nsec = end->tv_nsec - start->tv_nsec;

    return (double)sec + (double)nsec / (double)NS_PER_SEC;
}

static void fill_message(unsigned char *buf, size_t size)
{
    size_t i;

    for (i = 0; i < size; i++) {
        buf[i] = (unsigned char)('A' + (int)(i % 26));
    }
}

static void print_message_text(const void *msg, size_t size)
{
    if (size > 0) {
        fwrite(msg, 1, size, stdout);
    }
}

static void print_benchmark_summary(const char *label,
                                    int nr_proc,
                                    size_t msg_size,
                                    long repeat_count,
                                    double seconds,
                                    long total_messages)
{
    printf("[%s] status=OK proc=%d msg_size=%zu repeat=%ld total_messages=%ld time=%.6f s\n",
           label,
           nr_proc,
           msg_size,
           repeat_count,
           total_messages,
           seconds);
    fflush(stdout);
}

/* CHANGED: scenario 1 -> process 0 broadcasts the same message to all others. */
static void run_broadcast_only(int rank,
                               int nr_proc,
                               const unsigned char *send_buffer,
                               size_t msg_size,
                               long repeat_count)
{
    long iter;
    struct timespec start;
    struct timespec end;

    if (rank == 0) {
        if (clock_gettime(CLOCK_MONOTONIC, &start) == -1) {
            perror("clock_gettime(start scenario 1)");
            exit(1);
        }

        for (iter = 0; iter < repeat_count; iter++) {
            int dest;

            DEBUG(printf("[DEBUG][broadcast_only] iteration=%ld sender=0 action=broadcast_begin\n", iter + 1););
            for (dest = 1; dest < nr_proc; dest++) {
                DEBUG(printf("[DEBUG][broadcast_only] iteration=%ld sender=0 receiver=%d size=%zu message=\"",
                             iter + 1,
                             dest,
                             msg_size);
                      print_message_text(send_buffer, msg_size);
                      printf("\"\n"););;
                com_send(dest, (void *)send_buffer, msg_size);
                DEBUG(printf("[DEBUG][broadcast_only] iteration=%ld sender=0 receiver=%d status=sent_and_read\n",
                             iter + 1,
                             dest););
            }
        }

        if (clock_gettime(CLOCK_MONOTONIC, &end) == -1) {
            perror("clock_gettime(end scenario 1)");
            exit(1);
        }

#ifndef ENABLE_DEBUG
        print_benchmark_summary("broadcast_only",
                                nr_proc,
                                msg_size,
                                repeat_count,
                                elapsed_seconds(&start, &end),
                                repeat_count * (long)(nr_proc - 1));
#endif
    }
    else {
        for (iter = 0; iter < repeat_count; iter++) {
            void *recv_msg = NULL;
            size_t recv_size = 0;

            com_recv(&recv_msg, &recv_size);
            DEBUG(printf("[DEBUG][broadcast_only] iteration=%ld receiver=%d sender=0 size=%zu received=\"",
                         iter + 1,
                         rank,
                         recv_size);
                  print_message_text(recv_msg, recv_size);
                  printf("\"\n"););;
            free(recv_msg);
        }
    }
}

/* CHANGED: scenario 2 -> process 0 broadcasts, then each receiver sends the same message back to process 0. */
static void run_broadcast_and_return(int rank,
                                     int nr_proc,
                                     const unsigned char *send_buffer,
                                     size_t msg_size,
                                     long repeat_count)
{
    long iter;
    struct timespec start;
    struct timespec end;

    if (rank == 0) {
        if (clock_gettime(CLOCK_MONOTONIC, &start) == -1) {
            perror("clock_gettime(start scenario 2)");
            exit(1);
        }

        for (iter = 0; iter < repeat_count; iter++) {
            int peer;

            DEBUG(printf("[DEBUG][broadcast_return] iteration=%ld sender=0 action=broadcast_begin\n", iter + 1););
            for (peer = 1; peer < nr_proc; peer++) {
                DEBUG(printf("[DEBUG][broadcast_return] iteration=%ld sender=0 receiver=%d size=%zu message=\"",
                             iter + 1,
                             peer,
                             msg_size);
                      print_message_text(send_buffer, msg_size);
                      printf("\"\n"););;
                com_send(peer, (void *)send_buffer, msg_size);
                DEBUG(printf("[DEBUG][broadcast_return] iteration=%ld sender=0 receiver=%d status=sent_and_read\n",
                             iter + 1,
                             peer););
            }

            for (peer = 1; peer < nr_proc; peer++) {
                void *reply = NULL;
                size_t reply_size = 0;

                com_recv(&reply, &reply_size);
                DEBUG(printf("[DEBUG][broadcast_return] iteration=%ld sender=%d receiver=0 size=%zu reply=\"",
                             iter + 1,
                             peer,
                             reply_size);
                      print_message_text(reply, reply_size);
                      printf("\"\n"););;
                free(reply);
            }
        }

        if (clock_gettime(CLOCK_MONOTONIC, &end) == -1) {
            perror("clock_gettime(end scenario 2)");
            exit(1);
        }

#ifndef ENABLE_DEBUG
        print_benchmark_summary("broadcast_and_return",
                                nr_proc,
                                msg_size,
                                repeat_count,
                                elapsed_seconds(&start, &end),
                                repeat_count * (long)(nr_proc - 1) * 2L);
#endif
    }
    else {
        for (iter = 0; iter < repeat_count; iter++) {
            void *recv_msg = NULL;
            size_t recv_size = 0;

            com_recv(&recv_msg, &recv_size);
            DEBUG(printf("[DEBUG][broadcast_return] iteration=%ld receiver=%d sender=0 size=%zu received=\"",
                         iter + 1,
                         rank,
                         recv_size);
                  print_message_text(recv_msg, recv_size);
                  printf("\"\n"););;

            com_send(0, recv_msg, recv_size);
            DEBUG(printf("[DEBUG][broadcast_return] iteration=%ld sender=%d receiver=0 size=%zu status=returned_same_message\n",
                         iter + 1,
                         rank,
                         recv_size););
            free(recv_msg);
        }
    }
}

int main(int argc, char *argv[])
{
    long nr_proc_long;
    long repeat_count;
    size_t msg_size;
    int nr_proc;
    int rank;
    unsigned char *buffer;

    if (argc != 4) {
        die_usage(argv[0]);
    }

    nr_proc_long = parse_positive_long(argv[1], "num_processes");
    if (nr_proc_long < 2) {
        fprintf(stderr, "num_processes must be at least 2\n");
        return 1;
    }

    msg_size = parse_size_value(argv[2], "message_size");
    repeat_count = parse_positive_long(argv[3], "repeat_count");
    nr_proc = (int)nr_proc_long;

    com_initialize(nr_proc, &rank);

    buffer = NULL;
    if (rank == 0 || rank > 0) {
        buffer = malloc(msg_size > 0 ? msg_size : 1);
        if (buffer == NULL) {
            fprintf(stderr, "malloc failed\n");
            exit(1);
        }
    }

    fill_message(buffer, msg_size);

    DEBUG(printf("[DEBUG] process=%d started proc=%d msg_size=%zu repeat=%ld\n",
                 rank,
                 nr_proc,
                 msg_size,
                 repeat_count););

    run_broadcast_only(rank, nr_proc, buffer, msg_size, repeat_count);
    run_broadcast_and_return(rank, nr_proc, buffer, msg_size, repeat_count);

    free(buffer);
    com_finalize();
    return 0;
}
