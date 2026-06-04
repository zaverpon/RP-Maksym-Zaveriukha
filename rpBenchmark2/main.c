#define _POSIX_C_SOURCE 200809L

#include <math.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
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

#define MSG_REQUEST    1
#define MSG_RESPONSE   2
#define MSG_STOP       3
#define MSG_LOCAL_DONE 4

#define REQUEST_QUEUE_CAPACITY 1024

typedef struct {
    long n;
    double mean;
    double M2;
} WelfordStats;

typedef struct {
    unsigned char type;
    int request_id;
    int source_rank;
    int key;
    size_t response_size;
} MessageHeader;

typedef struct {
    void *msg;
    size_t size;
} QueueItem;

typedef struct {
    QueueItem items[REQUEST_QUEUE_CAPACITY];
    int head;
    int tail;
    int count;
    int closed;

    pthread_mutex_t mutex;
    sem_t items_sem;
    sem_t spaces_sem;
} RequestQueue;

typedef struct {
    int request_id;
    size_t size;

    pthread_mutex_t mutex;
    sem_t empty_sem;
    sem_t full_sem;
} ResponseSlot;

typedef struct {
    int rank;
    int nr_proc;
    long requests_per_process;
    size_t response_size;

    RequestQueue request_queue;
    ResponseSlot response_slot;

    WelfordStats latency_stats;

    struct timespec benchmark_start;
    struct timespec benchmark_end;
} BenchmarkContext;


static void die_usage(const char *prog)
{
    fprintf(stderr,
            "Usage: %s <num_processes> <requests_per_process> <response_size>\n",
            prog);
    fprintf(stderr, "Example: %s 3 10000 1024\n", prog);
    fprintf(stderr, "Example: %s 4 5000 4096\n", prog);
    exit(1);
}

static long parse_positive_long(const char *text, const char *name)
{
    char *end;
    long val = strtol(text, &end, 10);

    if (*end != '\0' || val <= 0) {
        fprintf(stderr, "Invalid %s: %s\n", name, text);
        exit(1);
    }

    return val;
}

static size_t parse_size_value(const char *text, const char *name)
{
    char *end;
    unsigned long long val = strtoull(text, &end, 10);

    if (*end != '\0' || val == 0) {
        fprintf(stderr, "Invalid %s: %s\n", name, text);
        exit(1);
    }

    return (size_t)val;
}

static double elapsed_seconds(const struct timespec *start,
                              const struct timespec *end)
{
    long sec = end->tv_sec - start->tv_sec;
    long nsec = end->tv_nsec - start->tv_nsec;

    return (double)sec + (double)nsec / (double)NS_PER_SEC;
}

static void welford_init(WelfordStats *s)
{
    s->n = 0;
    s->mean = 0.0;
    s->M2 = 0.0;
}

static void welford_update(WelfordStats *s, double x)
{
    double delta;
    double delta2;

    s->n++;
    delta = x - s->mean;
    s->mean += delta / (double)s->n;
    delta2 = x - s->mean;
    s->M2 += delta * delta2;
}

static double welford_sample_variance(const WelfordStats *s)
{
    if (s->n < 2) {
        return 0.0;
    }

    return s->M2 / (double)(s->n - 1);
}

static double welford_stddev(const WelfordStats *s)
{
    return sqrt(welford_sample_variance(s));
}

static void fill_response_payload(void *buf,
                                  size_t size,
                                  int source_rank,
                                  int request_id,
                                  int key)
{
    unsigned char *bytes = (unsigned char *)buf;
    size_t i;
    int base;

    base = source_rank + request_id + key;

    for (i = 0; i < size; i++) {

        bytes[i] = (unsigned char)((base + (int)i) % 256);

    }
}

static uint64_t checksum_payload(const void *buf, size_t size)
{
    const unsigned char *bytes = (const unsigned char *)buf;

    uint64_t sum = 0;

    size_t i;

    /*
     * Read the whole response payload.
     *
     * This makes the benchmark include the cost of actually touching
     * all received bytes
     */

    for (i = 0; i < size; i++) {
        sum += (uint64_t)bytes[i];
    }
    return sum;
}


static void request_queue_init(RequestQueue *q)
{
    q->head = 0;
    q->tail = 0;
    q->count = 0;
    q->closed = 0;

    if (pthread_mutex_init(&q->mutex, NULL) != 0) {
        perror("pthread_mutex_init(request_queue)");
        exit(1);
    }

    if (sem_init(&q->items_sem, 0, 0) == -1) {
        perror("sem_init(request_queue.items_sem)");
        exit(1);
    }

    if (sem_init(&q->spaces_sem, 0, REQUEST_QUEUE_CAPACITY) == -1) {
        perror("sem_init(request_queue.spaces_sem)");
        exit(1);
    }
}

static void request_queue_destroy(RequestQueue *q)
{
    if (sem_destroy(&q->items_sem) == -1) {
        perror("sem_destroy(request_queue.items_sem)");
        exit(1);
    }

    if (sem_destroy(&q->spaces_sem) == -1) {
        perror("sem_destroy(request_queue.spaces_sem)");
        exit(1);
    }

    if (pthread_mutex_destroy(&q->mutex) != 0) {
        perror("pthread_mutex_destroy(request_queue)");
        exit(1);
    }
}

static void request_queue_push(RequestQueue *q, void *msg, size_t size)
{
    if (sem_wait(&q->spaces_sem) == -1) {
        perror("sem_wait(request_queue.spaces_sem)");
        exit(1);
    }

    if (pthread_mutex_lock(&q->mutex) != 0) {
        perror("pthread_mutex_lock(request_queue)");
        exit(1);
    }

    if (q->closed) {
        if (pthread_mutex_unlock(&q->mutex) != 0) {
            perror("pthread_mutex_unlock(request_queue)");
            exit(1);
        }

        if (sem_post(&q->spaces_sem) == -1) {
            perror("sem_post(request_queue.spaces_sem)");
            exit(1);
        }

        free(msg);
        return;
    }

    q->items[q->tail].msg = msg;
    q->items[q->tail].size = size;
    q->tail = (q->tail + 1) % REQUEST_QUEUE_CAPACITY;
    q->count++;

    if (pthread_mutex_unlock(&q->mutex) != 0) {
        perror("pthread_mutex_unlock(request_queue)");
        exit(1);
    }

    if (sem_post(&q->items_sem) == -1) {
        perror("sem_post(request_queue.items_sem)");
        exit(1);
    }
}

static int request_queue_pop(RequestQueue *q, void **msg, size_t *size)
{
    for (;;) {
        if (sem_wait(&q->items_sem) == -1) {
            perror("sem_wait(request_queue.items_sem)");
            exit(1);
        }

        if (pthread_mutex_lock(&q->mutex) != 0) {
            perror("pthread_mutex_lock(request_queue)");
            exit(1);
        }

        if (q->count > 0) {
            *msg = q->items[q->head].msg;
            *size = q->items[q->head].size;

            q->head = (q->head + 1) % REQUEST_QUEUE_CAPACITY;
            q->count--;

            if (pthread_mutex_unlock(&q->mutex) != 0) {
                perror("pthread_mutex_unlock(request_queue)");
                exit(1);
            }

            if (sem_post(&q->spaces_sem) == -1) {
                perror("sem_post(request_queue.spaces_sem)");
                exit(1);
            }

            return 1;
        }

        if (q->closed) {
            if (pthread_mutex_unlock(&q->mutex) != 0) {
                perror("pthread_mutex_unlock(request_queue)");
                exit(1);
            }

            return 0;
        }

        if (pthread_mutex_unlock(&q->mutex) != 0) {
            perror("pthread_mutex_unlock(request_queue)");
            exit(1);
        }
    }
}

static void request_queue_close(RequestQueue *q)
{
    if (pthread_mutex_lock(&q->mutex) != 0) {
        perror("pthread_mutex_lock(request_queue)");
        exit(1);
    }

    q->closed = 1;

    if (pthread_mutex_unlock(&q->mutex) != 0) {
        perror("pthread_mutex_unlock(request_queue)");
        exit(1);
    }


    if (sem_post(&q->items_sem) == -1) {
        perror("sem_post(request_queue.items_sem)");
        exit(1);
    }
}



static void response_slot_init(ResponseSlot *slot)
{
    slot->request_id = -1;
    slot->size = 0;

    if (pthread_mutex_init(&slot->mutex, NULL) != 0) {
        perror("pthread_mutex_init(response_slot)");
        exit(1);
    }

    if (sem_init(&slot->empty_sem, 0, 1) == -1) {
        perror("sem_init(response_slot.empty_sem)");
        exit(1);
    }

    if (sem_init(&slot->full_sem, 0, 0) == -1) {
        perror("sem_init(response_slot.full_sem)");
        exit(1);
    }
}

static void response_slot_destroy(ResponseSlot *slot)
{
    if (sem_destroy(&slot->empty_sem) == -1) {
        perror("sem_destroy(response_slot.empty_sem)");
        exit(1);
    }

    if (sem_destroy(&slot->full_sem) == -1) {
        perror("sem_destroy(response_slot.full_sem)");
        exit(1);
    }

    if (pthread_mutex_destroy(&slot->mutex) != 0) {
        perror("pthread_mutex_destroy(response_slot)");
        exit(1);
    }
}

static void response_slot_set(ResponseSlot *slot, int request_id, size_t size)
{
    if (sem_wait(&slot->empty_sem) == -1) {
        perror("sem_wait(response_slot.empty_sem)");
        exit(1);
    }

    if (pthread_mutex_lock(&slot->mutex) != 0) {
        perror("pthread_mutex_lock(response_slot)");
        exit(1);
    }

    slot->request_id = request_id;
    slot->size = size;

    if (pthread_mutex_unlock(&slot->mutex) != 0) {
        perror("pthread_mutex_unlock(response_slot)");
        exit(1);
    }

    if (sem_post(&slot->full_sem) == -1) {
        perror("sem_post(response_slot.full_sem)");
        exit(1);
    }
}

static void response_slot_wait(ResponseSlot *slot,
                               int expected_request_id,
                               size_t *response_size)
{
    int got_request_id;

    if (sem_wait(&slot->full_sem) == -1) {
        perror("sem_wait(response_slot.full_sem)");
        exit(1);
    }

    if (pthread_mutex_lock(&slot->mutex) != 0) {
        perror("pthread_mutex_lock(response_slot)");
        exit(1);
    }

    got_request_id = slot->request_id;
    *response_size = slot->size;

    slot->request_id = -1;
    slot->size = 0;

    if (pthread_mutex_unlock(&slot->mutex) != 0) {
        perror("pthread_mutex_unlock(response_slot)");
        exit(1);
    }

    if (sem_post(&slot->empty_sem) == -1) {
        perror("sem_post(response_slot.empty_sem)");
        exit(1);
    }

    if (got_request_id != expected_request_id) {
        fprintf(stderr,
                "response_slot_wait: expected request_id=%d, got request_id=%d\n",
                expected_request_id,
                got_request_id);
        exit(1);
    }
}

/* ---------------- IPC message helpers ---------------- */

static void send_raw_message(int dest_rank, const void *msg, size_t size)
{
    void *buf;

    buf = com_prepare_send_buffer(size);

    if (size > 0) {
        if (buf == NULL) {
            fprintf(stderr, "send_raw_message: NULL buffer for non-zero message\n");
            exit(1);
        }

        memcpy(buf, msg, size);
    }

    com_send(dest_rank, size);
    com_finish_send_buffer();
}

static void send_request(int dest_rank,
                         int source_rank,
                         int request_id,
                         int key,
                         size_t response_size)
{
    MessageHeader header;

    header.type = MSG_REQUEST;
    header.request_id = request_id;
    header.source_rank = source_rank;
    header.key = key;
    header.response_size = response_size;

    send_raw_message(dest_rank, &header, sizeof(header));
}

static void send_response(int dest_rank,
                          int source_rank,
                          int request_id,
                          int key,
                          size_t response_size)
{
    size_t total_size = sizeof(MessageHeader) + response_size;
    unsigned char *msg;
    MessageHeader *header;
    void *payload;

    msg = malloc(total_size);
    if (msg == NULL) {
        fprintf(stderr, "malloc response failed\n");
        exit(1);
    }

    header = (MessageHeader *)msg;
    header->type = MSG_RESPONSE;
    header->request_id = request_id;
    header->source_rank = source_rank;
    header->key = key;
    header->response_size = response_size;

    payload = msg + sizeof(MessageHeader);

    /*
     * Actually fill response with fake database data.
     */
    fill_response_payload(payload,
                          response_size,
                          source_rank,
                          request_id,
                          key);

    send_raw_message(dest_rank, msg, total_size);

    free(msg);
}

static void send_control_message(int dest_rank, int source_rank, unsigned char type)
{
    MessageHeader header;

    header.type = type;
    header.request_id = -1;
    header.source_rank = source_rank;
    header.key = 0;
    header.response_size = 0;

    send_raw_message(dest_rank, &header, sizeof(header));
}

/* ---------------- Threads ---------------- */

static void *compute_thread_main(void *arg)
{
    BenchmarkContext *ctx = (BenchmarkContext *)arg;
    long i;

    for (i = 0; i < ctx->requests_per_process; i++) {
        int target;
        int request_id;
        int key;
        size_t response_size;
        struct timespec start;
        struct timespec end;
        double latency_us;

        /*
         * Round-robin remote target.
         * Never send database request to self.
         */
        target = (ctx->rank + 1 + (int)(i % (ctx->nr_proc - 1))) % ctx->nr_proc;

        /*
         * Request IDs are local to one compute thread.
         * Since compute sends one request and waits for its response,
         * one outstanding request is enough for this benchmark.
         */
        request_id = (int)i;
        key = (ctx->rank * 1000000) + (int)i;

        clock_gettime(CLOCK_MONOTONIC, &start);

        send_request(target,
                     ctx->rank,
                     request_id,
                     key,
                     ctx->response_size);

        response_slot_wait(&ctx->response_slot,
                           request_id,
                           &response_size);

        clock_gettime(CLOCK_MONOTONIC, &end);

        if (response_size != ctx->response_size) {
            fprintf(stderr,
                    "[rank %d] response size mismatch: got %lu, expected %lu\n",
                    ctx->rank,
                    (unsigned long)response_size,
                    (unsigned long)ctx->response_size);
            exit(1);
        }

        latency_us = elapsed_seconds(&start, &end) * 1e6;
        welford_update(&ctx->latency_stats, latency_us);

        DEBUG(printf("[rank %d] compute request %ld/%ld target=%d latency=%.3f us\n",
                     ctx->rank,
                     i + 1,
                     ctx->requests_per_process,
                     target,
                     latency_us));
    }

    /*
     * Tell other processes that this compute thread will not send more requests.
     * Their request handler still stays alive until all STOP messages arrive.
     */
    for (i = 0; i < ctx->nr_proc; i++) {
        if ((int)i != ctx->rank) {
            send_control_message((int)i, ctx->rank, MSG_STOP);
        }
    }

    /*
     * Wake local receiver.
     * This avoids a deadlock if all remote STOP messages arrived before
     * the local compute thread finished.
     */
    send_control_message(ctx->rank, ctx->rank, MSG_LOCAL_DONE);

    return NULL;
}

static void *request_handler_thread_main(void *arg)
{
    BenchmarkContext *ctx = (BenchmarkContext *)arg;

    for (;;) {
        void *msg;
        size_t size;
        MessageHeader *header;

        if (!request_queue_pop(&ctx->request_queue, &msg, &size)) {
            break;
        }

        if (size < sizeof(MessageHeader)) {
            fprintf(stderr, "[rank %d] request too small\n", ctx->rank);
            free(msg);
            exit(1);
        }

        header = (MessageHeader *)msg;

        if (header->type != MSG_REQUEST) {
            fprintf(stderr, "[rank %d] request handler got non-request message\n", ctx->rank);
            free(msg);
            exit(1);
        }

        /*
         * Simulate local shard/layer access.
         *
         * In a real distributed database node, this is where the process
         * would read data from the partition/layer that it owns.
         */
        send_response(header->source_rank,
                      ctx->rank,
                      header->request_id,
                      header->key,
                      header->response_size);

        DEBUG(printf("[rank %d] handled request_id=%d from rank=%d response_size=%lu\n",
                     ctx->rank,
                     header->request_id,
                     header->source_rank,
                     (unsigned long)header->response_size));

        free(msg);
    }

    DEBUG(printf("[rank %d] request handler stopped\n", ctx->rank));

    return NULL;
}

static void *receiver_thread_main(void *arg)
{
    BenchmarkContext *ctx = (BenchmarkContext *)arg;
    int remote_stop_count = 0;
    int local_done = 0;

    while (!(remote_stop_count == ctx->nr_proc - 1 && local_done)) {
        void *msg;
        size_t size;
        MessageHeader *header;

        com_recv(&msg, &size);

        if (size < sizeof(MessageHeader)) {
            fprintf(stderr, "[rank %d] received too small message\n", ctx->rank);
            free(msg);
            exit(1);
        }

        header = (MessageHeader *)msg;

        if (header->type == MSG_REQUEST) {
            /*
             * Receiver routes REQUEST to request handler.
             * Handler becomes owner of msg and must free it.
             */
            request_queue_push(&ctx->request_queue, msg, size);
            msg = NULL;
        } else if (header->type == MSG_RESPONSE) {
            void *payload;
            uint64_t checksum;

            if (size != sizeof(MessageHeader) + header->response_size) {
                fprintf(stderr,
                        "[rank %d] response message size mismatch: got %lu, expected %lu\n",
                        ctx->rank,
                        (unsigned long)size,
                        (unsigned long)(sizeof(MessageHeader) + header->response_size));
                free(msg);
                exit(1);
            }

            payload = (unsigned char *)msg + sizeof(MessageHeader);

            /*
             * Receiver really reads the response payload.
             * Without this, benchmark could measure only metadata path.
             */
            checksum = checksum_payload(payload, header->response_size);

            if (header->response_size > 0 && checksum == 0) {
                fprintf(stderr,
                        "[rank %d] invalid response checksum for request_id=%d\n",
                        ctx->rank,
                        header->request_id);
                free(msg);
                exit(1);
            }

            DEBUG(printf("[rank %d] received response request_id=%d size=%lu checksum=%llu\n",
                         ctx->rank,
                         header->request_id,
                         (unsigned long)header->response_size,
                         (unsigned long long)checksum));

            /*
             * Receiver routes RESPONSE to compute thread.
             * Compute waits on response_slot.full_sem.
             */
            response_slot_set(&ctx->response_slot,
                              header->request_id,
                              header->response_size);
        } else if (header->type == MSG_STOP) {
            remote_stop_count++;

            DEBUG(printf("[rank %d] receiver got STOP %d/%d from rank=%d\n",
                         ctx->rank,
                         remote_stop_count,
                         ctx->nr_proc - 1,
                         header->source_rank));
        } else if (header->type == MSG_LOCAL_DONE) {
            local_done = 1;

            DEBUG(printf("[rank %d] receiver got LOCAL_DONE\n", ctx->rank));
        } else {
            fprintf(stderr,
                    "[rank %d] unknown message type: %u\n",
                    ctx->rank,
                    (unsigned int)header->type);
            free(msg);
            exit(1);
        }

        if (msg != NULL) {
            free(msg);
        }
    }

    /*
     * At this point:
     *   - local compute is done,
     *   - all remote compute threads sent STOP,
     *   - therefore no new REQUEST messages should arrive.
     *
     * Close local request queue so request handler can exit after draining it.
     */
    request_queue_close(&ctx->request_queue);

    DEBUG(printf("[rank %d] receiver stopped\n", ctx->rank));

    return NULL;
}

/* ---------------- Main benchmark ---------------- */

static void run_distributed_db_benchmark(int rank,
                                         int nr_proc,
                                         long requests_per_process,
                                         size_t response_size)
{
    BenchmarkContext ctx;
    pthread_t compute_thread;
    pthread_t receiver_thread;
    pthread_t request_handler_thread;
    double total_time;
    double mean_latency_us;
    double stddev_latency_us;
    double requests_per_second;
    double response_mib_s;
    long total_system_requests;

    ctx.rank = rank;
    ctx.nr_proc = nr_proc;
    ctx.requests_per_process = requests_per_process;
    ctx.response_size = response_size;

    welford_init(&ctx.latency_stats);
    request_queue_init(&ctx.request_queue);
    response_slot_init(&ctx.response_slot);

    clock_gettime(CLOCK_MONOTONIC, &ctx.benchmark_start);

    /*
     * Start receiver and request handler first.
     * Compute may immediately send requests and then wait for responses.
     */
    if (pthread_create(&receiver_thread, NULL, receiver_thread_main, &ctx) != 0) {
        perror("pthread_create(receiver)");
        exit(1);
    }

    if (pthread_create(&request_handler_thread, NULL, request_handler_thread_main, &ctx) != 0) {
        perror("pthread_create(request_handler)");
        exit(1);
    }

    if (pthread_create(&compute_thread, NULL, compute_thread_main, &ctx) != 0) {
        perror("pthread_create(compute)");
        exit(1);
    }

    if (pthread_join(compute_thread, NULL) != 0) {
        perror("pthread_join(compute)");
        exit(1);
    }

    if (pthread_join(receiver_thread, NULL) != 0) {
        perror("pthread_join(receiver)");
        exit(1);
    }

    if (pthread_join(request_handler_thread, NULL) != 0) {
        perror("pthread_join(request_handler)");
        exit(1);
    }

    clock_gettime(CLOCK_MONOTONIC, &ctx.benchmark_end);

    total_time = elapsed_seconds(&ctx.benchmark_start, &ctx.benchmark_end);
    mean_latency_us = ctx.latency_stats.mean;
    stddev_latency_us = welford_stddev(&ctx.latency_stats);

    total_system_requests = requests_per_process * (long)nr_proc;

    /*
     * This is system-level throughput estimated from this process's wall time.
     * Processes run concurrently, so every rank should report similar total_time.
     */
    requests_per_second = (double)total_system_requests / total_time;

    response_mib_s =
        ((double)total_system_requests * (double)response_size) /
        total_time /
        (1024.0 * 1024.0);

    printf("[distributed_db_rank] rank=%d processes=%d requests_per_process=%ld "
           "response_size=%lu "
           "total_time=%.6f s mean_latency_us=%.3f stddev_latency_us=%.3f "
           "requests_per_second=%.3f response_mib_s=%.3f\n",
           rank,
           nr_proc,
           requests_per_process,
           (unsigned long)response_size,
           total_time,
           mean_latency_us,
           stddev_latency_us,
           requests_per_second,
           response_mib_s);

    /*
     * Parse-friendly line.
     *
     * 1  = rank
     * 2  = processes
     * 3  = requests_per_process
     * 4  = response_size
     * 5  = total_time_sec
     * 6  = mean_latency_us
     * 7  = stddev_latency_us
     * 8  = estimated_system_requests_per_second
     * 9  = estimated_response_mib_s
     */
    printf("[data_db_rank] %d %d %ld %lu %.6f %.3f %.3f %.3f %.3f\n",
           rank,
           nr_proc,
           requests_per_process,
           (unsigned long)response_size,
           total_time,
           mean_latency_us,
           stddev_latency_us,
           requests_per_second,
           response_mib_s);

    request_queue_destroy(&ctx.request_queue);
    response_slot_destroy(&ctx.response_slot);
}

int main(int argc, char *argv[])
{
    long nr_proc_l;
    long requests_per_process;
    size_t response_size;

    int nr_proc;
    int rank;

    if (argc != 4) {
        die_usage(argv[0]);
    }

    nr_proc_l = parse_positive_long(argv[1], "num_processes");
    requests_per_process = parse_positive_long(argv[2], "requests_per_process");
    response_size = parse_size_value(argv[3], "response_size");

    if (nr_proc_l < 2) {
        fprintf(stderr, "num_processes must be at least 2\n");
        return 1;
    }

    if (nr_proc_l > 1024) {
        fprintf(stderr, "num_processes is too large\n");
        return 1;
    }

    nr_proc = (int)nr_proc_l;

    com_initialize(nr_proc, &rank);

    DEBUG(printf("[MAIN] rank=%d started\n", rank));

    run_distributed_db_benchmark(rank,
                                 nr_proc,
                                 requests_per_process,
                                 response_size);

    DEBUG(printf("[MAIN] rank=%d calling com_finalize()\n", rank));

    com_finalize();

    return 0;
}