#ifndef COM_H
#define COM_H

#include <stddef.h>

void com_initialize(int nr_proc, int *rank);
void *com_prepare_send_buffer(size_t size);
void com_send(int rank, size_t size);
void com_finish_send_buffer(void);
void com_recv(void **msg, size_t *size);
void com_mcast(void *msg, size_t size);
void com_finalize(void);

#endif
