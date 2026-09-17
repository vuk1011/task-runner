//
// Created by vukpe on 13-Sep-26.
//

#ifndef PRIORITY_QUEUE_H
#define PRIORITY_QUEUE_H

#include <stddef.h>

#include "task.h"

/*
 * A binary max-heap of task_t*, ordered by priority and then by insertion
 * sequence so that equal-priority tasks come out FIFO.
 *
 * NOT thread-safe: the owner (the pool) holds its own mutex
 * across every call, because "pop a task, or wait until one arrives or we
 * shut down" has to be one atomic decision under a single lock.
 *
 * The queue does not own the tasks it stores and never frees them.
 */
typedef struct pqueue pqueue_t;

pqueue_t *pqueue_create(size_t initial_capacity);

void pqueue_destroy(pqueue_t *q);

bool pqueue_push(pqueue_t *q, task_t *t);

task_t *pqueue_pop(pqueue_t *q);

task_t *pqueue_peek(const pqueue_t *q);

task_t *pqueue_find_by_id(const pqueue_t *q, uint64_t id);

size_t pqueue_size(const pqueue_t *q);

bool pqueue_is_empty(const pqueue_t *q);

#endif //PRIORITY_QUEUE_H
