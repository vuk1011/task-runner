//
// Created by vukpe on 13-Sep-26.
//

#ifndef POOL_H
#define POOL_H

#include <stddef.h>
#include <stdint.h>

#include "task.h"

/*
 * A fixed-size pool of worker threads that execute submitted tasks in
 * priority order. Submission is safe from any thread.
 *
 * pool_shutdown() and pool_destroy() are expected to be called by a single
 * owning thread , because they join the workers.
 */
typedef struct pool pool_t;

/* n_threads must be >= 1. NULL on failure. */
pool_t *pool_create(size_t n_threads);

/*
 * Queues fn(arg) at the given priority. Returns false if the pool is
 * shutting down or allocation failed, in which case arg_free(arg) is
 * called and nothing is queued. On success the task id is stored in
 * *out_id when out_id is non-NULL.
 */
bool pool_submit(pool_t *p, task_fn_t fn, void *arg, void (*arg_free)(void *arg),
                 task_priority_t priority, uint64_t *out_id);

bool pool_submit_retry(pool_t *p, task_fn_t fn, void *arg, void (*arg_free)(void *arg),
                       task_priority_t priority, unsigned max_attempts,
                       unsigned backoff_ms, uint64_t *out_id);

/* Cancels a task that is still queued. Returns false if it already started. */
bool pool_cancel(pool_t *p, uint64_t task_id);

/*
 * Stops accepting work and blocks until every worker has exited.
 *   drain == true  -> queued tasks all run first
 *   drain == false -> queued tasks are dropped (marked cancelled and freed);
 *                     tasks already running still finish
 * Calling it a second time is a no-op.
 */
void pool_shutdown(pool_t *p, bool drain);

/* Shuts down with drain == true if needed, then releases all resources. */
void pool_destroy(pool_t *p);

size_t pool_pending(pool_t *p); /* tasks queued, not yet started */
size_t pool_completed(pool_t *p); /* tasks that have finished running */

#endif //POOL_H
