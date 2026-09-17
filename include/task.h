//
// Created by vukpe on 13-Sep-26.
//

#ifndef TASK_H
#define TASK_H

#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>

/* Higher numeric value == runs first. */
typedef enum {
    TASK_PRIORITY_LOW      = 0,
    TASK_PRIORITY_NORMAL   = 1,
    TASK_PRIORITY_HIGH     = 2,
    TASK_PRIORITY_CRITICAL = 3
} task_priority_t;

typedef enum {
    TASK_STATUS_QUEUED,
    TASK_STATUS_RUNNING,
    TASK_STATUS_SUCCEEDED,
    TASK_STATUS_FAILED,    /* used by increment 2 (retry) */
    TASK_STATUS_CANCELLED  /* used by increment 2 (cancellation) */
} task_status_t;

/*
 * A unit of work. Must return 0 on success and non-zero on failure: that
 * return value is what the retry logic will key off in increment 2, which
 * is why this is not a void-returning function.
 */
typedef int (*task_fn_t)(void *arg);

typedef struct task {
    uint64_t    id;
    task_fn_t   fn;
    void       *arg;
    void      (*arg_free)(void *arg); /* optional; called by task_destroy */

    task_priority_t priority;
    uint64_t        seq;    /* insertion order, assigned by the pool; FIFO tiebreaker */
    task_status_t   status;

    /* --- populated but not yet acted on; increment 2 --- */
    unsigned    attempts;     /* completed attempts so far */
    unsigned    max_attempts; /* 1 == no retry */
    unsigned    backoff_ms;   /* base delay between attempts */
    atomic_bool cancelled;    /* set from any thread, polled by the worker */
    int         last_result;  /* return value of the most recent attempt */
} task_t;

/*
 * Allocates a task on the heap and assigns it a process-unique id.
 * Returns NULL if fn is NULL or allocation fails. seq is left at 0 for
 * the pool to assign under its lock.
 */
task_t *task_create(task_fn_t fn, void *arg, void (*arg_free)(void *arg),
                    task_priority_t priority);

/* Frees the task, first calling arg_free(arg) if one was supplied. */
void task_destroy(task_t *t);

const char *task_priority_name(task_priority_t priority);
const char *task_status_name(task_status_t status);

#endif //TASK_H
