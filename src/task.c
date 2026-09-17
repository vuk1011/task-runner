//
// Created by vukpe on 13-Sep-26.
//

#include "task.h"

#include <stdlib.h>

/* Ids are handed out without any lock. */
static _Atomic uint64_t g_next_task_id = 1;

task_t *task_create(const task_fn_t fn, void *arg, void (*arg_free)(void *arg),
                    const task_priority_t priority) {
    if (fn == NULL) {
        return nullptr;
    }

    task_t *t = calloc(1, sizeof *t);
    if (t == NULL) {
        return nullptr;
    }

    t->id = atomic_fetch_add(&g_next_task_id, 1);
    t->fn = fn;
    t->arg = arg;
    t->arg_free = arg_free;
    t->priority = priority;
    t->seq = 0;
    t->status = TASK_STATUS_QUEUED;

    t->attempts = 0;
    t->max_attempts = 1;
    t->backoff_ms = 0;
    t->last_result = 0;
    atomic_init(&t->cancelled, false);

    return t;
}

void task_destroy(task_t *t) {
    if (t == NULL) {
        return;
    }
    if (t->arg_free != NULL) {
        t->arg_free(t->arg);
    }
    free(t);
}

const char *task_priority_name(const task_priority_t priority) {
    switch (priority) {
        case TASK_PRIORITY_LOW: return "LOW";
        case TASK_PRIORITY_NORMAL: return "NORMAL";
        case TASK_PRIORITY_HIGH: return "HIGH";
        case TASK_PRIORITY_CRITICAL: return "CRITICAL";
        default: return "?";
    }
}

const char *task_status_name(const task_status_t status) {
    switch (status) {
        case TASK_STATUS_QUEUED: return "QUEUED";
        case TASK_STATUS_RUNNING: return "RUNNING";
        case TASK_STATUS_SUCCEEDED: return "SUCCEEDED";
        case TASK_STATUS_FAILED: return "FAILED";
        case TASK_STATUS_CANCELLED: return "CANCELLED";
        default: return "?";
    }
}
