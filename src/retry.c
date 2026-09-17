//
// Created by vukpe on 13-Sep-26.
//

#include "retry.h"

#include <stdatomic.h>
#include <time.h>

static void sleep_ms(const unsigned ms) {
    const struct timespec ts = {
        .tv_sec = ms / 1000,
        .tv_nsec = (long) (ms % 1000) * 1000000L,
    };
    nanosleep(&ts, nullptr);
}

static bool sleep_backoff_or_cancelled(const task_t *t) {
    unsigned remaining = t->backoff_ms;
    while (remaining > 0) {
        if (atomic_load(&t->cancelled)) {
            return true;
        }
        const unsigned chunk = remaining > 50 ? 50 : remaining;
        sleep_ms(chunk);
        remaining -= chunk;
    }
    return atomic_load(&t->cancelled);
}

void retry_configure(task_t *t, unsigned max_attempts, const unsigned backoff_ms) {
    if (t == NULL) {
        return;
    }
    if (max_attempts == 0) {
        max_attempts = 1;
    }
    t->max_attempts = max_attempts;
    t->backoff_ms = backoff_ms;
}

void retry_run(task_t *t) {
    if (t == NULL) {
        return;
    }

    if (atomic_load(&t->cancelled)) {
        t->status = TASK_STATUS_CANCELLED;
        return;
    }

    while (t->attempts < t->max_attempts) {
        t->status = TASK_STATUS_RUNNING;
        t->attempts++;
        t->last_result = t->fn(t->arg);

        if (t->last_result == 0) {
            t->status = TASK_STATUS_SUCCEEDED;
            return;
        }

        if (atomic_load(&t->cancelled)) {
            t->status = TASK_STATUS_CANCELLED;
            return;
        }

        if (t->attempts < t->max_attempts && t->backoff_ms > 0 &&
            sleep_backoff_or_cancelled(t)) {
            t->status = TASK_STATUS_CANCELLED;
            return;
        }
    }

    t->status = TASK_STATUS_FAILED;
}

void retry_cancel(task_t *t) {
    if (t != NULL) {
        atomic_store(&t->cancelled, true);
    }
}
