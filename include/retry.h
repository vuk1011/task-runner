//
// Created by vukpe on 13-Sep-26.
//

#ifndef RETRY_H
#define RETRY_H

#include "task.h"

/* Configures bounded retry. */
void retry_configure(task_t *t, unsigned max_attempts, unsigned backoff_ms);

/* Runs the task until it succeeds, exhausts attempts, or gets cancelled. */
void retry_run(task_t *t);

/* May be called from another thread. */
void retry_cancel(task_t *t);

#endif //RETRY_H
