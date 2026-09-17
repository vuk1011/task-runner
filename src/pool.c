//
// Created by vukpe on 13-Sep-26.
//

#include <pthread.h>
#include <stdlib.h>

#include "pool.h"
#include "priority_queue.h"
#include "retry.h"

struct pool {
    pthread_mutex_t lock;
    pthread_cond_t work_ready; /* workers block here when the queue is empty */
    pthread_cond_t all_idle;

    pthread_t *threads;
    size_t n_threads;

    pqueue_t *queue;
    uint64_t next_seq;

    bool shutting_down;
    bool drain_on_shutdown;

    size_t active;
    size_t completed;
};

/* Caller must hold p->lock. Drops and frees everything still queued. */
static void discard_queued_locked(pool_t *p) {
    task_t *t;
    while ((t = pqueue_pop(p->queue)) != NULL) {
        t->status = TASK_STATUS_CANCELLED;
        task_destroy(t);
    }
}

static void *worker_main(void *arg) {
    pool_t *p = arg;

    for (;;) {
        pthread_mutex_lock(&p->lock);

        while (!p->shutting_down && pqueue_is_empty(p->queue)) {
            pthread_cond_wait(&p->work_ready, &p->lock);
        }

        if (p->shutting_down &&
            (!p->drain_on_shutdown || pqueue_is_empty(p->queue))) {
            pthread_mutex_unlock(&p->lock);
            return NULL;
        }

        task_t *t = pqueue_pop(p->queue);
        t->status = TASK_STATUS_RUNNING;
        p->active++;
        pthread_mutex_unlock(&p->lock);

        retry_run(t);
        task_destroy(t);

        pthread_mutex_lock(&p->lock);
        p->active--;
        p->completed++;
        if (p->active == 0 && pqueue_is_empty(p->queue)) {
            pthread_cond_broadcast(&p->all_idle);
        }
        pthread_mutex_unlock(&p->lock);
    }
}

pool_t *pool_create(const size_t n_threads) {
    if (n_threads == 0) {
        return nullptr;
    }

    pool_t *p = calloc(1, sizeof *p);
    if (p == NULL) {
        return nullptr;
    }

    p->queue = pqueue_create(0);
    if (p->queue == NULL) {
        free(p);
        return nullptr;
    }

    p->threads = calloc(n_threads, sizeof *p->threads);
    if (p->threads == NULL) {
        pqueue_destroy(p->queue);
        free(p);
        return nullptr;
    }

    if (pthread_mutex_init(&p->lock, nullptr) != 0) {
        free(p->threads);
        pqueue_destroy(p->queue);
        free(p);
        return nullptr;
    }
    if (pthread_cond_init(&p->work_ready, nullptr) != 0) {
        pthread_mutex_destroy(&p->lock);
        free(p->threads);
        pqueue_destroy(p->queue);
        free(p);
        return nullptr;
    }
    if (pthread_cond_init(&p->all_idle, nullptr) != 0) {
        pthread_cond_destroy(&p->work_ready);
        pthread_mutex_destroy(&p->lock);
        free(p->threads);
        pqueue_destroy(p->queue);
        free(p);
        return nullptr;
    }

    p->n_threads = 0;
    p->next_seq = 0;
    p->shutting_down = false;
    p->drain_on_shutdown = true;
    p->active = 0;
    p->completed = 0;

    for (size_t i = 0; i < n_threads; i++) {
        if (pthread_create(&p->threads[i], nullptr, worker_main, p) != 0) {
            pool_destroy(p);
            return nullptr;
        }
        p->n_threads++;
    }

    return p;
}

bool pool_submit_retry(pool_t *p, const task_fn_t fn, void *arg,
                       void (*arg_free)(void *arg), const task_priority_t priority,
                       const unsigned max_attempts, const unsigned backoff_ms,
                       uint64_t *out_id) {
    if (p == NULL || fn == NULL) {
        if (arg_free != NULL) {
            arg_free(arg);
        }
        return false;
    }

    task_t *t = task_create(fn, arg, arg_free, priority);
    if (t == NULL) {
        if (arg_free != NULL) {
            arg_free(arg);
        }
        return false;
    }
    retry_configure(t, max_attempts, backoff_ms);

    const uint64_t id = t->id;

    pthread_mutex_lock(&p->lock);

    if (p->shutting_down) {
        pthread_mutex_unlock(&p->lock);
        task_destroy(t);
        return false;
    }

    t->seq = p->next_seq++;
    if (!pqueue_push(p->queue, t)) {
        pthread_mutex_unlock(&p->lock);
        task_destroy(t);
        return false;
    }

    pthread_cond_signal(&p->work_ready);
    pthread_mutex_unlock(&p->lock);

    if (out_id != NULL) {
        *out_id = id;
    }
    return true;
}

bool pool_submit(pool_t *p, const task_fn_t fn, void *arg,
                 void (*arg_free)(void *arg), const task_priority_t priority,
                 uint64_t *out_id) {
    return pool_submit_retry(p, fn, arg, arg_free, priority, 1, 0, out_id);
}

bool pool_cancel(pool_t *p, const uint64_t task_id) {
    if (p == NULL) {
        return false;
    }

    pthread_mutex_lock(&p->lock);
    task_t *t = pqueue_find_by_id(p->queue, task_id);
    if (t != NULL) {
        retry_cancel(t);
    }
    pthread_mutex_unlock(&p->lock);

    return t != NULL;
}

void pool_shutdown(pool_t *p, const bool drain) {
    if (p == NULL) {
        return;
    }

    pthread_mutex_lock(&p->lock);
    if (p->shutting_down) {
        pthread_mutex_unlock(&p->lock);
        return;
    }
    p->shutting_down = true;
    p->drain_on_shutdown = drain;
    if (!drain) {
        discard_queued_locked(p);
    }

    pthread_cond_broadcast(&p->work_ready);
    pthread_mutex_unlock(&p->lock);

    for (size_t i = 0; i < p->n_threads; i++) {
        pthread_join(p->threads[i], nullptr);
    }
}

void pool_destroy(pool_t *p) {
    if (p == NULL) {
        return;
    }

    pool_shutdown(p, true);

    pthread_mutex_lock(&p->lock);
    discard_queued_locked(p);
    pthread_mutex_unlock(&p->lock);

    pqueue_destroy(p->queue);
    pthread_cond_destroy(&p->all_idle);
    pthread_cond_destroy(&p->work_ready);
    pthread_mutex_destroy(&p->lock);
    free(p->threads);
    free(p);
}

size_t pool_pending(pool_t *p) {
    if (p == NULL) {
        return 0;
    }
    pthread_mutex_lock(&p->lock);
    const size_t pending = pqueue_size(p->queue);
    pthread_mutex_unlock(&p->lock);
    return pending;
}

size_t pool_completed(pool_t *p) {
    if (p == NULL) {
        return 0;
    }
    pthread_mutex_lock(&p->lock);
    const size_t completed = p->completed;
    pthread_mutex_unlock(&p->lock);
    return completed;
}
