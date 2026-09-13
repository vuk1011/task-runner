//
// Created by vukpe on 13-Sep-26.
//

#include "pool.h"

#include <pthread.h>
#include <stdlib.h>

#include "priority_queue.h"

struct pool {
    pthread_mutex_t lock;       /* guards everything below */
    pthread_cond_t  work_ready; /* workers block here when the queue is empty */
    pthread_cond_t  all_idle;   /* reserved for a future pool_wait_idle() */

    pthread_t *threads;
    size_t     n_threads;

    pqueue_t *queue;
    uint64_t  next_seq;

    bool   shutting_down;
    bool   drain_on_shutdown;
    size_t active;    /* tasks currently executing */
    size_t completed; /* tasks that have finished executing */
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

        /*
         * This must be a while, not an if. pthread_cond_wait can wake
         * spuriously, and even after a genuine signal another worker may
         * have taken the task before this thread reacquired the lock.
         */
        while (!p->shutting_down && pqueue_is_empty(p->queue)) {
            pthread_cond_wait(&p->work_ready, &p->lock);
        }

        /*
         * Two ways out: we are dropping pending work, or we are draining
         * and there is nothing left to drain.
         */
        if (p->shutting_down &&
            (!p->drain_on_shutdown || pqueue_is_empty(p->queue))) {
            pthread_mutex_unlock(&p->lock);
            return NULL;
        }

        task_t *t = pqueue_pop(p->queue);
        t->status = TASK_STATUS_RUNNING;
        p->active++;
        pthread_mutex_unlock(&p->lock);

        /* Run outside the lock, or the pool would be serial rather than parallel. */
        t->attempts++;
        t->last_result = t->fn(t->arg);
        t->status = t->last_result == 0 ? TASK_STATUS_SUCCEEDED : TASK_STATUS_FAILED;
        /* Increment 2 wraps the call above in the retry loop from retry.c. */
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
        return NULL;
    }

    pool_t *p = calloc(1, sizeof *p);
    if (p == NULL) {
        return NULL;
    }

    p->queue = pqueue_create(0);
    if (p->queue == NULL) {
        free(p);
        return NULL;
    }

    p->threads = calloc(n_threads, sizeof *p->threads);
    if (p->threads == NULL) {
        pqueue_destroy(p->queue);
        free(p);
        return NULL;
    }

    if (pthread_mutex_init(&p->lock, NULL) != 0) {
        free(p->threads);
        pqueue_destroy(p->queue);
        free(p);
        return NULL;
    }
    if (pthread_cond_init(&p->work_ready, NULL) != 0) {
        pthread_mutex_destroy(&p->lock);
        free(p->threads);
        pqueue_destroy(p->queue);
        free(p);
        return NULL;
    }
    if (pthread_cond_init(&p->all_idle, NULL) != 0) {
        pthread_cond_destroy(&p->work_ready);
        pthread_mutex_destroy(&p->lock);
        free(p->threads);
        pqueue_destroy(p->queue);
        free(p);
        return NULL;
    }

    p->n_threads         = 0;
    p->next_seq          = 0;
    p->shutting_down     = false;
    p->drain_on_shutdown = true;
    p->active            = 0;
    p->completed         = 0;

    /*
     * n_threads grows as threads actually start, so a partial failure below
     * only ever joins threads that exist.
     */
    for (size_t i = 0; i < n_threads; i++) {
        if (pthread_create(&p->threads[i], NULL, worker_main, p) != 0) {
            pool_destroy(p);
            return NULL;
        }
        p->n_threads++;
    }

    return p;
}

bool pool_submit(pool_t *p, const task_fn_t fn, void *arg,
                 void (*arg_free)(void *arg), const task_priority_t priority,
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

    /*
     * Read the id now. Once the task is queued and the lock is released a
     * worker may have already run and freed it, so touching t after that
     * would be a use-after-free.
     */
    const uint64_t id = t->id;

    pthread_mutex_lock(&p->lock);

    /* Checked under the lock: unlocked, this would race with a concurrent
     * shutdown and strand the task in a queue nobody will ever drain. */
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

    /* One task added, so waking one worker is enough. */
    pthread_cond_signal(&p->work_ready);
    pthread_mutex_unlock(&p->lock);

    if (out_id != NULL) {
        *out_id = id;
    }
    return true;
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
    p->shutting_down     = true;
    p->drain_on_shutdown = drain;
    if (!drain) {
        discard_queued_locked(p);
    }
    /*
     * Broadcast, not signal: every blocked worker has to see the flag. A
     * signal would wake one and leave the rest asleep forever, and the join
     * below would hang.
     */
    pthread_cond_broadcast(&p->work_ready);
    pthread_mutex_unlock(&p->lock);

    /* Joining with the lock held would deadlock: the workers need it to exit. */
    for (size_t i = 0; i < p->n_threads; i++) {
        pthread_join(p->threads[i], NULL);
    }
}

void pool_destroy(pool_t *p) {
    if (p == NULL) {
        return;
    }

    /*
     * Either this joins the workers, or an earlier pool_shutdown already
     * did. Both cases leave no worker running by the time we free anything,
     * which is why the API asks for a single owning thread here.
     */
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
