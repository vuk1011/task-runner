#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "pool.h"
#include "task.h"

#ifndef N_WORKERS
#define N_WORKERS 4
#endif
#define N_TASKS 20

/* printf is not atomic across threads; without this the lines interleave
 * mid-word and the output stops being readable evidence of anything. */
static pthread_mutex_t g_log_lock = PTHREAD_MUTEX_INITIALIZER;

static void log_printf(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    pthread_mutex_lock(&g_log_lock);
    vprintf(fmt, ap);
    fflush(stdout);
    pthread_mutex_unlock(&g_log_lock);
    va_end(ap);
}

static void sleep_ms(const unsigned ms) {
    const struct timespec ts = {
        .tv_sec = ms / 1000,
        .tv_nsec = (long) (ms % 1000) * 1000000L,
    };
    nanosleep(&ts, NULL);
}

/*
 * A start gate. Without it the first N_WORKERS tasks would be picked up
 * while main is still submitting, so the earliest lines of output would
 * reflect submission order rather than priority. Holding every worker
 * until all tasks are queued makes the ordering unambiguous.
 */
static pthread_mutex_t g_gate_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_gate_cv = PTHREAD_COND_INITIALIZER;
static bool g_gate_open = false;

static int gate_task(void *arg) {
    (void) arg;
    pthread_mutex_lock(&g_gate_lock);
    while (!g_gate_open) {
        pthread_cond_wait(&g_gate_cv, &g_gate_lock);
    }
    pthread_mutex_unlock(&g_gate_lock);
    return 0;
}

static void open_gate(void) {
    pthread_mutex_lock(&g_gate_lock);
    g_gate_open = true;
    pthread_cond_broadcast(&g_gate_cv);
    pthread_mutex_unlock(&g_gate_lock);
}

typedef struct {
    int n;
    task_priority_t priority;
    unsigned work_ms;
} demo_job_t;

static atomic_int g_jobs_done = 0;

static int demo_job_run(void *arg) {
    const demo_job_t *job = arg;

    log_printf("  job %2d  %-8s  start\n", job->n, task_priority_name(job->priority));
    sleep_ms(job->work_ms);
    log_printf("  job %2d  %-8s  done\n", job->n, task_priority_name(job->priority));

    atomic_fetch_add(&g_jobs_done, 1);
    return 0;
}

static bool submit_job(pool_t *pool, const int n, const task_priority_t priority) {
    demo_job_t *job = malloc(sizeof *job);
    if (job == NULL) {
        return false;
    }
    job->n = n;
    job->priority = priority;
    job->work_ms = 60 + (unsigned) (rand() % 120); /* 60-179 ms of "work" */

    /* free() as arg_free: the pool owns the job struct from here on. */
    return pool_submit(pool, demo_job_run, job, free, priority, NULL);
}

int main(void) {
    srand(1234); /* fixed seed so runs are comparable */

    pool_t *pool = pool_create(N_WORKERS);
    if (pool == NULL) {
        fprintf(stderr, "failed to create thread pool\n");
        return 1;
    }
    log_printf("pool started with %d workers\n\n", N_WORKERS);

    /* Occupy every worker so nothing runs until all tasks are queued. */
    for (int i = 0; i < N_WORKERS; i++) {
        if (!pool_submit(pool, gate_task, NULL, NULL, TASK_PRIORITY_CRITICAL, NULL)) {
            fprintf(stderr, "failed to submit gate task\n");
            open_gate();
            pool_destroy(pool);
            return 1;
        }
    }

    /*
     * Submitted lowest priority first, on purpose: if the output still comes
     * out CRITICAL -> HIGH -> NORMAL -> LOW, that is the queue ordering and
     * not an artifact of the order things were handed in.
     */
    static const struct {
        task_priority_t priority;
        int count;
    } batches[] = {
                {TASK_PRIORITY_LOW, 8},
                {TASK_PRIORITY_NORMAL, 6},
                {TASK_PRIORITY_HIGH, 4},
                {TASK_PRIORITY_CRITICAL, 2},
            };

    int submitted = 0;
    for (size_t b = 0; b < sizeof batches / sizeof *batches; b++) {
        for (int i = 0; i < batches[b].count; i++) {
            if (!submit_job(pool, ++submitted, batches[b].priority)) {
                fprintf(stderr, "failed to submit job %d\n", submitted);
                submitted--;
                break;
            }
            log_printf("submitted job %2d  %s\n", submitted,
                       task_priority_name(batches[b].priority));
        }
    }

    log_printf("\n%d jobs queued, %zu pending; releasing workers\n\n",
               submitted, pool_pending(pool));
    open_gate();

    pool_shutdown(pool, true); /* drain: every queued job runs before we exit */

    const int done = atomic_load(&g_jobs_done);
    log_printf("\nexecuted %d/%d jobs (%zu tasks total, including %d gate tasks)\n",
               done, N_TASKS, pool_completed(pool), N_WORKERS);

    pool_destroy(pool);
    return done == N_TASKS ? 0 : 1;
}
