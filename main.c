#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "pool.h"
#include "task.h"

#ifndef N_WORKERS
#define N_WORKERS 4
#endif
#define N_PRIORITY_TASKS 20

/* printf is not atomic across threads; this keeps demo output readable. */
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

static pthread_mutex_t g_gate_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_gate_cv = PTHREAD_COND_INITIALIZER;
static bool g_gate_open = false;

static void reset_gate(void) {
    pthread_mutex_lock(&g_gate_lock);
    g_gate_open = false;
    pthread_mutex_unlock(&g_gate_lock);
}

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
} priority_job_t;

static atomic_int g_priority_done = 0;

static int priority_job_run(void *arg) {
    const priority_job_t *job = arg;

    log_printf("  job %2d  %-8s  start\n", job->n, task_priority_name(job->priority));
    sleep_ms(job->work_ms);
    log_printf("  job %2d  %-8s  done\n", job->n, task_priority_name(job->priority));

    atomic_fetch_add(&g_priority_done, 1);
    return 0;
}

static bool submit_priority_job(pool_t *pool, const int n, const task_priority_t priority) {
    priority_job_t *job = malloc(sizeof *job);
    if (job == NULL) {
        return false;
    }
    job->n = n;
    job->priority = priority;
    job->work_ms = 60 + (unsigned) (rand() % 120);

    return pool_submit(pool, priority_job_run, job, free, priority, NULL);
}

static int demo_priority(void) {
    log_printf("\n=== priority demo ===\n");
    srand(1234);
    atomic_store(&g_priority_done, 0);
    reset_gate();

    pool_t *pool = pool_create(N_WORKERS);
    if (pool == NULL) {
        fprintf(stderr, "failed to create thread pool\n");
        return 1;
    }
    log_printf("pool started with %d workers\n\n", N_WORKERS);

    for (int i = 0; i < N_WORKERS; i++) {
        if (!pool_submit(pool, gate_task, NULL, NULL, TASK_PRIORITY_CRITICAL, NULL)) {
            fprintf(stderr, "failed to submit gate task\n");
            open_gate();
            pool_destroy(pool);
            return 1;
        }
    }

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
            if (!submit_priority_job(pool, ++submitted, batches[b].priority)) {
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

    pool_shutdown(pool, true);

    const int done = atomic_load(&g_priority_done);
    log_printf("\nexecuted %d/%d jobs (%zu tasks total, including %d gate tasks)\n",
               done, N_PRIORITY_TASKS, pool_completed(pool), N_WORKERS);

    pool_destroy(pool);
    return done == N_PRIORITY_TASKS ? 0 : 1;
}

typedef struct {
    const char *name;
    int attempt;
    int fail_until;
} flaky_job_t;

static int flaky_job_run(void *arg) {
    flaky_job_t *job = arg;
    job->attempt++;
    log_printf("  %s attempt %d\n", job->name, job->attempt);

    if (job->attempt <= job->fail_until) {
        log_printf("  %s failed\n", job->name);
        return 1;
    }

    log_printf("  %s succeeded\n", job->name);
    return 0;
}

static int run_single_flaky_demo(const char *title, const char *name,
                                 const int fail_until, const unsigned max_attempts) {
    log_printf("\n=== %s ===\n", title);

    pool_t *pool = pool_create(1);
    if (pool == NULL) {
        fprintf(stderr, "failed to create thread pool\n");
        return 1;
    }

    flaky_job_t *job = malloc(sizeof *job);
    if (job == NULL) {
        pool_destroy(pool);
        return 1;
    }
    job->name = name;
    job->attempt = 0;
    job->fail_until = fail_until;

    if (!pool_submit_retry(pool, flaky_job_run, job, free, TASK_PRIORITY_NORMAL,
                           max_attempts, 300, NULL)) {
        fprintf(stderr, "failed to submit retry task\n");
        pool_destroy(pool);
        return 1;
    }

    pool_shutdown(pool, true);
    log_printf("completed tasks: %zu\n", pool_completed(pool));
    pool_destroy(pool);
    return 0;
}

static int demo_retry_success(void) {
    return run_single_flaky_demo("retry success demo", "flaky-network-call", 2, 5);
}

static int demo_retry_fail(void) {
    return run_single_flaky_demo("retry fail demo", "broken-service-call", 100, 3);
}

typedef struct {
    int n;
    unsigned rounds;
} heavy_job_t;

static int heavy_job_run(void *arg) {
    const heavy_job_t *job = arg;
    volatile unsigned long long acc = 0;

    log_printf("  heavy job %d start\n", job->n);
    for (unsigned i = 0; i < job->rounds; i++) {
        acc += (unsigned long long) i * 2654435761ULL;
    }
    log_printf("  heavy job %d done, checksum=%llu\n", job->n, acc);
    return 0;
}

static int demo_heavy(void) {
    log_printf("\n=== heavy demo ===\n");

    pool_t *pool = pool_create(N_WORKERS);
    if (pool == NULL) {
        fprintf(stderr, "failed to create thread pool\n");
        return 1;
    }

    for (int i = 1; i <= N_WORKERS; i++) {
        heavy_job_t *job = malloc(sizeof *job);
        if (job == NULL) {
            pool_destroy(pool);
            return 1;
        }
        job->n = i;
        job->rounds = 20000000U;

        if (!pool_submit(pool, heavy_job_run, job, free, TASK_PRIORITY_HIGH, NULL)) {
            fprintf(stderr, "failed to submit heavy job\n");
            pool_destroy(pool);
            return 1;
        }
    }

    pool_shutdown(pool, true);
    log_printf("completed tasks: %zu\n", pool_completed(pool));
    pool_destroy(pool);
    return 0;
}

static int should_not_run(void *arg) {
    (void) arg;
    log_printf("  ERROR: cancelled task ran\n");
    return 1;
}

static int demo_cancel(void) {
    log_printf("\n=== cancel demo ===\n");
    reset_gate();

    pool_t *pool = pool_create(1);
    if (pool == NULL) {
        fprintf(stderr, "failed to create thread pool\n");
        return 1;
    }

    if (!pool_submit(pool, gate_task, NULL, NULL, TASK_PRIORITY_CRITICAL, NULL)) {
        fprintf(stderr, "failed to submit gate task\n");
        pool_destroy(pool);
        return 1;
    }

    uint64_t task_id = 0;
    if (!pool_submit_retry(pool, should_not_run, NULL, NULL, TASK_PRIORITY_LOW,
                           3, 200, &task_id)) {
        fprintf(stderr, "failed to submit cancellable task\n");
        open_gate();
        pool_destroy(pool);
        return 1;
    }

    const bool cancelled = pool_cancel(pool, task_id);
    log_printf("requested cancellation for task %llu: %s\n",
               (unsigned long long) task_id, cancelled ? "queued task marked cancelled" : "too late");

    open_gate();
    pool_shutdown(pool, true);
    log_printf("completed tasks: %zu (gate + cancelled task)\n", pool_completed(pool));
    pool_destroy(pool);
    return cancelled ? 0 : 1;
}

static void print_usage(const char *program) {
    printf("Usage: %s <demo>\n", program);
    printf("Demos:\n");
    printf("  priority       priority queue ordering\n");
    printf("  retry-success  task fails twice and then succeeds\n");
    printf("  retry-fail     task never succeeds, stops after 3 attempts\n");
    printf("  heavy          CPU-heavy background work\n");
    printf("  cancel         queued task cancellation\n");
    printf("  all            run every demo\n");
}

static int demo_all(void) {
    int rc = 0;
    rc |= demo_priority();
    rc |= demo_retry_success();
    rc |= demo_retry_fail();
    rc |= demo_heavy();
    rc |= demo_cancel();
    return rc == 0 ? 0 : 1;
}

int main(const int argc, char **argv) {
    if (argc != 2) {
        print_usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "priority") == 0) {
        return demo_priority();
    }
    if (strcmp(argv[1], "retry-success") == 0) {
        return demo_retry_success();
    }
    if (strcmp(argv[1], "retry-fail") == 0) {
        return demo_retry_fail();
    }
    if (strcmp(argv[1], "heavy") == 0) {
        return demo_heavy();
    }
    if (strcmp(argv[1], "cancel") == 0) {
        return demo_cancel();
    }
    if (strcmp(argv[1], "all") == 0) {
        return demo_all();
    }

    print_usage(argv[0]);
    return 1;
}
