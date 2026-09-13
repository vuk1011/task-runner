//
// Created by vukpe on 13-Sep-26.
//

#include "priority_queue.h"

#include <stdlib.h>

#define PQUEUE_DEFAULT_CAPACITY 16

struct pqueue {
    task_t **heap;
    size_t size;
    size_t capacity;
};

/*
 * True when a must run before b.
 *
 * The seq tiebreaker is what makes the queue fair: a bare heap gives no
 * ordering at all among equal keys, so without this two tasks of the same
 * priority would come out in whatever order the heap happened to shuffle
 * them into.
 */
static bool task_precedes(const task_t *a, const task_t *b) {
    if (a->priority != b->priority) {
        return a->priority > b->priority;
    }
    return a->seq < b->seq;
}

static void heap_swap(task_t **heap, const size_t i, const size_t j) {
    task_t *tmp = heap[i];
    heap[i] = heap[j];
    heap[j] = tmp;
}

static void sift_up(pqueue_t *q, size_t i) {
    while (i > 0) {
        const size_t parent = (i - 1) / 2;
        if (!task_precedes(q->heap[i], q->heap[parent])) {
            break;
        }
        heap_swap(q->heap, i, parent);
        i = parent;
    }
}

static void sift_down(pqueue_t *q, size_t i) {
    for (;;) {
        const size_t left = 2 * i + 1;
        const size_t right = 2 * i + 2;
        size_t best = i;

        if (left < q->size && task_precedes(q->heap[left], q->heap[best])) {
            best = left;
        }
        if (right < q->size && task_precedes(q->heap[right], q->heap[best])) {
            best = right;
        }
        if (best == i) {
            return;
        }

        heap_swap(q->heap, i, best);
        i = best;
    }
}

pqueue_t *pqueue_create(const size_t initial_capacity) {
    const size_t capacity = initial_capacity > 0 ? initial_capacity : PQUEUE_DEFAULT_CAPACITY;

    pqueue_t *q = malloc(sizeof *q);
    if (q == NULL) {
        return NULL;
    }

    q->heap = malloc(capacity * sizeof *q->heap);
    if (q->heap == NULL) {
        free(q);
        return NULL;
    }

    q->size = 0;
    q->capacity = capacity;
    return q;
}

void pqueue_destroy(pqueue_t *q) {
    if (q == NULL) {
        return;
    }
    free(q->heap);
    free(q);
}

bool pqueue_push(pqueue_t *q, task_t *t) {
    if (q == NULL || t == NULL) {
        return false;
    }

    if (q->size == q->capacity) {
        const size_t capacity = q->capacity * 2;
        task_t **heap = realloc(q->heap, capacity * sizeof *heap);
        if (heap == NULL) {
            return false; /* the old array is still intact */
        }
        q->heap = heap;
        q->capacity = capacity;
    }

    q->heap[q->size] = t;
    q->size++;
    sift_up(q, q->size - 1);
    return true;
}

task_t *pqueue_pop(pqueue_t *q) {
    if (q == NULL || q->size == 0) {
        return NULL;
    }

    task_t *top = q->heap[0];
    q->size--;
    q->heap[0] = q->heap[q->size];
    sift_down(q, 0);
    return top;
}

task_t *pqueue_peek(const pqueue_t *q) {
    if (q == NULL || q->size == 0) {
        return NULL;
    }
    return q->heap[0];
}

size_t pqueue_size(const pqueue_t *q) {
    return q == NULL ? 0 : q->size;
}

bool pqueue_is_empty(const pqueue_t *q) {
    return pqueue_size(q) == 0;
}
