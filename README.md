# Task runner

## Description

System for executing background tasks.

Supports priority, retries and cancellation.

## Status

**Increment 1 — done:** task type, priority queue, worker thread pool.

Tasks are executed by a fixed pool of worker threads in priority order
(`CRITICAL > HIGH > NORMAL > LOW`), first-in-first-out within a single
priority level. Shutdown can either drain the queue or drop what is left.

**Increment 2 — planned:** retry with backoff, and cancellation.
`src/retry.c` is still an empty stub; `task_t` already carries the
`attempts` / `max_attempts` / `backoff_ms` / `cancelled` fields these will use.

## Architecture

| File | Responsibility |
| --- | --- |
| `include/task.h`, `src/task.c` | The `task_t` unit of work: id, priority, status, lifecycle |
| `include/priority_queue.h`, `src/priority_queue.c` | Binary max-heap of tasks. Not thread-safe by design — the pool locks around it |
| `include/pool.h`, `src/pool.c` | Worker threads, submission, condition-variable wait, shutdown |
| `include/retry.h`, `src/retry.c` | Stub, increment 2 |
| `main.c` | Demo: 4 workers, 20 jobs, proves priority ordering |

Concurrency uses POSIX threads: one mutex guards the queue and all counters,
one condition variable parks idle workers. Tasks run outside the lock.

## Building and running the project

Requires CMake 3.30+ and a C compiler with pthreads. Verified on MinGW-w64
GCC 13.1 and 14.2.

### From CLion

Open the project and build the `task_runner` target. Nothing else to set up.

### From the command line

CLion's bundled MinGW needs its own `bin` directory on `PATH`, otherwise it
cannot find its DLLs and fails even on a trivial file:

```powershell
$clion = 'C:\Program Files\JetBrains\CLion 2024.3.5\bin'
$env:PATH = "$clion\mingw\bin;$env:PATH"

cmake -S . -B cmake-build-verify -G Ninja `
  -DCMAKE_C_COMPILER="$clion/mingw/bin/gcc.exe" `
  -DCMAKE_MAKE_PROGRAM="$clion/ninja/win/x64/ninja.exe"
cmake --build cmake-build-verify
./cmake-build-verify/task_runner.exe
```

On Linux, or with a compiler already on `PATH`:

```sh
cmake -S . -B build && cmake --build build && ./build/task_runner
```

### Expected output

Jobs are submitted lowest-priority-first, so the fact that they *run*
highest-priority-first is the queue at work rather than submission order:

```
20 jobs queued, 20 pending; releasing workers

  job 19  CRITICAL  start
  job 20  CRITICAL  start
  job 15  HIGH      start
  ...
executed 20/20 jobs (24 tasks total, including 4 gate tasks)
```

The demo holds every worker on a start gate until all 20 jobs are queued,
so the ordering is not an artifact of workers draining the queue mid-submit.

Note that with several workers the printed order can show a single swap at a
priority boundary: a worker takes its task while holding the pool lock but
prints after releasing it, so two threads can print out of order even though
they dequeued in the correct order. Running with one worker
(`-DN_WORKERS=1`) removes the interleaving and shows the exact order.
