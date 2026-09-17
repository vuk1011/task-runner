# Task Runner

A background task execution system written in C. Supports priorities,
retries with backoff, and cancellation of queued tasks.

Tasks are executed by a pool of worker threads, in priority order, FIFO within the same priority level.

## Files

| File | Description |
| --- | --- |
| `include/task.h`, `src/task.c` | Defines `task_t`, the unit of work: id, priority, status, lifecycle |
| `include/priority_queue.h`, `src/priority_queue.c` | Binary max-heap that orders tasks by priority |
| `include/pool.h`, `src/pool.c` | Thread pool: worker threads, task submission, shutdown |
| `include/retry.h`, `src/retry.c` | Retry logic with bounded attempts, backoff, and cancellation |
| `main.c` | Demo program with several runnable examples |

## Running on Windows (CLion + CMake)

1. Open the project in CLion.
2. CLion detects `CMakeLists.txt` automatically and configures the project.
3. Select the `task_runner` run configuration.
4. Click **Run** (or **Build**, then run the produced `task_runner.exe`).

The program takes one argument selecting which demo to run. This can be set
in the run configuration under **Program arguments**:

| Argument | Description |
| --- | --- |
| `priority` | Submits jobs of mixed priority and shows that they run in priority order, not submission order |
| `retry-success` | A task fails twice, then succeeds on a later attempt |
| `retry-fail` | A task keeps failing and stops retrying after 3 attempts |
| `heavy` | Runs several CPU-heavy jobs across the worker pool |
| `cancel` | Cancels a queued task before a worker starts it |
| `all` | Runs every demo above |
