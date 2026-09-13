cmake_minimum_required(VERSION 3.30)
project(task_runner C)

set(CMAKE_C_STANDARD 23)
set(CMAKE_C_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

# The core is a library so a future tests/ target can link it without
# dragging in main.c.
add_library(task_runner_core STATIC
        src/task.c
        src/priority_queue.c
        src/pool.c
        src/retry.c
)
target_include_directories(task_runner_core PUBLIC include)
target_link_libraries(task_runner_core PUBLIC Threads::Threads)
target_compile_options(task_runner_core PRIVATE -Wall -Wextra)

add_executable(task_runner main.c)
target_link_libraries(task_runner PRIVATE task_runner_core)
target_compile_options(task_runner PRIVATE -Wall -Wextra)
