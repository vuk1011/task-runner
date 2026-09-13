cmake_minimum_required(VERSION 3.30)
project(task_runner C)

set(CMAKE_C_STANDARD 23)

add_executable(task_runner main.c
        src/task.c
        include/task.h
        src/priority_queue.c
        include/priority_queue.h
        src/pool.c
        include/pool.h
        src/retry.c
        include/retry.h
)

target_include_directories(task_runner PUBLIC include)
