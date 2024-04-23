cmake_minimum_required(VERSION 3.13)

project(tsdb)

set(CMAKE_CXX_STANDARD 23)

set (FLAGS_ASAN "-fsanitize=address -fno-omit-frame-pointer")

set (CMAKE_CXX_FLAGS "-Wall -Werror")
set (CMAKE_CXX_FLAGS_DEBUG "-g -O2")
set (CMAKE_CXX_FLAGS_RELEASE "-g -O2 -DNDEBUG")

include_directories(include)

add_executable (main src/main.cpp)
