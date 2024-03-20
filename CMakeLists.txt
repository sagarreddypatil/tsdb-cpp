cmake_minimum_required(VERSION 3.13)

project(tsdb)

set(CMAKE_CXX_STANDARD 23)

set (CMAKE_CXX_FLAGS "-Wall -Werror")
set (CMAKE_CXX_FLAGS_DEBUG "-g -O2")
set (CMAKE_CXX_FLAGS_RELEASE "-O2")

include_directories(include)

add_executable (main src/main.cpp)