cmake_minimum_required(VERSION 3.14)
project(store_sqlite)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(store_sqlite
        store.cpp
        store.h)
