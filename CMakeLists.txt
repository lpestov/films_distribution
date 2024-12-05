cmake_minimum_required(VERSION 3.30)
project(films_distribution)

set(CMAKE_CXX_STANDARD 20)

include_directories(include)

add_executable(films_distribution
        src/main.cpp
        src/MovieProcessor.cpp
)

target_include_directories(films_distribution PRIVATE include)
