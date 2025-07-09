# Copyright 2024 Man Group Operations Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.28)

if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    project(sparrow-benchmarks CXX)
    find_package(sparrow REQUIRED CONFIG)
    set(SPARROW_INCLUDE_DIR ${sparrow_INCLUDE_DIRS})
endif()

if(NOT CMAKE_BUILD_TYPE)
    message(STATUS "Setting benchmarks build type to Release")
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
else()
    message(STATUS "Benchmarks build type is ${CMAKE_BUILD_TYPE}")
endif()

set(SPARROW_BENCHMARK_SOURCES
    main.cpp
    bench_primitive_array.cpp
)

add_executable(sparrow_benchmarks ${SPARROW_BENCHMARK_SOURCES})

# Set up compile options similar to the main target
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(sparrow_benchmarks PRIVATE
        -Wall -Wextra -Wpedantic -Werror -Wno-unused-parameter)
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    target_compile_options(sparrow_benchmarks PRIVATE
        /W4 /WX /bigobj)
endif()

target_link_libraries(sparrow_benchmarks
    PRIVATE
        sparrow
        benchmark::benchmark
        benchmark::benchmark_main
)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(sparrow_benchmarks PRIVATE
        -O3 -DNDEBUG
        -march=native
        -fno-omit-frame-pointer
    )
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    target_compile_options(sparrow_benchmarks PRIVATE
        /O2 /DNDEBUG
    )
endif()

# Benchmark targets
add_custom_target(run_benchmarks
    COMMAND sparrow_benchmarks
    DEPENDS sparrow_benchmarks
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running sparrow benchmarks with console output"
)

# Benchmark target with JSON output
add_custom_target(run_benchmarks_json
    COMMAND sparrow_benchmarks --benchmark_format=json --benchmark_out=sparrow_benchmarks.json
    DEPENDS sparrow_benchmarks
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running sparrow benchmarks with JSON output"
)

# Benchmark target with CSV output  
add_custom_target(run_benchmarks_csv
    COMMAND sparrow_benchmarks --benchmark_format=csv --benchmark_out=sparrow_benchmarks.csv
    DEPENDS sparrow_benchmarks
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running sparrow benchmarks with CSV output"
)

# Benchmark target with console output and JSON report
add_custom_target(run_benchmarks_console_json
    COMMAND sparrow_benchmarks --benchmark_format=console --benchmark_out_format=json --benchmark_out=sparrow_benchmarks.json
    DEPENDS sparrow_benchmarks
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running sparrow benchmarks with console output and JSON report"
)

# Benchmark target with console output and CSV report
add_custom_target(run_benchmarks_console_csv
    COMMAND sparrow_benchmarks --benchmark_format=console --benchmark_out_format=csv --benchmark_out=sparrow_benchmarks.csv
    DEPENDS sparrow_benchmarks
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running sparrow benchmarks with console output and CSV report"
)

# Benchmark target with repetitions for statistical analysis
add_custom_target(run_benchmarks_detailed
    COMMAND sparrow_benchmarks --benchmark_repetitions=5 --benchmark_display_aggregates_only=true --benchmark_format=console --benchmark_out_format=json --benchmark_out=sparrow_benchmarks_detailed.json
    DEPENDS sparrow_benchmarks
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running sparrow benchmarks with 5 repetitions and detailed statistics"
)

# Benchmark target for quick tests (smaller ranges)
add_custom_target(run_benchmarks_quick
    COMMAND sparrow_benchmarks --benchmark_filter=".*/(100|1000)$$"
    DEPENDS sparrow_benchmarks
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running sparrow benchmarks with quick test (small data sizes only)"
)

# Benchmark target for specific types
add_custom_target(run_benchmarks_int32
    COMMAND sparrow_benchmarks --benchmark_filter=".*int32.*"
    DEPENDS sparrow_benchmarks
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running sparrow benchmarks for int32_t only"
)

add_custom_target(run_benchmarks_double
    COMMAND sparrow_benchmarks --benchmark_filter=".*double.*"
    DEPENDS sparrow_benchmarks
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running sparrow benchmarks for double only"
)

add_custom_target(run_benchmarks_bool
    COMMAND sparrow_benchmarks --benchmark_filter=".*bool.*"
    DEPENDS sparrow_benchmarks
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running sparrow benchmarks for bool only"
)

# Help target to display available benchmark targets
add_custom_target(benchmarks_help
    COMMAND ${CMAKE_COMMAND} -E echo "Available benchmark targets:"
    COMMAND ${CMAKE_COMMAND} -E echo "  run_benchmarks          - Run all benchmarks with console output"
    COMMAND ${CMAKE_COMMAND} -E echo "  run_benchmarks_json     - Run all benchmarks with JSON output"
    COMMAND ${CMAKE_COMMAND} -E echo "  run_benchmarks_csv      - Run all benchmarks with CSV output"
    COMMAND ${CMAKE_COMMAND} -E echo "  run_benchmarks_console_json - Console output + JSON file"
    COMMAND ${CMAKE_COMMAND} -E echo "  run_benchmarks_console_csv  - Console output + CSV file"
    COMMAND ${CMAKE_COMMAND} -E echo "  run_benchmarks_detailed - 5 repetitions with statistics"
    COMMAND ${CMAKE_COMMAND} -E echo "  run_benchmarks_quick    - Quick test (small sizes only)"
    COMMAND ${CMAKE_COMMAND} -E echo "  run_benchmarks_int32    - Test int32_t type only"
    COMMAND ${CMAKE_COMMAND} -E echo "  run_benchmarks_double   - Test double type only"
    COMMAND ${CMAKE_COMMAND} -E echo "  run_benchmarks_bool     - Test bool type only"
    COMMAND ${CMAKE_COMMAND} -E echo "  benchmarks_help         - Show this help message"
    COMMENT "Displaying available benchmark targets"
)
