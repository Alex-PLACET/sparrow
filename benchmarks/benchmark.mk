# Sparrow Benchmark Makefile Aliases
# Include this file in your build process for easier benchmark execution

# Default target - show help
benchmarks: benchmarks_help

# Basic execution
benchmark: run_benchmarks
benchmark-json: run_benchmarks_json  
benchmark-csv: run_benchmarks_csv

# Quick testing
benchmark-quick: run_benchmarks_quick
benchmark-int32: run_benchmarks_int32
benchmark-double: run_benchmarks_double
benchmark-bool: run_benchmarks_bool

# Advanced analysis
benchmark-detailed: run_benchmarks_detailed
benchmark-console-json: run_benchmarks_console_json
benchmark-console-csv: run_benchmarks_console_csv

# Help
benchmark-help: benchmarks_help

.PHONY: benchmarks benchmark benchmark-json benchmark-csv benchmark-quick \
        benchmark-int32 benchmark-double benchmark-bool benchmark-detailed \
        benchmark-console-json benchmark-console-csv benchmark-help
