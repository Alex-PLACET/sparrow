#!/bin/bash

# Sparrow Benchmark Runner - Demonstrates different benchmark targets
# Run this script after building the benchmarks to see various execution modes

set -e

if [ ! -f "sparrow_benchmarks" ] && [ ! -f "sparrow_benchmarks.exe" ]; then
    echo "❌ Error: sparrow_benchmarks executable not found"
    echo "Please build the benchmarks first with: cmake --build . --target sparrow_benchmarks"
    exit 1
fi

echo "🏁 Running Sparrow Benchmark Demonstrations"
echo "============================================="
echo ""

echo "1️⃣  Quick benchmark (small sizes only)..."
make run_benchmarks_quick
echo ""

echo "2️⃣  Running int32_t benchmarks only..."
make run_benchmarks_int32
echo ""

echo "3️⃣  Running benchmarks with JSON output..."
make run_benchmarks_json
echo "📄 Results saved to: sparrow_benchmarks.json"
echo ""

echo "4️⃣  Running benchmarks with CSV output..."
make run_benchmarks_csv
echo "📊 Results saved to: sparrow_benchmarks.csv"
echo ""

echo "5️⃣  Running detailed benchmarks (with repetitions)..."
make run_benchmarks_detailed
echo "📈 Detailed results saved to: sparrow_benchmarks_detailed.json"
echo ""

echo "✅ Benchmark demonstration complete!"
echo ""
echo "Generated files:"
echo "  - sparrow_benchmarks.json (JSON format)"
echo "  - sparrow_benchmarks.csv (CSV format)"  
echo "  - sparrow_benchmarks_detailed.json (detailed statistics)"
echo ""
echo "For more options, run: make benchmarks_help"
