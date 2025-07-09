# Sparrow Benchmark Runner - Demonstrates different benchmark targets
# Run this script after building the benchmarks to see various execution modes

$ErrorActionPreference = "Stop"

if (-not (Test-Path "sparrow_benchmarks.exe") -and -not (Test-Path "sparrow_benchmarks")) {
    Write-Host "❌ Error: sparrow_benchmarks executable not found" -ForegroundColor Red
    Write-Host "Please build the benchmarks first with: cmake --build . --target sparrow_benchmarks"
    exit 1
}

Write-Host "🏁 Running Sparrow Benchmark Demonstrations" -ForegroundColor Green
Write-Host "============================================="
Write-Host ""

Write-Host "1️⃣  Quick benchmark (small sizes only)..." -ForegroundColor Yellow
cmake --build . --target run_benchmarks_quick
Write-Host ""

Write-Host "2️⃣  Running int32_t benchmarks only..." -ForegroundColor Yellow
cmake --build . --target run_benchmarks_int32
Write-Host ""

Write-Host "3️⃣  Running benchmarks with JSON output..." -ForegroundColor Yellow
cmake --build . --target run_benchmarks_json
Write-Host "📄 Results saved to: sparrow_benchmarks.json" -ForegroundColor Green
Write-Host ""

Write-Host "4️⃣  Running benchmarks with CSV output..." -ForegroundColor Yellow
cmake --build . --target run_benchmarks_csv
Write-Host "📊 Results saved to: sparrow_benchmarks.csv" -ForegroundColor Green
Write-Host ""

Write-Host "5️⃣  Running detailed benchmarks (with repetitions)..." -ForegroundColor Yellow
cmake --build . --target run_benchmarks_detailed
Write-Host "📈 Detailed results saved to: sparrow_benchmarks_detailed.json" -ForegroundColor Green
Write-Host ""

Write-Host "✅ Benchmark demonstration complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Generated files:"
Write-Host "  - sparrow_benchmarks.json (JSON format)"
Write-Host "  - sparrow_benchmarks.csv (CSV format)"
Write-Host "  - sparrow_benchmarks_detailed.json (detailed statistics)"
Write-Host ""
Write-Host "For more options, run: cmake --build . --target benchmarks_help"
