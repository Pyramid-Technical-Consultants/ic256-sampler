"""Quick script to run database benchmarks."""

import sys
from tests.benchmark_databases import DatabaseBenchmark

if __name__ == "__main__":
    print("=" * 80)
    print("Database Performance Benchmarks")
    print("=" * 80)
    print()
    
    benchmark = DatabaseBenchmark()
    benchmark.run_all_benchmarks()
    benchmark.print_results()
    
    print("\nBenchmarks completed!")
