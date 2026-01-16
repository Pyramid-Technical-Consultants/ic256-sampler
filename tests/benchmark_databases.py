"""Benchmark suite for IODatabase and VirtualDatabase performance.

This module provides comprehensive benchmarks to measure and compare
performance of database operations under various load conditions.
"""

import time
import statistics
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass
from ic256_sampler.io_database import IODatabase, DataPoint
from ic256_sampler.virtual_database import (
    VirtualDatabase,
    ColumnDefinition,
    ChannelPolicy,
)


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""
    operation: str
    data_size: str
    iterations: int
    total_time: float
    avg_time: float
    min_time: float
    max_time: float
    median_time: float
    std_dev: float
    throughput: float  # Operations per second


class DatabaseBenchmark:
    """Benchmark suite for database operations."""
    
    def __init__(self):
        self.results: List[BenchmarkResult] = []
    
    def _timeit(self, func, iterations: int = 1) -> Tuple[float, List[float]]:
        """Time a function execution.
        
        Args:
            func: Function to time (callable with no args)
            iterations: Number of iterations
            
        Returns:
            Tuple of (total_time, list of individual times)
        """
        times = []
        for _ in range(iterations):
            start = time.perf_counter()
            func()
            end = time.perf_counter()
            times.append(end - start)
        return sum(times), times
    
    def _record_result(
        self,
        operation: str,
        data_size: str,
        iterations: int,
        times: List[float],
    ) -> BenchmarkResult:
        """Record benchmark result."""
        total_time = sum(times)
        avg_time = statistics.mean(times)
        min_time = min(times)
        max_time = max(times)
        median_time = statistics.median(times)
        std_dev = statistics.stdev(times) if len(times) > 1 else 0.0
        throughput = iterations / total_time if total_time > 0 else 0.0
        
        result = BenchmarkResult(
            operation=operation,
            data_size=data_size,
            iterations=iterations,
            total_time=total_time,
            avg_time=avg_time,
            min_time=min_time,
            max_time=max_time,
            median_time=median_time,
            std_dev=std_dev,
            throughput=throughput,
        )
        self.results.append(result)
        return result
    
    def benchmark_io_add_data_point(
        self,
        num_channels: int = 10,
        points_per_channel: int = 1000,
        iterations: int = 5,
    ) -> BenchmarkResult:
        """Benchmark adding data points to IODatabase.
        
        Args:
            num_channels: Number of channels to create
            points_per_channel: Number of points per channel
            iterations: Number of benchmark iterations
        """
        def run_benchmark():
            db = IODatabase()
            base_timestamp = 1000000000000000000  # 1e18 nanoseconds
            timestamp_interval = int(1e6)  # 1ms intervals
            
            for channel_idx in range(num_channels):
                channel_path = f"/test/channel_{channel_idx}"
                for point_idx in range(points_per_channel):
                    timestamp = base_timestamp + (point_idx * timestamp_interval)
                    value = channel_idx * 1000 + point_idx
                    db.add_data_point(channel_path, value, timestamp)
        
        _, times = self._timeit(run_benchmark, iterations)
        data_size = f"{num_channels}ch_{points_per_channel}pts"
        return self._record_result("io_add_data_point", data_size, iterations, times)
    
    def benchmark_io_get_channel(
        self,
        num_channels: int = 10,
        points_per_channel: int = 1000,
        iterations: int = 1000,
    ) -> BenchmarkResult:
        """Benchmark getting channel from IODatabase.
        
        Args:
            num_channels: Number of channels in database
            points_per_channel: Number of points per channel
            iterations: Number of benchmark iterations
        """
        # Setup database
        db = IODatabase()
        base_timestamp = 1000000000000000000
        timestamp_interval = int(1e6)
        
        for channel_idx in range(num_channels):
            channel_path = f"/test/channel_{channel_idx}"
            for point_idx in range(points_per_channel):
                timestamp = base_timestamp + (point_idx * timestamp_interval)
                db.add_data_point(channel_path, point_idx, timestamp)
        
        channel_paths = [f"/test/channel_{i}" for i in range(num_channels)]
        
        def run_benchmark():
            import random
            channel_path = random.choice(channel_paths)
            _ = db.get_channel(channel_path)
        
        _, times = self._timeit(run_benchmark, iterations)
        data_size = f"{num_channels}ch_{points_per_channel}pts"
        return self._record_result("io_get_channel", data_size, iterations, times)
    
    def benchmark_io_get_data_at_time(
        self,
        num_channels: int = 10,
        points_per_channel: int = 1000,
        iterations: int = 100,
    ) -> BenchmarkResult:
        """Benchmark getting data at specific time from IODatabase.
        
        Args:
            num_channels: Number of channels in database
            points_per_channel: Number of points per channel
            iterations: Number of benchmark iterations
        """
        # Setup database
        db = IODatabase()
        base_timestamp = 1000000000000000000
        timestamp_interval = int(1e6)  # 1ms
        
        for channel_idx in range(num_channels):
            channel_path = f"/test/channel_{channel_idx}"
            for point_idx in range(points_per_channel):
                timestamp = base_timestamp + (point_idx * timestamp_interval)
                db.add_data_point(channel_path, point_idx, timestamp)
        
        # Calculate time range
        max_elapsed = (points_per_channel - 1) * (timestamp_interval / 1e9)
        
        def run_benchmark():
            import random
            target_time = random.uniform(0, max_elapsed)
            _ = db.get_data_at_time(target_time, tolerance=0.001)
        
        _, times = self._timeit(run_benchmark, iterations)
        data_size = f"{num_channels}ch_{points_per_channel}pts"
        return self._record_result("io_get_data_at_time", data_size, iterations, times)
    
    def benchmark_io_get_data_in_range(
        self,
        num_channels: int = 10,
        points_per_channel: int = 1000,
        iterations: int = 50,
    ) -> BenchmarkResult:
        """Benchmark getting data in time range from IODatabase.
        
        Args:
            num_channels: Number of channels in database
            points_per_channel: Number of points per channel
            iterations: Number of benchmark iterations
        """
        # Setup database
        db = IODatabase()
        base_timestamp = 1000000000000000000
        timestamp_interval = int(1e6)  # 1ms
        
        for channel_idx in range(num_channels):
            channel_path = f"/test/channel_{channel_idx}"
            for point_idx in range(points_per_channel):
                timestamp = base_timestamp + (point_idx * timestamp_interval)
                db.add_data_point(channel_path, point_idx, timestamp)
        
        max_elapsed = (points_per_channel - 1) * (timestamp_interval / 1e9)
        
        def run_benchmark():
            import random
            start = random.uniform(0, max_elapsed * 0.5)
            end = start + random.uniform(0.01, max_elapsed * 0.5)
            _ = db.get_data_in_range(start, end)
        
        _, times = self._timeit(run_benchmark, iterations)
        data_size = f"{num_channels}ch_{points_per_channel}pts"
        return self._record_result("io_get_data_in_range", data_size, iterations, times)
    
    def benchmark_io_get_statistics(
        self,
        num_channels: int = 10,
        points_per_channel: int = 1000,
        iterations: int = 10,
    ) -> BenchmarkResult:
        """Benchmark getting statistics from IODatabase.
        
        Args:
            num_channels: Number of channels in database
            points_per_channel: Number of points per channel
            iterations: Number of benchmark iterations
        """
        # Setup database
        db = IODatabase()
        base_timestamp = 1000000000000000000
        timestamp_interval = int(1e6)
        
        for channel_idx in range(num_channels):
            channel_path = f"/test/channel_{channel_idx}"
            for point_idx in range(points_per_channel):
                timestamp = base_timestamp + (point_idx * timestamp_interval)
                db.add_data_point(channel_path, point_idx, timestamp)
        
        def run_benchmark():
            _ = db.get_statistics()
        
        _, times = self._timeit(run_benchmark, iterations)
        data_size = f"{num_channels}ch_{points_per_channel}pts"
        return self._record_result("io_get_statistics", data_size, iterations, times)
    
    def benchmark_virtual_build(
        self,
        num_channels: int = 10,
        points_per_channel: int = 1000,
        sampling_rate: int = 500,
        iterations: int = 3,
    ) -> BenchmarkResult:
        """Benchmark building VirtualDatabase.
        
        Args:
            num_channels: Number of channels in IO database
            points_per_channel: Number of points per channel
            sampling_rate: Sampling rate in Hz
            iterations: Number of benchmark iterations
        """
        def run_benchmark():
            # Setup IO database
            io_db = IODatabase()
            base_timestamp = 1000000000000000000
            timestamp_interval = int(1e6)  # 1ms
            
            reference_channel = "/test/channel_0"
            for channel_idx in range(num_channels):
                channel_path = f"/test/channel_{channel_idx}"
                for point_idx in range(points_per_channel):
                    timestamp = base_timestamp + (point_idx * timestamp_interval)
                    io_db.add_data_point(channel_path, point_idx, timestamp)
            
            # Create columns
            columns = [
                ColumnDefinition(
                    name=f"Channel_{i}",
                    channel_path=f"/test/channel_{i}",
                    policy=ChannelPolicy.INTERPOLATED,
                )
                for i in range(num_channels)
            ]
            
            # Build virtual database
            virtual_db = VirtualDatabase(
                io_database=io_db,
                reference_channel=reference_channel,
                sampling_rate=sampling_rate,
                columns=columns,
            )
            virtual_db.build()
        
        _, times = self._timeit(run_benchmark, iterations)
        data_size = f"{num_channels}ch_{points_per_channel}pts_{sampling_rate}Hz"
        return self._record_result("virtual_build", data_size, iterations, times)
    
    def benchmark_virtual_rebuild(
        self,
        num_channels: int = 10,
        initial_points: int = 1000,
        new_points: int = 100,
        sampling_rate: int = 500,
        iterations: int = 10,
    ) -> BenchmarkResult:
        """Benchmark incremental rebuild of VirtualDatabase.
        
        Args:
            num_channels: Number of channels in IO database
            initial_points: Initial points per channel
            new_points: New points to add per channel
            sampling_rate: Sampling rate in Hz
            iterations: Number of benchmark iterations
        """
        def run_benchmark():
            # Setup IO database with initial data
            io_db = IODatabase()
            base_timestamp = 1000000000000000000
            timestamp_interval = int(1e6)  # 1ms
            
            reference_channel = "/test/channel_0"
            for channel_idx in range(num_channels):
                channel_path = f"/test/channel_{channel_idx}"
                for point_idx in range(initial_points):
                    timestamp = base_timestamp + (point_idx * timestamp_interval)
                    io_db.add_data_point(channel_path, point_idx, timestamp)
            
            # Create columns
            columns = [
                ColumnDefinition(
                    name=f"Channel_{i}",
                    channel_path=f"/test/channel_{i}",
                    policy=ChannelPolicy.INTERPOLATED,
                )
                for i in range(num_channels)
            ]
            
            # Build initial virtual database
            virtual_db = VirtualDatabase(
                io_database=io_db,
                reference_channel=reference_channel,
                sampling_rate=sampling_rate,
                columns=columns,
            )
            virtual_db.build()
            
            # Add new data points
            last_timestamp = base_timestamp + ((initial_points - 1) * timestamp_interval)
            for channel_idx in range(num_channels):
                channel_path = f"/test/channel_{channel_idx}"
                for point_idx in range(new_points):
                    timestamp = last_timestamp + ((point_idx + 1) * timestamp_interval)
                    io_db.add_data_point(channel_path, initial_points + point_idx, timestamp)
            
            # Rebuild incrementally
            virtual_db.rebuild()
        
        _, times = self._timeit(run_benchmark, iterations)
        data_size = f"{num_channels}ch_{initial_points}+{new_points}pts_{sampling_rate}Hz"
        return self._record_result("virtual_rebuild", data_size, iterations, times)
    
    def benchmark_virtual_get_row_data(
        self,
        num_channels: int = 10,
        points_per_channel: int = 1000,
        sampling_rate: int = 500,
        iterations: int = 100,
    ) -> BenchmarkResult:
        """Benchmark getting row data at time from VirtualDatabase.
        
        Args:
            num_channels: Number of channels in IO database
            points_per_channel: Number of points per channel
            sampling_rate: Sampling rate in Hz
            iterations: Number of benchmark iterations
        """
        # Setup IO database
        io_db = IODatabase()
        base_timestamp = 1000000000000000000
        timestamp_interval = int(1e6)  # 1ms
        
        reference_channel = "/test/channel_0"
        for channel_idx in range(num_channels):
            channel_path = f"/test/channel_{channel_idx}"
            for point_idx in range(points_per_channel):
                timestamp = base_timestamp + (point_idx * timestamp_interval)
                io_db.add_data_point(channel_path, point_idx, timestamp)
        
        # Create columns
        columns = [
            ColumnDefinition(
                name=f"Channel_{i}",
                channel_path=f"/test/channel_{i}",
                policy=ChannelPolicy.INTERPOLATED,
            )
            for i in range(num_channels)
        ]
        
        # Build virtual database
        virtual_db = VirtualDatabase(
            io_database=io_db,
            reference_channel=reference_channel,
            sampling_rate=sampling_rate,
            columns=columns,
        )
        virtual_db.build()
        
        # Calculate time range
        max_elapsed = (points_per_channel - 1) * (timestamp_interval / 1e9)
        row_interval = 1.0 / sampling_rate
        
        def run_benchmark():
            import random
            target_time = random.uniform(0, max_elapsed)
            _ = virtual_db._get_row_data_at_time(target_time, row_interval)
        
        _, times = self._timeit(run_benchmark, iterations)
        data_size = f"{num_channels}ch_{points_per_channel}pts_{sampling_rate}Hz"
        return self._record_result("virtual_get_row_data", data_size, iterations, times)
    
    def run_all_benchmarks(self) -> List[BenchmarkResult]:
        """Run all benchmarks with various data sizes."""
        print("Running IODatabase benchmarks...")
        
        # Small dataset only - quick benchmarks
        print("  Small dataset (3 channels, 50 points)...")
        self.benchmark_io_add_data_point(3, 50, 3)
        self.benchmark_io_get_channel(3, 50, 100)
        self.benchmark_io_get_data_at_time(3, 50, 20)
        self.benchmark_io_get_data_in_range(3, 50, 10)
        self.benchmark_io_get_statistics(3, 50, 3)
        
        # Medium dataset - reduced size
        print("  Medium dataset (5 channels, 200 points)...")
        self.benchmark_io_add_data_point(5, 200, 2)
        self.benchmark_io_get_channel(5, 200, 100)
        self.benchmark_io_get_data_at_time(5, 200, 20)
        self.benchmark_io_get_data_in_range(5, 200, 10)
        self.benchmark_io_get_statistics(5, 200, 2)
        
        print("\nRunning VirtualDatabase benchmarks...")
        
        # Small dataset only - quick benchmarks
        print("  Small dataset (3 channels, 50 points, 50 Hz)...")
        self.benchmark_virtual_build(3, 50, 50, 2)
        self.benchmark_virtual_rebuild(3, 50, 10, 50, 3)
        self.benchmark_virtual_get_row_data(3, 50, 50, 20)
        
        # Medium dataset - reduced size
        print("  Medium dataset (5 channels, 200 points, 200 Hz)...")
        self.benchmark_virtual_build(5, 200, 200, 1)
        self.benchmark_virtual_rebuild(5, 200, 20, 200, 2)
        self.benchmark_virtual_get_row_data(5, 200, 200, 20)
        
        # Large dataset - to test scalability
        print("  Large dataset (10 channels, 1000 points, 500 Hz)...")
        self.benchmark_virtual_build(10, 1000, 500, 1)
        self.benchmark_virtual_rebuild(10, 1000, 100, 500, 1)
        self.benchmark_virtual_get_row_data(10, 1000, 500, 10)
        
        # Extra large dataset - ~5k rows (10 channels, 5000 points, 1000 Hz)
        print("  Extra large dataset (10 channels, 5000 points, 1000 Hz)...")
        self.benchmark_virtual_build(10, 5000, 1000, 1)
        self.benchmark_virtual_rebuild(10, 5000, 500, 1000, 1)
        self.benchmark_virtual_get_row_data(10, 5000, 1000, 5)
        
        # Very large dataset - ~10k rows (10 channels, 10000 points, 1000 Hz)
        print("  Very large dataset (10 channels, 10000 points, 1000 Hz)...")
        self.benchmark_virtual_build(10, 10000, 1000, 1)
        self.benchmark_virtual_rebuild(10, 10000, 1000, 1000, 1)
        self.benchmark_virtual_get_row_data(10, 10000, 1000, 3)
        
        # Huge dataset - ~50k rows (10 channels, 50000 points, 1000 Hz)
        print("  Huge dataset (10 channels, 50000 points, 1000 Hz)...")
        self.benchmark_virtual_build(10, 50000, 1000, 1)
        self.benchmark_virtual_rebuild(10, 50000, 5000, 1000, 1)
        self.benchmark_virtual_get_row_data(10, 50000, 1000, 2)
        
        # Massive dataset - ~100k rows (10 channels, 100000 points, 1000 Hz)
        print("  Massive dataset (10 channels, 100000 points, 1000 Hz)...")
        self.benchmark_virtual_build(10, 100000, 1000, 1)
        self.benchmark_virtual_rebuild(10, 100000, 10000, 1000, 1)
        self.benchmark_virtual_get_row_data(10, 100000, 1000, 1)
        
        # Extreme dataset - ~250k rows (10 channels, 250000 points, 1000 Hz)
        print("  Extreme dataset (10 channels, 250000 points, 1000 Hz)...")
        self.benchmark_virtual_build(10, 250000, 1000, 1)
        self.benchmark_virtual_rebuild(10, 250000, 25000, 1000, 1)
        self.benchmark_virtual_get_row_data(10, 250000, 1000, 1)
        
        # Ultra dataset - ~500k rows (10 channels, 500000 points, 1000 Hz)
        print("  Ultra dataset (10 channels, 500000 points, 1000 Hz)...")
        self.benchmark_virtual_build(10, 500000, 1000, 1)
        self.benchmark_virtual_rebuild(10, 500000, 50000, 1000, 1)
        self.benchmark_virtual_get_row_data(10, 500000, 1000, 1)
        
        # Maximum dataset - ~1M rows (10 channels, 1000000 points, 1000 Hz)
        print("  Maximum dataset (10 channels, 1000000 points, 1000 Hz)...")
        self.benchmark_virtual_build(10, 1000000, 1000, 1)
        self.benchmark_virtual_rebuild(10, 1000000, 100000, 1000, 1)
        self.benchmark_virtual_get_row_data(10, 1000000, 1000, 1)
        
        return self.results
    
    def print_results(self):
        """Print benchmark results in a formatted table."""
        if not self.results:
            print("No benchmark results to display.")
            return
        
        print("\n" + "=" * 100)
        print("BENCHMARK RESULTS")
        print("=" * 100)
        print(f"{'Operation':<30} {'Data Size':<30} {'Avg Time (ms)':<15} {'Throughput (ops/s)':<20}")
        print("-" * 100)
        
        for result in self.results:
            avg_ms = result.avg_time * 1000
            print(
                f"{result.operation:<30} "
                f"{result.data_size:<30} "
                f"{avg_ms:>12.4f} ms  "
                f"{result.throughput:>15.2f}"
            )
        
        print("=" * 100)
        
        # Group by operation and show statistics
        print("\n" + "=" * 100)
        print("PERFORMANCE SUMMARY BY OPERATION")
        print("=" * 100)
        
        operations = {}
        for result in self.results:
            if result.operation not in operations:
                operations[result.operation] = []
            operations[result.operation].append(result)
        
        for operation, results in sorted(operations.items()):
            print(f"\n{operation}:")
            print(f"  {'Data Size':<30} {'Avg (ms)':<12} {'Min (ms)':<12} {'Max (ms)':<12} {'StdDev (ms)':<12}")
            print("  " + "-" * 78)
            for r in sorted(results, key=lambda x: x.avg_time):
                print(
                    f"  {r.data_size:<30} "
                    f"{r.avg_time*1000:>10.4f}  "
                    f"{r.min_time*1000:>10.4f}  "
                    f"{r.max_time*1000:>10.4f}  "
                    f"{r.std_dev*1000:>10.4f}"
                )


def main():
    """Run all benchmarks and display results."""
    benchmark = DatabaseBenchmark()
    benchmark.run_all_benchmarks()
    benchmark.print_results()


if __name__ == "__main__":
    main()
