"""Simple integration test to isolate statistics accuracy bug.

This test focuses specifically on verifying that ModelCollector statistics
match the actual CSV row count after collection completes.
"""

import pytest
import time
import threading
import csv
from pathlib import Path
from ic256_sampler.device_manager import DeviceManager, IC256_CONFIG
from ic256_sampler.model_collector import ModelCollector
from ic256_sampler.ic256_model import IC256Model


# Mark all tests as integration tests
pytestmark = pytest.mark.integration


class TestStatisticsAccuracy:
    """Tests to verify statistics accuracy in ModelCollector."""
    
    @pytest.mark.integration
    def test_collector_statistics_match_csv_rows_simple(self, require_ic256_device, tmp_path):
        """Simple test: Manually call collect_iteration to avoid complex finish detection.
        
        This avoids the hanging issue in collect_data_with_model by manually
        controlling the collection loop.
        """
        sampling_rate = 500
        collection_duration = 2.0
        
        # Setup
        device_manager = DeviceManager()
        stop_event = threading.Event()
        device_manager.stop_event = stop_event
        
        assert device_manager.add_device(IC256_CONFIG, require_ic256_device, sampling_rate)
        
        model = IC256Model()
        collector = ModelCollector(
            device_manager=device_manager,
            model=model,
            reference_channel=model.get_reference_channel(),
            sampling_rate=sampling_rate,
            file_path=str(tmp_path / "test_stats_simple.csv"),
            device_name="ic256_45",
            note="Statistics Test",
        )
        
        # Start collection
        collector.start()
        
        # Collect for duration - manually call collect_iteration
        start_time = time.time()
        iteration = 0
        while time.time() - start_time < collection_duration:
            collector.collect_iteration()
            iteration += 1
            time.sleep(0.001)  # Small delay between iterations
        
        # Stop device collection but continue processing
        collector.stop()
        
        # Process remaining data with timeout
        max_final_iterations = 1000
        for i in range(max_final_iterations):
            collector.collect_iteration()
            # Check if we're done
            if collector.is_finished():
                break
            if i % 100 == 0:
                time.sleep(0.01)  # Small sleep every 100 iterations
        
        # Finalize
        collector.finalize()
        
        # Get statistics
        stats = collector.get_statistics()
        stats_rows = stats.get('rows', 0)
        
        # Count actual CSV rows
        csv_file = tmp_path / "test_stats_simple.csv"
        csv_row_count = 0
        if csv_file.exists():
            with open(csv_file, 'r', newline='', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                next(reader, None)  # Skip header
                for row in reader:
                    if row:
                        csv_row_count += 1
        
        print(f"\nStatistics Accuracy Test:")
        print(f"  Iterations: {iteration}")
        print(f"  Collector statistics['rows']: {stats_rows}")
        print(f"  CSVWriter.rows_written: {collector.csv_writer.rows_written}")
        print(f"  Actual CSV row count: {csv_row_count}")
        print(f"  Difference: {abs(stats_rows - csv_row_count)}")
        
        # Cleanup
        stop_event.set()
        device_manager.stop()
        
        # These should match exactly
        assert stats_rows == csv_row_count, \
            f"Statistics rows ({stats_rows}) must match CSV row count ({csv_row_count})"
        
        assert collector.csv_writer.rows_written == csv_row_count, \
            f"CSVWriter.rows_written ({collector.csv_writer.rows_written}) must match CSV row count ({csv_row_count})"
    
    @pytest.mark.integration
    def test_statistics_update_after_each_iteration(self, require_ic256_device, tmp_path):
        """Test that statistics are updated after each collect_iteration call.
        
        This directly tests the statistics update mechanism.
        """
        sampling_rate = 500
        
        device_manager = DeviceManager()
        stop_event = threading.Event()
        device_manager.stop_event = stop_event
        
        assert device_manager.add_device(IC256_CONFIG, require_ic256_device, sampling_rate)
        
        model = IC256Model()
        collector = ModelCollector(
            device_manager=device_manager,
            model=model,
            reference_channel=model.get_reference_channel(),
            sampling_rate=sampling_rate,
            file_path=str(tmp_path / "test_stats_iteration.csv"),
            device_name="ic256_45",
            note="Test",
        )
        
        collector.start()
        
        # Run a few iterations and check statistics after each
        mismatches = []
        for i in range(50):
            collector.collect_iteration()
            
            stats = collector.get_statistics()
            stats_rows = stats.get('rows', 0)
            csv_writer_rows = collector.csv_writer.rows_written
            
            if stats_rows != csv_writer_rows:
                mismatches.append({
                    'iteration': i,
                    'stats_rows': stats_rows,
                    'csv_writer_rows': csv_writer_rows,
                })
            
            time.sleep(0.01)  # Small delay
        
        collector.stop()
        
        # Finalize
        collector.finalize()
        
        # Check final state
        final_stats = collector.get_statistics()
        final_csv_rows = collector.csv_writer.rows_written
        
        # Count actual CSV rows
        csv_file = tmp_path / "test_stats_iteration.csv"
        actual_csv_rows = 0
        if csv_file.exists():
            with open(csv_file, 'r', newline='', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if row:
                        actual_csv_rows += 1
        
        print(f"\nStatistics Update Test:")
        print(f"  Mismatches during collection: {len(mismatches)}")
        if mismatches:
            print(f"  First few mismatches:")
            for m in mismatches[:5]:
                print(f"    Iteration {m['iteration']}: stats={m['stats_rows']}, csv_writer={m['csv_writer_rows']}")
        print(f"  Final stats['rows']: {final_stats.get('rows', 0)}")
        print(f"  Final csv_writer.rows_written: {final_csv_rows}")
        print(f"  Actual CSV rows: {actual_csv_rows}")
        
        # Cleanup
        stop_event.set()
        device_manager.stop()
        
        # Final values must match
        assert final_stats.get('rows', 0) == final_csv_rows, \
            f"Final statistics must match csv_writer.rows_written"
        assert final_csv_rows == actual_csv_rows, \
            f"csv_writer.rows_written must match actual CSV rows"
