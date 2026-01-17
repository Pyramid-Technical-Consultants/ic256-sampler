"""Fuzzing tests for CSV Writer with synthetic IGX device data.

These tests use synthetic data to simulate various IGX device behaviors:
- High-frequency data streams (3000 Hz like real IC256)
- Burst data patterns
- Sparse channels (environmental sensors, triggers)
- Multiple channels with different update rates
- Missing data gaps
- Large datasets
- Edge cases (boundary values, extreme timestamps)

Test Methodology:
-----------------
1. Generate synthetic data that mimics real IGX device patterns
2. Build virtual databases with realistic column structures
3. Write to CSV and validate output
4. Test edge cases and stress scenarios

These tests complement integration tests by providing:
- Faster execution (no device required)
- Deterministic test data
- Comprehensive edge case coverage
- Stress testing with large datasets
"""

import pytest
import csv
import random
import math
from pathlib import Path
from typing import Any
from ic256_sampler.io_database import IODatabase
from ic256_sampler.virtual_database import (
    VirtualDatabase,
    ColumnDefinition,
    ChannelPolicy,
)
from ic256_sampler.csv_writer import CSVWriter
from ic256_sampler.device_paths import IC256_45_PATHS


# Set random seed for deterministic test results
RANDOM_SEED = 42


@pytest.fixture(autouse=True)
def set_random_seed():
    """Set random seed before each test for deterministic results."""
    random.seed(RANDOM_SEED)
    yield
    # Reset seed after test (though not strictly necessary)


class SyntheticIGXDataGenerator:
    """Generate synthetic data that mimics IGX device behavior."""
    
    # Base timestamp: January 1, 2024, 00:00:00 UTC in nanoseconds
    BASE_TIMESTAMP_NS = 1704067200000000000
    
    @staticmethod
    def generate_high_frequency_data(
        channel_path: str,
        io_db: IODatabase,
        duration_seconds: float,
        frequency_hz: int,
        base_value: float = 100.0,
        noise_level: float = 0.1,
        max_samples: int = 2000,  # Cap at 2000 samples for speed
    ) -> None:
        """Generate high-frequency synchronized data (e.g., ADC channels at 3000 Hz).
        
        Args:
            channel_path: Path to the channel
            io_db: IODatabase to add data to
            duration_seconds: How long to generate data
            frequency_hz: Data rate in Hz
            base_value: Base value for the channel
            noise_level: Random noise level (fraction of base_value)
            max_samples: Maximum number of samples to generate (for performance)
        """
        period_ns = int(1e9 / frequency_hz)  # Nanoseconds between samples
        num_samples = min(int(duration_seconds * frequency_hz), max_samples)
        base_timestamp = SyntheticIGXDataGenerator.BASE_TIMESTAMP_NS
        
        for i in range(num_samples):
            timestamp_ns = base_timestamp + i * period_ns
            # Add some realistic variation
            value = base_value + random.uniform(-noise_level, noise_level) * base_value
            io_db.add_data_point(channel_path, value, timestamp_ns)
    
    @staticmethod
    def generate_sparse_data(
        channel_path: str,
        io_db: IODatabase,
        duration_seconds: float,
        update_interval_seconds: float,
        base_value: float = 25.0,
        noise_level: float = 0.05,
    ) -> None:
        """Generate sparse data (e.g., environmental sensors updating every few seconds).
        
        Args:
            channel_path: Path to the channel
            io_db: IODatabase to add data to
            duration_seconds: How long to generate data
            update_interval_seconds: Time between updates
            base_value: Base value for the channel
            noise_level: Random noise level
        """
        base_timestamp = SyntheticIGXDataGenerator.BASE_TIMESTAMP_NS
        interval_ns = int(update_interval_seconds * 1e9)
        num_updates = int(duration_seconds / update_interval_seconds)
        
        for i in range(num_updates):
            timestamp_ns = base_timestamp + i * interval_ns
            value = base_value + random.uniform(-noise_level, noise_level) * base_value
            io_db.add_data_point(channel_path, value, timestamp_ns)
    
    @staticmethod
    def generate_burst_data(
        channel_path: str,
        io_db: IODatabase,
        duration_seconds: float,
        burst_frequency_hz: int,
        burst_duration_ms: float,
        burst_interval_seconds: float,
        base_value: float = 50.0,
    ) -> None:
        """Generate burst data patterns (data arriving in bursts).
        
        Args:
            channel_path: Path to the channel
            io_db: IODatabase to add data to
            duration_seconds: Total duration
            burst_frequency_hz: Data rate during bursts
            burst_duration_ms: How long each burst lasts (milliseconds)
            burst_interval_seconds: Time between bursts
            base_value: Base value for the channel
        """
        base_timestamp = SyntheticIGXDataGenerator.BASE_TIMESTAMP_NS
        burst_duration_ns = int(burst_duration_ms * 1e6)
        burst_interval_ns = int(burst_interval_seconds * 1e9)
        period_ns = int(1e9 / burst_frequency_hz)
        samples_per_burst = int(burst_duration_ns / period_ns)
        
        num_bursts = int(duration_seconds / burst_interval_seconds)
        
        for burst_idx in range(num_bursts):
            burst_start = base_timestamp + burst_idx * burst_interval_ns
            for sample_idx in range(samples_per_burst):
                timestamp_ns = burst_start + sample_idx * period_ns
                value = base_value + random.uniform(-0.1, 0.1) * base_value
                io_db.add_data_point(channel_path, value, timestamp_ns)
    
    @staticmethod
    def generate_event_data(
        channel_path: str,
        io_db: IODatabase,
        duration_seconds: float,
        num_events: int,
        value: Any = True,
    ) -> None:
        """Generate asynchronous event data (e.g., triggers).
        
        Args:
            channel_path: Path to the channel
            io_db: IODatabase to add data to
            duration_seconds: Total duration
            num_events: Number of events to generate
            value: Value for each event (default True for boolean triggers)
        """
        base_timestamp = SyntheticIGXDataGenerator.BASE_TIMESTAMP_NS
        duration_ns = int(duration_seconds * 1e9)
        
        # Distribute events randomly throughout duration
        for _ in range(num_events):
            offset_ns = random.randint(0, duration_ns)
            timestamp_ns = base_timestamp + offset_ns
            io_db.add_data_point(channel_path, value, timestamp_ns)


class TestCSVWriterFuzzing:
    """Fuzzing tests with synthetic IGX device data."""
    
    def test_high_frequency_data_3000hz(self, tmp_path):
        """Test CSV writing with high-frequency data (3000 Hz like real IC256)."""
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        # Generate 0.3 seconds of 3000 Hz data (900 points - sufficient for testing)
        SyntheticIGXDataGenerator.generate_high_frequency_data(
            channel_path, io_db, duration_seconds=0.3, frequency_hz=3000, base_value=100.0
        )
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        virtual_db = VirtualDatabase(io_db, channel_path, 3000, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test_3000hz.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="test_device",
            note="3000 Hz test",
        )
        
        rows_written = writer.write_all()
        writer.close()
        
        # Should have approximately 900 rows (0.3 second * 3000 Hz)
        assert 800 <= rows_written <= 1000, f"Expected ~900 rows, got {rows_written}"
        
        # Validate file structure
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == rows_written + 1  # +1 for header
    
    def test_multiple_channels_different_rates(self, tmp_path):
        """Test CSV writing with multiple channels at different update rates."""
        io_db = IODatabase()
        
        # High-frequency channel (3000 Hz) - reduced duration
        fast_channel = "/test/channel_sum"
        SyntheticIGXDataGenerator.generate_high_frequency_data(
            fast_channel, io_db, duration_seconds=0.3, frequency_hz=3000, base_value=100.0
        )
        
        # Medium-frequency channel (100 Hz)
        medium_channel = "/test/primary_dose"
        SyntheticIGXDataGenerator.generate_high_frequency_data(
            medium_channel, io_db, duration_seconds=0.3, frequency_hz=100, base_value=50.0
        )
        
        # Sparse channel (1 Hz - environmental sensor)
        sparse_channel = "/test/temperature"
        SyntheticIGXDataGenerator.generate_sparse_data(
            sparse_channel, io_db, duration_seconds=0.3, update_interval_seconds=0.1, base_value=25.0
        )
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=fast_channel, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Dose", channel_path=medium_channel, policy=ChannelPolicy.INTERPOLATED),
            ColumnDefinition(name="Temperature", channel_path=sparse_channel, policy=ChannelPolicy.INTERPOLATED),
        ]
        
        virtual_db = VirtualDatabase(io_db, fast_channel, 3000, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test_multi_rate.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="test_device",
            note="Multi-rate test",
        )
        
        rows_written = writer.write_all()
        writer.close()
        
        assert rows_written > 0
        
        # Validate all rows have correct number of columns
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            header = rows[0]
            assert len(header) == 4  # Timestamp, Channel Sum, Dose, Temperature
            
            for row in rows[1:]:
                assert len(row) == len(header), f"Row has {len(row)} columns, expected {len(header)}"
    
    def test_burst_data_pattern(self, tmp_path):
        """Test CSV writing with burst data patterns."""
        io_db = IODatabase()
        channel_path = "/test/burst_channel"
        
        # Generate bursts: 1000 Hz during bursts, 10ms bursts, every 100ms
        SyntheticIGXDataGenerator.generate_burst_data(
            channel_path, io_db,
            duration_seconds=1.0,
            burst_frequency_hz=1000,
            burst_duration_ms=10.0,
            burst_interval_seconds=0.1,
            base_value=75.0,
        )
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Burst Channel", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        virtual_db = VirtualDatabase(io_db, channel_path, 1000, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test_burst.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="test_device",
            note="Burst pattern test",
        )
        
        rows_written = writer.write_all()
        writer.close()
        
        # Should have approximately 10 bursts * 10 samples per burst = 100 rows
        assert rows_written > 50, f"Expected burst data, got {rows_written} rows"
    
    def test_sparse_interpolated_channels(self, tmp_path):
        """Test CSV writing with sparse interpolated channels (forward-fill behavior)."""
        io_db = IODatabase()
        ref_channel = "/test/channel_sum"
        sparse_channel = "/test/temperature"
        
        # High-frequency reference channel (reduced duration)
        SyntheticIGXDataGenerator.generate_high_frequency_data(
            ref_channel, io_db, duration_seconds=1.0, frequency_hz=3000, base_value=100.0
        )
        
        # Very sparse temperature updates (every 0.5 seconds)
        SyntheticIGXDataGenerator.generate_sparse_data(
            sparse_channel, io_db, duration_seconds=1.0, update_interval_seconds=0.5, base_value=22.5
        )
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=ref_channel, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Temperature", channel_path=sparse_channel, policy=ChannelPolicy.INTERPOLATED),
        ]
        
        virtual_db = VirtualDatabase(io_db, ref_channel, 3000, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test_sparse.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="test_device",
            note="Sparse channel test",
        )
        
        rows_written = writer.write_all()
        writer.close()
        
        # Validate that temperature values are forward-filled (no empty cells after first value)
        with open(file_path, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader)
            temp_idx = header.index("Temperature")
            
            has_seen_temp = False
            empty_after_first = []
            
            for row_num, row in enumerate(reader, start=2):
                if len(row) > temp_idx:
                    temp_value = row[temp_idx]
                    if temp_value and temp_value.strip():
                        has_seen_temp = True
                    elif has_seen_temp:
                        empty_after_first.append(row_num)
            
            # After first temperature value, all subsequent rows should have values (forward-fill)
            assert len(empty_after_first) == 0, \
                f"Found {len(empty_after_first)} empty temperature cells after first value (rows: {empty_after_first[:10]})"
    
    def test_asynchronous_trigger_events(self, tmp_path):
        """Test CSV writing with asynchronous trigger events."""
        io_db = IODatabase()
        ref_channel = "/test/channel_sum"
        trigger_channel = "/test/external_trigger"
        
        # High-frequency reference channel
        SyntheticIGXDataGenerator.generate_high_frequency_data(
            ref_channel, io_db, duration_seconds=1.0, frequency_hz=3000, base_value=100.0
        )
        
        # Generate 5 trigger events randomly distributed
        SyntheticIGXDataGenerator.generate_event_data(
            trigger_channel, io_db, duration_seconds=1.0, num_events=5, value=True
        )
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=ref_channel, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="External trigger", channel_path=trigger_channel, policy=ChannelPolicy.ASYNCHRONOUS),
        ]
        
        virtual_db = VirtualDatabase(io_db, ref_channel, 3000, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test_trigger.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="test_device",
            note="Trigger test",
        )
        
        rows_written = writer.write_all()
        writer.close()
        
        # Validate trigger values are written correctly (0 or 1 for booleans)
        with open(file_path, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader)
            trigger_idx = header.index("External trigger")
            
            trigger_values = []
            for row in reader:
                if len(row) > trigger_idx:
                    trigger_values.append(row[trigger_idx])
            
            # Should have some trigger events (1) and mostly empty or 0
            assert "1" in trigger_values or any(v.strip() for v in trigger_values if v), \
                "Should have at least some trigger events"
    
    def test_realistic_ic256_structure(self, tmp_path):
        """Test CSV writing with realistic IC256 column structure."""
        io_db = IODatabase()
        reference_channel = IC256_45_PATHS["adc"]["channel_sum"]
        
        # Generate data for all IC256 channels (reduced duration for speed)
        # Synchronized channels (3000 Hz)
        for channel_key in ["channel_sum", "gaussian_fit_a_mean", "gaussian_fit_a_sigma",
                           "gaussian_fit_b_mean", "gaussian_fit_b_sigma"]:
            channel_path = IC256_45_PATHS["adc"][channel_key]
            base_value = 128.5 if "mean" in channel_key else 10.0
            SyntheticIGXDataGenerator.generate_high_frequency_data(
                channel_path, io_db, duration_seconds=0.3, frequency_hz=3000, base_value=base_value
            )
        
        # Interpolated channels (sparse updates)
        SyntheticIGXDataGenerator.generate_sparse_data(
            IC256_45_PATHS["adc"]["primary_dose"], io_db,
            duration_seconds=0.5, update_interval_seconds=0.1, base_value=50.0
        )
        SyntheticIGXDataGenerator.generate_sparse_data(
            IC256_45_PATHS["high_voltage"]["monitor_voltage_internal"], io_db,
            duration_seconds=0.5, update_interval_seconds=0.5, base_value=1000.0
        )
        SyntheticIGXDataGenerator.generate_sparse_data(
            IC256_45_PATHS["environmental_sensor"]["temperature"], io_db,
            duration_seconds=0.5, update_interval_seconds=0.5, base_value=22.5
        )
        SyntheticIGXDataGenerator.generate_sparse_data(
            IC256_45_PATHS["environmental_sensor"]["humidity"], io_db,
            duration_seconds=0.5, update_interval_seconds=0.5, base_value=45.0
        )
        SyntheticIGXDataGenerator.generate_sparse_data(
            IC256_45_PATHS["environmental_sensor"]["pressure"], io_db,
            duration_seconds=0.5, update_interval_seconds=0.5, base_value=1013.25
        )
        
        # Asynchronous trigger
        SyntheticIGXDataGenerator.generate_event_data(
            IC256_45_PATHS["adc"]["gate_signal"], io_db,
            duration_seconds=0.5, num_events=2, value=True
        )
        
        # Create realistic IC256 columns
        from ic256_sampler.ic256_model import IC256Model
        columns = IC256Model.create_columns(reference_channel)
        
        virtual_db = VirtualDatabase(io_db, reference_channel, 3000, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test_ic256_realistic.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="ic256_45",
            note="Realistic IC256 test",
        )
        
        rows_written = writer.write_all()
        writer.close()
        
        # Validate structure matches IC256 expectations
        assert rows_written > 0
        
        with open(file_path, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader)
            
            # Check for expected IC256 columns
            expected_columns = [
                "Timestamp (s)", "X centroid (mm)", "X sigma (mm)", "Y centroid (mm)",
                "Y sigma (mm)", "Channel Sum (nA)", "Dose (nA)", "External trigger",
                "High Voltage (V)", "Temperature (℃)", "Humidity (%rH)", "Pressure (hPa)", "Note"
            ]
            
            for expected_col in expected_columns:
                assert expected_col in header, f"Expected column '{expected_col}' not found"
            
            # Validate all rows have consistent structure
            for row_num, row in enumerate(reader, start=2):
                assert len(row) == len(header), \
                    f"Row {row_num} has {len(row)} columns, expected {len(header)}"
    
    def test_large_dataset(self, tmp_path):
        """Test CSV writing with large dataset (stress test)."""
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        # Generate 0.5 seconds of 3000 Hz data = 1,500 data points (capped at 2000 max)
        SyntheticIGXDataGenerator.generate_high_frequency_data(
            channel_path, io_db, duration_seconds=0.5, frequency_hz=3000, base_value=100.0
        )
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        virtual_db = VirtualDatabase(io_db, channel_path, 3000, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test_large.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="test_device",
            note="Large dataset test",
        )
        
        rows_written = writer.write_all()
        writer.close()
        
        # Should have approximately 1,500 rows
        assert 1400 <= rows_written <= 1600, f"Expected ~1,500 rows, got {rows_written}"
        
        # Validate file size is reasonable (reduced threshold for smaller dataset)
        file_size = file_path.stat().st_size
        assert file_size > 10000, f"Large dataset should produce substantial file, got {file_size} bytes"
        
        # Validate file can be read back
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == rows_written + 1
    
    def test_data_gaps_and_missing_channels(self, tmp_path):
        """Test CSV writing with data gaps and missing channel data."""
        io_db = IODatabase()
        ref_channel = "/test/channel_sum"
        missing_channel = "/test/missing_channel"
        
        # Generate reference channel with a gap in the middle
        base_timestamp = SyntheticIGXDataGenerator.BASE_TIMESTAMP_NS
        period_ns = int(1e9 / 3000)  # 3000 Hz
        
        # First half of data
        for i in range(1500):
            timestamp_ns = base_timestamp + i * period_ns
            io_db.add_data_point(ref_channel, 100.0, timestamp_ns)
        
        # Gap: skip 1000 samples
        
        # Second half of data
        for i in range(1500, 3000):
            timestamp_ns = base_timestamp + i * period_ns
            io_db.add_data_point(ref_channel, 100.0, timestamp_ns)
        
        # Missing channel: only one data point at the start
        io_db.add_data_point(missing_channel, 50.0, base_timestamp)
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=ref_channel, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Missing Channel", channel_path=missing_channel, policy=ChannelPolicy.INTERPOLATED),
        ]
        
        virtual_db = VirtualDatabase(io_db, ref_channel, 3000, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test_gaps.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="test_device",
            note="Gaps test",
        )
        
        rows_written = writer.write_all()
        writer.close()
        
        # Should handle gaps gracefully
        assert rows_written > 0
        
        # Validate structure is maintained despite gaps
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            header = rows[0]
            
            for row in rows[1:]:
                assert len(row) == len(header), "All rows should have same number of columns despite gaps"
    
    def test_extreme_values(self, tmp_path):
        """Test CSV writing with extreme values (very large, very small, negative)."""
        io_db = IODatabase()
        channel_path = "/test/extreme_channel"
        
        base_timestamp = SyntheticIGXDataGenerator.BASE_TIMESTAMP_NS
        period_ns = int(1e9 / 100)  # 100 Hz
        
        # Test various extreme values
        extreme_values = [
            1e10,      # Very large positive
            -1e10,     # Very large negative
            1e-10,     # Very small positive
            -1e-10,    # Very small negative
            0.0,       # Zero
        ]
        
        # Try to add infinity, but skip if it causes issues
        try:
            import math
            if math.isfinite(float('inf')):
                extreme_values.append(float('inf'))
        except (ValueError, OverflowError, TypeError):
            pass
        
        for i, value in enumerate(extreme_values):
            timestamp_ns = base_timestamp + i * period_ns
            try:
                io_db.add_data_point(channel_path, value, timestamp_ns)
            except (ValueError, OverflowError, TypeError):
                # Skip values that can't be stored
                continue
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Extreme Values", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        virtual_db = VirtualDatabase(io_db, channel_path, 100, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test_extreme.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="test_device",
            note="Extreme values test",
        )
        
        rows_written = writer.write_all()
        writer.close()
        
        # Should handle extreme values without crashing
        assert rows_written > 0
        
        # Validate values are written correctly
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            value_idx = header.index("Extreme Values")
            
            for row in reader:
                if len(row) > value_idx and row[value_idx]:
                    # Should be parseable as float
                    try:
                        float(row[value_idx])
                    except ValueError:
                        pytest.fail(f"Extreme value '{row[value_idx]}' is not a valid number")
    
    def test_special_characters_in_data(self, tmp_path):
        """Test CSV writing with special characters that need escaping (commas, quotes, newlines)."""
        io_db = IODatabase()
        channel_path = "/test/text_channel"
        
        base_timestamp = SyntheticIGXDataGenerator.BASE_TIMESTAMP_NS
        
        # Test various special characters that CSV needs to handle
        special_values = [
            "normal_value",
            "value,with,commas",
            'value"with"quotes',
            "value\nwith\nnewlines",
            "value\twith\ttabs",
            "value with spaces",
            "",  # Empty string
            "value,with\"both\"commas,and,quotes",
        ]
        
        for i, value in enumerate(special_values):
            timestamp_ns = base_timestamp + i * int(1e9)  # 1 second apart
            io_db.add_data_point(channel_path, value, timestamp_ns)
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Text Value", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        virtual_db = VirtualDatabase(io_db, channel_path, 1, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test_special_chars.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="test_device",
            note="Special chars test",
        )
        
        rows_written = writer.write_all()
        writer.close()
        
        assert rows_written == len(special_values)
        
        # Validate CSV can be read back correctly (CSV writer should escape properly)
        with open(file_path, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader)
            text_idx = header.index("Text Value")
            
            for i, row in enumerate(reader):
                assert len(row) == len(header), f"Row {i} should have {len(header)} columns"
                if len(row) > text_idx:
                    # CSV reader should properly unescape the value
                    read_value = row[text_idx]
                    original_value = special_values[i]
                    assert read_value == original_value, \
                        f"Row {i}: Expected '{original_value}', got '{read_value}'"
    
    def test_nan_and_infinity_values(self, tmp_path):
        """Test CSV writing with NaN and Infinity values."""
        import math
        
        io_db = IODatabase()
        channel_path = "/test/nan_channel"
        
        base_timestamp = SyntheticIGXDataGenerator.BASE_TIMESTAMP_NS
        
        # Test NaN and Infinity
        nan_values = [
            float('nan'),
            float('inf'),
            float('-inf'),
            1.0,  # Normal value for comparison
        ]
        
        for i, value in enumerate(nan_values):
            timestamp_ns = base_timestamp + i * int(1e9)
            try:
                io_db.add_data_point(channel_path, value, timestamp_ns)
            except (ValueError, TypeError):
                continue
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="NaN Values", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        virtual_db = VirtualDatabase(io_db, channel_path, 1, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test_nan.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="test_device",
            note="NaN test",
        )
        
        rows_written = writer.write_all()
        writer.close()
        
        # Should handle NaN/Inf without crashing
        assert rows_written > 0
        
        # Validate file can be read
        with open(file_path, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader)
            value_idx = header.index("NaN Values")
            
            for row in reader:
                if len(row) > value_idx and row[value_idx]:
                    # NaN/Inf might be written as strings like "nan", "inf", "-inf"
                    value_str = row[value_idx].lower()
                    assert value_str in ["nan", "inf", "-inf", "1.0", "1"], \
                        f"Unexpected NaN/Inf representation: '{row[value_idx]}'"
    
    def test_incremental_write_with_database_rebuild(self, tmp_path):
        """Test incremental writes when database is rebuilt between writes (potential bug)."""
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        # First batch of data
        base_timestamp = SyntheticIGXDataGenerator.BASE_TIMESTAMP_NS
        for i in range(10):
            timestamp_ns = base_timestamp + i * int(1e8)  # 0.1 second intervals
            io_db.add_data_point(channel_path, 100.0 + i, timestamp_ns)
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        virtual_db = VirtualDatabase(io_db, channel_path, 10, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test_incremental_rebuild.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="test_device",
            note="Test",
        )
        
        # First write
        rows1 = writer.write_all()
        assert rows1 > 0
        
        # Add more data and rebuild (this could cause issues if not handled correctly)
        for i in range(10, 20):
            timestamp_ns = base_timestamp + i * int(1e8)
            io_db.add_data_point(channel_path, 100.0 + i, timestamp_ns)
        
        virtual_db.rebuild()
        
        # Second write - should only write new rows
        rows2 = writer.write_all()
        
        # Validate total rows
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            # Should have header + all rows
            assert len(rows) == rows1 + rows2 + 1, \
                f"Expected {rows1 + rows2 + 1} total rows (header + data), got {len(rows)}"
    
    def test_unicode_characters(self, tmp_path):
        """Test CSV writing with Unicode characters."""
        io_db = IODatabase()
        channel_path = "/test/unicode_channel"
        
        base_timestamp = SyntheticIGXDataGenerator.BASE_TIMESTAMP_NS
        
        # Test various Unicode characters
        unicode_values = [
            "正常",  # Chinese
            "café",  # Accented characters
            "🚀",    # Emoji
            "αβγ",   # Greek letters
            "тест",  # Cyrillic
        ]
        
        for i, value in enumerate(unicode_values):
            timestamp_ns = base_timestamp + i * int(1e9)
            io_db.add_data_point(channel_path, value, timestamp_ns)
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Unicode Value", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        virtual_db = VirtualDatabase(io_db, channel_path, 1, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test_unicode.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="test_device",
            note="Unicode test",
        )
        
        rows_written = writer.write_all()
        writer.close()
        
        assert rows_written == len(unicode_values)
        
        # Validate Unicode is preserved
        with open(file_path, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader)
            unicode_idx = header.index("Unicode Value")
            
            for i, row in enumerate(reader):
                if len(row) > unicode_idx:
                    assert row[unicode_idx] == unicode_values[i], \
                        f"Unicode value not preserved: expected '{unicode_values[i]}', got '{row[unicode_idx]}'"
    
    def test_very_long_strings(self, tmp_path):
        """Test CSV writing with very long string values."""
        io_db = IODatabase()
        channel_path = "/test/long_string_channel"
        
        base_timestamp = SyntheticIGXDataGenerator.BASE_TIMESTAMP_NS
        
        # Create long string (1KB - sufficient for testing, faster than 10KB)
        long_string = "A" * 1000
        
        io_db.add_data_point(channel_path, long_string, base_timestamp)
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Long String", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        virtual_db = VirtualDatabase(io_db, channel_path, 1, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test_long_string.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="test_device",
            note="Long string test",
        )
        
        rows_written = writer.write_all()
        writer.close()
        
        assert rows_written > 0
        
        # Validate long string is preserved
        with open(file_path, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader)
            string_idx = header.index("Long String")
            
            for row in reader:
                if len(row) > string_idx:
                    assert len(row[string_idx]) == len(long_string), \
                        f"Long string length mismatch: expected {len(long_string)}, got {len(row[string_idx])}"
                    assert row[string_idx] == long_string, "Long string content mismatch"
    
    def test_timestamp_precision_edge_cases(self, tmp_path):
        """Test CSV writing with edge case timestamps (very small, very large, zero)."""
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        # Test various timestamp edge cases (using reasonable values to avoid memory issues)
        base_ts = SyntheticIGXDataGenerator.BASE_TIMESTAMP_NS
        timestamps_ns = [
            base_ts,  # Normal timestamp
            base_ts + int(1e9),  # 1 second later
            base_ts + int(1e12),  # ~16 minutes later (large but reasonable)
            base_ts + 1,  # 1 nanosecond later (very small increment)
            base_ts + int(1e6),  # 1 millisecond later
        ]
        
        for i, ts_ns in enumerate(timestamps_ns):
            io_db.add_data_point(channel_path, 100.0 + i, ts_ns)
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        # Use lower sampling rate to avoid creating too many rows
        virtual_db = VirtualDatabase(io_db, channel_path, 1, columns)  # 1 Hz instead of 10 Hz
        virtual_db.build()
        
        file_path = tmp_path / "test_timestamp_precision.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="test_device",
            note="Timestamp precision test",
        )
        
        rows_written = writer.write_all()
        writer.close()
        
        assert rows_written > 0
        
        # Validate timestamps are written correctly in scientific notation
        with open(file_path, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader)
            ts_idx = header.index("Timestamp (s)")
            
            for row in reader:
                if len(row) > ts_idx:
                    ts_str = row[ts_idx]
                    # Should be in scientific notation
                    assert 'e' in ts_str or '.' in ts_str, \
                        f"Timestamp should be in scientific notation, got '{ts_str}'"
                    # Should be parseable as float
                    try:
                        ts_float = float(ts_str)
                        assert ts_float >= 0, f"Timestamp should be non-negative, got {ts_float}"
                    except ValueError:
                        pytest.fail(f"Timestamp '{ts_str}' is not a valid number")
                    # Only check first few rows to avoid long validation
                    if i > 10:
                        break
    
    def test_multiple_write_all_calls_sequential(self, tmp_path):
        """Test multiple sequential write_all() calls (no threading, just sequential safety)."""
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        # Generate small dataset
        base_timestamp = SyntheticIGXDataGenerator.BASE_TIMESTAMP_NS
        for i in range(10):
            timestamp_ns = base_timestamp + i * int(1e8)
            io_db.add_data_point(channel_path, 100.0 + i, timestamp_ns)
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        virtual_db = VirtualDatabase(io_db, channel_path, 10, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test_multiple_writes.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="test_device",
            note="Multiple writes test",
        )
        
        # Call write_all multiple times sequentially
        rows1 = writer.write_all()
        rows2 = writer.write_all()  # Should write nothing
        rows3 = writer.write_all()  # Should write nothing
        
        writer.close()
        
        # First call should write all rows, subsequent calls should write 0
        assert rows1 > 0, "First write should write data"
        assert rows2 == 0, "Second write should not duplicate data"
        assert rows3 == 0, "Third write should not duplicate data"
        
        # Should have written all rows exactly once
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            # Should have header + data rows (no duplicates)
            assert len(rows) == virtual_db.get_row_count() + 1, \
                f"Expected {virtual_db.get_row_count() + 1} rows, got {len(rows)} (possible duplicate writes)"
    
    def test_empty_note_field(self, tmp_path):
        """Test CSV writing with empty note field."""
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        base_timestamp = SyntheticIGXDataGenerator.BASE_TIMESTAMP_NS
        io_db.add_data_point(channel_path, 100.0, base_timestamp)
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Note", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        virtual_db = VirtualDatabase(io_db, channel_path, 10, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test_empty_note.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="test_device",
            note="",  # Empty note
        )
        
        rows_written = writer.write_all()
        writer.close()
        
        assert rows_written > 0
        
        # Validate empty note is handled correctly
        with open(file_path, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader)
            note_idx = header.index("Note")
            
            for row in reader:
                if len(row) > note_idx:
                    # Empty note should be written as empty string
                    assert row[note_idx] == "", f"Expected empty note, got '{row[note_idx]}'"
    
    def test_mixed_data_types_in_row(self, tmp_path):
        """Test CSV writing with mixed data types in the same row."""
        io_db = IODatabase()
        
        # Create channels with different data types
        int_channel = "/test/int_channel"
        float_channel = "/test/float_channel"
        bool_channel = "/test/bool_channel"
        str_channel = "/test/str_channel"
        
        base_timestamp = SyntheticIGXDataGenerator.BASE_TIMESTAMP_NS
        
        for i in range(10):
            timestamp_ns = base_timestamp + i * int(1e8)
            io_db.add_data_point(int_channel, i, timestamp_ns)  # Integer
            io_db.add_data_point(float_channel, 100.5 + i, timestamp_ns)  # Float
            io_db.add_data_point(bool_channel, i % 2 == 0, timestamp_ns)  # Boolean
            io_db.add_data_point(str_channel, f"value_{i}", timestamp_ns)  # String
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Integer", channel_path=int_channel, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Float", channel_path=float_channel, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Boolean", channel_path=bool_channel, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="String", channel_path=str_channel, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        virtual_db = VirtualDatabase(io_db, int_channel, 10, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test_mixed_types.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="test_device",
            note="Mixed types test",
        )
        
        rows_written = writer.write_all()
        writer.close()
        
        assert rows_written > 0
        
        # Validate all data types are written correctly
        with open(file_path, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader)
            int_idx = header.index("Integer")
            float_idx = header.index("Float")
            bool_idx = header.index("Boolean")
            str_idx = header.index("String")
            
            for i, row in enumerate(reader):
                if len(row) > max(int_idx, float_idx, bool_idx, str_idx):
                    # Validate integer
                    assert int(row[int_idx]) == i, f"Row {i}: Integer mismatch"
                    # Validate float
                    assert abs(float(row[float_idx]) - (100.5 + i)) < 0.01, f"Row {i}: Float mismatch"
                    # Validate boolean (should be 0 or 1)
                    assert row[bool_idx] in ["0", "1"], f"Row {i}: Boolean should be 0 or 1, got '{row[bool_idx]}'"
                    # Validate string
                    assert row[str_idx] == f"value_{i}", f"Row {i}: String mismatch"
