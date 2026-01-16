"""Model Collector - High-level data collection orchestrator.

This module provides the ModelCollector class which orchestrates the entire
data collection process using DeviceManager, VirtualDatabase, and CSVWriter.

The ModelCollector uses a DeviceManager to maintain network connections and
collects data from all devices into a shared IODatabase, then creates a
VirtualDatabase and CSVWriter for output.
"""

import time
import threading
from typing import Dict, List, Optional, Any
from pathlib import Path
from .device_manager import DeviceManager
from .io_database import IODatabase
from .virtual_database import VirtualDatabase, ColumnDefinition
from .csv_writer import CSVWriter


class ModelCollector:
    """High-level orchestrator for data collection using a DeviceManager.
    
    This class coordinates:
    1. DeviceManager: Manages multiple device connections and shared IODatabase
    2. VirtualDatabase: Synthetic row generation at sampling rate
    3. CSVWriter: Asynchronous file writing
    
    The collector works with any model that provides:
    - create_columns(reference_channel) -> List[ColumnDefinition]
    - get_reference_channel() -> str
    """
    
    def __init__(
        self,
        device_manager: DeviceManager,
        model: Any,  # Model class (e.g., IC256Model)
        reference_channel: str,
        sampling_rate: int,
        file_path: str,
        device_name: str,
        note: str,
    ):
        """Initialize the model collector.
        
        Args:
            device_manager: DeviceManager instance managing device connections
            model: Model instance (e.g., IC256Model) that provides create_columns()
            reference_channel: Channel path to use as timing reference
            sampling_rate: Sampling rate in Hz (rows per second)
            file_path: Full path to CSV output file
            device_name: Name of the device (for CSV metadata)
            note: Note string to include in CSV
        """
        self.device_manager = device_manager
        self.model = model
        self.reference_channel = reference_channel
        self.sampling_rate = sampling_rate
        self.file_path = file_path
        self.device_name = device_name
        self.note = note
        
        # Use shared IODatabase from DeviceManager
        self.io_database = device_manager.get_io_database()
        
        # Create column definitions from model
        self.columns = model.create_columns(reference_channel)
        
        # Create VirtualDatabase using shared IODatabase
        self.virtual_database = VirtualDatabase(
            io_database=self.io_database,
            reference_channel=reference_channel,
            sampling_rate=sampling_rate,
            columns=self.columns,
        )
        
        # Create CSVWriter
        self.csv_writer = CSVWriter(
            virtual_database=self.virtual_database,
            file_path=file_path,
            device_name=device_name,
            note=note,
        )
        
        # Statistics tracking
        self.statistics: Dict[str, Any] = {
            "rows": 0,
            "file_size": 0,
            "file_path": file_path,
        }
        
        # Thread safety
        self._lock = threading.Lock()
        self._running = False
    
    def start(self) -> None:
        """Start data collection.
        
        Starts the DeviceManager to begin collecting data from all devices,
        then marks this collector as running.
        """
        # Start DeviceManager to begin data collection from all devices
        self.device_manager.start()
        
        self._running = True
    
    def collect_iteration(self) -> None:
        """Perform one iteration of data collection.
        
        This method:
        1. Rebuilds VirtualDatabase with new data from shared IODatabase
        2. Writes new rows to CSV
        3. Updates statistics
        4. Prunes old rows if safe
        
        Note: Data collection from devices happens in DeviceManager threads.
        This method only processes the collected data.
        
        This should be called in a loop while collection is active.
        This method will continue processing even after _running is False
        to ensure all collected data is written to CSV.
        """
        
        # STEP 1: Rebuild virtual database with new data from shared IODatabase
        # This creates rows at the sampling rate with all conversions applied
        self.virtual_database.rebuild()
        
        # STEP 2: Write new rows to CSV
        self.csv_writer.write_all()
        
        # STEP 3: Update statistics
        with self._lock:
            total_rows = self.csv_writer.rows_written
            if total_rows != self.statistics["rows"]:
                self.statistics["rows"] = total_rows
                if total_rows % 1000 == 0:  # Update file size less frequently
                    try:
                        self.statistics["file_size"] = self.csv_writer.file_size
                    except (OSError, AttributeError):
                        pass
        
        # STEP 4: Flush/sync periodically
        if self.csv_writer.rows_written % 1000 == 0:
            self.csv_writer.flush()
        if self.csv_writer.rows_written % 5000 == 0:
            self.csv_writer.sync()
        
        # STEP 5: Pruning disabled for performance
        # Memory usage: 6 kHz * 6 min = 2.16M rows * ~150 bytes/row = ~325 MB
        # IODatabase: ~1.3 GB for 15 channels
        # Total: ~1.6 GB - acceptable for modern systems
        # Pruning adds overhead and complexity without significant benefit
    
    def stop(self) -> None:
        """Stop data collection (but continue processing existing data).
        
        This marks the collector as stopped, which will stop new data collection
        from DeviceManager, but processing will continue until all data is written.
        """
        self._running = False
    
    def finalize(self) -> None:
        """Finalize output after all data has been processed.
        
        This should be called after all data collection and processing is complete.
        It performs final flush, sync, and close operations.
        """
        # Final flush and close
        self.csv_writer.flush()
        self.csv_writer.sync()
        self.csv_writer.close()
        
        # Final statistics update
        with self._lock:
            try:
                self.statistics["file_size"] = self.csv_writer.file_size
            except (OSError, AttributeError):
                pass
    
    def is_finished(self) -> bool:
        """Check if all data has been processed and written.
        
        Returns:
            True if all data has been processed (no new rows to write)
        """
        # Quick check without expensive rebuild - just check if we've written all current rows
        # This is a lightweight check that doesn't require rebuilding
        total_rows = self.virtual_database.get_row_count()
        return self.csv_writer.rows_written >= total_rows
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get current collection statistics.
        
        Returns:
            Dictionary with keys: rows, file_size, file_path
        """
        with self._lock:
            return self.statistics.copy()
    




def collect_data_with_model(
    collector: ModelCollector,
    stop_event: threading.Event,
    update_interval: float = 0.001,
) -> None:
    """Run data collection loop with a ModelCollector.
    
    This function:
    1. Starts data collection
    2. Processes data while collection is active
    3. When stop_event is set, stops new data collection but continues processing
    4. Continues processing until all collected data is written to CSV
    5. Finalizes output
    
    Args:
        collector: ModelCollector instance
        stop_event: Threading event to signal stop (stops new data collection)
        update_interval: Time between collection iterations (seconds)
    """
    collector.start()
    
    try:
        # Phase 1: Active collection - collect and process data
        while not stop_event.is_set():
            collector.collect_iteration()
            time.sleep(update_interval)
        
        # Phase 2: Stop new data collection but continue processing existing data
        collector.stop()  # This stops DeviceManager from collecting new data
        
        # Continue processing until all data is written
        # Process aggressively with smart finish detection
        max_iterations = 50000  # Safety limit
        iteration = 0
        consecutive_no_change = 0
        previous_rows_written = 0
        previous_virtual_rows = 0
        
        # Get initial state
        collector.collect_iteration()  # One iteration to get initial state
        previous_rows_written = collector.csv_writer.rows_written
        previous_virtual_rows = collector.virtual_database.get_row_count()
        
        while iteration < max_iterations:
            collector.collect_iteration()
            iteration += 1
            
            # Check progress every 25 iterations (reduces overhead)
            if iteration % 25 == 0:
                current_rows = collector.csv_writer.rows_written
                current_virtual_rows = collector.virtual_database.get_row_count()
                
                # Check if we've caught up: written rows should match virtual rows
                # AND virtual rows should not be growing (all IODatabase data processed)
                if (current_rows == previous_rows_written and 
                    current_virtual_rows == previous_virtual_rows and
                    current_rows >= current_virtual_rows - 10):  # Allow small difference
                    consecutive_no_change += 1
                    # If no change for 8 checks (200 iterations), we're done
                    if consecutive_no_change >= 8:
                        break
                else:
                    consecutive_no_change = 0
                    previous_rows_written = current_rows
                    previous_virtual_rows = current_virtual_rows
            
            # Minimal sleep - only every 200 iterations
            if iteration % 200 == 0:
                time.sleep(0.00005)  # 50 microseconds
        
        # Final processing pass - ensure everything is written
        for _ in range(15):
            collector.collect_iteration()
            if _ % 5 == 0:
                time.sleep(0.0001)
        
    finally:
        # Finalize output (flush, sync, close)
        collector.finalize()