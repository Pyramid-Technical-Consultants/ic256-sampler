"""Model Collector - High-level data collection orchestrator.

This module provides the ModelCollector class which orchestrates the entire
data collection process using DeviceManager, VirtualDatabase, and CSVWriter.

The ModelCollector uses a DeviceManager to maintain network connections and
collects data from all devices into a shared IODatabase, then creates a
VirtualDatabase and CSVWriter for output.
"""

import time
import threading
from typing import Dict, List, Optional, Any, Callable
from .device_manager import DeviceManager, IC256_CONFIG, TX2_CONFIG
from .io_database import IODatabase
from .virtual_database import VirtualDatabase, ColumnDefinition
from .csv_writer import CSVWriter
from .file_path_generator import get_file_path_for_primary_device
from .gui.utils import log_message_safe


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
        log_callback: Optional[Callable[[str, str], None]] = None,
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
            log_callback: Optional callback function(message: str, level: str) for logging
        """
        self.device_manager = device_manager
        self.model = model
        self.reference_channel = reference_channel
        self.sampling_rate = sampling_rate
        self.file_path = file_path
        self.device_name = device_name
        self.note = note
        self.log_callback = log_callback
        
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
            log_callback=log_callback,
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
        4. Prunes old data from IODatabase if safe to prevent unbounded growth
        
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
        rows_written_this_iteration = self.csv_writer.write_all()
        
        # STEP 3: Update statistics - ALWAYS update after write_all() to ensure accuracy
        with self._lock:
            # Always update rows count to match actual rows written
            self.statistics["rows"] = self.csv_writer.rows_written
            # Update file size periodically or if rows were written this iteration
            if rows_written_this_iteration > 0 or self.csv_writer.rows_written % 1000 == 0:
                try:
                    self.statistics["file_size"] = self.csv_writer.file_size
                except (OSError, AttributeError):
                    pass
        
        # STEP 4: Flush/sync periodically
        if self.csv_writer.rows_written % 1000 == 0:
            self.csv_writer.flush()
        if self.csv_writer.rows_written % 5000 == 0:
            self.csv_writer.sync()
        
        # STEP 5: Prune old data from IODatabase to prevent unbounded growth
        # Only prune if we have a significant amount of data and have written rows
        # Check every 1000 rows written to avoid overhead
        if self.csv_writer.rows_written > 0 and self.csv_writer.rows_written % 1000 == 0:
            # Get the last built time from virtual database (what's been processed)
            # Use a safety margin of 1 second to ensure we don't prune data that might be needed
            last_built_time = self.virtual_database._last_built_time
            if last_built_time is not None and last_built_time > 1.0:
                # Prune data older than (last_built_time - 1 second safety margin)
                # This ensures we keep a small buffer of data that might be needed for interpolation
                prune_before_time = max(0.0, last_built_time - 1.0)
                pruned_counts = self.io_database.prune_old_data(
                    min_elapsed_time=prune_before_time,
                    max_points_per_channel=100000
                )
                
                # Log if significant pruning occurred (only log if > 1000 points pruned total)
                total_pruned = sum(pruned_counts.values())
                if total_pruned > 1000:
                    if self.log_callback:
                        self.log_callback(
                            f"Pruned {total_pruned:,} old data points from IODatabase "
                            f"(pruned before {prune_before_time:.3f}s elapsed time)",
                            "INFO"
                        )
        
    
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
        
        # Final statistics update - CRITICAL: Update rows count as well as file_size
        with self._lock:
            # Update rows count to match actual rows written
            self.statistics["rows"] = self.csv_writer.rows_written
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
    
    def prepare_devices_for_collection(
        self,
        devices_added: List[str],
        sampling_rate: int,
        stop_event: threading.Event,
        log_callback: Optional[Callable[[str, str], None]] = None,
    ) -> bool:
        """Prepare devices for data collection.
        
        Sets up devices, creates threads, and prepares for collection.
        
        Args:
            devices_added: List of device names to prepare
            sampling_rate: Sampling rate in Hz
            stop_event: Threading event for stopping collection
            log_callback: Optional callback for logging (message, level)
            
        Returns:
            True if preparation succeeded, False otherwise
        """
        # CRITICAL: Set stop_event BEFORE calling stop() so stop() uses the correct event
        self.device_manager.stop_event = stop_event
        
        # CRITICAL: Stop any previous acquisition to reset _running state
        # This ensures start() will actually start the threads
        self.device_manager.stop()
        
        # Clear the database for new acquisition
        self.device_manager.clear_database()
        
        # Update sampling rate on existing connections and create new threads
        for device_name in devices_added:
            # Get connection reference (validate it exists)
            with self.device_manager._lock:
                if device_name not in self.device_manager.connections:
                    if log_callback:
                        log_callback(
                            f"Device {device_name} not found in connections",
                            "ERROR"
                        )
                    continue
                
                connection = self.device_manager.connections[device_name]
                # Store a reference to config and ip_address while holding lock
                # to avoid issues if connection is removed
                config = connection.config
                ip_address = connection.ip_address
                client = connection.client
                channels = connection.channels
                model = connection.model
                field_to_path = connection.field_to_path
            
            # Setup device with error recovery (releases lock internally)
            setup_success = self.device_manager.setup_device_for_collection(
                device_name,
                sampling_rate,
                log_callback
            )
            
            if not setup_success:
                if log_callback:
                    log_callback(
                        f"Failed to setup {device_name} for collection",
                        "WARNING"
                    )
                continue
            
            # Validate connection still exists and get thread reference
            with self.device_manager._lock:
                if device_name not in self.device_manager.connections:
                    if log_callback:
                        log_callback(
                            f"Device {device_name} connection was removed during setup",
                            "WARNING"
                        )
                    continue
                # Re-get connection in case it was replaced
                connection = self.device_manager.connections[device_name]
                
                # Ensure old thread is stopped before creating new one
                if connection.thread and connection.thread.is_alive():
                    # Old thread is still running - wait for it to stop
                    old_thread = connection.thread
                else:
                    old_thread = None
            
            # Join old thread outside of lock to avoid deadlock
            if old_thread:
                old_thread.join(timeout=1.0)
            
            # Create new data collection thread for this acquisition
            thread = threading.Thread(
                target=self.device_manager._collect_from_device,
                name=f"{config.device_type.lower()}_device_{ip_address}",
                daemon=True,
                args=(config, client, channels, model, field_to_path, ip_address),
            )
            
            # Update connection thread reference (with lock)
            with self.device_manager._lock:
                if device_name in self.device_manager.connections:
                    self.device_manager.connections[device_name].thread = thread
                else:
                    # Connection was removed, don't start thread
                    if log_callback:
                        log_callback(
                            f"Device {device_name} connection removed before thread start",
                            "WARNING"
                        )
                    continue
            
            # Start thread outside of lock
            thread.start()
        
        return True
    
    def run_collection(self, stop_event: threading.Event) -> None:
        """Run the complete data collection lifecycle.
        
        This method:
        1. Starts data collection
        2. Processes data while collection is active
        3. When stop_event is set, stops new data collection but continues processing
        4. Continues processing until all collected data is written to CSV
        5. Finalizes output
        
        Args:
            stop_event: Threading event to signal stop (stops new data collection)
        """
        self.start()
        
        try:
            # Phase 1: Active collection - collect and process data
            while not stop_event.is_set():
                self.collect_iteration()
                time.sleep(0.001)  # update_interval
            
            # Phase 2: Stop new data collection but continue processing existing data
            self.stop()  # This stops DeviceManager from collecting new data
            
            # Continue processing until all data is written
            max_iterations = 50000
            max_time = 30.0
            start_time = time.time()
            iteration = 0
            consecutive_no_change = 0
            previous_rows_written = 0
            previous_virtual_rows = 0
            
            # Get initial state
            self.collect_iteration()
            previous_rows_written = self.csv_writer.rows_written
            previous_virtual_rows = self.virtual_database.get_row_count()
            
            while iteration < max_iterations and (time.time() - start_time) < max_time:
                self.collect_iteration()
                iteration += 1
                
                # Check progress every 25 iterations
                if iteration % 25 == 0:
                    current_rows = self.csv_writer.rows_written
                    current_virtual_rows = self.virtual_database.get_row_count()
                    
                    if (current_rows == previous_rows_written and 
                        current_virtual_rows == previous_virtual_rows and
                        current_rows >= current_virtual_rows - 10):
                        consecutive_no_change += 1
                        if consecutive_no_change >= 8:
                            break
                    else:
                        consecutive_no_change = 0
                        previous_rows_written = current_rows
                        previous_virtual_rows = current_virtual_rows
                
                if iteration % 200 == 0:
                    time.sleep(0.00005)
            
            # Final processing pass
            for _ in range(15):
                self.collect_iteration()
                if _ % 5 == 0:
                    time.sleep(0.0001)
        
        finally:
            # Finalize output (flush, sync, close)
            self.finalize()
    
    @classmethod
    def create_for_collection(
        cls,
        device_manager: DeviceManager,
        devices_added: List[str],
        sampling_rate: int,
        save_folder: str,
        note: str,
        device_statistics: Dict[str, Dict[str, Any]],
        log_callback: Optional[Callable[[str, str], None]] = None,
    ) -> Optional['ModelCollector']:
        """Create a ModelCollector for data collection (factory method).
        
        Args:
            device_manager: DeviceManager instance
            devices_added: List of device names that were added
            sampling_rate: Sampling rate in Hz
            save_folder: Directory to save CSV file
            note: Note string for CSV
            device_statistics: Dictionary to store statistics (will be updated)
            log_callback: Optional callback function(message: str, level: str) for logging
            
        Returns:
            ModelCollector instance, or None if creation failed
        """
        # Get file path for primary device
        file_path, primary_device_config = get_file_path_for_primary_device(
            save_folder,
            devices_added
        )
        
        # Create ModelCollector using the primary device's model and reference channel
        primary_model = primary_device_config.model_creator()
        reference_channel = primary_model.get_reference_channel()
        
        collector = cls(
            device_manager=device_manager,
            model=primary_model,
            reference_channel=reference_channel,
            sampling_rate=sampling_rate,
            file_path=file_path,
            device_name=primary_device_config.device_type.lower(),
            note=note,
            log_callback=log_callback,
        )
        
        # Initialize statistics - reset to ensure clean state for new acquisition
        # This ensures file size and row counts start at 0 for the new acquisition
        device_statistics.clear()
        device_statistics.update({
            device: {"rows": 0, "file_size": 0, "file_path": ""} 
            for device in devices_added
        })
        collector.statistics = device_statistics.get(primary_device_config.device_name, {})
        
        return collector
    
    @staticmethod
    def get_devices_added(
        device_manager: DeviceManager,
        ic256_ip: Optional[str],
        tx2_ip: Optional[str],
    ) -> List[str]:
        """Get list of device names that should be added based on IP addresses.
        
        Args:
            device_manager: DeviceManager instance
            ic256_ip: IC256 IP address (None or empty means not added)
            tx2_ip: TX2 IP address (None or empty means not added)
            
        Returns:
            List of device names that are configured
        """
        devices_added = []
        with device_manager._lock:
            if ic256_ip and IC256_CONFIG.device_name in device_manager.connections:
                devices_added.append(IC256_CONFIG.device_name)
            if tx2_ip and TX2_CONFIG.device_name in device_manager.connections:
                devices_added.append(TX2_CONFIG.device_name)
        return devices_added
    




def collect_data_with_model(
    collector: ModelCollector,
    stop_event: threading.Event,
    update_interval: float = 0.001,
) -> None:
    """Run data collection loop with a ModelCollector.
    
    This is a convenience function that calls collector.run_collection().
    Kept for backward compatibility.
    
    Args:
        collector: ModelCollector instance
        stop_event: Threading event to signal stop (stops new data collection)
        update_interval: Time between collection iterations (seconds) - ignored, uses 0.001
    """
    collector.run_collection(stop_event)