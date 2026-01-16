"""CSV Writer for writing VirtualDatabase to disk.

This module provides a CSVWriter class that writes VirtualDatabase rows to CSV
files. It operates completely asynchronously from network code and safely
manages file operations.
"""

import csv
import os
import threading
from typing import Dict, List, Optional, Any
from pathlib import Path
from .virtual_database import VirtualDatabase, VirtualRow
from .data_collection import (
    write_row,
    process_gaussian_values,
    ERROR_VALUE,
)


class CSVWriter:
    """Writer for writing VirtualDatabase to CSV file.
    
    This class takes a VirtualDatabase and writes all rows to a CSV file.
    It tracks which rows have been written and can signal the VirtualDatabase
    when it's safe to prune written rows.
    
    Attributes:
        virtual_database: The VirtualDatabase to write
        file_path: Path to the CSV file
        device_name: Name of the device (for header formatting)
        environment: Environment data [temperature, humidity, pressure]
        note: Note string to include in each row
        primary_units: Units for primary dose channel
        probe_units: Units for probe channels (TX2 only)
        rows_written: Number of rows written to file
        file_size: Current file size in bytes
    """
    
    def __init__(
        self,
        virtual_database: VirtualDatabase,
        file_path: str,
        device_name: str,
        environment: List[str],
        note: str,
    ):
        """Initialize CSV writer.
        
        Args:
            virtual_database: VirtualDatabase to write (defines headers and columns)
            file_path: Path to CSV file
            device_name: Name of the device (e.g., "ic256_45", "tx2")
            environment: Environment data [temperature, humidity, pressure]
            note: Note string to include in each row
        """
        self.virtual_database = virtual_database
        self.file_path = Path(file_path)
        self.device_name = device_name
        self.environment = environment
        self.note = note
        self.rows_written: int = 0
        self.file_size: int = 0
        self._file_handle = None
        self._writer = None
        self._lock = threading.Lock()  # Thread safety for file operations
    
    def write_all(self) -> int:
        """Write all rows from VirtualDatabase to CSV file.
        
        This method writes all rows in the virtual database to the CSV file.
        It's safe to call multiple times - it will only write new rows.
        
        Returns:
            Number of rows written
        """
        with self._lock:
            # Open file if not already open
            if self._file_handle is None:
                try:
                    # Ensure directory exists
                    self.file_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Open file for writing
                    self._file_handle = open(
                        self.file_path,
                        mode="w",
                        newline="",
                        encoding="utf-8-sig",
                        buffering=1,  # Line buffering
                    )
                    self._writer = csv.writer(self._file_handle)
                    
                    # Write headers from virtual database
                    headers = self.virtual_database.get_headers()
                    self._writer.writerow(headers)
                    self._file_handle.flush()
                    
                except (IOError, OSError) as e:
                    print(f"Error opening file {self.file_path}: {e}")
                    return 0
            
            # Get all rows from virtual database
            rows = self.virtual_database.get_rows()
            
            # Write rows starting from where we left off
            rows_to_write = rows[self.rows_written:]
            
            for row in rows_to_write:
                try:
                    # Convert row data to list format in column order
                    # VirtualRow.data is a dict mapping column names to values
                    row_data = self._convert_row_data_to_list(row.data, row.timestamp)
                    
                    # Write the row
                    self._write_row_data(row_data)
                    self.rows_written += 1
                    
                    # Flush periodically
                    if self.rows_written % 1000 == 0:
                        self._file_handle.flush()
                    
                except Exception as e:
                    print(f"Error writing row at timestamp {row.timestamp}: {e}")
                    continue
            
            # Final flush
            if rows_to_write:
                self._file_handle.flush()
            
            # Update file size
            try:
                self.file_size = os.path.getsize(self.file_path)
            except (OSError, AttributeError):
                pass
            
            return len(rows_to_write)
    
    def _convert_row_data_to_list(
        self,
        row_data: Dict[str, Any],
        timestamp: float,
    ) -> List[Any]:
        """Convert row data dictionary to list format in column order.
        
        Converts the dictionary of column_name -> value to a list in the
        correct order matching the virtual database columns.
        
        Args:
            row_data: Dictionary mapping column names to values
            timestamp: Timestamp for the row
            
        Returns:
            List of values in correct order for CSV row
        """
        result = []
        
        for col_def in self.virtual_database.columns:
            if col_def.name == "Timestamp (s)":
                # Timestamp is computed
                result.append(f"{timestamp:.12e}")
            elif col_def.name == "Note":
                # Note is provided
                result.append(self.note)
            elif "X centroid" in col_def.name or "X sigma" in col_def.name or \
                 "Y centroid" in col_def.name or "Y sigma" in col_def.name:
                # Gaussian fields - process and convert to mm
                value = row_data.get(col_def.name)
                if value is None:
                    result.append(ERROR_VALUE)
                else:
                    result.append(value)
            elif "Dose" in col_def.name:
                # Primary dose - may need units appended to name
                value = row_data.get(col_def.name)
                result.append(value if value is not None else "")
            elif col_def.name in ["Temperature (℃)", "Humidity (%rH)", "Pressure (hPa)"]:
                # Environment - use provided values
                env_map = {
                    "Temperature (℃)": 0,
                    "Humidity (%rH)": 1,
                    "Pressure (hPa)": 2,
                }
                idx = env_map.get(col_def.name, -1)
                if idx >= 0 and idx < len(self.environment):
                    result.append(self.environment[idx])
                else:
                    result.append(row_data.get(col_def.name, ""))
            else:
                # Other channels - use value from row_data
                value = row_data.get(col_def.name)
                result.append(value if value is not None else "")
        
        return result
    
    def _write_row_data(self, row_data: List[Any]) -> None:
        """Write a single row to CSV.
        
        Args:
            row_data: List of values in column order
        """
        if "ic256" in self.device_name.lower():
            # IC256: Process gaussian values (first 4 fields)
            if len(row_data) >= 5:
                x_mean, x_sigma, y_mean, y_sigma = process_gaussian_values(
                    row_data[1], row_data[2], row_data[3], row_data[4]
                )
                # Replace gaussian fields with processed values
                row = (
                    [row_data[0]] +  # Timestamp
                    [x_mean, x_sigma, y_mean, y_sigma] +  # Processed gaussian
                    row_data[5:]  # Rest of data
                )
            else:
                row = row_data
        else:
            # TX2: Just write data as-is
            row = row_data
        
        self._writer.writerow(row)
    
    def flush(self) -> None:
        """Flush file buffer to disk."""
        with self._lock:
            if self._file_handle:
                self._file_handle.flush()
    
    def sync(self) -> None:
        """Force OS to write buffered data to disk."""
        with self._lock:
            if self._file_handle:
                self._file_handle.flush()
                try:
                    os.fsync(self._file_handle.fileno())
                except (OSError, AttributeError):
                    pass
    
    def close(self) -> None:
        """Close the CSV file."""
        with self._lock:
            if self._file_handle:
                self._file_handle.flush()
                try:
                    os.fsync(self._file_handle.fileno())
                except (OSError, AttributeError):
                    pass
                self._file_handle.close()
                self._file_handle = None
                self._writer = None
            
            # Update final file size
            try:
                self.file_size = os.path.getsize(self.file_path)
            except (OSError, AttributeError):
                pass
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about the CSV writer.
        
        Returns:
            Dictionary with statistics
        """
        return {
            'rows_written': self.rows_written,
            'file_size': self.file_size,
            'file_path': str(self.file_path),
            'virtual_db_rows': self.virtual_database.get_row_count(),
        }
    
    def can_prune_rows(self, rows_to_keep: int = 1000) -> bool:
        """Check if it's safe to prune rows from VirtualDatabase.
        
        This indicates that enough rows have been written to disk that
        it's safe to prune older rows from the virtual database to save memory.
        
        Args:
            rows_to_keep: Number of most recent rows to keep in memory
            
        Returns:
            True if it's safe to prune, False otherwise
        """
        total_rows = self.virtual_database.get_row_count()
        return self.rows_written >= (total_rows - rows_to_keep)
    
    def get_prunable_row_count(self, rows_to_keep: int = 1000) -> int:
        """Get the number of rows that can be safely pruned.
        
        Args:
            rows_to_keep: Number of most recent rows to keep in memory
            
        Returns:
            Number of rows that can be pruned
        """
        total_rows = self.virtual_database.get_row_count()
        if total_rows <= rows_to_keep:
            return 0
        return max(0, self.rows_written - rows_to_keep)
