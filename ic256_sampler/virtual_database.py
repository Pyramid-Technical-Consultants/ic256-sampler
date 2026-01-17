"""Virtual Database for creating synthetic table rows from IO database.

This module provides a VirtualDatabase class that uses an IODatabase to create
a synthetic table database with rows at regular intervals. The database defines
column policies (synchronized, interpolated, asynchronous) and header names.
It also supports converters to transform raw data values to desired units.
"""

from typing import Dict, List, Tuple, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
import bisect
from collections import deque
from .io_database import IODatabase, DataPoint, ChannelData


class ChannelPolicy(Enum):
    """Policy for how a channel should be matched to reference timestamps."""
    SYNCHRONIZED = "synchronized"  # Must match exact timestamp (within tolerance)
    INTERPOLATED = "interpolated"  # Interpolate between surrounding points
    ASYNCHRONOUS = "asynchronous"  # Snap to nearest point (no interpolation)


# Converter function type - generic function that transforms a value
Converter = Callable[[Any], Any]


def identity_converter(value: Any) -> Any:
    """Identity converter - returns value as-is."""
    return value


@dataclass
class ColumnDefinition:
    """Definition for a column in the virtual database.
    
    Attributes:
        name: Column name (for CSV header)
        channel_path: Path to the IO channel (None for computed columns)
        policy: How to match this channel to reference timestamps
        converter: Function to convert raw value to desired units (default: identity)
    """
    name: str
    channel_path: Optional[str] = None
    policy: ChannelPolicy = ChannelPolicy.INTERPOLATED
    converter: Converter = field(default_factory=lambda: identity_converter)


@dataclass
class VirtualRow:
    """A single row in the virtual database.
    
    Attributes:
        timestamp: Elapsed time in seconds
        data: Dictionary mapping column names to values
    """
    timestamp: float
    data: Dict[str, Any]


class VirtualDatabase:
    """Virtual database that creates synthetic rows from IO database.
    
    This class takes an IODatabase and creates a synthetic table with rows
    at regular intervals. It uses a reference channel for timing and applies
    different policies (synchronized, interpolated, asynchronous) to match
    other channels to the reference timestamps.
    
    The columns in the virtual database match 1:1 with CSV columns, and this
    class defines the header names and collection policies. Converters can be
    applied to transform raw data values to desired units.
    
    Attributes:
        io_database: The underlying IO database
        reference_channel: Channel path to use as timing reference
        sampling_rate: Sampling rate in Hz (rows per second)
        columns: List of column definitions
        rows: List of VirtualRow objects
    """
    
    def __init__(
        self,
        io_database: IODatabase,
        reference_channel: str,
        sampling_rate: int,
        columns: List[ColumnDefinition],
        log_callback: Optional[Callable[[str, str], None]] = None,
    ):
        """Initialize virtual database.
        
        Args:
            io_database: IODatabase containing all collected data
            reference_channel: Channel path to use as timing reference
            sampling_rate: Sampling rate in Hz (determines row spacing)
            columns: List of column definitions (defines CSV structure)
            log_callback: Optional callback function(message: str, level: str) for logging
        """
        self.io_database = io_database
        self.reference_channel = reference_channel
        self.sampling_rate = sampling_rate
        self.columns = columns
        self.rows: List[VirtualRow] = []
        self._built = False
        self._last_built_time: Optional[float] = None  # Track last built timestamp for incremental builds
        self._log_callback = log_callback
        
        # Track diagnostic information for better error reporting
        self._last_build_failure_reason: Optional[str] = None
        self._last_build_failure_time: Optional[float] = None
        self._consecutive_build_failures: int = 0
        
        # Performance optimization: cache search positions for each channel
        # Maps channel_path -> last search index to enable incremental search
        self._search_cache: Dict[str, int] = {}
        
        # Performance optimization: cache channel snapshots and elapsed times
        # Maps channel_path -> (snapshot, elapsed_times_list, timestamps_list)
        # Only valid during a single build/rebuild operation
        self._snapshot_cache: Dict[str, Tuple[List[DataPoint], List[float], List[int]]] = {}
        
        # Performance optimization: cache channel data and snapshots
        # Maps channel_path -> (ChannelData, snapshot, elapsed_times_list, timestamps_list)
        self._channel_cache: Dict[str, Tuple[Optional[ChannelData], Optional[List[DataPoint]], Optional[List[float]], Optional[List[int]]]] = {}
        self._ref_channel_cache: Optional[ChannelData] = None
    
    def get_headers(self) -> List[str]:
        """Get CSV header names for all columns.
        
        Returns:
            List of header strings in column order
        """
        return [col.name for col in self.columns]
    
    def _log(self, message: str, level: str = "WARNING") -> None:
        """Log a message using the callback if available.
        
        Args:
            message: Message to log
            level: Log level (INFO, WARNING, ERROR)
        """
        if self._log_callback:
            try:
                self._log_callback(message, level)
            except Exception:
                pass  # Don't fail if logging callback has issues
    
    def _get_diagnostic_info(self) -> Dict[str, Any]:
        """Get diagnostic information about the current state.
        
        Returns:
            Dictionary with diagnostic information
        """
        total_points = self.io_database.get_total_count()
        all_channels = self.io_database.get_all_channels()
        ref_channel = self.io_database.get_channel(self.reference_channel)
        
        # Check column channels
        column_channels = {}
        missing_channels = []
        for col_def in self.columns:
            if col_def.channel_path:
                channel = self.io_database.get_channel(col_def.channel_path)
                if channel:
                    column_channels[col_def.channel_path] = {
                        'count': channel.count,
                        'exists': True,
                    }
                else:
                    missing_channels.append(col_def.channel_path)
                    column_channels[col_def.channel_path] = {
                        'count': 0,
                        'exists': False,
                    }
        
        return {
            'total_io_points': total_points,
            'total_io_channels': len(all_channels),
            'reference_channel': self.reference_channel,
            'reference_channel_exists': ref_channel is not None,
            'reference_channel_count': ref_channel.count if ref_channel else 0,
            'column_channels': column_channels,
            'missing_channels': missing_channels,
            'virtual_rows': len(self.rows),
            'built': self._built,
            'last_failure_reason': self._last_build_failure_reason,
            'consecutive_failures': self._consecutive_build_failures,
        }
    
    def _is_primed(self) -> bool:
        """Check if all channels have at least one data point (primed).
        
        Only checks channels that actually exist in the IODatabase.
        If a channel path is in column definitions but not in IODatabase
        (e.g., not subscribed/collected), it's skipped for priming.
        
        CRITICAL: Only requires the reference channel to be primed.
        Other channels are optional - if they don't have data, rows will
        still be created with None values for those columns.
        
        Returns:
            True if reference channel has at least one data point, False otherwise.
        """
        # Only require reference channel to be primed
        # This allows rows to be created even if some optional channels (like environmental)
        # haven't received data yet
        ref_channel = self.io_database.get_channel(self.reference_channel)
        if not ref_channel or ref_channel.count == 0:
            return False
        
        return True
    
    def build(self) -> None:
        """Build the virtual database by creating rows at regular intervals.
        
        Uses the reference channel to determine the time range, then creates
        rows at regular intervals (1/sampling_rate) for all channels.
        
        Will not create rows until all channels have at least one data point (primed).
        """
        if self._built:
            return
        
        # Clear snapshot cache for fresh build
        self._snapshot_cache.clear()
        self._ref_channel_cache = None
        
        # Get reference channel data
        ref_channel = self.io_database.get_channel(self.reference_channel)
        total_io_points = self.io_database.get_total_count()
        
        # Check if we have data but can't build - this is the problematic case
        if total_io_points > 0:
            if not ref_channel:
                reason = f"Reference channel '{self.reference_channel}' does not exist in IO database"
                self._last_build_failure_reason = reason
                self._consecutive_build_failures += 1
                if self._consecutive_build_failures == 1 or self._consecutive_build_failures % 10 == 0:
                    # Log every 10th failure to avoid spam, but always log the first
                    diag = self._get_diagnostic_info()
                    available_channels = list(diag['column_channels'].keys())[:5]
                    channels_str = ', '.join(available_channels)
                    if len(diag['column_channels']) > 5:
                        channels_str += "..."
                    self._log(
                        f"Virtual database cannot build rows: {reason}. "
                        f"IO database has {total_io_points} total points across {diag['total_io_channels']} channels. "
                        f"Available channels: {channels_str}",
                        "ERROR" if self._consecutive_build_failures >= 10 else "WARNING"
                    )
                return
            
            if ref_channel.count == 0:
                reason = f"Reference channel '{self.reference_channel}' exists but has no data points"
                self._last_build_failure_reason = reason
                self._consecutive_build_failures += 1
                if self._consecutive_build_failures == 1 or self._consecutive_build_failures % 10 == 0:
                    diag = self._get_diagnostic_info()
                    self._log(
                        f"Virtual database cannot build rows: {reason}. "
                        f"IO database has {total_io_points} total points, but reference channel is empty. "
                        f"Check if data is being collected for '{self.reference_channel}'.",
                        "ERROR" if self._consecutive_build_failures >= 10 else "WARNING"
                    )
                return
        
        if not ref_channel or ref_channel.count == 0:
            # No data at all - this is expected early on, don't log
            self._consecutive_build_failures = 0
            self._last_build_failure_reason = None
            return
        
        # Check if all channels are primed (have at least one data point)
        if not self._is_primed():
            # This should not happen if ref_channel.count > 0, but check anyway
            if ref_channel.count > 0:
                reason = f"Reference channel has {ref_channel.count} points but priming check failed"
                self._last_build_failure_reason = reason
                self._consecutive_build_failures += 1
                if self._consecutive_build_failures == 1 or self._consecutive_build_failures % 10 == 0:
                    diag = self._get_diagnostic_info()
                    self._log(
                        f"Virtual database cannot build rows: {reason}. "
                        f"IO database has {total_io_points} total points. "
                        f"Reference channel '{self.reference_channel}' has {ref_channel.count} points.",
                        "ERROR" if self._consecutive_build_failures >= 10 else "WARNING"
                    )
            return
        
        # Reset failure tracking on successful build
        if self._consecutive_build_failures > 0:
            self._log(
                f"Virtual database build succeeded after {self._consecutive_build_failures} failures. "
                f"Building rows from {ref_channel.count} reference channel points.",
                "INFO"
            )
            self._consecutive_build_failures = 0
            self._last_build_failure_reason = None
        
        # Cache reference channel
        self._ref_channel_cache = ref_channel
        
        # Get time range efficiently - deques support indexing for first/last
        if len(ref_channel.data_points) == 0:
            # This should not happen if we passed the checks above, but handle it
            total_io_points = self.io_database.get_total_count()
            if total_io_points > 0:
                reason = f"Reference channel data_points deque is empty despite count={ref_channel.count}"
                self._last_build_failure_reason = reason
                self._consecutive_build_failures += 1
                if self._consecutive_build_failures == 1 or self._consecutive_build_failures % 10 == 0:
                    self._log(
                        f"Virtual database cannot build rows: {reason}. "
                        f"This may indicate a data structure inconsistency.",
                        "ERROR" if self._consecutive_build_failures >= 10 else "WARNING"
                    )
            return
        first_elapsed = ref_channel.data_points[0].elapsed_time
        last_elapsed = ref_channel.data_points[-1].elapsed_time
        
        # Safety check: validate time range
        if last_elapsed < first_elapsed:
            self._log(
                f"Invalid time range: last_elapsed ({last_elapsed}) < first_elapsed ({first_elapsed})",
                "ERROR"
            )
            return
        
        time_span = last_elapsed - first_elapsed
        if time_span < 0:
            self._log(
                f"Invalid time span: {time_span} (first: {first_elapsed}, last: {last_elapsed})",
                "ERROR"
            )
            return
        
        # CRITICAL: Check for corrupted elapsed_time values
        # If elapsed_time is way too large, it means timestamps are corrupted or from different sessions
        # Recalculate elapsed_time based on actual timestamps
        if time_span > 86400 or abs(first_elapsed) > 86400:  # More than 24 hours or negative/very large
            self._log(
                f"Suspicious elapsed_time values detected: first={first_elapsed:.3f}s, "
                f"last={last_elapsed:.3f}s, span={time_span:.1f}s. "
                f"Recalculating from timestamps...",
                "WARNING"
            )
            # Recalculate elapsed_time from actual timestamps using the first timestamp as reference
            first_ts = ref_channel.data_points[0].timestamp_ns
            last_ts = ref_channel.data_points[-1].timestamp_ns
            time_span_from_ts = (last_ts - first_ts) / 1e9
            
            if time_span_from_ts > 86400:
                self._log(
                    f"Even timestamp-based span is huge: {time_span_from_ts:.1f}s. "
                    f"Timestamps may be corrupted: first={first_ts}, last={last_ts}",
                    "ERROR"
                )
                return
            
            # Use timestamp-based calculation
            first_elapsed = 0.0  # Reset to 0 for first point
            last_elapsed = time_span_from_ts
            time_span = time_span_from_ts
            
            self._log(
                f"Recalculated time span: {time_span:.3f}s (from timestamps)",
                "INFO"
            )
        
        if time_span > 86400:  # More than 24 hours (after recalculation check)
            self._log(
                f"Very large time span: {time_span:.1f}s ({time_span/3600:.1f} hours) - "
                f"this may cause performance issues",
                "WARNING"
            )
        
        # Calculate row interval
        row_interval = 1.0 / self.sampling_rate
        
        # Safety check: ensure row_interval is valid
        # Note: row_interval can be >= 1.0 for low sampling rates (e.g., 0.5 Hz -> 2.0 seconds)
        if row_interval <= 0 or not isinstance(row_interval, (int, float)):
            self._log(
                f"Invalid row_interval: {row_interval} (sampling_rate: {self.sampling_rate})",
                "ERROR"
            )
            return
        
        # Pre-compute snapshots for all channels to avoid repeated conversions
        # This is a one-time cost that pays off during the build loop
        # Also pre-compute elapsed_times and timestamps lists for binary search efficiency
        # SAFETY: Limit snapshot size to prevent hangs with huge datasets
        max_snapshot_size = 100000  # Cap at 100k points per channel
        
        channel_snapshots: Dict[str, List[DataPoint]] = {}
        channel_elapsed_times: Dict[str, List[float]] = {}
        channel_timestamps: Dict[str, List[int]] = {}  # For synchronized channels
        for col_def in self.columns:
            if col_def.channel_path and col_def.channel_path not in channel_snapshots:
                channel_data = self.io_database.get_channel(col_def.channel_path)
                if channel_data:
                    # Create snapshot once per channel with size limit
                    if channel_data.count > max_snapshot_size:
                        # Use only the most recent points to avoid huge snapshots
                        snapshot = list(channel_data.data_points)[-max_snapshot_size:]
                        self._log(
                            f"Channel {col_def.channel_path} has {channel_data.count} points, "
                            f"limiting snapshot to {max_snapshot_size} most recent points",
                            "WARNING"
                        )
                    else:
                        snapshot = list(channel_data.data_points)
                    channel_snapshots[col_def.channel_path] = snapshot
                    # Pre-compute elapsed_times for binary search (one-time cost)
                    channel_elapsed_times[col_def.channel_path] = [p.elapsed_time for p in snapshot]
                    # Pre-compute timestamps for synchronized channels (one-time cost)
                    if col_def.policy == ChannelPolicy.SYNCHRONIZED:
                        channel_timestamps[col_def.channel_path] = [p.timestamp_ns for p in snapshot]
        
        # Also snapshot reference channel and pre-compute its elapsed_times with size limit
        if ref_channel.count > max_snapshot_size:
            ref_snapshot = list(ref_channel.data_points)[-max_snapshot_size:]
            self._log(
                f"Reference channel has {ref_channel.count} points, "
                f"limiting snapshot to {max_snapshot_size} most recent points",
                "WARNING"
            )
        else:
            ref_snapshot = list(ref_channel.data_points)
        ref_elapsed_times = [p.elapsed_time for p in ref_snapshot]
        
        # Pre-index column data to avoid dictionary lookups in hot loop
        # Create a list of tuples: (col_name, col_policy, converter, snapshot, elapsed_times, timestamps)
        # Cache all attributes to reduce attribute access overhead
        column_data = []
        column_names = []  # Pre-compute column names for dict.fromkeys()
        for col_def in self.columns:
            col_name = col_def.name
            col_policy = col_def.policy
            converter = col_def.converter
            column_names.append(col_name)  # Pre-compute for dict creation
            if col_def.channel_path is None:
                column_data.append((col_name, col_policy, converter, None, None, None))
            else:
                snapshot = channel_snapshots.get(col_def.channel_path)
                elapsed_times = channel_elapsed_times.get(col_def.channel_path) if snapshot else None
                timestamps = channel_timestamps.get(col_def.channel_path) if col_policy == ChannelPolicy.SYNCHRONIZED else None
                column_data.append((col_name, col_policy, converter, snapshot, elapsed_times, timestamps))
        
        # Handle case where all points have same timestamp (time_span == 0)
        # We should still create at least one row with the available data
        if time_span == 0:
            # Create a single row at the first_elapsed time
            # This ensures we have at least one row even when all timestamps are identical
            # Pre-allocate dictionary with known size
            row_data = dict.fromkeys(column_names, None)
            
            # Get reference timestamp for synchronized channels
            ref_timestamp_ns = ref_snapshot[0].timestamp_ns if ref_snapshot else None
            
            # Fill in data for each column (same logic as main loop)
            for col_name, col_policy, converter, snapshot, elapsed_times, timestamps in column_data:
                if snapshot is None:
                    # Computed column (e.g., "Timestamp (s)") - handled by CSV writer, skip here
                    continue
                
                raw_value = None
                
                if col_policy == ChannelPolicy.SYNCHRONIZED:
                    # Find exact timestamp match
                    if ref_timestamp_ns is not None and timestamps:
                        ts_idx = bisect.bisect_left(timestamps, ref_timestamp_ns)
                        for i in [ts_idx, ts_idx - 1, ts_idx + 1]:
                            if 0 <= i < len(snapshot):
                                if abs(timestamps[i] - ref_timestamp_ns) <= 1000:  # 1 microsecond tolerance
                                    raw_value = snapshot[i].value
                                    break
                elif col_policy == ChannelPolicy.INTERPOLATED or col_policy == ChannelPolicy.ASYNCHRONOUS:
                    # Use first point's value (no interpolation needed when all points are at same time)
                    if snapshot:
                        raw_value = snapshot[0].value
                
                # Apply converter if value found
                if raw_value is not None:
                    row_data[col_name] = converter(raw_value) if converter else raw_value
            
            # Create the single row
            self.rows = [VirtualRow(timestamp=first_elapsed, data=row_data)]
            self._built = True
            self._last_built_time = first_elapsed
            self._consecutive_build_failures = 0
            self._last_build_failure_reason = None
            return
        
        # Estimate number of rows and pre-allocate
        estimated_rows = int((last_elapsed - first_elapsed) * self.sampling_rate) + 1
        self.rows = []
        if estimated_rows > 0:
            # Pre-allocate with None to avoid repeated reallocations
            self.rows = [None] * min(estimated_rows, 1000000)  # Cap at 1M to avoid huge allocations
        
        # Create rows at regular intervals
        current_time = first_elapsed
        last_row_time = None
        row_idx = 0
        
        # Use incremental search - start from beginning for first row
        ref_search_idx = 0
        
        # Pre-compute tolerance values
        ref_tolerance = row_interval * 0.5
        interp_tolerance = row_interval * 2.0
        
        # Track last known values for forward-fill (for INTERPOLATED and ASYNCHRONOUS channels)
        last_known_values: Dict[str, Any] = {}
        
        # Safety check: prevent infinite loops from floating-point precision issues
        max_iterations = estimated_rows + 1000  # Add buffer for safety
        iteration_count = 0
        
        # Progress logging for debugging
        progress_interval = max(1000, estimated_rows // 10)  # Log every 10% or every 1000 rows
        last_logged = 0
        
        while current_time <= last_elapsed:
            # Safety check to prevent infinite loops
            iteration_count += 1
            if iteration_count > max_iterations:
                # Floating-point precision issue - force exit
                self._log(
                    f"Build loop exceeded max_iterations ({max_iterations}). "
                    f"Estimated rows: {estimated_rows}, Built: {row_idx}, "
                    f"Current time: {current_time}, Last elapsed: {last_elapsed}",
                    "ERROR"
                )
                break
            
            # Progress logging for debugging
            if iteration_count - last_logged >= progress_interval:
                self._log(
                    f"Build progress: {row_idx}/{estimated_rows} rows "
                    f"({100*row_idx/max(1,estimated_rows):.1f}%), "
                    f"time: {current_time:.3f}s/{last_elapsed:.3f}s",
                    "INFO"
                )
                last_logged = iteration_count
            # Find reference point using optimized incremental search
            # Use binary search with incremental start position for better performance
            ref_timestamp_ns = None
            if ref_snapshot and ref_elapsed_times:
                # Use binary search starting from last known position
                idx = bisect.bisect_left(ref_elapsed_times, current_time, lo=ref_search_idx)
                # Check both candidates (idx and idx-1) for closest match
                best_idx = None
                best_diff = ref_tolerance + 1.0
                
                if idx < len(ref_elapsed_times):
                    diff = abs(ref_elapsed_times[idx] - current_time)
                    if diff <= ref_tolerance and diff < best_diff:
                        best_diff = diff
                        best_idx = idx
                
                if idx > 0:
                    diff = abs(ref_elapsed_times[idx - 1] - current_time)
                    if diff <= ref_tolerance and diff < best_diff:
                        best_diff = diff
                        best_idx = idx - 1
                
                if best_idx is not None:
                    point = ref_snapshot[best_idx]
                    ref_timestamp_ns = point.timestamp_ns
                    ref_search_idx = best_idx
                else:
                    # No match found, advance search index to avoid rechecking
                    ref_search_idx = max(0, idx - 1)
            
            # Process columns (fully inlined for speed)
            # Pre-allocate dictionary with known size to avoid rehashing
            row_data = dict.fromkeys(column_names, None)
            
            # Inline all policy-specific matching to avoid function call overhead
            for col_name, col_policy, converter, snapshot, elapsed_times, timestamps in column_data:
                if snapshot is None:
                    continue
                
                raw_value = None
                
                # Inline SYNCHRONIZED policy
                if col_policy == ChannelPolicy.SYNCHRONIZED:
                    if ref_timestamp_ns is not None and timestamps:
                        # Always use binary search - it's faster even for small datasets
                        ts_idx = bisect.bisect_left(timestamps, ref_timestamp_ns)
                        for i in [ts_idx, ts_idx - 1, ts_idx + 1]:
                            if 0 <= i < len(snapshot):
                                if abs(timestamps[i] - ref_timestamp_ns) <= 1000:
                                    raw_value = snapshot[i].value
                                    break
                
                # Inline INTERPOLATED policy
                elif col_policy == ChannelPolicy.INTERPOLATED:
                    if elapsed_times:
                        # Always use binary search - it's faster even for small datasets
                        # O(log n) is better than O(n) even for n < 50 when called thousands of times
                        idx = bisect.bisect_left(elapsed_times, current_time)
                        before_idx = idx - 1
                        after_idx = idx
                        before_valid = before_idx >= 0 and abs(elapsed_times[before_idx] - current_time) <= interp_tolerance
                        after_valid = after_idx < len(snapshot) and abs(elapsed_times[after_idx] - current_time) <= interp_tolerance
                        
                        if before_valid and after_valid:
                            before_point = snapshot[before_idx]
                            after_point = snapshot[after_idx]
                            t1, v1 = before_point.elapsed_time, before_point.value
                            t2, v2 = after_point.elapsed_time, after_point.value
                            if abs(t2 - t1) >= 1e-9:
                                try:
                                    if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                                        raw_value = v1 + (v2 - v1) * (current_time - t1) / (t2 - t1)
                                    else:
                                        raw_value = v1 if abs(current_time - t1) < abs(current_time - t2) else v2
                                except (TypeError, ValueError):
                                    raw_value = v1
                            else:
                                raw_value = v1
                        elif before_valid:
                            raw_value = snapshot[before_idx].value
                        elif after_valid:
                            raw_value = snapshot[after_idx].value
                
                # Inline ASYNCHRONOUS policy
                elif col_policy == ChannelPolicy.ASYNCHRONOUS:
                    if elapsed_times:
                        # Always use binary search - it's faster even for small datasets
                        idx = bisect.bisect_left(elapsed_times, current_time)
                        closest = None
                        min_diff = float('inf')
                        
                        if idx < len(snapshot):
                            diff = abs(elapsed_times[idx] - current_time)
                            if diff < min_diff and diff <= interp_tolerance:
                                min_diff = diff
                                closest = snapshot[idx].value
                        
                        if idx > 0:
                            diff = abs(elapsed_times[idx - 1] - current_time)
                            if diff < min_diff and diff <= interp_tolerance:
                                closest = snapshot[idx - 1].value
                        
                        raw_value = closest
                
                # Apply converter
                if raw_value is not None:
                    try:
                        converted_value = converter(raw_value)
                        row_data[col_name] = converted_value
                        # Update last known value for forward-fill (INTERPOLATED and ASYNCHRONOUS)
                        if col_policy in (ChannelPolicy.INTERPOLATED, ChannelPolicy.ASYNCHRONOUS):
                            last_known_values[col_name] = converted_value
                    except Exception:
                        pass
                elif col_policy in (ChannelPolicy.INTERPOLATED, ChannelPolicy.ASYNCHRONOUS) and col_name in last_known_values:
                    # Forward-fill: use last known value if matching failed
                    row_data[col_name] = last_known_values[col_name]
            
            # Create virtual row
            if row_idx < len(self.rows):
                self.rows[row_idx] = VirtualRow(timestamp=current_time, data=row_data)
            else:
                self.rows.append(VirtualRow(timestamp=current_time, data=row_data))
            row_idx += 1
            last_row_time = current_time
            
            # Advance to next row time
            prev_time = current_time
            current_time += row_interval
            
            # Safety check: ensure current_time is actually advancing
            if current_time <= prev_time:
                self._log(
                    f"current_time not advancing: {prev_time} -> {current_time} "
                    f"(row_interval: {row_interval})",
                    "ERROR"
                )
                break
        
        # Trim to actual size
        if row_idx < len(self.rows):
            self.rows = self.rows[:row_idx]
        
        self._built = True
        # Set to last row's timestamp, not last data point's elapsed time
        # This ensures incremental rebuild starts from the correct position
        self._last_built_time = last_row_time if last_row_time is not None else last_elapsed
        
        # Clear snapshot cache after build
        self._snapshot_cache.clear()
    
    def _get_row_data_at_time(
        self,
        target_elapsed: float,
        row_interval: float,
    ) -> Dict[str, Any]:
        """Get data for all columns at a specific elapsed time.
        
        Applies the appropriate policy (synchronized, interpolated, asynchronous)
        for each column based on its definition, then applies the converter.
        
        Args:
            target_elapsed: Target elapsed time in seconds
            row_interval: Time interval between rows (for tolerance calculation)
            
        Returns:
            Dictionary mapping column names to converted values
        """
        result = {}
        
        # Cache reference channel lookup (set during build/rebuild)
        ref_channel = self._ref_channel_cache
        if not ref_channel:
            return result
        
        ref_point = self._find_point_at_time(ref_channel.data_points, target_elapsed, row_interval * 0.5)
        # For synchronized channels, we need the original timestamp_ns from the reference point
        # This allows us to match channels that arrived together
        ref_timestamp_ns = ref_point[2] if ref_point else None
        
        # Cache channel lookups to avoid redundant dictionary access
        # Many columns may share the same channel_path
        channel_cache: Dict[str, Optional[ChannelData]] = {}
        
        # Process each column according to its policy
        for col_def in self.columns:
            if col_def.channel_path is None:
                # Computed column (no IO channel) - skip for now
                result[col_def.name] = None
                continue
            
            # Use cached channel lookup if available
            channel_data = channel_cache.get(col_def.channel_path)
            if channel_data is None:
                channel_data = self.io_database.get_channel(col_def.channel_path)
                channel_cache[col_def.channel_path] = channel_data
            
            if not channel_data:
                result[col_def.name] = None
                continue
            
            # Apply policy-specific matching
            raw_value = None
            if col_def.policy == ChannelPolicy.SYNCHRONIZED:
                # Must match exact timestamp (within small tolerance)
                # Use original timestamp_ns for matching
                raw_value = self._find_synchronized_value(
                    channel_data.data_points,
                    ref_timestamp_ns,
                    tolerance_ns=1000,  # 1 microsecond tolerance
                )
            elif col_def.policy == ChannelPolicy.INTERPOLATED:
                # Interpolate between surrounding points
                raw_value = self._interpolate_value(
                    channel_data.data_points,
                    target_elapsed,
                    tolerance=row_interval * 2.0,
                )
            elif col_def.policy == ChannelPolicy.ASYNCHRONOUS:
                # Snap to nearest point
                raw_value = self._find_nearest_value(
                    channel_data.data_points,
                    target_elapsed,
                    tolerance=row_interval * 2.0,
                )
            
            # Apply converter to transform raw value
            if raw_value is not None:
                try:
                    converted_value = col_def.converter(raw_value)
                    result[col_def.name] = converted_value
                except Exception as e:
                    # If conversion fails, use None
                    result[col_def.name] = None
            else:
                result[col_def.name] = None
        
        return result
    
    def _get_row_data_at_time_optimized(
        self,
        target_elapsed: float,
        row_interval: float,
        ref_snapshot: List[DataPoint],
        ref_elapsed_times: List[float],
        channel_snapshots: Dict[str, List[DataPoint]],
        channel_elapsed_times: Dict[str, List[float]],
        channel_timestamps: Dict[str, List[int]],
        start_search_idx: int = 0,
    ) -> Dict[str, Any]:
        """Optimized version that uses pre-computed snapshots and incremental binary search.
        
        This is used during build() to avoid repeated snapshot creation and elapsed_times list creation.
        """
        result = {}
        
        # Find reference point using incremental binary search (much faster for large arrays)
        ref_point = None
        ref_idx = start_search_idx
        if ref_snapshot and ref_elapsed_times:
            # Use binary search starting from last position for incremental search
            # First, narrow down the search range using the start index
            if ref_idx < len(ref_elapsed_times) and ref_elapsed_times[ref_idx] <= target_elapsed:
                # Start binary search from last known position
                search_start = ref_idx
            else:
                search_start = 0
            
            # Use binary search on pre-computed elapsed_times list
            idx = bisect.bisect_left(ref_elapsed_times, target_elapsed, lo=search_start)
            
            # Check the point at idx and idx-1 (the two closest points)
            candidates = []
            if idx < len(ref_snapshot):
                candidates.append((idx, ref_snapshot[idx]))
            if idx > 0:
                candidates.append((idx - 1, ref_snapshot[idx - 1]))
            
            # Find the closest candidate within tolerance
            closest_idx = None
            min_diff = float('inf')
            tolerance = row_interval * 0.5
            for candidate_idx, point in candidates:
                diff = abs(point.elapsed_time - target_elapsed)
                if diff < min_diff and diff <= tolerance:
                    min_diff = diff
                    closest_idx = candidate_idx
                    ref_point = (point.value, point.elapsed_time, point.timestamp_ns)
            
            if closest_idx is not None:
                ref_idx = closest_idx
        
        ref_timestamp_ns = ref_point[2] if ref_point else None
        result['_ref_idx'] = ref_idx  # Store for next iteration
        
        # Process each column using cached snapshots
        for col_def in self.columns:
            if col_def.channel_path is None:
                result[col_def.name] = None
                continue
            
            snapshot = channel_snapshots.get(col_def.channel_path)
            if not snapshot:
                result[col_def.name] = None
                continue
            
            # Apply policy-specific matching using snapshot and pre-computed elapsed_times
            elapsed_times = channel_elapsed_times.get(col_def.channel_path)
            raw_value = None
            if col_def.policy == ChannelPolicy.SYNCHRONIZED:
                # Use pre-computed timestamps list for synchronized search
                timestamps = channel_timestamps.get(col_def.channel_path)
                raw_value = self._find_synchronized_value_with_timestamps(
                    snapshot, timestamps, ref_timestamp_ns, tolerance_ns=1000
                )
            elif col_def.policy == ChannelPolicy.INTERPOLATED:
                raw_value = self._interpolate_value_in_snapshot_with_elapsed(
                    snapshot, elapsed_times, target_elapsed, tolerance=row_interval * 2.0
                )
            elif col_def.policy == ChannelPolicy.ASYNCHRONOUS:
                raw_value = self._find_nearest_value_in_snapshot_with_elapsed(
                    snapshot, elapsed_times, target_elapsed, tolerance=row_interval * 2.0
                )
            
            # Apply converter
            if raw_value is not None:
                try:
                    converted_value = col_def.converter(raw_value)
                    result[col_def.name] = converted_value
                except Exception:
                    result[col_def.name] = None
            else:
                result[col_def.name] = None
        
        return result
    
    def _find_synchronized_value_in_snapshot(
        self, snapshot: List[DataPoint], ref_timestamp_ns: Optional[int], tolerance_ns: int
    ) -> Optional[Any]:
        """Find synchronized value in pre-computed snapshot (fallback method)."""
        return self._find_synchronized_value_in_snapshot_optimized(snapshot, ref_timestamp_ns, tolerance_ns)
    
    def _find_synchronized_value_in_snapshot_optimized(
        self, snapshot: List[DataPoint], ref_timestamp_ns: Optional[int], tolerance_ns: int
    ) -> Optional[Any]:
        """Find synchronized value in pre-computed snapshot (fallback - creates timestamps)."""
        if not snapshot or ref_timestamp_ns is None:
            return None
        timestamps = [p.timestamp_ns for p in snapshot]
        return self._find_synchronized_value_with_timestamps(snapshot, timestamps, ref_timestamp_ns, tolerance_ns)
    
    def _find_synchronized_value_with_timestamps(
        self, snapshot: List[DataPoint], timestamps: Optional[List[int]], ref_timestamp_ns: Optional[int], tolerance_ns: int
    ) -> Optional[Any]:
        """Find synchronized value using pre-computed timestamps list."""
        return self._find_synchronized_fast(snapshot, timestamps, ref_timestamp_ns)
    
    def _find_synchronized_fast(
        self, snapshot: List[DataPoint], timestamps: Optional[List[int]], ref_timestamp_ns: Optional[int], tolerance_ns: int = 1000
    ) -> Optional[Any]:
        """Fast synchronized value finder (inlined for hot loop)."""
        if not snapshot or ref_timestamp_ns is None:
            return None
        
        # If timestamps not provided, create them (fallback)
        if timestamps is None:
            timestamps = [p.timestamp_ns for p in snapshot]
        
        # Always use binary search - it's faster even for small datasets when called repeatedly
        idx = bisect.bisect_left(timestamps, ref_timestamp_ns)
        
        # Check points around the insertion point (within tolerance)
        for i in [idx, idx - 1, idx + 1]:
            if 0 <= i < len(snapshot):
                if abs(timestamps[i] - ref_timestamp_ns) <= tolerance_ns:
                    return snapshot[i].value
        return None
    
    def _interpolate_fast(
        self, snapshot: List[DataPoint], elapsed_times: Optional[List[float]], target_elapsed: float, tolerance: float
    ) -> Optional[Any]:
        """Fast interpolate (inlined for hot loop)."""
        if not snapshot or not elapsed_times:
            return None
        
        if len(snapshot) < 50:
            # Linear search for small datasets
            before_point = None
            after_point = None
            before_diff = float('inf')
            after_diff = float('inf')
            
            for i, point in enumerate(snapshot):
                diff = elapsed_times[i] - target_elapsed
                if diff <= 0 and abs(diff) < before_diff:
                    before_point = point
                    before_diff = abs(diff)
                elif diff > 0 and diff < after_diff:
                    after_point = point
                    after_diff = diff
            
            if before_point and after_point:
                t1, v1 = before_point.elapsed_time, before_point.value
                t2, v2 = after_point.elapsed_time, after_point.value
                if abs(t2 - t1) < 1e-9:
                    return v1
                try:
                    if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                        return v1 + (v2 - v1) * (target_elapsed - t1) / (t2 - t1)
                    else:
                        return v1 if abs(target_elapsed - t1) < abs(target_elapsed - t2) else v2
                except (TypeError, ValueError):
                    return v1
            elif before_point:
                return before_point.value
            elif after_point:
                return after_point.value
            return None
        
        # Binary search using pre-computed elapsed_times
        idx = bisect.bisect_left(elapsed_times, target_elapsed)
        
        # Optimize: check tolerance before accessing points
        before_idx = idx - 1
        after_idx = idx
        before_valid = before_idx >= 0 and abs(elapsed_times[before_idx] - target_elapsed) <= tolerance
        after_valid = after_idx < len(snapshot) and abs(elapsed_times[after_idx] - target_elapsed) <= tolerance
        
        if before_valid and after_valid:
            before_point = snapshot[before_idx]
            after_point = snapshot[after_idx]
            t1, v1 = before_point.elapsed_time, before_point.value
            t2, v2 = after_point.elapsed_time, after_point.value
            if abs(t2 - t1) < 1e-9:
                return v1
            try:
                if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                    return v1 + (v2 - v1) * (target_elapsed - t1) / (t2 - t1)
                else:
                    return v1 if abs(target_elapsed - t1) < abs(target_elapsed - t2) else v2
            except (TypeError, ValueError):
                return v1
        elif before_valid:
            return snapshot[before_idx].value
        elif after_valid:
            return snapshot[after_idx].value
        return None
    
    def _find_nearest_fast(
        self, snapshot: List[DataPoint], elapsed_times: Optional[List[float]], target_elapsed: float, tolerance: float
    ) -> Optional[Any]:
        """Fast nearest finder (inlined for hot loop)."""
        if not snapshot or not elapsed_times:
            return None
        
        # Always use binary search - it's faster even for small datasets when called repeatedly
        idx = bisect.bisect_left(elapsed_times, target_elapsed)
        
        # Optimize: check both candidates and pick closest
        closest = None
        min_diff = float('inf')
        
        if idx < len(snapshot):
            diff = abs(elapsed_times[idx] - target_elapsed)
            if diff < min_diff and diff <= tolerance:
                min_diff = diff
                closest = snapshot[idx].value
        
        if idx > 0:
            diff = abs(elapsed_times[idx - 1] - target_elapsed)
            if diff < min_diff and diff <= tolerance:
                closest = snapshot[idx - 1].value
        
        return closest
    
    def _interpolate_value_in_snapshot(
        self, snapshot: List[DataPoint], target_elapsed: float, tolerance: float
    ) -> Optional[Any]:
        """Interpolate value in pre-computed snapshot (fallback method)."""
        if not snapshot:
            return None
        elapsed_times = [p.elapsed_time for p in snapshot]
        return self._interpolate_value_in_snapshot_with_elapsed(snapshot, elapsed_times, target_elapsed, tolerance)
    
    def _interpolate_value_in_snapshot_with_elapsed(
        self, snapshot: List[DataPoint], elapsed_times: List[float], target_elapsed: float, tolerance: float
    ) -> Optional[Any]:
        """Interpolate value using pre-computed snapshot and elapsed_times list."""
        if not snapshot or not elapsed_times:
            return None
        
        # Always use binary search - it's faster even for small datasets when called repeatedly
        idx = bisect.bisect_left(elapsed_times, target_elapsed)
        
        before_point = snapshot[idx - 1] if idx > 0 and abs(elapsed_times[idx - 1] - target_elapsed) <= tolerance else None
        after_point = snapshot[idx] if idx < len(snapshot) and abs(elapsed_times[idx] - target_elapsed) <= tolerance else None
        
        if before_point and after_point:
            t1, v1 = before_point.elapsed_time, before_point.value
            t2, v2 = after_point.elapsed_time, after_point.value
            if abs(t2 - t1) < 1e-9:
                return v1
            try:
                if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                    return v1 + (v2 - v1) * (target_elapsed - t1) / (t2 - t1)
                else:
                    return v1 if abs(target_elapsed - t1) < abs(target_elapsed - t2) else v2
            except (TypeError, ValueError):
                return v1
        elif before_point:
            return before_point.value
        elif after_point:
            return after_point.value
        return None
    
    def _find_nearest_value_in_snapshot(
        self, snapshot: List[DataPoint], target_elapsed: float, tolerance: float
    ) -> Optional[Any]:
        """Find nearest value in pre-computed snapshot (fallback method)."""
        if not snapshot:
            return None
        elapsed_times = [p.elapsed_time for p in snapshot]
        return self._find_nearest_value_in_snapshot_with_elapsed(snapshot, elapsed_times, target_elapsed, tolerance)
    
    def _find_nearest_value_in_snapshot_with_elapsed(
        self, snapshot: List[DataPoint], elapsed_times: List[float], target_elapsed: float, tolerance: float
    ) -> Optional[Any]:
        """Find nearest value using pre-computed snapshot and elapsed_times list."""
        if not snapshot or not elapsed_times:
            return None
        
        # Always use binary search - it's faster even for small datasets when called repeatedly
        idx = bisect.bisect_left(elapsed_times, target_elapsed)
        
        candidates = []
        if idx < len(snapshot):
            candidates.append((elapsed_times[idx], snapshot[idx]))
        if idx > 0:
            candidates.append((elapsed_times[idx - 1], snapshot[idx - 1]))
        
        closest = None
        min_diff = float('inf')
        for elapsed, point in candidates:
            diff = abs(elapsed - target_elapsed)
            if diff < min_diff and diff <= tolerance:
                min_diff = diff
                closest = point.value
        
        return closest
    
    def _find_point_at_time(
        self,
        data_points: List[DataPoint],
        target_elapsed: float,
        tolerance: float,
    ) -> Optional[Tuple[Any, float, int]]:
        """Find data point closest to target elapsed time using binary search.
        
        Returns:
            Tuple of (value, elapsed_time, timestamp_ns) or None
        """
        if not data_points:
            return None
        
        # CRITICAL: Always create snapshot first to avoid "deque mutated during iteration" error
        # Even for small datasets, we need a snapshot to prevent race conditions
        count = len(data_points) if hasattr(data_points, '__len__') else None
        
        # Create snapshot (safe from concurrent modification)
        if hasattr(data_points, '__getitem__') and not isinstance(data_points, deque):
            # It's already a list-like structure that supports indexing and is not a deque
            snapshot = data_points
        else:
            # It's a deque or unknown type - create snapshot
            # For very large datasets, filter to time window first to reduce snapshot size
            if count is not None and count > 10000:
                time_window = max(tolerance * 10, 0.01)
                min_time = target_elapsed - time_window
                max_time = target_elapsed + time_window
                # Create snapshot first, then filter (safe from mutation)
                full_snapshot = list(data_points)
                snapshot = [p for p in full_snapshot if min_time <= p.elapsed_time <= max_time]
            else:
                snapshot = list(data_points)
        
        # For small datasets, linear search is faster due to cache locality
        if len(snapshot) < 50:
            # Linear search on snapshot (safe from mutation)
            closest = None
            min_diff = float('inf')
            for point in snapshot:
                diff = abs(point.elapsed_time - target_elapsed)
                if diff < min_diff and diff <= tolerance:
                    min_diff = diff
                    closest = (point.value, point.elapsed_time, point.timestamp_ns)
            return closest
        
        # For larger datasets, use binary search on snapshot
        
        if not snapshot:
            return None
        
        # Binary search using bisect
        # Create elapsed_times list only once for this snapshot
        # For very small snapshots, just check all points
        if len(snapshot) < 10:
            closest = None
            min_diff = float('inf')
            for point in snapshot:
                diff = abs(point.elapsed_time - target_elapsed)
                if diff < min_diff and diff <= tolerance:
                    min_diff = diff
                    closest = (point.value, point.elapsed_time, point.timestamp_ns)
            return closest
        
        # Use bisect on elapsed_times list
        elapsed_times = [p.elapsed_time for p in snapshot]
        idx = bisect.bisect_left(elapsed_times, target_elapsed)
        
        # Check the point at idx and idx-1 (the two closest points)
        candidates = []
        if idx < len(snapshot):
            candidates.append(snapshot[idx])
        if idx > 0:
            candidates.append(snapshot[idx - 1])
        
        # Find the closest candidate within tolerance
        closest = None
        min_diff = float('inf')
        for point in candidates:
            diff = abs(point.elapsed_time - target_elapsed)
            if diff < min_diff and diff <= tolerance:
                min_diff = diff
                closest = (point.value, point.elapsed_time, point.timestamp_ns)
        
        return closest
    
    def _find_synchronized_value(
        self,
        data_points: List[DataPoint],
        ref_timestamp_ns: Optional[int],
        tolerance_ns: int = 1000,
    ) -> Optional[Any]:
        """Find value with exact timestamp match using binary search (for synchronized channels).
        
        Args:
            data_points: List or deque of data points to search
            ref_timestamp_ns: Reference timestamp in nanoseconds
            tolerance_ns: Tolerance in nanoseconds
            
        Returns:
            Value if found within tolerance, None otherwise
        """
        if ref_timestamp_ns is None:
            return None
        
        # CRITICAL: Always create snapshot first to avoid "deque mutated during iteration" error
        count = len(data_points) if hasattr(data_points, '__len__') else None
        
        # Create snapshot (safe from concurrent modification)
        if hasattr(data_points, '__getitem__') and not isinstance(data_points, deque):
            snapshot = data_points
        else:
            # It's a deque or unknown type - create snapshot
            if count is not None and count > 10000:
                window_ns = max(tolerance_ns * 10, 10000)
                min_timestamp_ns = ref_timestamp_ns - window_ns
                max_timestamp_ns = ref_timestamp_ns + window_ns
                # Create snapshot first, then filter (safe from mutation)
                full_snapshot = list(data_points)
                snapshot = [p for p in full_snapshot if min_timestamp_ns <= p.timestamp_ns <= max_timestamp_ns]
            else:
                snapshot = list(data_points)
        
        # For small datasets, linear search is faster (on snapshot, safe from mutation)
        if len(snapshot) < 50:
            for point in snapshot:
                if abs(point.timestamp_ns - ref_timestamp_ns) <= tolerance_ns:
                    return point.value
            return None
        
        # For larger datasets, use binary search on snapshot
        
        if not snapshot:
            return None
        
        # Binary search on timestamps
        timestamps = [p.timestamp_ns for p in snapshot]
        idx = bisect.bisect_left(timestamps, ref_timestamp_ns)
        
        # Check points around the insertion point (within tolerance)
        candidates = []
        if idx < len(snapshot):
            candidates.append(snapshot[idx])
        if idx > 0:
            candidates.append(snapshot[idx - 1])
        if idx + 1 < len(snapshot):
            candidates.append(snapshot[idx + 1])
        
        # Find the first candidate within tolerance
        for point in candidates:
            if abs(point.timestamp_ns - ref_timestamp_ns) <= tolerance_ns:
                return point.value
        
        return None
    
    def _interpolate_value(
        self,
        data_points: List[DataPoint],
        target_elapsed: float,
        tolerance: float,
    ) -> Optional[Any]:
        """Interpolate value at target time using binary search (for interpolated channels).
        
        Args:
            data_points: List or deque of data points to search
            target_elapsed: Target elapsed time
            tolerance: Maximum time difference for interpolation
            
        Returns:
            Interpolated value or None
        """
        if not data_points:
            return None
        
        # CRITICAL: Always create snapshot first to avoid "deque mutated during iteration" error
        count = len(data_points) if hasattr(data_points, '__len__') else None
        
        # Create snapshot (safe from concurrent modification)
        if hasattr(data_points, '__getitem__') and not isinstance(data_points, deque):
            snapshot = data_points
        else:
            # It's a deque or unknown type - create snapshot
            if count is not None and count > 10000:
                time_window = max(tolerance * 10, 0.01)
                min_time = target_elapsed - time_window
                max_time = target_elapsed + time_window
                # Create snapshot first, then filter (safe from mutation)
                full_snapshot = list(data_points)
                snapshot = [p for p in full_snapshot if min_time <= p.elapsed_time <= max_time]
            else:
                snapshot = list(data_points)
        
        # For small datasets, linear search is faster (on snapshot, safe from mutation)
        if len(snapshot) < 50:
            # Linear search for very small datasets
            before_point = None
            after_point = None
            before_diff = float('inf')
            after_diff = float('inf')
            
            for point in snapshot:
                diff = point.elapsed_time - target_elapsed
                if diff <= 0 and abs(diff) < before_diff:
                    before_point = point
                    before_diff = abs(diff)
                elif diff > 0 and diff < after_diff:
                    after_point = point
                    after_diff = diff
            
            # Interpolate if we have both points
            if before_point and after_point:
                t1, v1 = before_point.elapsed_time, before_point.value
                t2, v2 = after_point.elapsed_time, after_point.value
                if abs(t2 - t1) < 1e-9:
                    return v1
                try:
                    if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                        interpolated = v1 + (v2 - v1) * (target_elapsed - t1) / (t2 - t1)
                        return interpolated
                    else:
                        return v1 if abs(target_elapsed - t1) < abs(target_elapsed - t2) else v2
                except (TypeError, ValueError):
                    return v1
            elif before_point:
                return before_point.value
            elif after_point:
                return after_point.value
            return None
        
        # For larger datasets, use binary search on snapshot
        
        if not snapshot:
            return None
        
        # Binary search for large datasets
        elapsed_times = [p.elapsed_time for p in snapshot]
        idx = bisect.bisect_left(elapsed_times, target_elapsed)
        
        # Find points before and after target
        before_point = None
        after_point = None
        
        # Point before (or at) target
        if idx > 0:
            before_point = snapshot[idx - 1]
            if abs(before_point.elapsed_time - target_elapsed) > tolerance:
                before_point = None
        
        # Point after target
        if idx < len(snapshot):
            after_point = snapshot[idx]
            if abs(after_point.elapsed_time - target_elapsed) > tolerance:
                after_point = None
        
        # Also check point at idx-2 and idx+1 for better interpolation
        if idx > 1 and before_point is None:
            candidate = snapshot[idx - 2]
            if abs(candidate.elapsed_time - target_elapsed) <= tolerance:
                before_point = candidate
        
        if idx + 1 < len(snapshot) and after_point is None:
            candidate = snapshot[idx + 1]
            if abs(candidate.elapsed_time - target_elapsed) <= tolerance:
                after_point = candidate
        
        # If we have points on both sides, interpolate
        if before_point and after_point:
            t1, v1 = before_point.elapsed_time, before_point.value
            t2, v2 = after_point.elapsed_time, after_point.value
            
            # Avoid division by zero
            if abs(t2 - t1) < 1e-9:
                return v1
            
            # Linear interpolation: v = v1 + (v2 - v1) * (t - t1) / (t2 - t1)
            try:
                if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                    interpolated = v1 + (v2 - v1) * (target_elapsed - t1) / (t2 - t1)
                    return interpolated
                else:
                    # Non-numeric values - use closest
                    return v1 if abs(target_elapsed - t1) < abs(target_elapsed - t2) else v2
            except (TypeError, ValueError):
                return v1
        
        # If we only have one side, use it
        if before_point:
            return before_point.value
        elif after_point:
            return after_point.value
        
        return None
    
    def _find_nearest_value(
        self,
        data_points: List[DataPoint],
        target_elapsed: float,
        tolerance: float,
    ) -> Optional[Any]:
        """Find nearest value (for asynchronous channels).
        
        Args:
            data_points: List or deque of data points to search
            target_elapsed: Target elapsed time
            tolerance: Maximum time difference
            
        Returns:
            Nearest value or None
        """
        if not data_points:
            return None
        
        # CRITICAL: Always create snapshot first to avoid "deque mutated during iteration" error
        count = len(data_points) if hasattr(data_points, '__len__') else None
        
        # Create snapshot (safe from concurrent modification)
        if hasattr(data_points, '__getitem__') and not isinstance(data_points, deque):
            snapshot = data_points
        else:
            # It's a deque or unknown type - create snapshot
            if count is not None and count > 10000:
                time_window = max(tolerance * 10, 0.01)
                min_time = target_elapsed - time_window
                max_time = target_elapsed + time_window
                # Create snapshot first, then filter (safe from mutation)
                full_snapshot = list(data_points)
                snapshot = [p for p in full_snapshot if min_time <= p.elapsed_time <= max_time]
            else:
                snapshot = list(data_points)
        
        # For small datasets, linear search is faster (on snapshot, safe from mutation)
        if len(snapshot) < 50:
            # Linear search for very small datasets
            closest = None
            min_diff = float('inf')
            for point in snapshot:
                diff = abs(point.elapsed_time - target_elapsed)
                if diff < min_diff and diff <= tolerance:
                    min_diff = diff
                    closest = point.value
            return closest
        
        # For larger datasets, use binary search on snapshot
        
        if not snapshot:
            return None
        
        # Binary search for large datasets
        elapsed_times = [p.elapsed_time for p in snapshot]
        idx = bisect.bisect_left(elapsed_times, target_elapsed)
        
        # Check the point at idx and idx-1 (the two closest points)
        candidates = []
        if idx < len(snapshot):
            candidates.append(snapshot[idx])
        if idx > 0:
            candidates.append(snapshot[idx - 1])
        
        # Find the closest candidate within tolerance
        closest = None
        min_diff = float('inf')
        for point in candidates:
            diff = abs(point.elapsed_time - target_elapsed)
            if diff < min_diff and diff <= tolerance:
                min_diff = diff
                closest = point.value
        
        return closest
    
    def get_row_count(self) -> int:
        """Get the number of rows in the virtual database."""
        if not self._built:
            self.build()
        return len(self.rows)
    
    def get_rows(self) -> List[VirtualRow]:
        """Get all rows in the virtual database."""
        if not self._built:
            self.build()
        return self.rows
    
    def get_row_at_index(self, index: int) -> Optional[VirtualRow]:
        """Get a row by index."""
        if not self._built:
            self.build()
        if 0 <= index < len(self.rows):
            return self.rows[index]
        return None
    
    def get_row_at_time(
        self,
        target_elapsed: float,
        tolerance: float = 0.0001,
    ) -> Optional[VirtualRow]:
        """Get a row closest to a target elapsed time."""
        if not self._built:
            self.build()
        
        closest = None
        min_diff = float('inf')
        
        for row in self.rows:
            diff = abs(row.timestamp - target_elapsed)
            if diff < min_diff and diff <= tolerance:
                min_diff = diff
                closest = row
        
        return closest
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about the virtual database."""
        if not self._built:
            self.build()
        
        if not self.rows:
            return {
                'row_count': 0,
                'time_span': 0.0,
                'sampling_rate': self.sampling_rate,
                'expected_rows': 0,
                'actual_rows': 0,
            }
        
        first_time = self.rows[0].timestamp
        last_time = self.rows[-1].timestamp
        time_span = last_time - first_time
        expected_rows = int(time_span * self.sampling_rate)
        
        return {
            'row_count': len(self.rows),
            'time_span': time_span,
            'first_timestamp': first_time,
            'last_timestamp': last_time,
            'sampling_rate': self.sampling_rate,
            'expected_rows': expected_rows,
            'actual_rows': len(self.rows),
            'coverage': len(self.rows) / expected_rows if expected_rows > 0 else 0.0,
        }
    
    def clear(self) -> None:
        """Clear all rows from the virtual database."""
        self.rows.clear()
        self._built = False
        self._last_built_time = None
        self._snapshot_cache.clear()
        self._ref_channel_cache = None
    
    def rebuild(self) -> None:
        """Rebuild the virtual database from the IO database.
        
        This method is incremental - it only builds new rows since the last build,
        making it much more efficient than a full rebuild.
        
        Will not create rows until all channels have at least one data point (primed).
        """
        # Use cached reference channel if available, otherwise get it
        # CRITICAL: Always refresh cache to get latest channel data
        # This handles the case where IODatabase was cleared - the cache might point to old (empty) channel
        # By always getting fresh reference, we ensure we're using the current channel state
        self._ref_channel_cache = self.io_database.get_channel(self.reference_channel)
        ref_channel = self._ref_channel_cache
        
        total_io_points = self.io_database.get_total_count()
        
        # Check if we have data but can't rebuild - this is the problematic case
        if total_io_points > 0:
            if not ref_channel:
                reason = f"Reference channel '{self.reference_channel}' does not exist in IO database (rebuild)"
                self._last_build_failure_reason = reason
                self._consecutive_build_failures += 1
                if self._consecutive_build_failures == 1 or self._consecutive_build_failures % 50 == 0:
                    # Log less frequently for rebuild failures to avoid spam
                    diag = self._get_diagnostic_info()
                    self._log(
                        f"Virtual database cannot rebuild rows: {reason}. "
                        f"IO database has {total_io_points} total points. "
                        f"Virtual database has {len(self.rows)} existing rows.",
                        "ERROR" if self._consecutive_build_failures >= 50 else "WARNING"
                    )
                return
            
            if ref_channel.count == 0:
                reason = f"Reference channel '{self.reference_channel}' exists but has no data points (rebuild)"
                self._last_build_failure_reason = reason
                self._consecutive_build_failures += 1
                if self._consecutive_build_failures == 1 or self._consecutive_build_failures % 50 == 0:
                    diag = self._get_diagnostic_info()
                    self._log(
                        f"Virtual database cannot rebuild rows: {reason}. "
                        f"IO database has {total_io_points} total points, but reference channel is empty. "
                        f"Virtual database has {len(self.rows)} existing rows.",
                        "ERROR" if self._consecutive_build_failures >= 50 else "WARNING"
                    )
                return
        
        if not ref_channel or ref_channel.count == 0:
            # No data at all - this is expected early on, don't log
            # Only reset failure tracking if we were previously built
            if self._built:
                self._consecutive_build_failures = 0
                self._last_build_failure_reason = None
            return
        
        # For incremental rebuild, we only need the last data point to check if there's new data
        # We can avoid creating a full snapshot if we just need to check the last point
        if not ref_channel.data_points:
            # This should not happen if ref_channel.count > 0, but handle it
            if total_io_points > 0 and ref_channel.count > 0:
                reason = f"Reference channel data_points deque is empty despite count={ref_channel.count} (rebuild)"
                self._last_build_failure_reason = reason
                self._consecutive_build_failures += 1
                if self._consecutive_build_failures == 1 or self._consecutive_build_failures % 50 == 0:
                    self._log(
                        f"Virtual database cannot rebuild rows: {reason}. "
                        f"This may indicate a data structure inconsistency.",
                        "ERROR" if self._consecutive_build_failures >= 50 else "WARNING"
                    )
            return
        
        # Reset failure tracking on successful rebuild
        if self._consecutive_build_failures > 0:
            self._log(
                f"Virtual database rebuild succeeded after {self._consecutive_build_failures} failures. "
                f"Rebuilding from {ref_channel.count} reference channel points, {len(self.rows)} existing rows.",
                "INFO"
            )
            self._consecutive_build_failures = 0
            self._last_build_failure_reason = None
        
        # Get last data point without full snapshot (more efficient)
        # Access last element directly from deque (O(1) operation)
        last_data_point = ref_channel.data_points[-1]
        last_elapsed = last_data_point.elapsed_time
        
        # CRITICAL: Check for corrupted elapsed_time values (same as in build())
        # If elapsed_time is way too large, recalculate from timestamps
        if abs(last_elapsed) > 86400:  # More than 24 hours
            self._log(
                f"Suspicious elapsed_time in rebuild: last={last_elapsed:.3f}s. "
                f"Recalculating from timestamps...",
                "WARNING"
            )
            # Recalculate elapsed_time from actual timestamps
            first_ts = ref_channel.data_points[0].timestamp_ns
            last_ts = last_data_point.timestamp_ns
            time_span_from_ts = (last_ts - first_ts) / 1e9
            
            if time_span_from_ts > 86400:
                self._log(
                    f"Even timestamp-based span is huge: {time_span_from_ts:.1f}s. "
                    f"Timestamps may be corrupted: first={first_ts}, last={last_ts}",
                    "ERROR"
                )
                return
            
            # Use timestamp-based calculation
            last_elapsed = time_span_from_ts
            self._log(
                f"Recalculated last_elapsed: {last_elapsed:.3f}s (from timestamps)",
                "INFO"
            )
        
        # If not built yet, do a full build
        if not self._built or self._last_built_time is None:
            self.clear()
            self.build()  # build() will check for priming
            return
        
        # NOTE: We do NOT check priming again after initial build.
        # Once the initial build is complete, we continue with incremental rebuilds
        # even if some channels temporarily have no data. The initial build already
        # verified that all channels were primed, and incremental rebuilds should
        # continue building rows from the reference channel data.
        # The previous priming check here was causing rebuilds to stop if any channel
        # temporarily had no data, which prevented new rows from being created.
        
        # Calculate row interval
        row_interval = 1.0 / self.sampling_rate
        
        # Start from the next row after the last built time
        # Since _last_built_time is the timestamp of the last row built,
        # we simply add row_interval to get the next row time
        start_time = self._last_built_time + row_interval
        
        # If there's not enough new data to create at least one more row, skip
        # We need at least one row_interval worth of new data beyond the last built time
        # However, if we have significantly more data points than rows, we should create more rows
        # even if the time hasn't advanced much (handles case where data arrives in bursts)
        if last_elapsed < start_time - 1e-9:
            # Check if we have many more data points than rows - if so, create rows based on point count
            current_point_count = ref_channel.count
            current_row_count = len(self.rows)
            # If we have at least 2x more points than rows, create rows even if time hasn't advanced
            # This handles the case where multiple data points arrive at the same timestamp
            if current_point_count > current_row_count * 2:
                # Extend last_elapsed to allow creating more rows
                # Estimate how many rows we should have based on point count
                estimated_rows = min(current_point_count, int(current_point_count * 0.1))  # Conservative estimate
                last_elapsed = max(last_elapsed, start_time + (estimated_rows - current_row_count) * row_interval)
            else:
                return
        
        # Pre-compute snapshots for incremental rebuild (similar to build)
        # OPTIMIZATION: Only create snapshots for data we actually need
        # For incremental rebuild, we only need data from start_time onwards
        # This avoids creating expensive full snapshots when deques are large
        channel_snapshots: Dict[str, List[DataPoint]] = {}
        channel_elapsed_times: Dict[str, List[float]] = {}
        channel_timestamps: Dict[str, List[int]] = {}  # For synchronized channels
        
        # Calculate time window for incremental snapshot (only need data from start_time - some margin)
        # This dramatically reduces snapshot size when deques are large
        time_margin = row_interval * 100  # 100 row intervals of margin for interpolation
        snapshot_start_time = max(0.0, start_time - time_margin) if self._last_built_time is not None else 0.0
        
        for col_def in self.columns:
            if col_def.channel_path and col_def.channel_path not in channel_snapshots:
                channel_data = self.io_database.get_channel(col_def.channel_path)
                if channel_data and channel_data.count > 0:
                    # OPTIMIZATION: Only snapshot data points we actually need
                    # For incremental rebuild, we only need points from snapshot_start_time onwards
                    # This avoids expensive full deque-to-list conversion
                    # SAFETY: Limit snapshot size to prevent hangs (max 100k points per channel)
                    max_snapshot_size = 100000
                    
                    if channel_data.count > 1000 and self._last_built_time is not None:
                        # Large dataset - create incremental snapshot
                        # CRITICAL: Create snapshot first to avoid "deque mutated during iteration" error
                        # Limit snapshot size to prevent performance issues
                        if channel_data.count > max_snapshot_size:
                            # Too many points - use only the most recent points
                            snapshot = list(channel_data.data_points)[-max_snapshot_size:]
                        else:
                            # Create full snapshot (safe from concurrent modification)
                            snapshot = list(channel_data.data_points)
                        
                        # Now filter by time from the snapshot (safe to iterate)
                        filtered_snapshot = []
                        elapsed_times_list = []
                        timestamps_list = []
                        for point in snapshot:
                            if point.elapsed_time >= snapshot_start_time:
                                filtered_snapshot.append(point)
                                elapsed_times_list.append(point.elapsed_time)
                                if col_def.policy == ChannelPolicy.SYNCHRONIZED:
                                    timestamps_list.append(point.timestamp_ns)
                                # Safety: limit snapshot size
                                if len(filtered_snapshot) >= max_snapshot_size:
                                    break
                        
                        snapshot = filtered_snapshot
                        
                        # If we didn't find enough points, use the full snapshot we already created
                        if len(snapshot) < 10:
                            # Use the snapshot we already created (no need to recreate)
                            elapsed_times_list = [p.elapsed_time for p in snapshot]
                            if col_def.policy == ChannelPolicy.SYNCHRONIZED:
                                timestamps_list = [p.timestamp_ns for p in snapshot]
                    else:
                        # Small dataset - create full snapshot (cheap)
                        # But still limit size for safety
                        if channel_data.count > max_snapshot_size:
                            snapshot = list(channel_data.data_points)[-max_snapshot_size:]
                        else:
                            snapshot = list(channel_data.data_points)
                        elapsed_times_list = [p.elapsed_time for p in snapshot]
                        if col_def.policy == ChannelPolicy.SYNCHRONIZED:
                            timestamps_list = [p.timestamp_ns for p in snapshot]
                    
                    channel_snapshots[col_def.channel_path] = snapshot
                    channel_elapsed_times[col_def.channel_path] = elapsed_times_list
                    if col_def.policy == ChannelPolicy.SYNCHRONIZED:
                        channel_timestamps[col_def.channel_path] = timestamps_list
        
        # Also snapshot reference channel and pre-compute its elapsed_times
        # OPTIMIZATION: Use incremental snapshot for reference channel too
        # SAFETY: Limit snapshot size to prevent hangs
        max_snapshot_size = 100000
        
        if ref_channel.count > 1000 and self._last_built_time is not None:
            # Large dataset - create incremental snapshot
            # CRITICAL: Create snapshot first to avoid "deque mutated during iteration" error
            # Limit snapshot size to prevent performance issues
            if ref_channel.count > max_snapshot_size:
                # Too many points - use only the most recent points
                ref_snapshot = list(ref_channel.data_points)[-max_snapshot_size:]
            else:
                # Create full snapshot (safe from concurrent modification)
                ref_snapshot = list(ref_channel.data_points)
            
            # Now filter by time from the snapshot (safe to iterate)
            filtered_snapshot = []
            ref_elapsed_times = []
            for point in ref_snapshot:
                if point.elapsed_time >= snapshot_start_time:
                    filtered_snapshot.append(point)
                    ref_elapsed_times.append(point.elapsed_time)
                    # Safety: limit snapshot size
                    if len(filtered_snapshot) >= max_snapshot_size:
                        break
            
            ref_snapshot = filtered_snapshot
            
            # If we didn't find enough points, use the full snapshot we already created
            if len(ref_snapshot) < 10:
                # Use the snapshot we already created (no need to recreate)
                ref_elapsed_times = [p.elapsed_time for p in ref_snapshot]
        else:
            # Small dataset - create full snapshot (cheap)
            # But still limit size for safety
            if ref_channel.count > max_snapshot_size:
                ref_snapshot = list(ref_channel.data_points)[-max_snapshot_size:]
            else:
                ref_snapshot = list(ref_channel.data_points)
            ref_elapsed_times = [p.elapsed_time for p in ref_snapshot]
        
        # Pre-index column data to avoid dictionary lookups in hot loop
        # Cache all attributes to reduce attribute access overhead
        column_data = []
        column_names = []  # Pre-compute column names for dict.fromkeys()
        for col_def in self.columns:
            col_name = col_def.name
            col_policy = col_def.policy
            converter = col_def.converter
            column_names.append(col_name)  # Pre-compute for dict creation
            if col_def.channel_path is None:
                column_data.append((col_name, col_policy, converter, None, None, None))
            else:
                snapshot = channel_snapshots.get(col_def.channel_path)
                elapsed_times = channel_elapsed_times.get(col_def.channel_path) if snapshot else None
                timestamps = channel_timestamps.get(col_def.channel_path) if col_policy == ChannelPolicy.SYNCHRONIZED else None
                column_data.append((col_name, col_policy, converter, snapshot, elapsed_times, timestamps))
        
        # Build only new rows incrementally
        # Limit rows per rebuild to avoid blocking (process in batches)
        # At 6 kHz, we might accumulate many rows between rebuilds
        # Increased limit for faster processing when catching up
        max_rows_per_rebuild = 10000  # Process up to 10k rows per rebuild call
        rows_built = 0
        
        # Use incremental search - start from a reasonable position using binary search
        ref_search_idx = 0
        if ref_elapsed_times and self._last_built_time is not None:
            # Use binary search to find starting position
            ref_search_idx = bisect.bisect_left(ref_elapsed_times, self._last_built_time)
            # Start a bit before to be safe
            ref_search_idx = max(0, ref_search_idx - 10)
        
        # Pre-compute tolerance values
        ref_tolerance = row_interval * 0.5
        interp_tolerance = row_interval * 2.0
        
        # Track last known values for forward-fill (for INTERPOLATED and ASYNCHRONOUS channels)
        # Initialize from existing rows if available
        last_known_values: Dict[str, Any] = {}
        if self.rows:
            # Get last known values from the last row
            last_row = self.rows[-1]
            for col_name, col_policy, _, _, _, _ in column_data:
                if col_policy in (ChannelPolicy.INTERPOLATED, ChannelPolicy.ASYNCHRONOUS) and col_name in last_row.data:
                    value = last_row.data[col_name]
                    if value is not None:
                        last_known_values[col_name] = value
        
        current_time = start_time
        last_row_time = self._last_built_time
        while current_time <= last_elapsed and rows_built < max_rows_per_rebuild:
            # Find reference point using optimized incremental search
            # Use binary search with incremental start position for better performance
            ref_timestamp_ns = None
            if ref_snapshot and ref_elapsed_times:
                # Use binary search starting from last known position
                idx = bisect.bisect_left(ref_elapsed_times, current_time, lo=ref_search_idx)
                # Check both candidates (idx and idx-1) for closest match
                best_idx = None
                best_diff = ref_tolerance + 1.0
                
                if idx < len(ref_elapsed_times):
                    diff = abs(ref_elapsed_times[idx] - current_time)
                    if diff <= ref_tolerance and diff < best_diff:
                        best_diff = diff
                        best_idx = idx
                
                if idx > 0:
                    diff = abs(ref_elapsed_times[idx - 1] - current_time)
                    if diff <= ref_tolerance and diff < best_diff:
                        best_diff = diff
                        best_idx = idx - 1
                
                if best_idx is not None:
                    point = ref_snapshot[best_idx]
                    ref_timestamp_ns = point.timestamp_ns
                    ref_search_idx = best_idx
                else:
                    # No match found, advance search index to avoid rechecking
                    ref_search_idx = max(0, idx - 1)
            
            # Process columns (fully inlined for speed)
            # Pre-allocate dictionary with known size to avoid rehashing
            row_data = dict.fromkeys(column_names, None)
            
            # Inline all policy-specific matching to avoid function call overhead
            for col_name, col_policy, converter, snapshot, elapsed_times, timestamps in column_data:
                if snapshot is None:
                    continue
                
                raw_value = None
                
                # Inline SYNCHRONIZED policy
                if col_policy == ChannelPolicy.SYNCHRONIZED:
                    if ref_timestamp_ns is not None and timestamps:
                        # Always use binary search - it's faster even for small datasets
                        ts_idx = bisect.bisect_left(timestamps, ref_timestamp_ns)
                        for i in [ts_idx, ts_idx - 1, ts_idx + 1]:
                            if 0 <= i < len(snapshot):
                                if abs(timestamps[i] - ref_timestamp_ns) <= 1000:
                                    raw_value = snapshot[i].value
                                    break
                
                # Inline INTERPOLATED policy
                elif col_policy == ChannelPolicy.INTERPOLATED:
                    if elapsed_times:
                        # Always use binary search - it's faster even for small datasets
                        # O(log n) is better than O(n) even for n < 50 when called thousands of times
                        idx = bisect.bisect_left(elapsed_times, current_time)
                        before_idx = idx - 1
                        after_idx = idx
                        before_valid = before_idx >= 0 and abs(elapsed_times[before_idx] - current_time) <= interp_tolerance
                        after_valid = after_idx < len(snapshot) and abs(elapsed_times[after_idx] - current_time) <= interp_tolerance
                        
                        if before_valid and after_valid:
                            before_point = snapshot[before_idx]
                            after_point = snapshot[after_idx]
                            t1, v1 = before_point.elapsed_time, before_point.value
                            t2, v2 = after_point.elapsed_time, after_point.value
                            if abs(t2 - t1) >= 1e-9:
                                try:
                                    if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                                        raw_value = v1 + (v2 - v1) * (current_time - t1) / (t2 - t1)
                                    else:
                                        raw_value = v1 if abs(current_time - t1) < abs(current_time - t2) else v2
                                except (TypeError, ValueError):
                                    raw_value = v1
                            else:
                                raw_value = v1
                        elif before_valid:
                            raw_value = snapshot[before_idx].value
                        elif after_valid:
                            raw_value = snapshot[after_idx].value
                
                # Inline ASYNCHRONOUS policy
                elif col_policy == ChannelPolicy.ASYNCHRONOUS:
                    if elapsed_times:
                        # Always use binary search - it's faster even for small datasets
                        idx = bisect.bisect_left(elapsed_times, current_time)
                        closest = None
                        min_diff = float('inf')
                        
                        if idx < len(snapshot):
                            diff = abs(elapsed_times[idx] - current_time)
                            if diff < min_diff and diff <= interp_tolerance:
                                min_diff = diff
                                closest = snapshot[idx].value
                        
                        if idx > 0:
                            diff = abs(elapsed_times[idx - 1] - current_time)
                            if diff < min_diff and diff <= interp_tolerance:
                                closest = snapshot[idx - 1].value
                        
                        raw_value = closest
                
                # Apply converter
                if raw_value is not None:
                    try:
                        converted_value = converter(raw_value)
                        row_data[col_name] = converted_value
                        # Update last known value for forward-fill (INTERPOLATED and ASYNCHRONOUS)
                        if col_policy in (ChannelPolicy.INTERPOLATED, ChannelPolicy.ASYNCHRONOUS):
                            last_known_values[col_name] = converted_value
                    except Exception:
                        pass
                elif col_policy in (ChannelPolicy.INTERPOLATED, ChannelPolicy.ASYNCHRONOUS) and col_name in last_known_values:
                    # Forward-fill: use last known value if matching failed
                    row_data[col_name] = last_known_values[col_name]
            
            # Create virtual row
            self.rows.append(VirtualRow(timestamp=current_time, data=row_data))
            last_row_time = current_time
            rows_built += 1
            
            # Advance to next row time
            current_time += row_interval
        
        # Update last built time to the timestamp of the last row built
        # This ensures we don't skip rows in the next incremental build
        if last_row_time is not None:
            self._last_built_time = last_row_time
    
    def prune_rows(self, keep_last_n: int) -> int:
        """Prune old rows from the virtual database to save memory.
        
        This removes rows from the beginning of the list, keeping only
        the most recent N rows. This should only be called after those
        rows have been safely written to disk.
        
        Args:
            keep_last_n: Number of most recent rows to keep
            
        Returns:
            Number of rows pruned
        """
        if not self._built or len(self.rows) <= keep_last_n:
            return 0
        
        rows_to_remove = len(self.rows) - keep_last_n
        # Remove rows from the beginning of the list
        self.rows = self.rows[-keep_last_n:]
        
        return rows_to_remove




def create_tx2_columns(
    io_database: IODatabase,
    reference_channel: str,
) -> List[ColumnDefinition]:
    """Create column definitions for TX2 device.
    
    Args:
        io_database: IODatabase to get channel paths from
        reference_channel: Channel path to use as reference
        
    Returns:
        List of ColumnDefinition objects in CSV order
    """
    from .device_paths import TX2_PATHS
    
    columns = [
        ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
        ColumnDefinition(
            name="Probe A",
            channel_path=TX2_PATHS["adc"]["channel_5"],
            policy=ChannelPolicy.INTERPOLATED,
        ),
        ColumnDefinition(
            name="Probe B",
            channel_path=TX2_PATHS["adc"]["channel_1"],
            policy=ChannelPolicy.INTERPOLATED,
        ),
        ColumnDefinition(
            name="FR2",
            channel_path=TX2_PATHS["adc"]["fr2"],
            policy=ChannelPolicy.INTERPOLATED,
        ),
        ColumnDefinition(name="Note", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
    ]
    
    return columns
