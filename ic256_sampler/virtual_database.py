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
        self._consecutive_build_failures: int = 0
        
        # Performance optimization: cache reference channel data
        self._ref_channel_cache: Optional[ChannelData] = None
        
        # Track channels that have been warned about large snapshots to reduce log spam
        self._snapshot_warning_counts: Dict[str, int] = {}
    
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
    
    def _find_reference_timestamp(
        self,
        ref_snapshot: List[DataPoint],
        ref_elapsed_times: List[float],
        current_time: float,
        ref_tolerance: float,
        ref_search_idx: int,
    ) -> Tuple[Optional[int], int]:
        """Find reference timestamp for synchronized channels.
        
        Args:
            ref_snapshot: Reference channel snapshot
            ref_elapsed_times: Pre-computed elapsed times for reference channel
            current_time: Current row time
            ref_tolerance: Tolerance for matching
            ref_search_idx: Starting search index (for incremental search)
            
        Returns:
            Tuple of (ref_timestamp_ns, updated_search_idx)
        """
        if not ref_snapshot or not ref_elapsed_times:
            return None, ref_search_idx
        
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
            ref_timestamp_ns = ref_snapshot[best_idx].timestamp_ns
            return ref_timestamp_ns, best_idx
        
        # No match found, advance search index to avoid rechecking
        return None, max(0, idx - 1)
    
    def _create_channel_snapshots(
        self,
        max_snapshot_size: int = 100000,
        snapshot_start_time: Optional[float] = None,
    ) -> Tuple[Dict[str, List[DataPoint]], Dict[str, List[float]], Dict[str, List[int]], List[DataPoint], List[float]]:
        """Create snapshots for all channels and pre-compute elapsed times and timestamps.
        
        Args:
            max_snapshot_size: Maximum number of points per channel snapshot
            snapshot_start_time: Optional start time for incremental snapshots (filters points before this time)
            
        Returns:
            Tuple of (channel_snapshots, channel_elapsed_times, channel_timestamps, ref_snapshot, ref_elapsed_times)
        """
        channel_snapshots: Dict[str, List[DataPoint]] = {}
        channel_elapsed_times: Dict[str, List[float]] = {}
        channel_timestamps: Dict[str, List[int]] = {}
        
        for col_def in self.columns:
            if col_def.channel_path and col_def.channel_path not in channel_snapshots:
                channel_data = self.io_database.get_channel(col_def.channel_path)
                if channel_data and channel_data.count > 0:
                    # Create snapshot with size limit
                    if channel_data.count > max_snapshot_size:
                        snapshot = list(channel_data.data_points)[-max_snapshot_size:]
                        # Only warn periodically to reduce log spam (every 50 rebuilds or first time)
                        warning_count = self._snapshot_warning_counts.get(col_def.channel_path, 0)
                        if warning_count == 0 or warning_count % 50 == 0:
                            self._log(
                                f"Channel {col_def.channel_path} has {channel_data.count} points, "
                                f"limiting snapshot to {max_snapshot_size} most recent points",
                                "WARNING"
                            )
                        self._snapshot_warning_counts[col_def.channel_path] = warning_count + 1
                    else:
                        snapshot = list(channel_data.data_points)
                        # Reset warning count if channel is back to normal size
                        if col_def.channel_path in self._snapshot_warning_counts:
                            del self._snapshot_warning_counts[col_def.channel_path]
                    
                    # Filter by time if needed (for incremental rebuild)
                    if snapshot_start_time is not None and channel_data.count > 1000:
                        filtered_snapshot = [
                            p for p in snapshot
                            if p.elapsed_time >= snapshot_start_time
                        ]
                        if len(filtered_snapshot) >= 10:  # Only use filtered if we have enough points
                            snapshot = filtered_snapshot[:max_snapshot_size]
                    
                    channel_snapshots[col_def.channel_path] = snapshot
                    channel_elapsed_times[col_def.channel_path] = [p.elapsed_time for p in snapshot]
                    if col_def.policy == ChannelPolicy.SYNCHRONIZED:
                        channel_timestamps[col_def.channel_path] = [p.timestamp_ns for p in snapshot]
        
        # Snapshot reference channel
        ref_channel = self._ref_channel_cache
        if not ref_channel:
            return channel_snapshots, channel_elapsed_times, channel_timestamps, [], []
        
        if ref_channel.count > max_snapshot_size:
            ref_snapshot = list(ref_channel.data_points)[-max_snapshot_size:]
            # Only warn periodically to reduce log spam (every 50 rebuilds or first time)
            ref_warning_key = "__REFERENCE_CHANNEL__"
            warning_count = self._snapshot_warning_counts.get(ref_warning_key, 0)
            if warning_count == 0 or warning_count % 50 == 0:
                self._log(
                    f"Reference channel has {ref_channel.count} points, "
                    f"limiting snapshot to {max_snapshot_size} most recent points",
                    "WARNING"
                )
            self._snapshot_warning_counts[ref_warning_key] = warning_count + 1
        else:
            ref_snapshot = list(ref_channel.data_points)
            # Reset warning count if reference channel is back to normal size
            ref_warning_key = "__REFERENCE_CHANNEL__"
            if ref_warning_key in self._snapshot_warning_counts:
                del self._snapshot_warning_counts[ref_warning_key]
        
        # Filter reference channel by time if needed
        if snapshot_start_time is not None and ref_channel.count > 1000:
            filtered_snapshot = [
                p for p in ref_snapshot
                if p.elapsed_time >= snapshot_start_time
            ]
            if len(filtered_snapshot) >= 10:
                ref_snapshot = filtered_snapshot[:max_snapshot_size]
        
        ref_elapsed_times = [p.elapsed_time for p in ref_snapshot]
        
        return channel_snapshots, channel_elapsed_times, channel_timestamps, ref_snapshot, ref_elapsed_times
    
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
        
        Returns:
            True if all channels that exist in IODatabase have at least one data point, False otherwise.
        """
        # Check reference channel first (required)
        ref_channel = self.io_database.get_channel(self.reference_channel)
        if not ref_channel or ref_channel.count == 0:
            return False
        
        # Check all other column channels that exist in IODatabase
        for col_def in self.columns:
            if col_def.channel_path and col_def.channel_path != self.reference_channel:
                channel = self.io_database.get_channel(col_def.channel_path)
                # Only check channels that exist in IODatabase
                # If channel doesn't exist, skip it (allows optional channels)
                if channel and channel.count == 0:
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
        
        # Clear reference channel cache for fresh build
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
        if not ref_channel.data_points:
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
        
        # CRITICAL: Check for corrupted elapsed_time values
        # If elapsed_time is way too large (looks like an absolute timestamp in seconds, > 1e9),
        # it means timestamps are corrupted or elapsed_time was incorrectly calculated
        # Recalculate elapsed_time based on actual timestamps
        # Note: first_elapsed should typically be 0.0 or a small positive value
        # Values > 1e9 seconds (~31 years) are definitely wrong and look like absolute timestamps
        if time_span > 86400 or abs(first_elapsed) > 1e9:  # More than 24 hours span, or first_elapsed looks like absolute timestamp
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
        # Safety check: ensure sampling_rate is valid before division
        if self.sampling_rate <= 0:
            self._log(
                f"Invalid sampling_rate: {self.sampling_rate} (must be > 0)",
                "ERROR"
            )
            return
        
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
        max_snapshot_size = 100000  # Cap at 100k points per channel
        channel_snapshots, channel_elapsed_times, channel_timestamps, ref_snapshot, ref_elapsed_times = \
            self._create_channel_snapshots(max_snapshot_size)
        
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
            ref_timestamp_ns, ref_search_idx = self._find_reference_timestamp(
                ref_snapshot, ref_elapsed_times, current_time, ref_tolerance, ref_search_idx
            )
            
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
                        
                        # Check if we have points on both sides (for interpolation)
                        has_before = before_idx >= 0
                        has_after = after_idx < len(snapshot)
                        
                        # Use strict tolerance for single-side matching, but allow interpolation
                        # when we have points on both sides even if slightly outside tolerance
                        before_valid = has_before and abs(elapsed_times[before_idx] - current_time) <= interp_tolerance
                        after_valid = has_after and abs(elapsed_times[after_idx] - current_time) <= interp_tolerance
                        
                        # If we have points on both sides, allow interpolation even if outside strict tolerance
                        # This handles cases where points are slightly further apart than the tolerance
                        if has_before and has_after:
                            before_point = snapshot[before_idx]
                            after_point = snapshot[after_idx]
                            t1, v1 = before_point.elapsed_time, before_point.value
                            t2, v2 = after_point.elapsed_time, after_point.value
                            
                            # Use a more lenient tolerance for interpolation (5x row_interval)
                            lenient_tolerance = row_interval * 5.0
                            if abs(t1 - current_time) <= lenient_tolerance and abs(t2 - current_time) <= lenient_tolerance:
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
        """Get a row closest to a target elapsed time.
        
        Uses binary search for efficient lookup since rows are sorted by timestamp.
        """
        if not self._built:
            self.build()
        
        if not self.rows:
            return None
        
        # Use binary search since rows are sorted by timestamp
        timestamps = [row.timestamp for row in self.rows]
        idx = bisect.bisect_left(timestamps, target_elapsed)
        
        # Check the row at idx and idx-1 (the two closest rows)
        closest = None
        min_diff = float('inf')
        
        for i in [idx, idx - 1]:
            if 0 <= i < len(self.rows):
                diff = abs(self.rows[i].timestamp - target_elapsed)
                if diff < min_diff and diff <= tolerance:
                    min_diff = diff
                    closest = self.rows[i]
        
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
        self._ref_channel_cache = None
        self._snapshot_warning_counts.clear()
    
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
        # If elapsed_time is way too large (looks like an absolute timestamp in seconds, > 1e9),
        # recalculate from timestamps
        # Values > 1e9 seconds (~31 years) are definitely wrong and look like absolute timestamps
        if abs(last_elapsed) > 1e9:  # last_elapsed looks like absolute timestamp
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
        
        # Pre-compute snapshots for incremental rebuild
        # OPTIMIZATION: Only create snapshots for data we actually need
        # For incremental rebuild, we only need data from start_time onwards
        # This avoids creating expensive full snapshots when deques are large
        max_snapshot_size = 100000
        time_margin = row_interval * 100  # 100 row intervals of margin for interpolation
        snapshot_start_time = max(0.0, start_time - time_margin) if self._last_built_time is not None else None
        
        channel_snapshots, channel_elapsed_times, channel_timestamps, ref_snapshot, ref_elapsed_times = \
            self._create_channel_snapshots(max_snapshot_size, snapshot_start_time)
        
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
            ref_timestamp_ns, ref_search_idx = self._find_reference_timestamp(
                ref_snapshot, ref_elapsed_times, current_time, ref_tolerance, ref_search_idx
            )
            
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
                        
                        # Check if we have points on both sides (for interpolation)
                        has_before = before_idx >= 0
                        has_after = after_idx < len(snapshot)
                        
                        # Use strict tolerance for single-side matching, but allow interpolation
                        # when we have points on both sides even if slightly outside tolerance
                        before_valid = has_before and abs(elapsed_times[before_idx] - current_time) <= interp_tolerance
                        after_valid = has_after and abs(elapsed_times[after_idx] - current_time) <= interp_tolerance
                        
                        # If we have points on both sides, allow interpolation even if outside strict tolerance
                        # This handles cases where points are slightly further apart than the tolerance
                        if has_before and has_after:
                            before_point = snapshot[before_idx]
                            after_point = snapshot[after_idx]
                            t1, v1 = before_point.elapsed_time, before_point.value
                            t2, v2 = after_point.elapsed_time, after_point.value
                            
                            # Use a more lenient tolerance for interpolation (5x row_interval)
                            lenient_tolerance = row_interval * 5.0
                            if abs(t1 - current_time) <= lenient_tolerance and abs(t2 - current_time) <= lenient_tolerance:
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
