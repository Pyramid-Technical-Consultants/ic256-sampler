"""Debug script to check channel sizes in IODatabase.

This script connects to a device and checks how many points are accumulating
in each channel to diagnose the 100k+ point warnings.
"""

import sys
import json
from pathlib import Path
from ic256_sampler.io_database import IODatabase
from ic256_sampler.igx_client import IGXWebsocketClient
from ic256_sampler.device_paths import IC256_45_PATHS
from ic256_sampler.simple_capture import capture_to_database
from ic256_sampler.ic256_model import IC256Model
from ic256_sampler.debug_tools import diagnose_io_database, print_diagnosis
from ic256_sampler.utils import is_valid_device, is_valid_ipv4


def load_config():
    """Load device configuration from config.json."""
    project_root = Path(__file__).parent
    config_path = project_root / "config.json"
    
    default_config = {
        "ic256_45": "10.11.25.67",
        "tx2": "10.11.25.202",
        "save_path": str(project_root / "data"),
        "sampling_rate": 3000,
    }
    
    if config_path.exists():
        try:
            with open(config_path) as f:
                config = json.load(f)
                default_config.update(config)
        except (json.JSONDecodeError, IOError):
            pass
    
    return default_config


def main():
    """Main debug function."""
    print("="*60)
    print("Channel Size Debugging Tool")
    print("="*60)
    
    # Load config
    config = load_config()
    ic256_ip = config.get("ic256_45", "10.11.25.67")
    
    # Validate IP
    if not is_valid_ipv4(ic256_ip):
        print(f"ERROR: Invalid IP address: {ic256_ip}")
        return 1
    
    # Validate device
    print(f"\nChecking device at {ic256_ip}...")
    if not is_valid_device(ic256_ip, "IC256"):
        print(f"ERROR: Device at {ic256_ip} is not reachable or not responding")
        return 1
    
    print("OK: Device is reachable")
    
    # Capture data for a short period to see accumulation
    print(f"\nCapturing 5 seconds of data to observe accumulation...")
    try:
        client = IGXWebsocketClient(ic256_ip)
        
        # Set up channels
        reference_channel = IC256_45_PATHS["adc"]["channel_sum"]
        model = IC256Model()
        columns = model.create_columns(reference_channel)
        channel_paths = [
            col_def.channel_path 
            for col_def in columns 
            if col_def.channel_path is not None
        ]
        
        # Add reference channel
        if reference_channel not in channel_paths:
            channel_paths.append(reference_channel)
        
        print(f"Monitoring {len(channel_paths)} channels...")
        
        # Capture data
        io_db = capture_to_database(client, channel_paths, duration=5.0)
        client.close()
        
        print(f"OK: Captured data")
        
    except Exception as e:
        print(f"ERROR: Error capturing data: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    # Diagnose IODatabase
    print(f"\nDiagnosing IODatabase...")
    io_diagnosis = diagnose_io_database(io_db)
    print_diagnosis(io_diagnosis)
    
    # Print detailed channel information
    print("\n" + "="*60)
    print("DETAILED CHANNEL ANALYSIS")
    print("="*60)
    
    stats = io_db.get_statistics()
    all_channels = io_db.get_all_channels()
    
    print(f"\nTotal Channels: {len(all_channels)}")
    print(f"Total Data Points: {stats.get('total_data_points', 0):,}")
    
    # Sort channels by point count
    channel_counts = []
    for channel_path in all_channels:
        channel_data = io_db.get_channel(channel_path)
        if channel_data:
            channel_counts.append((channel_path, channel_data.count))
    
    channel_counts.sort(key=lambda x: x[1], reverse=True)
    
    print(f"\nChannels sorted by point count:")
    print(f"{'Channel Path':<60} {'Points':>12} {'Time Span (s)':>15} {'Rate (Hz)':>12}")
    print("-" * 100)
    
    for channel_path, count in channel_counts:
        channel_data = io_db.get_channel(channel_path)
        if channel_data:
            stats = channel_data.get_statistics()
            time_span = stats.get('time_span', 0.0)
            rate = stats.get('rate', 0.0)
            print(f"{channel_path:<60} {count:>12,} {time_span:>15.3f} {rate:>12.1f}")
    
    # Check for channels exceeding 100k
    print(f"\nChannels exceeding 100,000 points:")
    exceeded = [(path, count) for path, count in channel_counts if count > 100000]
    if exceeded:
        for path, count in exceeded:
            print(f"  {path}: {count:,} points")
    else:
        print("  None")
    
    print("\n" + "="*60)
    print("Debugging complete!")
    print("="*60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
