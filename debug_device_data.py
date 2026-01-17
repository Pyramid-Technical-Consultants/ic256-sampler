"""Debug script to diagnose device data and virtual database issues.

Usage:
    python debug_device_data.py

This script will:
1. Connect to the device
2. Capture a small amount of data
3. Diagnose the IODatabase state
4. Attempt to build the virtual database
5. Report any issues found
"""

import sys
import json
from pathlib import Path
from ic256_sampler.io_database import IODatabase
from ic256_sampler.igx_client import IGXWebsocketClient
from ic256_sampler.device_paths import IC256_45_PATHS
from ic256_sampler.simple_capture import capture_to_database
from ic256_sampler.ic256_model import IC256Model
from ic256_sampler.virtual_database import VirtualDatabase
from ic256_sampler.debug_tools import diagnose_io_database, diagnose_virtual_database_build, print_diagnosis
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
    print("Device Data Debugging Tool")
    print("="*60)
    
    # Load config
    config = load_config()
    ic256_ip = config.get("ic256_45", "10.11.25.67")
    
    # Validate IP
    if not is_valid_ipv4(ic256_ip):
        print(f"❌ Invalid IP address: {ic256_ip}")
        return 1
    
    # Validate device
    print(f"\n🔍 Checking device at {ic256_ip}...")
    if not is_valid_device(ic256_ip, "IC256"):
        print(f"❌ Device at {ic256_ip} is not reachable or not responding")
        return 1
    
    print("✅ Device is reachable")
    
    # Capture a small amount of data
    print(f"\n📡 Capturing 1 second of data...")
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
        
        # Capture data
        io_db = capture_to_database(client, channel_paths, duration=1.0)
        client.close()
        
        print(f"✅ Captured data")
        
    except Exception as e:
        print(f"❌ Error capturing data: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    # Diagnose IODatabase
    print(f"\n🔍 Diagnosing IODatabase...")
    io_diagnosis = diagnose_io_database(io_db)
    print_diagnosis(io_diagnosis)
    
    # Try to build virtual database
    print(f"\n🔍 Attempting to build VirtualDatabase...")
    try:
        sampling_rate = 3000
        virtual_db = VirtualDatabase(io_db, reference_channel, sampling_rate, columns)
        
        # Diagnose before build
        vdb_diagnosis = diagnose_virtual_database_build(virtual_db)
        print_diagnosis(vdb_diagnosis)
        
        # Attempt build with timeout simulation
        import time
        start_time = time.time()
        print(f"\n⏱️  Starting build (this may take a moment)...")
        
        virtual_db.build()
        
        elapsed = time.time() - start_time
        print(f"\n✅ Build completed in {elapsed:.2f} seconds")
        print(f"   Rows built: {len(virtual_db.rows):,}")
        
    except Exception as e:
        print(f"\n❌ Error building VirtualDatabase: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    print("\n" + "="*60)
    print("Debugging complete!")
    print("="*60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
