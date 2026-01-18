"""Main application entry point for IC256 data collection."""

import os
import sys
import portalocker
import atexit
import tempfile
from .application import Application

LOCK_FILE_NAME: str = "my_app.lock"

lock_file_path = os.path.join(tempfile.gettempdir(), LOCK_FILE_NAME)
lock_file = open(lock_file_path, "w")


def cleanup_lock_file() -> None:
    """Clean up lock file on program exit."""
    try:
        if lock_file and not lock_file.closed:
            portalocker.unlock(lock_file)
            lock_file.close()
    except Exception:
        pass


atexit.register(cleanup_lock_file)

try:
    portalocker.lock(lock_file, portalocker.LOCK_EX | portalocker.LOCK_NB)
except portalocker.LockException:
    print("Another instance is already running.")
    lock_file.close()
    sys.exit(1)


__all__ = ['Application', 'main']


def main() -> None:
    """Main entry point for the IC256 Sampler application."""
    app = Application()
    try:
        app.run()
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        app.cleanup()
        sys.exit(0)
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        app.cleanup()
        sys.exit(1)


if __name__ == "__main__":
    main()
