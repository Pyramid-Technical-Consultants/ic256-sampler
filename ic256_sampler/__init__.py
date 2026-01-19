"""IC256 Sampler - Data collection application for IC256 and TX2 devices.

A modern Python application for collecting and managing data from
IC256 and TX2 measurement devices.
"""


def _get_version() -> str:
    """Get version from package metadata or pyproject.toml.
    
    Tries to read from installed package metadata first (standard way),
    then falls back to reading pyproject.toml directly for development.
    """
    # Try to get version from installed package metadata (standard way)
    try:
        from importlib.metadata import version, PackageNotFoundError
        try:
            return version("ic256-sampler")
        except PackageNotFoundError:
            pass
    except ImportError:
        # Python < 3.8: try importlib_metadata
        try:
            from importlib_metadata import version
            return version("ic256-sampler")
        except (ImportError, Exception):
            pass
    
    # Fallback: read from pyproject.toml for development
    from pathlib import Path
    import re
    
    pyproject_path = Path(__file__).parent.parent.parent / "pyproject.toml"
    if pyproject_path.exists():
        try:
            content = pyproject_path.read_text(encoding="utf-8")
            # Simple regex to extract version (works without TOML parser)
            match = re.search(r'version\s*=\s*"([^"]+)"', content)
            if match:
                return match.group(1)
        except Exception:
            pass
    
    return "0.0.0"  # Fallback if nothing found


__version__ = _get_version()
__author__ = "Pyramid Technical Consultants, Inc."
