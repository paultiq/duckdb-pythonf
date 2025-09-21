#!/usr/bin/env python3
"""Setup duckdb_packaging module for use with --no-isolation builds."""

import sys
import os
import shutil
from pathlib import Path

def setup_duckdb_packaging():
    """Set up duckdb_packaging in virtual environment site-packages without building main project."""

    # Find the virtual environment site-packages directory
    import site
    venv_site_packages = None
    for path in sys.path:
        if 'site-packages' in path and '.venv' in path:
            venv_site_packages = Path(path)
            break

    if not venv_site_packages:
        # Fallback to constructing the path
        venv_site_packages = Path("/project/.venv/lib/python3.14t/site-packages")

    # Source and destination paths
    source_dir = Path("/project/duckdb_packaging")
    dest_dir = venv_site_packages / "duckdb_packaging"

    print(f"Installing duckdb_packaging from {source_dir} to {dest_dir}")

    # Remove existing installation if present
    if dest_dir.exists():
        print(f"Removing existing installation at {dest_dir}")
        shutil.rmtree(dest_dir)

    # Copy the entire duckdb_packaging directory to site-packages
    shutil.copytree(source_dir, dest_dir)

    print("duckdb_packaging installed to site-packages!")

    # Test import
    try:
        import duckdb_packaging.build_backend
        print("✓ Import test successful: duckdb_packaging.build_backend is available")
        return True
    except ImportError as e:
        print(f"✗ Import test failed: {e}")
        return False

if __name__ == "__main__":
    # Run from a safe directory to avoid triggering project build
    os.chdir("/tmp")
    success = setup_duckdb_packaging()
    sys.exit(0 if success else 1)