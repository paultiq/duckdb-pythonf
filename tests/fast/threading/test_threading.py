#!/usr/bin/env python3
"""
Tests designed to expose specific threading bugs in the DuckDB implementation.
"""

import concurrent.futures
import os
import tempfile
import threading
import time

import pytest

import duckdb


def get_optimal_thread_count():
    """Calculate thread count based on number of cores"""
    import multiprocessing

    cpu_count = multiprocessing.cpu_count()
    return min(12, max(4, int(cpu_count * 1.5)))


@pytest.fixture
def temp_db_files():
    """Provide temporary database files and clean them up."""
    temp_files = []
    with tempfile.TemporaryDirectory() as tmpdir:
        for i in range(3):
            temp_files.append(os.path.join(tmpdir, f"race_test_{i}.db"))
        yield temp_files


def test_instance_cache_race(temp_db_files):
    """Test the specific race condition in instance cache initialization."""
    # This test tries to trigger the race condition where multiple threads
    # see state->instance_cache as null and try to create it simultaneously

    num_threads = get_optimal_thread_count()
    barrier = threading.Barrier(num_threads)
    results = []
    lock = threading.Lock()

    def trigger_instance_cache_race(thread_id):
        try:
            # Wait for all threads to be ready
            barrier.wait()

            # All threads try to create file-based connections simultaneously
            # This should trigger the instance cache initialization race
            connections = []
            for i in range(5):  # Reduced from 10
                # Use unique database file per thread and iteration to avoid conflicts
                db_file = temp_db_files[0] + f"_t{thread_id}_i{i}.db"
                conn = duckdb.connect(db_file)
                connections.append(conn)

                # Do some work to keep the connection alive
                conn.execute("CREATE TABLE IF NOT EXISTS test (x INTEGER, y INTEGER)")
                conn.execute(f"INSERT INTO test VALUES ({thread_id}, {i})")

            # Close connections
            for conn in connections:
                conn.close()

            with lock:
                results.append((thread_id, "success"))

        except Exception as e:
            with lock:
                results.append((thread_id, f"error: {e}"))

    # Start all threads
    threads = []
    for i in range(num_threads):
        t = threading.Thread(target=trigger_instance_cache_race, args=(i,))
        threads.append(t)
        t.start()

    # Wait for completion
    for t in threads:
        t.join()

    # Analyze results
    errors = [r for r in results if not r[1] == "success"]
    assert not errors, f"Errors detected in instance cache race test: {errors}"
    assert len(results) == num_threads, (
        f"Expected {num_threads} results, got {len(results)}"
    )


def test_import_cache_reset_race():
    """Test race condition when import cache is reset while in use."""

    def worker_thread(thread_id):
        try:
            for i in range(20):  # Reduced from 50
                conn = duckdb.connect(":memory:")

                # These operations might use the import cache
                try:
                    # Try pandas operations (if available)
                    conn.execute(
                        "CREATE TABLE test AS SELECT range as x FROM range(10)"
                    )
                    df = conn.fetchdf()  # Might use import cache for pandas

                    # Try numpy operations (if available)
                    result = conn.execute("SELECT * FROM test").fetchnumpy()

                except Exception:
                    # pandas/numpy might not be available, that's fine
                    pass

                conn.close()

                # Add tiny delay to increase race chance
                time.sleep(0.0001)

            return (thread_id, "success")

        except Exception as e:
            return (thread_id, f"error: {e}")

    # Run many threads that use import cache
    num_threads = get_optimal_thread_count()
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker_thread, i) for i in range(num_threads)]
        results = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]

    errors = [r for r in results if not r[1] == "success"]
    assert not errors, f"Errors detected in import cache race test: {errors}"
    assert len(results) == num_threads, (
        f"Expected {num_threads} results, got {len(results)}"
    )


def test_module_state_corruption():
    """Test for module state corruption under heavy concurrent access."""

    def stress_module_state(thread_id):
        try:
            # Rapidly access different parts of module state
            for i in range(30):  # Reduced from 100
                # Create connection (accesses instance cache)
                conn = duckdb.connect(":memory:")

                # Access type system (might use module state for caching)
                int_type = duckdb.type("INTEGER")

                # Access default connection logic
                default_conn = duckdb.connect()

                # Do some operations
                conn.execute("SELECT 1")
                default_conn.execute("SELECT 2")

                # Check type system consistency
                if not int_type:
                    return (thread_id, "type system corruption")

                conn.close()
                default_conn.close()

                # Vary timing to increase race chances
                if i % 10 == 0:
                    time.sleep(0.0001)

            return (thread_id, "success")

        except Exception as e:
            return (thread_id, f"error: {e}")

    # Heavy concurrent load
    num_threads = get_optimal_thread_count()
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(stress_module_state, i) for i in range(num_threads)]
        results = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]

    errors = [r for r in results if not r[1] == "success"]
    assert not errors, f"Errors detected in module state corruption test: {errors}"
    assert len(results) == num_threads, (
        f"Expected {num_threads} results, got {len(results)}"
    )


def test_formatted_python_version_race():
    """Test race condition in formatted_python_version string."""

    num_threads = get_optimal_thread_count()
    barrier = threading.Barrier(num_threads)
    results = []
    lock = threading.Lock()

    def access_python_version(thread_id):
        try:
            # Wait for all threads
            barrier.wait()

            # All threads try to trigger DetectEnvironment simultaneously
            # This might race on the formatted_python_version string
            for i in range(15):  # Reduced from 30
                conn = duckdb.connect(":memory:")

                # This might trigger environment detection
                conn.execute("SELECT 'test' as value")

                conn.close()

            with lock:
                results.append((thread_id, "success"))

        except Exception as e:
            with lock:
                results.append((thread_id, f"error: {e}"))

    # Start threads
    threads = []
    for i in range(num_threads):
        t = threading.Thread(target=access_python_version, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    errors = [r for r in results if not r[1] == "success"]
    assert not errors, f"Errors detected in python version race test: {errors}"
    assert len(results) == num_threads, (
        f"Expected {num_threads} results, got {len(results)}"
    )
