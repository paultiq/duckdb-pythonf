"""
Concurrent access tests for DuckDB Python bindings with free threading support.

These tests verify that the DuckDB Python module can handle concurrent access
from multiple threads safely, testing module state isolation, memory management,
and connection handling under various stress conditions.
"""

import gc
import random
import time
import concurrent.futures
from typing import Tuple

import pytest

import duckdb

@pytest.mark.parametrize("num_threads", [10, 25, 50])
def test_concurrent_connections(num_threads):
    """Test creating many connections concurrently from multiple threads."""

    def create_connection_and_query(thread_id: int) -> Tuple[int, Tuple[int, int]]:
        conn = duckdb.connect(':memory:')
        try:
            result = conn.execute(f"SELECT {thread_id} as thread_id, {thread_id * 2} as doubled").fetchone()
            return (thread_id, result)
        finally:
            conn.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(create_connection_and_query, i) for i in range(num_threads)]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]

    # Verify results are correct
    assert len(results) == num_threads
    for thread_id, result in results:
        expected = (thread_id, thread_id * 2)
        assert result == expected
    

@pytest.mark.parametrize("num_threads,iterations", [(10, 5), (20, 10)])
def test_shared_connection_stress(num_threads, iterations):
    """Test concurrent operations on shared connection using cursors."""

    with duckdb.connect(':memory:') as connection:
        connection.execute("CREATE TABLE stress_test (id INTEGER, thread_id INTEGER, value TEXT)")

        def worker_thread(thread_id: int) -> None:
            cursor = connection.cursor()
            for i in range(iterations):
                cursor.execute(
                    "INSERT INTO stress_test VALUES (?, ?, ?)",
                    [i, thread_id, f"thread_{thread_id}_value_{i}"]
                )
                cursor.execute(
                    "SELECT COUNT(*) FROM stress_test WHERE thread_id = ?",
                    [thread_id]
                ).fetchone()
                time.sleep(random.uniform(0.0001, 0.001))

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker_thread, i) for i in range(num_threads)]
            # Wait for all to complete, will raise if any fail
            for future in concurrent.futures.as_completed(futures):
                future.result()

        total_rows = connection.execute("SELECT COUNT(*) FROM stress_test").fetchone()[0]
        expected_rows = num_threads * iterations
        assert total_rows == expected_rows


def test_module_state_isolation():
    """Test that module state is properly isolated and accessible from all threads."""

    def check_module_state(_thread_id: int) -> dict:
        with duckdb.connect(':memory:'):
            env_info = [
                hasattr(duckdb, '__version__'),
                hasattr(duckdb, '__free_threading__'),
            ]

            # Test default connection functionality (if available)
            try:
                with duckdb.connect() as default_conn:
                    default_conn.execute("SELECT 'default' as type").fetchone()
                    has_default = True
            except Exception:
                has_default = False

            int_type = duckdb.type('INTEGER')
            string_type = duckdb.type('VARCHAR')

            return {
                'env_info': env_info,
                'has_default': has_default,
                'types_work': bool(int_type and string_type),
            }

    num_threads = 30
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(check_module_state, i) for i in range(num_threads)]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]

    # All threads should see the same module state
    assert len(results) == num_threads
    first_result = results[0]
    for result in results[1:]:
        assert result == first_result, f"Inconsistent module state: {result} != {first_result}"

def test_memory_pressure():
    """Test memory management under high pressure with many concurrent operations."""

    def memory_intensive_work(thread_id: int) -> None:
        connections = []

        # Create multiple connections
        for i in range(5):
            conn = duckdb.connect(':memory:')
            connections.append(conn)

            # Create some data
            conn.execute(f"""
                CREATE TABLE data_{i} AS
                SELECT range as id,
                       'thread_{thread_id}_conn_{i}_row_' || range as value
                FROM range(100)
            """)

            # Do some queries
            result = conn.execute(f"SELECT COUNT(*) FROM data_{i}").fetchone()[0]
            assert result == 100

        # Force some GC pressure
        large_data = []
        for _ in range(10):
            large_data.append([random.random() for _ in range(1000)])

        # Clean up connections
        for conn in connections:
            conn.close()

        # Force garbage collection
        del large_data
        gc.collect()

    # Run memory-intensive work across many threads
    num_threads = 25
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(memory_intensive_work, i) for i in range(num_threads)]
        for future in concurrent.futures.as_completed(futures):
            future.result()  # Will raise if any thread failed

@pytest.mark.parametrize("num_threads,connections_per_thread", [(10, 25), (15, 50)])
def test_rapid_connect_disconnect(num_threads, connections_per_thread):
    """Test rapid connection creation and destruction to stress module state."""

    def rapid_connections(_thread_id: int) -> None:
        for i in range(connections_per_thread):
            conn = duckdb.connect(':memory:')
            try:
                result = conn.execute("SELECT 1").fetchone()[0]
                assert result == 1
            finally:
                conn.close()

            # Sometimes force GC to increase pressure
            if i % 10 == 0:
                gc.collect()

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(rapid_connections, i) for i in range(num_threads)]
        for future in concurrent.futures.as_completed(futures):
            future.result()  # Will raise if any thread failed

def test_exception_handling():
    """Test that exceptions in one thread don't affect module state for others."""

    def worker_with_exceptions(_thread_id: int) -> None:
        conn = duckdb.connect(':memory:')
        try:
            # Do some successful operations
            conn.execute("CREATE TABLE test (x INTEGER)")
            conn.execute("INSERT INTO test VALUES (1), (2), (3)")

            # Intentionally cause errors every few operations
            for i in range(10):
                try:
                    if i % 3 == 0:
                        # This should fail
                        conn.execute("SELECT * FROM nonexistent_table")
                    else:
                        # This should succeed
                        result = conn.execute("SELECT COUNT(*) FROM test").fetchone()[0]
                        assert result == 3
                except Exception:
                    # Expected for every 3rd operation when querying nonexistent table
                    pass
        finally:
            conn.close()

    num_threads = 20
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker_with_exceptions, i) for i in range(num_threads)]
        for future in concurrent.futures.as_completed(futures):
            future.result()  # Will raise if any thread failed

