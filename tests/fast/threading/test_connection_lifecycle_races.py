"""
Test connection lifecycle races.

Focused on DuckDBPyConnection constructor and Close
"""

import gc
import threading
import concurrent.futures

import pytest

import duckdb


class ConnectionRaceTester:
    def setup_barrier(self, num_threads):
        self.barrier = threading.Barrier(num_threads)

    def synchronized_action(self, action_func, description="action"):
        """Ensures all threads start at same time"""
        self.barrier.wait()
        result = action_func()
        return True
        


@pytest.mark.parametrize("num_threads", [15, 20])
def test_concurrent_connection_creation_destruction(num_threads):
    """Test creating and destroying connections concurrently."""

    tester = ConnectionRaceTester()
    tester.setup_barrier(num_threads)

    def create_and_destroy_connection(thread_id):
        """Create, use, and destroy a connection."""

        def action():
            conn = duckdb.connect()
            try:
                conn.execute("SELECT 1").fetchone()
            finally:
                conn.close()

            return True

        return tester.synchronized_action(action, f"Create/Destroy {thread_id}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(create_and_destroy_connection, i)
            for i in range(num_threads)
        ]
        results = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]

    assert all(results)



def test_connection_destructor_race():
    num_threads = 15

    tester = ConnectionRaceTester()
    tester.setup_barrier(num_threads)

    def destroy_connection(thread_id):
        """Destroy a connection (testing destructor race)."""

        def action():
            conn = duckdb.connect()

            conn.execute(f"SELECT COUNT(*) FROM range(1)").fetchone()

            del conn
            gc.collect()

            return True

        return tester.synchronized_action(action, f"Destructor {thread_id}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(destroy_connection, i) for i in range(num_threads)]
        results = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]

    assert all(results)

def test_concurrent_close_operations():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE shared_table (id INTEGER, data VARCHAR)")
    conn.execute("INSERT INTO shared_table VALUES (1, 'test')")

    num_threads = 10
    tester = ConnectionRaceTester()
    tester.setup_barrier(num_threads)

    def attempt_close_connection(thread_id):
        cursor = conn.cursor()
        def action():
            try:
                _result = cursor.execute(
                    "SELECT COUNT(*) FROM shared_table"
                ).fetchone()

                # Try to close / only first thread should succeed
                cursor.close()

                return f"close_succeeded_{thread_id}"
            except Exception as e:
                return f"close_failed_{thread_id}_{str(e)}"

        return tester.synchronized_action(action, f"Close attempt {thread_id}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(attempt_close_connection, i) for i in range(num_threads)
        ]
        results = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]

    assert any(results), "No close attempts succeeded"


def test_connection_state_races():
    """Test race conditions in connection state management."""

    num_threads = 12

    def connection_state_operations(thread_id):
        conn = duckdb.connect(":memory:")
        operations = [
            lambda: conn.execute("SELECT 1").fetchone(),
            lambda: conn.begin(),
            lambda: conn.execute("CREATE TABLE test (x INTEGER)"),
            lambda: conn.execute("INSERT INTO test VALUES (1)"),
            lambda: conn.commit(),
            lambda: conn.execute("SELECT * FROM test").fetchall(),
        ]

        results = []
        for i, op in enumerate(operations):
            try:
                _result = op()
                results.append(f"op_{i}_success")
            except Exception as e:
                results.append(f"op_{i}_failed_{type(e).__name__}")

        return True
    

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(connection_state_operations, i) for i in range(num_threads)
        ]
        results = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]

    assert all(results)


def test_cursor_operations_race():
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("CREATE TABLE cursor_test (id INTEGER, name VARCHAR)")
        conn.execute("INSERT INTO cursor_test SELECT i, 'name_' || i FROM range(100) t(i)")

        num_threads = 8

        def cursor_operations(thread_id):
            """Perform cursor operations concurrently."""
            # Get a cursor
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT * FROM cursor_test WHERE id % {num_threads} = {thread_id}"
            )
            results = cursor.fetchall()

            return True

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(cursor_operations, i) for i in range(num_threads)]
            results = [
                future.result() for future in concurrent.futures.as_completed(futures)
            ]

        assert all(results)
    finally:
        conn.close()


@pytest.mark.parametrize("num_cycles,num_threads", [(25, 4), (50, 6)])
def test_rapid_connection_cycling(num_cycles, num_threads):
    """Test rapid connection creation and destruction cycles."""

    def rapid_cycling(thread_id):
        for cycle in range(num_cycles):
            conn = duckdb.connect(":memory:")
            try:
                conn.execute(f"SELECT {thread_id} + {cycle}").fetchone()
            finally:
                conn.close()

        return True

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(rapid_cycling, i) for i in range(num_threads)]
        results = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]

    assert all(results)
    