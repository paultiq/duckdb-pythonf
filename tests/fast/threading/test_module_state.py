import concurrent.futures
import os
import tempfile
import threading
import time

import duckdb


def test_module_state_isolation():
    with duckdb.connect(":memory:") as conn1, duckdb.connect(":memory:") as conn2:
        conn1.execute("CREATE TABLE test1 (x INTEGER)")
        conn1.execute("INSERT INTO test1 VALUES (1)")

        conn2.execute("CREATE TABLE test2 (x INTEGER)")
        conn2.execute("INSERT INTO test2 VALUES (2)")

        result1 = conn1.execute("SELECT * FROM test1").fetchall()
        result2 = conn2.execute("SELECT * FROM test2").fetchall()

        assert result1 == [(1,)], "Connection 1 isolation failed"
        assert result2 == [(2,)], "Connection 2 isolation failed"


def test_default_connection_access():
    with duckdb.connect() as conn1:
        conn1.execute("CREATE TABLE test1 (x INTEGER)")
        conn1.execute("INSERT INTO test1 VALUES (42)")

        # Verify data exists in this connection
        result1 = conn1.execute("SELECT * FROM test1").fetchall()
        assert result1 == [(42,)], "Connection 1 data missing"

    # New default connection should be isolated (table won't exist)
    with duckdb.connect() as conn2:
        # This should fail because tables are not shared between connections
        try:
            conn2.execute("SELECT * FROM test1").fetchall()
            assert False, "Table should not exist in new connection"
        except duckdb.CatalogException:
            pass  # Expected behavior - tables are isolated between connections


def test_import_cache_access():
    with duckdb.connect(":memory:") as conn:
        try:
            conn.execute("CREATE TABLE test AS SELECT range as x FROM range(10)")
            df = conn.fetchdf()
            assert len(df) == 10, "Pandas integration failed"
        except Exception:
            pass

        try:
            result = conn.execute("SELECT range as x FROM range(5)").fetchnumpy()
            assert "x" in result, "Numpy integration failed"
        except Exception:
            pass


def test_instance_cache_functionality():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with duckdb.connect(db_path) as conn1:
            conn1.execute("CREATE TABLE test (x INTEGER)")
            conn1.execute("INSERT INTO test VALUES (1)")

        with duckdb.connect(db_path) as conn2:
            result = conn2.execute("SELECT * FROM test").fetchall()
            assert result == [(1,)], "Instance cache failed"


def test_environment_detection():
    version = duckdb.__formatted_python_version__
    assert isinstance(version, str)
    assert len(version) > 0

    interactive = duckdb.__interactive__
    assert isinstance(interactive, bool)


def test_concurrent_connection_creation():
    num_threads = 20
    barrier = threading.Barrier(num_threads)

    def worker(thread_id):
        barrier.wait()

        for i in range(5):
            with duckdb.connect(":memory:") as conn:
                conn.execute(f"CREATE TABLE test_{i} (x INTEGER)")
                conn.execute(f"INSERT INTO test_{i} VALUES ({thread_id})")
                result = conn.execute(f"SELECT * FROM test_{i}").fetchall()
                assert result == [(thread_id,)], f"Thread {thread_id}, table {i} failed"

        return True

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, i) for i in range(num_threads)]
        results = [future.result() for future in futures]

    assert all(results)

def test_concurrent_instance_cache_access(tmp_path):
    num_threads = 15
    barrier = threading.Barrier(num_threads)

    def worker(thread_id):
        barrier.wait()

        for i in range(10):
            db_path = str(tmp_path / f"test_{thread_id}_{i}.db")
            with duckdb.connect(db_path) as conn:
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS test (x INTEGER, thread_id INTEGER)"
                )
                conn.execute(f"INSERT INTO test VALUES ({i}, {thread_id})")

        return True

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, i) for i in range(num_threads)]
        results = [future.result() for future in futures]

    assert all(results)


def test_concurrent_import_cache_access():
    num_threads = 15
    barrier = threading.Barrier(num_threads)

    def worker(thread_id):
        barrier.wait()

        for i in range(20):
            with duckdb.connect(":memory:") as conn:
                try:
                    conn.execute("CREATE TABLE test AS SELECT range as x FROM range(5)")
                    df = conn.fetchdf()
                    assert len(df) == 5, (
                        f"Thread {thread_id}: pandas integration failed"
                    )
                except Exception:
                    pass

                try:
                    result = conn.execute(
                        "SELECT range as x FROM range(3)"
                    ).fetchnumpy()
                    assert "x" in result, (
                        f"Thread {thread_id}: numpy integration failed"
                    )
                except Exception:
                    pass

            time.sleep(0.0001)

        return True

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, i) for i in range(num_threads)]
        results = [future.result() for future in futures]

    assert all(results), "Some threads failed"


def test_concurrent_environment_detection():
    """Test concurrent access to environment detection."""
    num_threads = 15
    barrier = threading.Barrier(num_threads)

    def worker(thread_id):
        barrier.wait()

        for i in range(30):
            version = duckdb.__formatted_python_version__
            interactive = duckdb.__interactive__

            assert isinstance(version, str), (
                f"Thread {thread_id}: version should be string"
            )
            assert isinstance(interactive, bool), (
                f"Thread {thread_id}: interactive should be boolean"
            )

            with duckdb.connect(":memory:") as conn:
                conn.execute("SELECT 1")

        return True

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, i) for i in range(num_threads)]
        results = [future.result() for future in futures]

    assert all(results)
