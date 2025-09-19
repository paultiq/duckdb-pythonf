"""
Test concurrent query execution races.

This tests race conditions in query execution paths where GIL is released
during query processing, as identified in pyconnection.cpp.
"""

import random
import threading
import time
import concurrent.futures

import pytest

import duckdb


class QueryRaceTester:
    """Helper class to coordinate query execution race condition tests."""
    
    def setup_barrier(self, num_threads):
        self.barrier = threading.Barrier(num_threads)
        
    def synchronized_execute(self, db, query, description="query"):
        """Wait for all threads to be ready, then execute query."""
        try:
            with db.cursor() as conn: 
                self.barrier.wait()  # Synchronize thread starts for maximum contention
                result = conn.execute(query).fetchall()
                return {"success": True, "result": result, "description": description}
        except Exception as e:
            return {"success": False, "error": str(e), "description": description}


@pytest.mark.parametrize("num_threads", [8, 12])
def test_concurrent_prepare_execute(num_threads):
    """Test concurrent PrepareQuery and ExecuteInternal paths."""

    conn = duckdb.connect(':memory:')
    try:
        conn.execute("CREATE TABLE test_data (id INTEGER, value VARCHAR)")
        conn.execute("INSERT INTO test_data SELECT i, 'value_' || i FROM range(1000) t(i)")

        tester = QueryRaceTester()
        tester.setup_barrier(num_threads)

        def prepare_and_execute(thread_id):
            queries = [
                f"SELECT COUNT(*) FROM test_data WHERE id > {thread_id * 10}",
                f"SELECT value FROM test_data WHERE id = {thread_id + 1}",
                f"SELECT id, value FROM test_data WHERE id BETWEEN {thread_id} AND {thread_id + 10}",
                f"INSERT INTO test_data VALUES ({1000 + thread_id}, 'thread_{thread_id}')",
                f"UPDATE test_data SET value = 'updated_{thread_id}' WHERE id = {thread_id + 500}"
            ]

            query = queries[thread_id % len(queries)]
            return tester.synchronized_execute(conn, query, f"Prepared query {thread_id}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(prepare_and_execute, i) for i in range(num_threads)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        successful = [r for r in results if r["success"]]
        assert len(successful) >= num_threads * 0.9, f"Only {len(successful)}/{num_threads} operations succeeded"
    finally:
        conn.close()


def test_concurrent_pending_query_execution():

    conn = duckdb.connect(':memory:')
    try:
        conn.execute("CREATE TABLE large_data AS SELECT i, i*2 as double_val, 'row_' || i as str_val FROM range(10000) t(i)")

        num_threads = 8
        tester = QueryRaceTester()
        tester.setup_barrier(num_threads)

        def execute_long_query(thread_id):
            queries = [
                "SELECT COUNT(*), AVG(double_val) FROM large_data",
                "SELECT str_val, double_val FROM large_data WHERE i % 100 = 0 ORDER BY double_val",
                f"SELECT * FROM large_data WHERE i BETWEEN {thread_id * 1000} AND {(thread_id + 1) * 1000}",
                "SELECT i, double_val, str_val FROM large_data WHERE double_val > 5000 ORDER BY i DESC",
                f"SELECT COUNT(*) as cnt FROM large_data WHERE str_val LIKE '%{thread_id}%'"
            ]

            query = queries[thread_id % len(queries)]
            return tester.synchronized_execute(conn, query, f"Long query {thread_id}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(execute_long_query, i) for i in range(num_threads)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        successful = [r for r in results if r["success"]]
        assert len(successful) == num_threads, f"Only {len(successful)}/{num_threads} long queries succeeded"
    finally:
        conn.close()


def test_execute_many_race():

    with duckdb.connect() as conn: 
        conn.execute("CREATE TABLE batch_data (id INTEGER, name VARCHAR)")

        num_threads = 10
        iterations = 10
        tester = QueryRaceTester()
        tester.setup_barrier(num_threads)

        def execute_many_batch(thread_id):
            with conn.cursor() as conn2: 
                batch_data = [(thread_id * 100 + i, f'name_{thread_id}_{i}') for i in range(iterations)]
                tester.barrier.wait()
                conn2.executemany("INSERT INTO batch_data VALUES (?, ?)", batch_data)
                result = conn2.execute(f"SELECT COUNT(*) FROM batch_data WHERE name LIKE 'name_{thread_id}_%'").fetchone()

                return True

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(execute_many_batch, i) for i in range(num_threads)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        total_rows = conn.execute("SELECT COUNT(*) FROM batch_data").fetchone()[0]
        assert total_rows == num_threads * iterations
        assert all(results)



def test_query_interruption_race():

    conn = duckdb.connect(':memory:')
    try:
        conn.execute("CREATE TABLE interrupt_test AS SELECT i FROM range(100000) t(i)")

        num_threads = 6

        def run_interruptible_query(thread_id):

            with conn.cursor() as conn2:
                if thread_id % 2 == 0:
                    # Fast query
                    result = conn2.execute("SELECT COUNT(*) FROM interrupt_test").fetchall()
                    return True
                else:
                    # Potentially slower query
                    result = conn2.execute("SELECT i, i*i FROM interrupt_test WHERE i % 1000 = 0 ORDER BY i").fetchall()
                    return True

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(run_interruptible_query, i) for i in range(num_threads)]
            results = [future.result() for future in concurrent.futures.as_completed(futures, timeout=30)]

        assert all(results)
    finally:
        conn.close()


@pytest.mark.parametrize("num_threads", [10, 15, 40])
def test_mixed_query_operations(num_threads):

    def mixed_query_operations(thread_id, db):
            
        queries = [
            f"SELECT COUNT(*) FROM mixed_ops WHERE id > {thread_id * 50}",
            f"INSERT INTO mixed_ops VALUES ({10000 + thread_id}, 'thread_{thread_id}', {thread_id * 2.5})",
            f"UPDATE mixed_ops SET data = 'updated_{thread_id}' WHERE id = {thread_id + 100}",
            "SELECT AVG(num_val), MAX(id) FROM mixed_ops WHERE data LIKE 'initial_%'",
            """
                SELECT m1.id, m1.data, m2.num_val
                FROM mixed_ops m1
                JOIN mixed_ops m2 ON m1.id = m2.id - 1
                LIMIT 10
            """
        ]

        with duckdb.connect(db) as conn2:
            conn2.execute("CREATE TABLE mixed_ops (id INTEGER PRIMARY KEY, data VARCHAR, num_val DOUBLE)")
            conn2.execute("INSERT INTO mixed_ops SELECT i, 'initial_' || i, i * 1.5 FROM range(1000) t(i)")

            query = queries[thread_id % len(queries)]

            conn2.execute(query)
            return True

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(mixed_query_operations, i, f":memory:{i}") for i in range(num_threads)]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]

    assert all(results)