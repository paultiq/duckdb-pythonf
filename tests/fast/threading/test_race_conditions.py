import concurrent.futures
import gc
import random
import threading
import time
import weakref

import duckdb



def test_module_state_race(num_threads_testing):
    barrier = threading.Barrier(num_threads_testing)

    def worker(thread_id):
        barrier.wait()

        for i in range(30): 
            with duckdb.connect(":memory:") as conn:
                conn.execute("SELECT 1")
                int_type = duckdb.type("INTEGER")
                assert int_type is not None, f"Thread {thread_id}: type creation failed"

            if i % 10 == 0:
                time.sleep(0.0001)

        return True

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads_testing) as executor:
        futures = [executor.submit(worker, i) for i in range(num_threads_testing)]
        results = [future.result() for future in futures]

    assert all(results)


def test_connection_instance_cache_race(tmp_path, num_threads_testing):
    num_threads = num_threads_testing
    barrier = threading.Barrier(num_threads)

    def worker(thread_id):
        barrier.wait()

        for i in range(10):
            db_path = tmp_path / f"race_test_t{thread_id}_i{i}.db"
            with duckdb.connect(str(db_path)) as conn:
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS thread_{thread_id}_data_{i} (x INTEGER)"
                )
                conn.execute(
                    f"INSERT INTO thread_{thread_id}_data_{i} VALUES ({thread_id}), ({i})"
                )

                time.sleep(random.uniform(0.0001, 0.001))

                result = conn.execute(
                    f"SELECT COUNT(*) FROM thread_{thread_id}_data_{i}"
                ).fetchone()[0]
                assert result == 2, (
                    f"Thread {thread_id}, iteration {i}: expected 2 rows, got {result}"
                )

        return True

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, i) for i in range(num_threads)]
        results = [future.result() for future in futures]

    assert all(results)


def test_cleanup_race():
    num_threads = 20

    def worker(thread_id):
        weak_refs = []

        for i in range(50):
            conn = duckdb.connect(":memory:")
            weak_refs.append(weakref.ref(conn))
            try:
                conn.execute("CREATE TABLE test (x INTEGER)")
                conn.execute("INSERT INTO test VALUES (1), (2), (3)")
            finally:
                conn.close()
                conn = None

            if i % 3 == 0:
                with duckdb.connect(":memory:") as new_conn:
                    new_conn.execute("SELECT 1")

            if i % 10 == 0:
                gc.collect()
                time.sleep(random.uniform(0.0001, 0.0005))

        gc.collect()
        time.sleep(0.1)
        gc.collect()

        alive_refs = [ref for ref in weak_refs if ref() is not None]
        if len(alive_refs) > 10: 
            assert False, (
                f"Thread {thread_id}: {len(alive_refs)} connections still alive (expected < 10)"
            )
        return True

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, i) for i in range(num_threads)]
        results = [future.result() for future in futures]

    assert all(results), "Some threads failed"


def test_default_connection_race():
    num_threads = 25
    barrier = threading.Barrier(num_threads)

    def worker(thread_id):
        barrier.wait()

        for i in range(30):
            with duckdb.connect() as conn1:
                r1 = conn1.execute("SELECT 1").fetchone()[0]
                assert r1 == 1, f"Thread {thread_id}: expected 1, got {r1}"

            with duckdb.connect(":memory:") as conn2:
                r2 = conn2.execute("SELECT 2").fetchone()[0]
                assert r2 == 2, f"Thread {thread_id}: expected 2, got {r2}"

            with duckdb.connect("") as conn3:
                r3 = conn3.execute("SELECT 3").fetchone()[0]
                assert r3 == 3, f"Thread {thread_id}: expected 3, got {r3}"

            time.sleep(0.0001)

        return True

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, i) for i in range(num_threads)]
        results = [future.result() for future in futures]

    assert all(results)


def test_type_system_race():
    num_threads = 20
    barrier = threading.Barrier(num_threads)

    def worker(thread_id):
        barrier.wait()

        for i in range(100):
            types = [
                duckdb.type("INTEGER"),
                duckdb.type("VARCHAR"),
                duckdb.type("DOUBLE"),
                duckdb.type("BOOLEAN"),
                duckdb.list_type(duckdb.type("INTEGER")),
                duckdb.struct_type(
                    {"a": duckdb.type("INTEGER"), "b": duckdb.type("VARCHAR")}
                ),
            ]

            for t in types:
                assert t is not None, f"Thread {thread_id}: type creation failed"

            if i % 5 == 0:
                with duckdb.connect(":memory:") as conn:
                    conn.execute(
                        "CREATE TABLE test (a INTEGER, b VARCHAR, c DOUBLE, d BOOLEAN)"
                    )

        return True

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, i) for i in range(num_threads)]
        results = [future.result() for future in futures]

    assert all(results)


def test_import_cache_race():
    num_threads = 15

    def worker(thread_id):
        for i in range(50):
            with duckdb.connect(":memory:") as conn:
                try:
                    conn.execute(
                        "CREATE TABLE test AS SELECT range as x FROM range(10)"
                    )
                    result = conn.fetchdf()
                    assert len(result) > 0, f"Thread {thread_id}: fetchdf failed"
                except:
                    pass

                try:
                    result = conn.execute(
                        "SELECT range as x FROM range(5)"
                    ).fetchnumpy()
                    assert len(result["x"]) == 5, (
                        f"Thread {thread_id}: fetchnumpy failed"
                    )
                except:
                    pass

                try:
                    conn.execute("DROP TABLE test")
                except:
                    pass

                time.sleep(0.0001)

        return True

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, i) for i in range(num_threads)]
        results = [future.result() for future in futures]

    assert all(results)
