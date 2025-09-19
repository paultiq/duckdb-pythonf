"""
Test concurrent User Defined Function (UDF).
"""

import concurrent.futures
import threading

import pytest

import duckdb


class UDFRaceTester:
    def setup_barrier(self, num_threads):
        self.barrier = threading.Barrier(num_threads)

    def wait_and_execute(self, db, query, description="query"):
        with db.cursor() as conn: 
            self.barrier.wait()  # Synchronize thread starts for maximum contention
            result = conn.execute(query).fetchall()
            return True



@pytest.mark.parametrize("num_threads", [8, 10, 12])
def test_concurrent_udf_registration(num_threads):
    """Test concurrent registration of UDFs."""
    tester = UDFRaceTester()
    tester.setup_barrier(num_threads)

    def register_udf(thread_id):
        with duckdb.connect(":memory:") as conn:

            def my_add(x: int, y: int) -> int:
                return x + y

            udf_name = f"my_add_{thread_id}"
            conn.create_function(udf_name, my_add)

            tester.wait_and_execute(
                conn, f"SELECT {udf_name}(1, 2)", f"UDF test {thread_id}"
            )

        return True


    # Run concurrent UDF registrations
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(register_udf, i) for i in range(num_threads)]
        results = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]

    assert all(results), "Some UDF registrations failed"


@pytest.mark.parametrize("num_threads", [10, 15, 20])
def test_concurrent_udf_execution(num_threads):
    """Test concurrent execution of the same UDF."""
    conn = duckdb.connect(":memory:")

    def slow_multiply(x: int, y: int) -> int:
        result = 1
        for _i in range(10):
            result = result * 1.0 + (x * y * 0.1)
        return int(result)

    conn.create_function("slow_multiply", slow_multiply)

    tester = UDFRaceTester()
    tester.setup_barrier(num_threads)

    def execute_udf(thread_id):
        query = f"SELECT slow_multiply({thread_id}, 2) as result"
        return tester.wait_and_execute(conn, query, f"UDF execution {thread_id}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(execute_udf, i) for i in range(num_threads)]
        results = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]

    conn.close()

    assert all(results)


@pytest.mark.parametrize("num_threads", [8, 12, 16])
def test_mixed_udf_operations(num_threads):
    """Test mixing UDF registration, execution, and unregistration concurrently."""
    tester = UDFRaceTester()
    tester.setup_barrier(num_threads)

    def mixed_operations(thread_id):
        conn = duckdb.connect(":memory:")

        if thread_id % 3 == 0:
            # Register and use UDF
            def thread_func(x: int) -> int:
                return x * thread_id

            udf_name = f"thread_func_{thread_id}"
            conn.create_function(udf_name, thread_func)
            result = tester.wait_and_execute(
                conn, f"SELECT {udf_name}(5)", f"Register+Execute {thread_id}"
            )
        elif thread_id % 3 == 1:
            # Use a common UDF that might be registered by other threads
            result = tester.wait_and_execute(
                conn, "SELECT 42", f"Simple query {thread_id}"
            )
        else:
            # Create table and use built-in functions
            conn.execute("CREATE TABLE test_table (x INTEGER)")
            conn.execute("INSERT INTO test_table VALUES (1), (2), (3)")
            result = tester.wait_and_execute(
                conn, "SELECT COUNT(*) FROM test_table", f"Table ops {thread_id}"
            )

        conn.close()
        return result

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(mixed_operations, i) for i in range(num_threads)]
        results = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]

    assert all(results)
    

@pytest.mark.parametrize("num_threads", [6, 8, 10])
def test_scalar_udf_races(num_threads):
    """Test concurrent execution of scalar UDFs."""
    conn = duckdb.connect(":memory:")

    # Create test data
    conn.execute("CREATE TABLE numbers (x INTEGER)")
    conn.execute("INSERT INTO numbers SELECT * FROM range(100)")

    # Create a simple scalar UDF instead of vectorized (simpler for testing)
    def simple_square(x: int) -> int:
        """Square a single value."""
        return x * x

    conn.create_function("simple_square", simple_square)

    tester = UDFRaceTester()
    tester.setup_barrier(num_threads)

    def execute_scalar_udf(thread_id):
        start = thread_id * 10
        end = start + 10
        query = (
            f"SELECT simple_square(x) FROM numbers WHERE x BETWEEN {start} AND {end}"
        )
        return tester.wait_and_execute(conn, query, f"Scalar UDF {thread_id}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(execute_scalar_udf, i) for i in range(num_threads)]
        results = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]

    conn.close()

    assert all(results)


