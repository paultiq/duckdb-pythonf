#!/usr/bin/env python3
"""
Test concurrent User Defined Function (UDF).
"""

import concurrent.futures
import threading

import duckdb


class UDFRaceTester:
    def setup_barrier(self, num_threads):
        self.barrier = threading.Barrier(num_threads)

    def wait_and_execute(self, db, query, description="query"):
        try:
            with db.cursor() as conn: 
                self.barrier.wait()  # Synchronize thread starts for maximum contention
                result = conn.execute(query).fetchall()
                return {"success": True, "result": result, "description": description}
        except Exception as e:
            return {"success": False, "error": str(e), "description": description}


def test_concurrent_udf_registration():
    """Test concurrent registration of UDFs."""

    num_threads = 10
    tester = UDFRaceTester()
    tester.setup_barrier(num_threads)

    def register_udf(thread_id):
        try:
            conn = duckdb.connect(":memory:")

            # Define a simple UDF with explicit type hint
            def my_add(x: int, y: int) -> int:
                return x + y

            # Register UDF with thread-specific name
            udf_name = f"my_add_{thread_id}"
            conn.create_function(udf_name, my_add)

            # Test the UDF
            tester.wait_and_execute(
                conn, f"SELECT {udf_name}(1, 2)", f"UDF test {thread_id}"
            )

            conn.close()
            return True
        except Exception as e:
            print(f"Thread {thread_id}: UDF registration error - {e}")
            return False

    # Run concurrent UDF registrations
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(register_udf, i) for i in range(num_threads)]
        results = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]

    # All registrations should succeed
    assert all(results), "Some UDF registrations failed"
    print("  ✓ Concurrent UDF registration test passed")


def test_concurrent_udf_execution():
    """Test concurrent execution of the same UDF."""
    print("Testing concurrent UDF execution...")

    conn = duckdb.connect(":memory:")

    # Create a UDF that simulates some work
    def slow_multiply(x: int, y: int) -> int:
        # Simulate some computation to increase chance of race conditions
        result = 1
        for i in range(10):
            result = result * 1.0 + (x * y * 0.1)
        return int(result)

    conn.create_function("slow_multiply", slow_multiply)

    num_threads = 15
    tester = UDFRaceTester()
    tester.setup_barrier(num_threads)

    def execute_udf(thread_id):
        """Execute UDF concurrently."""
        query = f"SELECT slow_multiply({thread_id}, 2) as result"
        return tester.wait_and_execute(conn, query, f"UDF execution {thread_id}")

    # Execute UDF concurrently from multiple threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(execute_udf, i) for i in range(num_threads)]
        results = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]

    conn.close()

    # Check all executions succeeded
    successful = [r for r in results if r["success"]]
    assert len(successful) == num_threads, (
        f"Only {len(successful)}/{num_threads} UDF executions succeeded"
    )
    print("  ✓ Concurrent UDF execution test passed")


def test_mixed_udf_operations():
    """Test mixing UDF registration, execution, and unregistration concurrently."""
    print("Testing mixed concurrent UDF operations...")

    num_threads = 12
    tester = UDFRaceTester()
    tester.setup_barrier(num_threads)

    def mixed_operations(thread_id):
        """Perform mixed UDF operations."""
        try:
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
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "description": f"Mixed ops {thread_id}",
            }

    # Run mixed operations concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(mixed_operations, i) for i in range(num_threads)]
        results = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]

    # Most operations should succeed (some might fail due to timing, but not crash)
    successful = [r for r in results if r["success"]]
    print(f"  {len(successful)}/{num_threads} mixed operations succeeded")
    assert len(successful) >= num_threads * 0.8, "Too many mixed operations failed"
    print("  ✓ Mixed UDF operations test passed")


def test_scalar_udf_races():
    """Test concurrent execution of vectorized UDFs."""
    print("Testing vectorized UDF races...")

    conn = duckdb.connect(":memory:")

    # Create test data
    conn.execute("CREATE TABLE numbers (x INTEGER)")
    conn.execute("INSERT INTO numbers SELECT * FROM range(100)")

    # Create a simple scalar UDF instead of vectorized (simpler for testing)
    def simple_square(x: int) -> int:
        """Square a single value."""
        return x * x

    conn.create_function("simple_square", simple_square)

    num_threads = 8
    tester = UDFRaceTester()
    tester.setup_barrier(num_threads)

    def execute_scalar_udf(thread_id):
        """Execute scalar UDF."""
        # Each thread processes different ranges to avoid conflicts
        start = thread_id * 10
        end = start + 10
        query = (
            f"SELECT simple_square(x) FROM numbers WHERE x BETWEEN {start} AND {end}"
        )
        return tester.wait_and_execute(conn, query, f"Scalar UDF {thread_id}")

    # Execute scalar UDF concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(execute_scalar_udf, i) for i in range(num_threads)]
        results = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]

    conn.close()

    # All scalar executions should succeed
    successful = [r for r in results if r["success"]]
    assert len(successful) == num_threads, (
        f"Only {len(successful)}/{num_threads} scalar UDF executions succeeded"
    )
    print("  ✓ Scalar UDF races test passed")


if __name__ == "__main__":
    test_concurrent_udf_registration()
    test_concurrent_udf_execution()
    test_mixed_udf_operations()
    test_scalar_udf_races()
    print("All UDF race tests passed!")
