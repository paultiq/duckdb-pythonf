"""
Test module lifecycle 

Reloading and unload are not expected nor required behaviors -
these tests are to document current behavior so that changes
are visible. 
"""
import sys
import importlib
import pytest
from concurrent.futures import ThreadPoolExecutor


def test_module_reload_safety():
    """Test module reloading scenarios to detect use-after-free issues."""
    import duckdb

    with duckdb.connect(':memory:') as conn1:
        conn1.execute("CREATE TABLE test (id INTEGER)")
        conn1.execute("INSERT INTO test VALUES (1)")
        result1 = conn1.execute("SELECT * FROM test").fetchone()[0]
        assert result1 == 1

        initial_module_id = id(sys.modules['duckdb'])

        # Test importlib.reload() - this does NOT create new module instance in Python
        importlib.reload(duckdb)

        # Verify module instance is the same (expected Python behavior)
        reload_module_id = id(sys.modules['duckdb'])
        assert initial_module_id == reload_module_id, "importlib.reload() should reuse same module instance"

        # Test if old connection still works after importlib.reload()
        try:
            result2 = conn1.execute("SELECT * FROM test").fetchone()[0]
            assert result2 == 1
        except Exception as e:
            pytest.fail(f"Old connection failed after importlib.reload(): {e}")

        # Test new connection after importlib.reload()
        with duckdb.connect(':memory:') as conn2:
            conn2.execute("CREATE TABLE test2 (id INTEGER)")
            conn2.execute("INSERT INTO test2 VALUES (2)")
            result3 = conn2.execute("SELECT * FROM test2").fetchone()[0]
            assert result3 == 2


def test_dynamic_module_loading():
    """Test module loading/unloading cycles."""
    import duckdb

    with duckdb.connect(':memory:') as conn:
        conn.execute("SELECT 1").fetchone()
        
    module_id_1 = id(sys.modules['duckdb'])

    # "Unload" module (not really, just to try it)
    if 'duckdb' in sys.modules:
        del sys.modules['duckdb']

    # Remove from local namespace
    if 'duckdb' in locals():
        del duckdb

    # Verify module is unloaded
    assert 'duckdb' not in sys.modules, "Module not properly unloaded"

    # import (load) module
    import duckdb
    module_id_2 = id(sys.modules['duckdb'])

    # Verify we have a new module instance
    assert module_id_1 != module_id_2, "Module not actually reloaded"

    # Test functionality after reload
    with duckdb.connect(':memory:') as conn:
        conn.execute("CREATE TABLE test (id INTEGER)")
        conn.execute("INSERT INTO test VALUES (42)")
        result = conn.execute("SELECT * FROM test").fetchone()[0]
        assert result == 42


def test_complete_module_unload_with_live_connections():
    """Test the dangerous scenario: complete module unload with live connections."""

    import duckdb
    conn1 = duckdb.connect(':memory:')
    conn1.execute("CREATE TABLE danger_test (id INTEGER)")
    conn1.execute("INSERT INTO danger_test VALUES (123)")

    module_id_1 = id(sys.modules['duckdb'])

    if 'duckdb' in sys.modules:
        del sys.modules['duckdb']
    del duckdb

    # TODO: Rethink this behavior - the module is unloaded, but we 
    # didn't invalidate all the connections and state... so even after
    # unload, conn1 works. 

    result = conn1.execute("SELECT * FROM danger_test").fetchone()[0]
    assert result == 123

    # Reimport creates new module state, but static cache should be reset
    import duckdb
    module_id_2 = id(sys.modules['duckdb'])
    assert module_id_1 != module_id_2, "Should have different module instances"

    conn2 = duckdb.connect(':memory:')
    conn2.execute("CREATE TABLE safe_test (id INTEGER)")
    conn2.execute("INSERT INTO safe_test VALUES (456)")
    result2 = conn2.execute("SELECT * FROM safe_test").fetchone()[0]
    assert result2 == 456

    conn2.close()
    try:
        conn1.close()
    except:
        pass


def test_concurrent_module_access():

    import duckdb

    def worker(thread_id):
        with duckdb.connect(':memory:') as conn:
            conn.execute(f"CREATE TABLE test_{thread_id} (id INTEGER)")
            conn.execute(f"INSERT INTO test_{thread_id} VALUES ({thread_id})")
            result = conn.execute(f"SELECT * FROM test_{thread_id}").fetchone()[0]
            conn.close()
            return True

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(worker, i) for i in range(5)]
        results = [f.result() for f in futures]

    assert all(results)


def test_import_cache_consistency():
    """Test that import cache remains consistent across module operations."""

    import duckdb
    import pandas as pd

    conn = duckdb.connect(':memory:')

    df = pd.DataFrame({'a': [1, 2, 3]})

    conn.register('test_df', df)
    result = conn.execute("SELECT COUNT(*) FROM test_df").fetchone()[0]
    assert result == 3

    conn.close()


def test_module_state_memory_safety():
    """Test memory safety of module state access patterns."""

    import duckdb

    connections = []
    for i in range(10):
        conn = duckdb.connect(':memory:')
        conn.execute(f"CREATE TABLE test_{i} (id INTEGER)")
        conn.execute(f"INSERT INTO test_{i} VALUES ({i})")
        connections.append(conn)

    import gc
    gc.collect()

    for i, conn in enumerate(connections):
        try:
            result = conn.execute(f"SELECT * FROM test_{i}").fetchone()[0]
            assert result == i
        except Exception as e:
            pytest.fail(f"Connection {i} failed after GC: {e}")

    for conn in connections:
        conn.close()


def test_static_cache_stress():
    """Stress test static cache with rapid module state access."""

    import duckdb

    def rapid_access_worker(iterations):
        """Rapidly access module state."""
        results = []
        for i in range(iterations):
            conn = duckdb.connect(':memory:')
            conn.execute("SELECT 1").fetchone()
            conn.close()
            results.append(True)
        
        assert len(results) == iterations
        return True

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(rapid_access_worker, 50) for _ in range(3)]
        all_results = [f.result() for f in futures]

    assert all(all_results)