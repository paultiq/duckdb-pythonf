#!/usr/bin/env python3

import duckdb
import pyarrow as pa
from duckdb.functional import PythonTVFType


def test_arrow_small(tmp_path):
    with duckdb.connect(tmp_path / "test.duckdb") as conn:

        def simple_arrow_generator(count: int = 5):
            data = {"x": [*range(count)], "y": [f"name_{i}" for i in range(count)]}
            return pa.table(data)

        conn.create_table_function(
            "simple_arrow",
            simple_arrow_generator,
            schema=[("x", "BIGINT"), ("y", "VARCHAR")],
            type=PythonTVFType.ARROW_TABLE,
        )

        # Test fetchall
        result = conn.execute("SELECT * FROM simple_arrow()").fetchall()
        assert len(result) == 5
        assert result[0] == (0, "name_0")
        assert result[4] == (4, "name_4")

        # Test df
        df = conn.execute("SELECT * FROM simple_arrow()").df()
        assert len(df) == 5
        assert df["x"].tolist() == [0, 1, 2, 3, 4]

        # Test arrow
        arrow_result = conn.execute("SELECT * FROM simple_arrow()").fetch_arrow_table()
        assert len(arrow_result) == 5


def test_arrow_large(tmp_path):
    """Tests a 1M row Arrow table with different data types"""
    with duckdb.connect(tmp_path / "test.duckdb") as conn:
        n = 2048 * 1000

        def large_arrow_generator(count: int = 1_000_000):
            import pyarrow as pa

            data = {
                "id": list(range(n)),
                "value": [i * 2 for i in range(n)],
                "name": [f"row_{i}" for i in range(n)],
            }
            return pa.table(data)

        conn.create_table_function(
            "large_arrow",
            large_arrow_generator,
            schema=[("id", "BIGINT"), ("value", "BIGINT"), ("name", "VARCHAR")],
            type="arrow_table",
        )

        result = conn.execute(
            "SELECT COUNT(*) FROM large_arrow(?)", parameters=(n,)
        ).fetchone()
        assert result[0] == n

        df = conn.sql("SELECT * FROM large_arrow() LIMIT 10").df()
        assert len(df) == 10
        assert df["id"].tolist() == list(range(10))

        arrow_result = conn.execute(
            "SELECT * FROM large_arrow(?)", parameters=(n,)
        ).fetch_arrow_table()
        assert len(arrow_result) == n

        result = conn.sql(
            "SELECT SUM(value) FROM large_arrow(?)", params=(n,)
        ).fetchone()
        expected_sum = sum(i * 2 for i in range(n))
        assert result[0] == expected_sum
