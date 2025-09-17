import duckdb
import duckdb.typing
import pytest
from conftest import NumpyPandas, ArrowPandas

pa = pytest.importorskip("pyarrow")


def is_dunder_method(method_name: str) -> bool:
    if len(method_name) < 4:
        return False
    if method_name.startswith('_pybind11'):
        return True
    return method_name[:2] == '__' and method_name[:-3:-1] == '__'


@pytest.fixture(scope="session")
def tmp_database(tmp_path_factory):
    database = tmp_path_factory.mktemp("databases", numbered=True) / "tmp.duckdb"
    return database


# This file contains tests for DuckDBPyConnection methods,
# wrapped by the 'duckdb' module, to execute with the 'default_connection'
class TestDuckDBConnection(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_append(self, pandas, duckdb_cursor):
        duckdb_cursor.execute("Create table integers (i integer)")
        df_in = pandas.DataFrame(
            {
                'numbers': [1, 2, 3, 4, 5],
            }
        )
        duckdb_cursor.append('integers', df_in)
        assert duckdb_cursor.execute('select count(*) from integers').fetchone()[0] == 5
        # cleanup
        duckdb_cursor.execute("drop table integers")

    # Not thread safe because it creates a table in the default connection
    @pytest.mark.thread_unsafe
    def test_default_connection_from_connect(self, duckdb_cursor):
        duckdb.sql('create or replace table connect_default_connect (i integer)')
        con = duckdb.connect(':default:')
        con.sql('select i from connect_default_connect')
        duckdb.sql('drop table connect_default_connect')
        with pytest.raises(duckdb.Error):
            con.sql('select i from connect_default_connect')

        # not allowed with additional options
        with pytest.raises(
            duckdb.InvalidInputException, match='Default connection fetching is only allowed without additional options'
        ):
            con = duckdb.connect(':default:', read_only=True)

    def test_arrow(self):
        pyarrow = pytest.importorskip("pyarrow")
        duckdb.execute("select [1,2,3]")
        result = duckdb.fetch_arrow_table()

    def test_begin_commit(self, duckdb_cursor):
        duckdb_cursor.begin()
        duckdb_cursor.execute("create table tbl as select 1")
        duckdb_cursor.commit()
        res = duckdb_cursor.table("tbl")
        duckdb_cursor.execute("drop table tbl")

    def test_begin_rollback(self, duckdb_cursor):
        duckdb_cursor.begin()
        duckdb_cursor.execute("create table tbl as select 1")
        duckdb_cursor.rollback()
        with pytest.raises(duckdb.CatalogException):
            # Table does not exist
            res = duckdb_cursor.table("tbl")


    # Not thread safe because it creates a table in the default connection
    @pytest.mark.thread_unsafe
    def test_cursor(self):
        duckdb.execute("create table tbl as select 3")
        duckdb_cursor = duckdb.cursor()
        res = duckdb_cursor.table("tbl").fetchall()
        assert res == [(3,)]
        duckdb_cursor.execute("drop table tbl")
        with pytest.raises(duckdb.CatalogException):
            # 'tbl' no longer exists
            duckdb.table("tbl")

    def test_cursor_lifetime(self):
        con = duckdb.connect()

        def use_cursors():
            cursors = []
            for _ in range(10):
                cursors.append(con.cursor())

            for cursor in cursors:
                print("closing cursor")
                cursor.close()

        use_cursors()
        con.close()

    @pytest.mark.thread_unsafe
    def test_df(self):
        ref = [([1, 2, 3],)]
        duckdb.execute("select [1,2,3]")
        res_df = duckdb.fetch_df()
        res = duckdb.query("select * from res_df").fetchall()
        assert res == ref

    def test_duplicate(self, duckdb_cursor):
        duckdb_cursor.execute("create table tbl as select 5")
        dup_conn = duckdb_cursor.duplicate()
        dup_conn.table("tbl").fetchall()
        duckdb_cursor.execute("drop table tbl")
        with pytest.raises(duckdb.CatalogException):
            dup_conn.table("tbl").fetchall()

    def test_readonly_properties(self):
        duckdb.execute("select 42")
        description = duckdb.description()
        rowcount = duckdb.rowcount()
        assert description == [('42', 'INTEGER', None, None, None, None, None)]
        assert rowcount == -1

    def test_execute(self):
        assert [([4, 2],)] == duckdb.execute("select [4,2]").fetchall()

    def test_executemany(self, duckdb_cursor):
        # executemany does not keep an open result set
        # TODO: shouldn't we also have a version that executes a query multiple times with different parameters, returning all of the results?
        duckdb_cursor.execute("create table tbl (i integer, j varchar)")
        duckdb_cursor.executemany("insert into tbl VALUES (?, ?)", [(5, 'test'), (2, 'duck'), (42, 'quack')])
        res = duckdb_cursor.table("tbl").fetchall()
        assert res == [(5, 'test'), (2, 'duck'), (42, 'quack')]
        duckdb_cursor.execute("drop table tbl")

    def test_pystatement(self, duckdb_cursor):
        with pytest.raises(duckdb.ParserException, match='seledct'):
            statements = duckdb_cursor.extract_statements('seledct 42; select 21')

        statements = duckdb_cursor.extract_statements('select $1; select 21')
        assert len(statements) == 2
        assert statements[0].query == 'select $1'
        assert statements[0].type == duckdb.StatementType.SELECT
        assert statements[0].named_parameters == set('1')
        assert statements[0].expected_result_type == [duckdb.ExpectedResultType.QUERY_RESULT]

        assert statements[1].query == ' select 21'
        assert statements[1].type == duckdb.StatementType.SELECT
        assert statements[1].named_parameters == set()

        with pytest.raises(
            duckdb.InvalidInputException,
            match='Please provide either a DuckDBPyStatement or a string representing the query',
        ):
            rel = duckdb_cursor.query(statements)

        with pytest.raises(duckdb.BinderException, match="This type of statement can't be prepared!"):
            rel = duckdb_cursor.query(statements[0])

        assert duckdb_cursor.query(statements[1]).fetchall() == [(21,)]
        assert duckdb_cursor.execute(statements[1]).fetchall() == [(21,)]

        with pytest.raises(
            duckdb.InvalidInputException,
            match='Values were not provided for the following prepared statement parameters: 1',
        ):
            duckdb_cursor.execute(statements[0])
        assert duckdb_cursor.execute(statements[0], {'1': 42}).fetchall() == [(42,)]

        duckdb_cursor.execute("create table tbl(a integer)")
        statements = duckdb_cursor.extract_statements('insert into tbl select $1')
        assert statements[0].expected_result_type == [
            duckdb.ExpectedResultType.CHANGED_ROWS,
            duckdb.ExpectedResultType.QUERY_RESULT,
        ]
        with pytest.raises(
            duckdb.InvalidInputException, match='executemany requires a non-empty list of parameter sets to be provided'
        ):
            duckdb_cursor.executemany(statements[0])
        duckdb_cursor.executemany(statements[0], [(21,), (22,), (23,)])
        assert duckdb_cursor.table('tbl').fetchall() == [(21,), (22,), (23,)]
        duckdb_cursor.execute("drop table tbl")

    def test_fetch_arrow_table(self, duckdb_cursor):
        # Needed for 'fetch_arrow_table'
        pyarrow = pytest.importorskip("pyarrow")

        duckdb_cursor.execute("Create Table test (a integer)")

        for i in range(1024):
            for j in range(2):
                duckdb_cursor.execute("Insert Into test values ('" + str(i) + "')")
        duckdb_cursor.execute("Insert Into test values ('5000')")
        duckdb_cursor.execute("Insert Into test values ('6000')")
        sql = '''
        SELECT  a, COUNT(*) AS repetitions
        FROM    test
        GROUP BY a
        '''

        result_df = duckdb_cursor.execute(sql).df()

        arrow_table = duckdb_cursor.execute(sql).fetch_arrow_table()

        arrow_df = arrow_table.to_pandas()
        assert result_df['repetitions'].sum() == arrow_df['repetitions'].sum()
        duckdb_cursor.execute("drop table test")

    @pytest.mark.thread_unsafe
    def test_fetch_df(self):
        ref = [([1, 2, 3],)]
        duckdb.execute("select [1,2,3]")
        res_df = duckdb.fetch_df()
        res = duckdb.query("select * from res_df").fetchall()
        assert res == ref

    def test_fetch_df_chunk(self, duckdb_cursor):
        duckdb_cursor.execute("CREATE table t as select range a from range(3000);")
        query = duckdb_cursor.execute("SELECT a FROM t")
        cur_chunk = query.fetch_df_chunk()
        assert cur_chunk['a'][0] == 0
        assert len(cur_chunk) == 2048
        cur_chunk = query.fetch_df_chunk()
        assert cur_chunk['a'][0] == 2048
        assert len(cur_chunk) == 952
        duckdb_cursor.execute("DROP TABLE t")

    def test_fetch_record_batch(self, duckdb_cursor):
        # Needed for 'fetch_arrow_table'
        pyarrow = pytest.importorskip("pyarrow")

        duckdb_cursor.execute("CREATE table t as select range a from range(3000);")
        duckdb_cursor.execute("SELECT a FROM t")
        record_batch_reader = duckdb_cursor.fetch_record_batch(1024)
        chunk = record_batch_reader.read_all()
        assert len(chunk) == 3000

    def test_fetchall(self):
        assert [([1, 2, 3],)] == duckdb.execute("select [1,2,3]").fetchall()

    @pytest.mark.thread_unsafe
    def test_fetchdf(self):
        ref = [([1, 2, 3],)]
        duckdb.execute("select [1,2,3]")
        res_df = duckdb.fetchdf()
        res = duckdb.query("select * from res_df").fetchall()
        assert res == ref

    def test_fetchmany(self):
        assert [(0,), (1,)] == duckdb.execute("select * from range(5)").fetchmany(2)

    def test_fetchnumpy(self):
        numpy = pytest.importorskip("numpy")
        duckdb.execute("SELECT BLOB 'hello'")
        results = duckdb.fetchall()
        assert results[0][0] == b'hello'

        duckdb.execute("SELECT BLOB 'hello' AS a")
        results = duckdb.fetchnumpy()
        assert results['a'] == numpy.array([b'hello'], dtype=object)

    def test_fetchone(self):
        assert (0,) == duckdb.execute("select * from range(5)").fetchone()

    def test_from_arrow(self):
        assert None != duckdb.from_arrow

    def test_from_csv_auto(self):
        assert None != duckdb.from_csv_auto

    def test_from_df(self):
        assert None != duckdb.from_df

    def test_from_parquet(self):
        assert None != duckdb.from_parquet

    def test_from_query(self):
        assert None != duckdb.from_query

    def test_get_table_names(self):
        assert None != duckdb.get_table_names

    def test_install_extension(self):
        assert None != duckdb.install_extension

    def test_load_extension(self):
        assert None != duckdb.load_extension

    def test_query(self):
        assert [(3,)] == duckdb.query("select 3").fetchall()

    def test_register(self):
        assert None != duckdb.register

    def test_register_relation(self, duckdb_cursor):
        rel = duckdb_cursor.sql('select [5,4,3]')
        duckdb_cursor.register("relation", rel)

        duckdb_cursor.sql("create table tbl as select * from relation")
        assert duckdb_cursor.table('tbl').fetchall() == [([5, 4, 3],)]

    def test_unregister_problematic_behavior(self, duckdb_cursor):
        # We have a VIEW called 'vw' in the Catalog
        duckdb_cursor.execute("create temporary view vw as from range(100)")
        assert duckdb_cursor.execute("select * from vw").fetchone() == (0,)

        # Create a registered object called 'vw'
        arrow_result = duckdb_cursor.execute("select 42").fetch_arrow_table()
        with pytest.raises(duckdb.CatalogException, match='View with name "vw" already exists'):
            duckdb_cursor.register('vw', arrow_result)

        # Temporary views take precedence over registered objects
        assert duckdb_cursor.execute("select * from vw").fetchone() == (0,)

        # Decide that we're done with this registered object..
        duckdb_cursor.unregister('vw')

        # This should not have affected the existing view:
        assert duckdb_cursor.execute("select * from vw").fetchone() == (0,)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_relation_out_of_scope(self, pandas, duckdb_cursor):
        def temporary_scope():
            # Create a connection, we will return this
            # Create a dataframe
            df = pandas.DataFrame({'a': [1, 2, 3]})
            # The dataframe has to be registered as well
            # making sure it does not go out of scope
            duckdb_cursor.register("df", df)
            rel = duckdb_cursor.sql('select * from df')
            duckdb_cursor.register("relation", rel)
            return duckdb_cursor

        duckdb_cursor = temporary_scope()
        res = duckdb_cursor.sql('select * from relation').fetchall()
        print(res)

    def test_table(self, duckdb_cursor):
        duckdb_cursor.execute("create table tbl as select 1")
        assert [(1,)] == duckdb_cursor.table("tbl").fetchall()

    def test_table_function(self):
        assert None != duckdb.table_function

    def test_unregister(self):
        assert None != duckdb.unregister

    def test_values(self):
        assert None != duckdb.values

    def test_view(self, duckdb_cursor):
        duckdb_cursor.execute("create view vw as select range(5)")
        assert [([0, 1, 2, 3, 4],)] == duckdb_cursor.view("vw").fetchall()
        duckdb_cursor.execute("drop view vw")

    def test_close(self, duckdb_cursor):
        assert None != duckdb_cursor.close

    def test_interrupt(self, duckdb_cursor):
        assert None != duckdb_cursor.interrupt

    def test_wrap_shadowing(self):
        pd = NumpyPandas()
        import duckdb

        df = pd.DataFrame({"a": [1, 2, 3]})
        res = duckdb.sql("from df").fetchall()
        assert res == [(1,), (2,), (3,)]

    def test_wrap_coverage(self):
        con = duckdb.default_connection

        # Skip all of the initial __xxxx__ methods
        connection_methods = dir(con)
        filtered_methods = [method for method in connection_methods if not is_dunder_method(method)]
        for method in filtered_methods:
            # Assert that every method of DuckDBPyConnection is wrapped by the 'duckdb' module
            assert method in dir(duckdb)

    def test_connect_with_path(self, tmp_database):
        import pathlib

        assert isinstance(tmp_database, pathlib.Path)
        con = duckdb.connect(tmp_database)
        assert con.sql("select 42").fetchall() == [(42,)]

        with pytest.raises(
            duckdb.InvalidInputException, match="Please provide either a str or a pathlib.Path, not <class 'int'>"
        ):
            con = duckdb.connect(5)

    def test_set_pandas_analyze_sample_size(self):
        con = duckdb.connect(":memory:named", config={"pandas_analyze_sample": 0})
        res = con.sql("select current_setting('pandas_analyze_sample')").fetchone()
        assert res == (0,)

        # Find the cached config
        con2 = duckdb.connect(":memory:named", config={"pandas_analyze_sample": 0})
        con2.execute(f"SET GLOBAL pandas_analyze_sample=2")

        # This change is reflected in 'con' because the instance was cached
        res = con.sql("select current_setting('pandas_analyze_sample')").fetchone()
        assert res == (2,)
