import platform
import threading
import time

import duckdb
import pytest


class TestConnectionInterrupt(object):
    @pytest.mark.xfail(
        condition=platform.system() == "Emscripten",
        reason="threads not allowed on Emscripten",
    )
    def test_connection_interrupt(self):
        conn = duckdb.connect()

        def interrupt():
            # Wait for query to start running before interrupting
            time.sleep(1)
            conn.interrupt()

        thread = threading.Thread(target=interrupt)
        thread.start()
        with pytest.raises(duckdb.InterruptException):
            conn.execute('select * from range(100000) t1,range(100000) t2').fetchall()

        thread.join()

    def test_interrupt_closed_connection(self):
        conn = duckdb.connect()
        conn.close()
        with pytest.raises(duckdb.ConnectionException):
            conn.interrupt()
