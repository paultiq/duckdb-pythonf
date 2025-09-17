import duckdb
import time
import pytest

import platform
import threading
import _thread as thread


def send_keyboard_interrupt():
    # Wait a little, so we're sure the 'execute' has started
    time.sleep(0.1)
    # Send an interrupt to the main thread
    thread.interrupt_main()


class TestQueryInterruption(object):
    @pytest.mark.xfail(
        condition=platform.system() == "Emscripten",
        reason="Emscripten builds cannot use threads",
    )
    @pytest.mark.timeout(5)
    def test_query_interruption(self):
        con = duckdb.connect()
        thread = threading.Thread(target=send_keyboard_interrupt)
        # Start the thread
        thread.start()
        try:
            with pytest.raises(KeyboardInterrupt):
                res = con.execute('select * from range(100000),range(100000)').fetchall()

        finally:
            # Ensure the thread completes regardless of what happens
            thread.join()
