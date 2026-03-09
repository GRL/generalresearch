import logging
import os
import signal
import time
from collections import defaultdict
from concurrent import futures
from concurrent.futures.process import BrokenProcessPool
from typing import Optional

logger = logging.getLogger()

signal_int_name = defaultdict(
    lambda: "Unknown", {x.value: x.name for x in signal.Signals}
)


class HealingProcessPoolExecutor:
    def __init__(
        self,
        max_workers: Optional[int] = None,
        name: Optional[str] = None,
    ):
        if not name:
            try:
                name = f"Process Pool: {__file__}"
            except NameError:
                name = "Process Pool"
        self._name = name
        self._max_workers = max_workers
        self._pool = futures.ProcessPoolExecutor(max_workers=max_workers)
        self._pool.submit(do_nothing)
        # noinspection PyUnresolvedReferences
        self._processes = self._pool._processes

    def get_qsize(self):
        # noinspection PyUnresolvedReferences
        return len(self._pool._pending_work_items)

    def submit(self, *args, **kwargs):
        try:
            return self._pool.submit(*args, **kwargs)
        except BrokenProcessPool:
            ps = list(self._processes.values())
            exit_codes = [signal_int_name[abs(p.exitcode)] for p in ps]

            msg = f"{self._name} is broken. Restarting executor."
            msg += "\n" + f"exit codes: {exit_codes}"
            logger.warning(msg)

            self._pool.shutdown(wait=True)
            self._pool = futures.ProcessPoolExecutor(max_workers=self._max_workers)
            # Submitting "do_nothing" here is probably not useful anymore.

            # This call happens on the pool's submit so if it is still broken, it will
            #   now raise an exception
            return self._pool.submit(*args, **kwargs)


def do_nothing():
    # We submit this to process pools on init in order to have the needed processes fork
    #   before we load up a lot of stuff in the parent process.
    test_logger = logging.getLogger("test")
    test_logger.setLevel(logging.INFO)
    test_logger.info("doing nothing")
    time.sleep(2)
    test_logger.info("did nothing")


def test():
    pool = HealingProcessPoolExecutor(2, name="test")
    pool.submit(do_nothing)
    time.sleep(0.5)

    # Kill a process in the pool
    pid = list(pool._processes.keys())[0]
    os.kill(pid, signal.SIGKILL)
    time.sleep(0.5)

    # re-schedule a job
    pool.submit(do_nothing)
