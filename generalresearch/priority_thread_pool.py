import time
from concurrent.futures.thread import ThreadPoolExecutor, _WorkItem
from queue import PriorityQueue
from uuid import uuid4


class WorkItemPriorityQueue(PriorityQueue):
    """
    Custom Class that overloads get and put so that the priority is handled in
    a way as to not break ThreadPoolExecutor, which expect a regular Queue
    """

    def get(self, block=True, timeout=None):
        """
        Assumes the items are of format: (priority, tie-breaker, item)
        """
        res = super().get(block, timeout)
        assert type(res) == tuple and len(res) == 3
        return res[2]

    def put(self, item, block=True, timeout=None) -> None:
        """
        Assumes `item` is a concurrent.futures.thread._WorkItem (but can also
            be None). If not None, attempts to pull out `priority` from the
            kwargs and use it to build the (priority, tie-breaker, item) tuple,
            which is put on the PriorityQueue.
        """
        if item is None:
            item = (0, uuid4().hex, None)
        elif type(item) == _WorkItem:
            priority = item.kwargs.pop("priority", 0)
            item = (priority, uuid4().hex, item)
        else:
            raise ValueError("unexpected item, type: ", type(item))
        super().put(item, block, timeout)


class PriorityThreadPoolExecutor(ThreadPoolExecutor):
    """
    Set the priority of a job using the kwarg 'priority'. Note, if you are
        attempting to run a function that itself has a kwarg called `priority`,
        this will not work as expected.

    Example usage:
      >> q = PriorityThreadPoolExecutor(max_workers=1)
      >> q.submit(do_nothing, 'high', priority=-1)
    """

    def __init__(self, max_workers=None, thread_name_prefix=""):
        super().__init__(max_workers, thread_name_prefix)
        self._work_queue = WorkItemPriorityQueue()


def do_nothing(input):
    print("working: ", input)
    time.sleep(1)
    print("done: ", input)


if __name__ == "__main__":
    q = PriorityThreadPoolExecutor(max_workers=1)
    q.submit(do_nothing, "low")
    q.submit(do_nothing, "low")
    q.submit(do_nothing, "low")
    q.submit(do_nothing, "high", priority=-1)
    q.submit(do_nothing, "super-high!", priority=-100)
    q.shutdown(wait=False)
