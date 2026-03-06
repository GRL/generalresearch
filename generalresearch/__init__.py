import threading
import time
from functools import wraps

from decorator import decorator
from wrapt import FunctionWrapper, ObjectProxy


def retry(exceptions, tries=4, delay=0.5, backoff=2, logger=None):
    """
    https://www.calazan.com/retry-decorator-for-python-3/
    Retry calling the decorated function using an exponential backoff.

    Args:
        exceptions: The exception to check. may be a tuple of
            exceptions to check.
        tries: Number of times to try (not retry) before giving up.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier (e.g. value of 2 will double the delay
            each retry).
        logger: Logger to use. If None, print.
    """

    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    msg = "{}, Retrying in {} seconds...".format(e, mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


def synchronized(wrapped):
    # https://wrapt.readthedocs.io/en/latest/examples.html#thread-synchronization

    # Determine if being passed an object which is a synchronization
    # primitive. We can't check by type for Lock, RLock, Semaphore etc,
    # as the means of creating them isn't the type. Therefore use the
    # existence of acquire() and release() methods. This is more
    # extensible anyway as it allows custom synchronization mechanisms.

    if hasattr(wrapped, "acquire") and hasattr(wrapped, "release"):
        # We remember what the original lock is and then return a new
        # decorator which accesses and locks it. When returning the new
        # decorator we wrap it with an object proxy so we can override
        # the context manager methods in case it is being used to wrap
        # synchronized statements with a 'with' statement.

        lock = wrapped

        @decorator
        def _synchronized(wrapped, instance, args, kwargs):
            # Execute the wrapped function while the original supplied
            # lock is held.

            with lock:
                return wrapped(*args, **kwargs)

        class _PartialDecorator(ObjectProxy):

            def __enter__(self):
                lock.acquire()
                return lock

            def __exit__(self, *args):
                lock.release()

        return _PartialDecorator(wrapped=_synchronized)

    # Following only apply when the lock is being created
    # automatically based on the context of what was supplied. In
    # this case we supply a final decorator, but need to use
    # FunctionWrapper directly as we want to derive from it to add
    # context manager methods in case it is being used to wrap
    # synchronized statements with a 'with' statement.

    def _synchronized_lock(context):
        # Attempt to retrieve the lock for the specific context.

        lock = vars(context).get("_synchronized_lock", None)

        if lock is None:
            # There is no existing lock defined for the context we
            # are dealing with so we need to create one. This needs
            # to be done in a way to guarantee there is only one
            # created, even if multiple threads try and create it at
            # the same time. We can't always use the setdefault()
            # method on the __dict__ for the context. This is the
            # case where the context is a class, as __dict__ is
            # actually a dictproxy. What we therefore do is use a
            # meta lock on this wrapper itself, to control the
            # creation and assignment of the lock attribute against
            # the context.

            meta_lock = vars(synchronized).setdefault(
                "_synchronized_meta_lock", threading.Lock()
            )

            with meta_lock:
                # We need to check again for whether the lock we want
                # exists in case two threads were trying to create it
                # at the same time and were competing to create the
                # meta lock.

                lock = vars(context).get("_synchronized_lock", None)

                if lock is None:
                    lock = threading.RLock()
                    setattr(context, "_synchronized_lock", lock)

        return lock

    def _synchronized_wrapper(wrapped, instance, args, kwargs):
        # Execute the wrapped function while the lock for the
        # desired context is held. If instance is None then the
        # wrapped function is used as the context.

        with _synchronized_lock(instance or wrapped):
            return wrapped(*args, **kwargs)

    class _FinalDecorator(FunctionWrapper):

        def __enter__(self):
            self._self_lock = _synchronized_lock(self.__wrapped__)
            self._self_lock.acquire()
            return self._self_lock

        def __exit__(self, *args):
            self._self_lock.release()

    return _FinalDecorator(wrapped=wrapped, wrapper=_synchronized_wrapper)
