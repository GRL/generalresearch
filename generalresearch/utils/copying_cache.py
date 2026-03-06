from copy import deepcopy
from functools import wraps
from typing import Callable


def deepcopy_return(fn: Callable) -> Callable:
    """
    Using this as a decorator to decorate lru_cached functions, because if we
        store mutable objects in the cache and then modify them in place,
        it would mutate in the cache, which typically we don't want.

    See also: # https://stackoverflow.com/a/54909677/1991066, which I'm not
        using because it wraps the lru_cache and prevents us from accessing
        the methods on it (like cache_clear()).
    """

    @wraps(fn)
    def wrapper(*args, **kwargs):
        return deepcopy(fn(*args, **kwargs))

    return wrapper
