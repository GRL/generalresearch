from cachetools import TTLCache


class InstrumentedTTLCache(TTLCache):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hits = 0
        self.misses = 0

    def __getitem__(self, key):
        try:
            value = super().__getitem__(key)
            self.hits += 1
            return value
        except KeyError:
            self.misses += 1
            raise

    def cache_info(self):
        return {
            "hits": self.hits,
            "misses": self.misses,
            "currsize": self.currsize,
            "maxsize": self.maxsize,
        }
