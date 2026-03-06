from generalresearch import retry


class RetryCache:
    # Simple pylibmc.Client wrapper that implements a retry on each method

    def __init__(self, client, tries=4, delay=1, backoff=1.5):
        import pylibmc

        self.client = client
        self.f = retry(pylibmc.Error, tries=tries, delay=delay, backoff=backoff)

    def get(self, key):
        @self.f
        def _get(key):
            return self.client.get(key)

        return _get(key)

    def set(self, key, value, timeout=0):
        @self.f
        def _set(key, value, timeout):
            return self.client.set(key, value, time=timeout)

        return _set(key, value, timeout)

    def delete_multi(self, keys):
        @self.f
        def _delete_multi(keys):
            return self.client.delete_multi(keys)

        return _delete_multi(keys)

    def delete(self, key):
        @self.f
        def _delete(key):
            return self.client.delete(key)

        return _delete(key)


if __name__ == "__main__":
    import pylibmc

    CACHE = RetryCache(pylibmc.Client(["127.0.0.1:11211"], binary=True))
    CACHE.set("foo", "bar")
    print(CACHE.get("foo"))
