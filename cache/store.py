from . import index, thread_safe_store


class StoreKeyError(KeyError):
    def __init__(self, obj):
        self.obj = obj


def new_store(key_func):
    return _Cache(
        thread_safe_store.ThreadSafeStore(index.Indexers(), index.Indices()), key_func
    )


def new_indexer(key_func, indexers):
    return _Cache(
        thread_safe_store.ThreadSafeStore(indexers, index.Indices()), key_func
    )


class _Cache:
    def __init__(self, cache_storage, key_func):
        self._cache_storage = cache_storage
        self._key_func = key_func

    def add(self, obj):
        try:
            key = self._key_func(obj)
        except Exception as e:
            raise StoreKeyError(obj) from e
        self._cache_storage.add(key, obj)

    def update(self, obj):
        try:
            key = self._key_func(obj)
        except Exception as e:
            raise StoreKeyError(obj) from e
        self._cache_storage.update(key, obj)

    def delete(self, obj):
        try:
            key = self._key_func(obj)
        except Exception as e:
            raise StoreKeyError(obj) from e
        self._cache_storage.delete(key, obj)

    def list(self):
        return self._cache_storage.list()

    def list_keys(self):
        return self._cache_storage.list_keys()

    def get(self, obj):
        try:
            key = self._key_func(obj)
        except Exception as e:
            raise StoreKeyError(obj) from e
        return self.get_by_key(key)

    def get_by_key(self, key):
        return self._cache_storage.get(key)

    def replace(self, list_, resource_version):
        items = {}
        for item in list_:
            try:
                key = self._key_func(item)
            except Exception as e:
                raise StoreKeyError(item) from e
            items[key] = item
        self._cache_storage.replace(items, resource_version)

    def resync(self):
        self._cache_storage.resync()

    def get_indexers(self):
        return self._cache_storage.get_indexers()

    def index(self, index_name, obj):
        return self._cache_storage.index(index_name, obj)

    def index_keys(self, index_name, index_key):
        return self._cache_storage.index_keys(index_name, index_key)

    def list_index_func_values(self, index_name):
        return self._cache_storage.list_index_func_values(index_name)

    def by_index(self, index_name, index_key):
        return self._cache_storage.by_index(index_name, index_key)

    def add_indexers(self, new_indexers):
        self._cache_storage.add_indexers(new_indexers)
