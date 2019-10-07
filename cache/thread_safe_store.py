import threading

from . import index as _index


class ThreadSafeStore:
    def __init__(self, indexers, indices):
        self._lock = threading.Lock()
        self._items = {}
        self._indexers = indexers
        self._indices = indices

    def add(self, key, obj):
        with self._lock:
            old_object = self._items.get(key)
            self._items[key] = obj
            self._update_indices(old_object, obj, key)

    def update(self, key, obj):
        with self._lock:
            old_object = self._items.get(key)
            self._items[key] = obj
            self._update_indices(old_object, obj, key)

    def delete(self, key):
        with self._lock:
            try:
                obj = self._items[key]
            except KeyError:
                pass
            else:
                self._delete_from_indices(obj, key)
                del self._items[key]

    def get(self, key):
        with self._lock:
            return self._items.get(key)

    def list(self):
        with self._lock:
            return list(self._items.values())

    def list_keys(self):
        with self._lock:
            return list(self._items)

    def replace(self, items, resource_version):
        with self._lock:
            self._items = items
            self._indices = _index.Indices()
            for key, item in self._items.items():
                self._update_indices(None, item, key)

    def index(self, index_name, obj):
        with self._lock:
            index_func = self._indexers[index_name]
            index_keys = index_func(obj)
            index = self._indices[index_name]
            if len(index_keys) == 1:
                return_key_set = index[index_keys[0]]
            else:
                return_key_set = {
                    key for index_key in index_keys for key in index[index_key]
                }
            return [self._items[absolute_key] for absolute_key in return_key_set]

    def index_keys(self, index_name, index_key):
        with self._lock:
            if index_name not in self._indexers:
                raise KeyError(f"Index with name {index_name} does not exist")
            index = self._indices[index_name]
            set_ = index[index_key]
            return list(set_)

    def list_index_func_values(self, index_name):
        with self._lock:
            index = self._indices[index_name]
            names = list(index)
            return names

    def by_index(self, index_name, index_key):
        with self._lock:
            if index_name not in self._indexers:
                raise KeyError(f"Index with name {index_name} does not exist")
            index = self._indices[index_name]
            set_ = index[index_key]
            return [self._items[key] for key in set_]

    def get_indexers(self):
        return self._indexers

    def add_indexers(self, new_indexers):
        with self._lock:
            if self._items:
                raise RuntimeError("cannot add indexers to running index")
            old_keys = self._indexers.keys()
            new_keys = new_indexers.keys()
            if old_keys | new_keys:
                raise RuntimeError(f"indexer conflict: {old_keys & new_keys}")
            self._indexers.update(new_indexers)

    def resync(self):
        pass

    def _update_indices(self, old_obj, new_obj, key):
        if old_obj is not None:
            self._delete_from_indices(old_obj, key)
        for name, index_func in self._indexers.items():
            index_values = index_func(new_obj)
            index = self._indices.setdefault(name, _index.Index())
            for index_value in index_values:
                set_ = index.setdefault(index_value, set())
                set_.add(key)

    def _delete_from_indices(self, obj, key):
        for name, index_func in self._indexers.items():
            index_values = index_func(obj)
            index = self._indices.get(name)
            if index is None:
                continue
            for index_value in index_values:
                set_ = index.get(index_value)
                if set_:
                    set_.remove(key)
