from typing import Any, Callable, NamedTuple, Optional, Sequence


class FakeCustomStore(NamedTuple):
    add_func: Optional[Callable[[Any], None]] = None
    update_func: Optional[Callable[[Any], None]] = None
    delete_func: Optional[Callable[[Any], None]] = None
    list_func: Optional[Callable[[], Sequence[Any]]] = None
    list_keys_func: Optional[Callable[[], Sequence[str]]] = None
    get_func: Optional[Callable[[Any], Any]] = None
    get_by_key_func: Optional[Callable[[str], Any]] = None
    replace_func: Optional[Callable[[Sequence[Any], str], None]] = None
    resync_func: Optional[Callable[[], None]] = None

    async def add(self, obj):
        if self.add_func:
            self.add_func(obj)

    async def update(self, obj):
        if self.update_func:
            self.update_func(obj)

    async def delete(self, obj):
        if self.delete_func:
            self.delete_func(obj)

    def list(self):
        return self.list_func() if self.list_func else []

    def list_keys(self):
        return self.list_keys_func() if self.list_keys_func else []

    def get(self, obj):
        return self.get_func(obj) if self.get_func else None

    def get_by_key(self, key):
        return self.get_by_key_func(key) if self.get_by_key_func else None

    async def replace(self, list_, resource_version):
        if self.replace_func:
            self.replace_func(list_, resource_version)

    async def resync(self):
        if self.resync_func:
            self.resync_func()
