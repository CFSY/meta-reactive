import threading
import weakref
from datetime import datetime
from typing import Generic, Dict, List, Optional, Iterator

from .types import K, V, Change


class Collection(Generic[K, V]):
    def __init__(self, name: str):
        self.name = name
        self._data: Dict[K, V] = {}
        self._lock = threading.RLock()
        self._observers: List[weakref.ref] = []
        self._last_modified = datetime.now()

    def get(self, key: K) -> Optional[V]:
        with self._lock:
            return self._data.get(key)

    def get_all(self) -> Dict[K, V]:
        with self._lock:
            return self._data.copy()

    def set(self, key: K, value: V) -> Change[K, V]:
        with self._lock:
            old_value = self._data.get(key)
            self._data[key] = value
            self._last_modified = datetime.now()
            change = Change(key=key, old_value=old_value, new_value=value)
            self.handle_change(change)
            return change

    def delete(self, key: K) -> Change[K, V]:
        with self._lock:
            old_value = self._data.pop(key, None)
            self._last_modified = datetime.now()
            change = Change(key=key, old_value=old_value, new_value=None)
            self.handle_change(change)
            return change

    def iter_items(self) -> Iterator[tuple[K, V]]:
        with self._lock:
            return iter(self._data.items())

    def handle_change(self, change: Change[K, V]) -> None:
        """Override this in derived classes to handle changes from dependencies"""
        raise NotImplementedError
