import threading
import weakref
from datetime import datetime
from typing import Generic, Dict, List, Optional, Iterator

from .types import K, V, Change, ComputeResult


class Collection(Generic[K, V]):
    def __init__(self, name: str):
        self.name = name
        self._data: Dict[K, V] = {}
        self._cache: Dict[str, ComputeResult[K, V]] = {}
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
            self._notify_observers(change)
            return change

    def delete(self, key: K) -> Change[K, V]:
        with self._lock:
            old_value = self._data.pop(key, None)
            self._last_modified = datetime.now()
            change = Change(key=key, old_value=old_value, new_value=None)
            self._notify_observers(change)
            return change

    def iter_items(self) -> Iterator[tuple[K, V]]:
        with self._lock:
            return iter(self._data.items())

    def add_observer(self, observer: 'Collection[K, V]') -> None:
        with self._lock:
            self._observers.append(weakref.ref(observer))

    def remove_observer(self, observer: 'Collection[K, V]') -> None:
        with self._lock:
            self._observers = [ref for ref in self._observers
                             if ref() is not None and ref() is not observer]

    def _notify_observers(self, change: Change[K, V]) -> None:
        for ref in self._observers[:]:
            observer = ref()
            if observer is not None:
                observer._handle_change(self, change)
            else:
                self._observers.remove(ref)

    def _handle_change(self, source: 'Collection[K, V]', change: Change[K, V]) -> None:
        """Override this in derived classes to handle changes from dependencies"""
        pass

    def invalidate_cache(self) -> None:
        with self._lock:
            self._cache.clear()

    def get_cached_result(self, cache_key: str) -> Optional[ComputeResult[K, V]]:
        with self._lock:
            return self._cache.get(cache_key)

    def set_cached_result(self, cache_key: str, result: ComputeResult[K, V]) -> None:
        with self._lock:
            self._cache[cache_key] = result
