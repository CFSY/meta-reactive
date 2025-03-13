from typing import Generic, TypeVar, Iterator, Optional

K1 = TypeVar("K1")
V1 = TypeVar("V1")
K2 = TypeVar("K2")
V2 = TypeVar("V2")


class Mapper(Generic[K1, V1, K2, V2]):
    """Base class for all mappers that transform data from one collection to another."""

    def map_element(self, key: K1, value: V1) -> Iterator[tuple[K2, V2]]:
        """Maps a single key-value pair, potentially producing multiple output pairs."""
        raise NotImplementedError


class OneToOneMapper(Mapper[K1, V1, K1, V2]):
    """Mapper that transforms each value to a new value with the same key."""

    def map_element(self, key: K1, value: V1) -> Iterator[tuple[K1, V2]]:
        result = self.map_value(value)
        if result is not None:
            yield key, result

    def map_value(self, value: V1) -> Optional[V2]:
        """Transform a single value into a new value. Return None to filter out the element."""
        raise NotImplementedError


class ManyToOneMapper(Mapper[K1, V1, K1, V2]):
    """Mapper that transforms a list of values with the same key into a single value."""

    def map_element(self, key: K1, values: list[V1]) -> Iterator[tuple[K1, V2]]:
        result = self.map_values(values)
        if result is not None:
            yield key, result

    def map_values(self, values: list[V1]) -> Optional[V2]:
        """Transform a list of values into a single value. Return None to filter out the element."""
        raise NotImplementedError
