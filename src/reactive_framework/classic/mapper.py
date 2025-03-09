from typing import Generic, TypeVar, Protocol, Iterator

from ..core.compute_graph import ComputeGraph, ComputedCollection

K1 = TypeVar("K1")
V1 = TypeVar("V1")
K2 = TypeVar("K2")
V2 = TypeVar("V2")


class Mapper(Generic[K1, V1, K2, V2], Protocol):
    def map_element(self, key: K1, value: V1) -> Iterator[tuple[K2, V2]]: ...


class OneToOneMapper(Generic[K1, V1, V2]):
    def map_element(self, key: K1, value: V1) -> Iterator[tuple[K1, V2]]:
        result = self.map_value(value)
        if result is not None:
            yield key, result

    def map_value(self, value: V1) -> V2:
        raise NotImplementedError


class ManyToOneMapper(Generic[K1, V1, V2]):
    def map_element(self, key: K1, values: list[V1]) -> Iterator[tuple[K1, V2]]:
        result = self.map_values(values)
        if result is not None:
            yield key, result

    def map_values(self, values: list[V1]) -> V2:
        raise NotImplementedError


def create_mapped_collection(
    source: ComputedCollection[K1, V1],
    mapper: Mapper[K1, V1, K2, V2],
    compute_graph: ComputeGraph,
    name: str,
) -> ComputedCollection[K2, V2]:
    result = ComputedCollection[K2, V2](name, compute_graph)

    def compute_func() -> dict[K2, V2]:
        new_data: dict[K2, V2] = {}
        for key, value in source.iter_items():
            for mapped_key, mapped_value in mapper.map_element(key, value):
                new_data[mapped_key] = mapped_value
        return new_data

    result.set_compute_func(compute_func)
    compute_graph.add_dependency(result, source)

    return result
