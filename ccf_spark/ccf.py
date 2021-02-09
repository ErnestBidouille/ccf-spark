from typing import Generator, Iterable
from pyspark.accumulators import Accumulator

LEN_ERROR = ValueError('x len should be equal to 2')


class Ccf:
    class Iterate:
        @staticmethod
        def _map(key: int, value: int) -> tuple:
            return ((key, value), (value, key))

        @staticmethod
        def _reduce(key: int,
                    values: Iterable[int],
                    accumulator: Accumulator = None) -> Generator:
            value_list = []
            min_value = key
            for value in values:
                if value < min_value:
                    min_value = value
                value_list.append(value)
            if (min_value < key):
                yield (key, min_value)
                for value in value_list:
                    if min_value != value:
                        yield (value, min_value)
                        if accumulator:
                            accumulator.add(1)

        @staticmethod
        def map(x: Iterable) -> tuple:
            if len(x) != 2:
                raise LEN_ERROR
            return __class__._map(*x)

        @staticmethod
        def reduce(x: Iterable, accumulator: Accumulator = None) -> Generator:
            if len(x) != 2:
                raise LEN_ERROR
            yield from __class__._reduce(*x, accumulator)

    class IterateSecondarySorting(Iterate):
        @staticmethod
        def _reduce(key: int,
                    values: Iterable[int],
                    accumulator: Accumulator = None) -> Generator:
            min_value = values[0]
            if min_value < key:
                yield (key, min_value)
                for value in values[1:]:
                    yield (value, min_value)
                    if accumulator:
                        accumulator.add(1)

        @staticmethod
        def reduce(x: Iterable, accumulator: Accumulator = None) -> Generator:
            if len(x) != 2:
                raise LEN_ERROR
            yield from __class__._reduce(*x, accumulator)

    class Dedup:
        @staticmethod
        def _map(key: int, value: int) -> tuple:
            return ((key, value), None)

        @staticmethod
        def _reduce(key: Iterable, value: object) -> tuple:
            return (key[0], key[1])

        @staticmethod
        def map(x: Iterable) -> tuple:
            if len(x) != 2:
                raise LEN_ERROR
            return __class__._map(*x)

        @staticmethod
        def reduce(x: Iterable) -> tuple:
            if len(x) != 2:
                raise LEN_ERROR
            return __class__._reduce(*x)
