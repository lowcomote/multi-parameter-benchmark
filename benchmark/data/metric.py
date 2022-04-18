# to type hint a method with the type of the enclosing class: https://stackoverflow.com/a/33533514
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class Metric(ABC):

    @abstractmethod
    def gt(self, other: Metric) -> bool:
        pass

    @abstractmethod
    def gteq(self, other: Metric) -> bool:
        pass

    @abstractmethod
    def lt(self, other: Metric) -> bool:
        pass

    @abstractmethod
    def lteq(self, other: Metric) -> bool:
        pass


@dataclass
class LongMetric(Metric):

    def __init__(self, value: int) -> None:
        super().__init__()
        self._value = value

    def gt(self, other: Metric) -> bool:
        return True  # TODO

    def gteq(self, other: Metric) -> bool:
        return True  # TODO

    def lt(self, other: Metric) -> bool:
        return False  # TODO

    def lteq(self, other: Metric) -> bool:
        return False  # TODO


@dataclass
class Tuple2Metric(Metric):

    def __init__(self, v1: Metric, v2: Metric) -> None:
        super().__init__()
        self._v1 = v1
        self._v2 = v2

    def gt(self, other: Metric) -> bool:
        return True  # TODO

    def gteq(self, other: Metric) -> bool:
        return True  # TODO

    def lt(self, other: Metric) -> bool:
        return False  # TODO

    def lteq(self, other: Metric) -> bool:
        return False  # TODO


@dataclass
class Tuple3Metric(Metric):

    def __init__(self, v1: Metric, v2: Metric, v3: Metric) -> None:
        super().__init__()
        self._v1 = v1
        self._v2 = v2
        self._v3 = v3

    def gt(self, other: Metric) -> bool:
        return True  # TODO

    def gteq(self, other: Metric) -> bool:
        return True  # TODO

    def lt(self, other: Metric) -> bool:
        return False  # TODO

    def lteq(self, other: Metric) -> bool:
        return False  # TODO
