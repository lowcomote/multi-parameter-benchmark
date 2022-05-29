# to type hint a method with the type of the enclosing class: https://stackoverflow.com/a/33533514
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class Metric(ABC):

    @abstractmethod
    def __gt__(self, other: Metric) -> bool:
        '''Return true if self > other'''
        pass

    @abstractmethod
    def __ge__(self, other: Metric) -> bool:
        '''Return true if self >= other'''
        pass

    @abstractmethod
    def __lt__(self, other: Metric) -> bool:
        '''Return true if self < other'''
        pass

    @abstractmethod
    def __le__(self, other: Metric) -> bool:
        '''Return true if self <= other'''
        pass

    @abstractmethod
    def __add__(self, other: Metric) -> Metric:
        pass

    @abstractmethod
    def __iadd__(self, other: Metric):
        pass

    @abstractmethod
    def __truediv__(self, num: int) -> Metric:
        pass

    @abstractmethod
    def __str__(self) -> str:
        pass

    @abstractmethod
    def __repr__(self):
        pass

    @abstractmethod
    def get_value(self):
        pass

    @staticmethod
    def from_string(array_as_string: str):
        array = array_as_string[1:-1].split(",")
        metrics = [LongMetric(element) for element in array]
        number_of_metrics = len(array)
        if number_of_metrics == 3:
            return Tuple3Metric(metrics[0], metrics[1], metrics[2])
        elif number_of_metrics == 2:
            return Tuple2Metric(metrics[0], metrics[1])
        elif number_of_metrics == 1:
            return metrics[0]
        else:
            raise Exception(f"Array contains more than three or zero metrics {metrics}.")


@dataclass
class LongMetric(Metric):

    def __init__(self, value: int):
        super().__init__()
        self._value = int(value)

    def __gt__(self, other: LongMetric) -> bool:
        '''Return true if self > other'''
        return self._value > other._value

    def __ge__(self, other: LongMetric) -> bool:
        '''Return true if self >= other'''
        return self._value >= other._value

    def __lt__(self, other: LongMetric) -> bool:
        '''Return true if self < other'''
        return self._value < other._value

    def __le__(self, other: LongMetric) -> bool:
        '''Return true if self <= other'''
        return self._value <= other._value

    def __add__(self, other: LongMetric) -> LongMetric:
        return LongMetric(self._value + other._value)

    def __iadd__(self, other: LongMetric):
        self._value += other._value
        return self

    def __truediv__(self, num: int) -> LongMetric:
        return LongMetric(round(self._value / num))

    def __str__(self) -> str:
        return f"[{self._value}]"

    def __repr__(self):
        return self.__str__()

    def get_value(self):
        return self._value


@dataclass
class Tuple2Metric(Metric):

    def __init__(self, v1: Metric, v2: Metric):
        super().__init__()
        self._v1 = v1
        self._v2 = v2

    def __gt__(self, other: Tuple2Metric) -> bool:
        '''Return true if self > other'''
        return (self._v1 > other._v1) and (self._v2 > other._v2)

    def __ge__(self, other: Tuple2Metric) -> bool:
        '''Return true if self >= other'''
        return (self._v1 >= other._v1) and (self._v2 >= other._v2)

    def __lt__(self, other: Tuple2Metric) -> bool:
        '''Return true if self < other'''
        return (self._v1 < other._v1) and (self._v2 < other._v2)

    def __le__(self, other: Tuple2Metric) -> bool:
        '''Return true if self <= other'''
        return (self._v1 <= other._v1) and (self._v2 <= other._v2)

    def __add__(self, other: Tuple2Metric) -> Tuple2Metric:
        return Tuple2Metric(self._v1 + other._v1, self._v2 + other._v2)

    def __iadd__(self, other: Tuple2Metric):
        self._v1 += other._v1
        self._v2 += other._v2
        return self

    def __truediv__(self, num: int) -> Tuple2Metric:
        return Tuple2Metric(self._v1 / num, self._v2 / num)

    def __str__(self) -> str:
        return f"[{self._v1.get_value()},{self._v2.get_value()}]"

    def __repr__(self):
        return self.__str__()

    def get_value(self):
        return self


@dataclass
class Tuple3Metric(Metric):

    def __init__(self, v1: Metric, v2: Metric, v3: Metric):
        super().__init__()
        self._v1 = v1
        self._v2 = v2
        self._v3 = v3

    def __gt__(self, other: Tuple3Metric) -> bool:
        '''Return true if self > other'''
        return (self._v1 > other._v1) and (self._v2 > other._v2) and (self._v3 > other._v3)

    def __ge__(self, other: Tuple3Metric) -> bool:
        '''Return true if self >= other'''
        return (self._v1 >= other._v1) and (self._v2 >= other._v2) and (self._v3 >= other._v3)

    def __lt__(self, other: Tuple3Metric) -> bool:
        '''Return true if self < other'''
        return (self._v1 < other._v1) and (self._v2 < other._v2) and (self._v3 < other._v3)

    def __le__(self, other: Tuple3Metric) -> bool:
        '''Return true if self <= other'''
        return (self._v1 <= other._v1) and (self._v2 <= other._v2) and (self._v3 <= other._v3)

    def __add__(self, other: Tuple3Metric) -> Tuple3Metric:
        return Tuple3Metric(self._v1 + other._v1, self._v2 + other._v2, self._v3 + other._v3)

    def __iadd__(self, other: Tuple3Metric):
        self._v1 += other._v1
        self._v2 += other._v2
        self._v3 += other._v3
        return self

    def __truediv__(self, num: int) -> Tuple3Metric:
        return Tuple3Metric(self._v1 / num, self._v2 / num, self._v3 / num)

    def __str__(self) -> str:
        return f"[{self._v1.get_value()},{self._v2.get_value()},{self._v3.get_value()}]"

    def __repr__(self):
        return self.__str__()

    def get_value(self):
        return self
