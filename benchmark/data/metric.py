from abc import ABC, abstractmethod
from dataclasses import dataclass
from pickletools import long1

@dataclass
class Metric(ABC):
    
    @abstractmethod
    def gt(v: Metric) -> bool: pass

    @abstractmethod
    def gteq(v: Metric) -> bool: pass

    @abstractmethod
    def lt(v: Metric) -> bool: pass
    
    @abstractmethod
    def lteq(v: Metric) -> bool: pass

@dataclass
class LongMetric(Metric): 

    def __init__(self, value: long) -> None:
        super().__init__()
        self._value = value    
    
    def gt(v: Metric) -> bool: True # TODO

    def gteq(v: Metric) -> bool: True # TODO

    def lt(v: Metric) -> bool: False # TODO
    
    def lteq(v: Metric) -> bool: False # TODO

@dataclass
class Tuple2Metric(Metric):

    def __init__(self, v1: Metric, v2: Metric) -> None:
        super().__init__()
        self._v1 = v1    
        self._v2 = v2    
    
    def gt(v: Metric) -> bool: True # TODO

    def gteq(v: Metric) -> bool: True # TODO

    def lt(v: Metric) -> bool: False # TODO
    
    def lteq(v: Metric) -> bool: False # TODO


@dataclass
class Tuple3Metric(Metric):

    def __init__(self, v1: Metric, v2: Metric, v3: Metric) -> None:
        super().__init__()
        self._v1 = v1    
        self._v2 = v2    
        self._v3 = v3    
    
    def gt(v: Metric) -> bool: True # TODO

    def gteq(v: Metric) -> bool: True # TODO

    def lt(v: Metric) -> bool: False # TODO
    
    def lteq(v: Metric) -> bool: False # TODO