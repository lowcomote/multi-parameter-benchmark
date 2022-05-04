from marshmallow_dataclass import dataclass
from typing import List
from enum import Enum


@dataclass
class BenchmarkConfig:
    warmup_rounds: int
    measurement_rounds: int


@dataclass
class ClusterConfig:
    '''
    Cluster configuration parameters needed for G5k.
    '''

    dummy_param: str


@dataclass
class SparkConfig:
    spark_home: str
    java_home: str

@dataclass
class Configuration:
    cluster_config: ClusterConfig
    spark_config: SparkConfig
    benchmark_config: BenchmarkConfig


class ConstraintType(Enum):
    MUST = 1
    FORBIDDEN = 2


@dataclass
class ApplicationParameterConstraint:
    source: str
    target: str
    type: ConstraintType


@dataclass
class ApplicationParameter:
    name: str
    priority: int
    values: List[str]
    constraints: List[ApplicationParameterConstraint]


@dataclass
class ApplicationParameters:
    parameters: List[ApplicationParameter]
