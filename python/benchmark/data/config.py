from marshmallow_dataclass import dataclass
from dataclasses import dataclass as python_dataclass, asdict
from typing import List, Optional
from enum import Enum


@dataclass
class BenchmarkConfig:
    warmup_rounds: int
    measurement_rounds: int
    all_in_one_benchmark_results_csv_path: str
    application_metrics_csv_param_name: Optional[str]
    application_metrics_csv_path: Optional[str]


@dataclass
@python_dataclass
class G5kClusterConfig:
    site: str
    cluster: str
    worker: int
    jobname: Optional[str]
    time: Optional[str]
    start: Optional[str]

    def filter_none_fields(self):
        dict_self = asdict(self)
        return {key: value for key, value in dict_self.items() if value is not None}


@dataclass
class SparkConfig:
    spark_home: str
    java_home: str
    application_jar_path: str
    application_classname: str


@dataclass
class Configuration:
    cluster_config: Optional[G5kClusterConfig]
    spark_config: SparkConfig
    benchmark_config: BenchmarkConfig


class ConstraintType(Enum):
    MUST = 1
    FORBIDDEN = 2


@dataclass
class ParameterBinding:
    name: str
    value: str


@dataclass
class ApplicationParameterConstraint:
    source: ParameterBinding
    targets: List[ParameterBinding]


@dataclass
class ApplicationParameter:
    name: str
    priority: int
    values: List[str]


@dataclass
class ApplicationParameters:
    parameters: List[ApplicationParameter]
    constraints: Optional[List[ApplicationParameterConstraint]]
