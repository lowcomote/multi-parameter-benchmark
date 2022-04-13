from marshmallow_dataclass import dataclass


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
class Configuration:
    cluster_config: ClusterConfig
    benchmark_config: BenchmarkConfig
