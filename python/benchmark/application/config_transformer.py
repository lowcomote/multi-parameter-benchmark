from abc import ABC, abstractmethod
from execo_engine import HashableDict
from benchmark.data.utils import DictUtil


class ApplicationConfigTransformer(ABC):
    def __init__(self, config: HashableDict):
        self.config = config

    @abstractmethod
    def transform(self):
        pass


class ToCliConfigTransformer(ApplicationConfigTransformer):
    def transform(self):
        return DictUtil.clone(self.config)


class ToCsvConfigTransformer(ApplicationConfigTransformer):
    def transform(self):
        cli_arguments = [f"{key}={value}" for key, value in self.config.items()]
        return ",".join(cli_arguments)
