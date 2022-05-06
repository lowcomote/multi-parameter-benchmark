from abc import ABC, abstractmethod
from execo_engine import HashableDict


class ApplicationConfigTransformer(ABC):
    def __init__(self, config: HashableDict):
        self.config = config

    @abstractmethod
    def transform(self):
        pass


class ToCliConfigTransformer(ApplicationConfigTransformer):
    def transform(self):
        return self.config.items()


class ToCsvConfigTransformer(ApplicationConfigTransformer):
    def transform(self):
        cli_arguments = ["{0}={1}".format(key, value) for key, value in self.config.items()]
        return ",".join(cli_arguments)
