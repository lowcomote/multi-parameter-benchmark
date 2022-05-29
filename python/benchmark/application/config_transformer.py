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
        cli_arguments = list()
        for key, value in self.config.items():
            cli_prefix_cut = key
            while cli_prefix_cut[0] == "-":
                cli_prefix_cut = cli_prefix_cut[1:]
            cli_arguments.append(f"{cli_prefix_cut}={value}")

        return ",".join(cli_arguments)
