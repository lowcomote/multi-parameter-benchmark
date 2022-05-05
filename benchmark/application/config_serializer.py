from abc import ABC, abstractmethod
from execo_engine import HashableDict


class ApplicationConfigSerializer(ABC):
    def __init__(self, config: HashableDict):
        self.config = config

    @abstractmethod
    def serialize(self) -> str:
        pass


class DefaultConfigSerializer(ApplicationConfigSerializer):
    def serialize(self) -> str:
        cli_arguments = ["-{0} \"{1}\"".format(key, value) for key, value in self.config.items()]
        return " ".join(cli_arguments)