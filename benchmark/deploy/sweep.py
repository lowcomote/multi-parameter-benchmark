from execo_engine import ParamSweeper, sweep
from data.config import ApplicationParameter
from typing import List
from pathlib import Path
import shutil, os


# TODO : extend execo_engine.ParamSweeper
# https://github.com/lovasoa/execo/blob/master/src/execo_engine/sweep.py
# with tree + heuristic mechanism
# and some monte carlo tree-like properties and features

class Sweeper:
    def __init__(self, parameters: List[ApplicationParameter], remove_workdir: bool = False):
        # setup workdir
        workdir_path = str(Path("sweeper_workdir"))
        '''
        Parameter configurations are persisted in the workdir. Remove the workdir if you want to restart the 
        combinations again. See https://github.com/lovasoa/execo/blob/master/src/execo_engine/sweep.py#L197
        '''
        if remove_workdir and os.path.isdir(workdir_path):
            shutil.rmtree(workdir_path)

        # setup parameter_dict
        parameters_dict = self._to_parameters_dict(parameters)
        sweeps = sweep(parameters_dict)
        self.sweeper = ParamSweeper(
            persistence_dir=workdir_path, sweeps=sweeps, save_sweeps=True
        )

    @staticmethod
    def _to_parameters_dict(parameters: List[ApplicationParameter]):
        # TODO priorities and constraints are discarded
        result = dict()
        for parameter in parameters:
            result[parameter.name] = parameter.values
        return result

    def get_next(self):
        return self.sweeper.get_next()

    def done(self, config):
        self.sweeper.done(config)

    def skip(self, config):
        self.sweeper.skip(config)


if __name__ == '__main__':
    def get_test_parameters():
        parameters_dict = dict(
            param1=["param1_1", "param1_2"],
            param2=["param2_1", "param2_2"],
            param3=["param3_1", "param3_2"],
            param4=["param4_1", "param4_2"]
        )
        return [ApplicationParameter(name=key, priority=1, values=values, constraints=[])
                for key, values in parameters_dict.items()]


    parameters = get_test_parameters()
    sweeper = Sweeper(parameters=parameters, remove_workdir=False)
    config = sweeper.get_next()
    while config:
        try:
            p1 = config["param1"]
            p2 = config["param2"]
            p3 = config["param3"]
            p4 = config["param4"]
            print(f"{p1}; {p2}; {p3}; {p4}")
            sweeper.done(config)
        except Exception as e:
            sweeper.skip(config)
        finally:
            config = sweeper.get_next()
