from execo_engine import ParamSweeper, sweep
from pathlib import Path
import shutil, os


# TODO : extend execo_engine.ParamSweeper
# https://github.com/lovasoa/execo/blob/master/src/execo_engine/sweep.py
# with tree + heuristic mechanism
# and some monte carlo tree-like properties and features

class Sweeper:
    def __init__(self, parameters, remove_workdir=False):
        workdir_path = str(Path("sweeper_workdir"))

        '''
        Rarameter configurations are persisted in the workdir. Remove the workdir if you want to restart the 
        combinations again. See https://github.com/lovasoa/execo/blob/master/src/execo_engine/sweep.py#L197
        '''
        if remove_workdir and os.path.isdir(workdir_path):
            shutil.rmtree(workdir_path)

        sweeps = sweep(parameters)
        self.sweeper = ParamSweeper(
            persistence_dir=workdir_path, sweeps=sweeps, save_sweeps=True
        )

    def get_next(self):
        return self.sweeper.get_next()

    def done(self, config):
        self.sweeper.done(config)

    def skip(self, config):
        self.sweeper.skip(config)


if __name__ == '__main__':
    parameters = dict(
        param1=["param1_1", "param1_2"],
        param2=["param2_1", "param2_2"],
        param3=["param3_1", "param3_2"],
        param4=["param4_1", "param4_2"]
    )

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
