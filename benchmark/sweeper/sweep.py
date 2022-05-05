from execo_engine import ParamSweeper, sweep
from data.config import ApplicationParameter
from data.metric import Metric
from typing import List
from pathlib import Path
import shutil, os, random

# TODO : extend execo_engine.ParamSweeper
# https://github.com/lovasoa/execo/blob/master/src/execo_engine/sweep.py
# with tree + heuristic mechanism
# and some monte carlo tree-like properties and features

class Sweeper:
    def __init__(self, parameters: List[ApplicationParameter], train: int, remove_workdir: bool = False):
        # setup workdir
        workdir_path = str(Path("../deploy/sweeper_workdir"))
        '''
        Parameter configurations are persisted in the workdir. Remove the workdir if you want to restart the 
        combinations again. See https://github.com/lovasoa/execo/blob/master/src/execo_engine/sweep.py#L197
        '''
        if remove_workdir and os.path.isdir(workdir_path):
            shutil.rmtree(workdir_path)

        # setup parameter_dict
        self.__parameters_dict = self._to_parameters_dict(parameters)
        sweeps = sweep(self.__parameters_dict)
        self.sweeper = ParamSweeper(
            persistence_dir=workdir_path, sweeps=sweeps, save_sweeps=True
        )
        # setup dict for score
        """
        __scores :  mapping between a configuration and a list of results 
                    (e.g, {config1 -> [2s, 3s, 1.5s], config3 -> [2s, 3s, 1.5s], ...}). 
                    Instantiated empty
        __not_scored : a list of configuration that have not been scored yet
        """
        self.__train = train # maximal number of train before picking one configuration for a parameter
        self.__remaining_train = train
        self.__scores = dict() # keeping a record of current calculated scores
        self.__not_scored = sweeps
        # TODO self.__constraints = self._to_parameters_constraint(parameters)
        self.__current_parameter_index = 0
        self.__selected = list()

    # TODO
    # @staticmethod
    # def _to_parameters_constraint(parameters: List[ApplicationParameter]):
    #     """ return a set of (str, str, str, str, typeofconstraint) ? (Varname1, value1, varname2, value2, type) """"
    #     pass

    @staticmethod
    def _check_constraints(config, constraints) -> bool:
        # TODO
        pass

    @staticmethod
    def _to_parameters_dict(parameters: List[ApplicationParameter]):
        result = dict()
        for parameter in parameters:
            result[parameter.name] = parameter.values
        return result

    def done(self, config):
        self.__not_scored.remove(config)
        self.sweeper.done(config)

    def skip(self, config):
       self.sweeper.skip(config)

    def _find_best(self, scores: dict, starting_with: List[str], lower: bool = True):
        '''
        scores: dict: List[str] -> Metric 
        The returned solution must start with starting_with
        '''
        best_config = None
        best_score = None
        for config in scores:
            score = self.get_score(config)
            if best_config is None:
                best_config = config
                best_score = score
            elif lower:
                if score.lt(best_score):
                    best_config = config
                    best_score = score
            else:
                if score.gt(best_score):
                    best_config = config
                    best_score = score
        return best_config

    @staticmethod
    def _all_start_with(sequences: List[List[str]], start: List[str]):
        '''
        return the sequences which starts with start
        '''
        result = list()
        for sequence in sequences:
            if ''.join(sequence).startswith(''.join(start)): 
                result.append(sequence)
        return result

    def get_next(self):
        if self.__remaining_train == 0: 
            best = self._find_best(self.__scores, self.__selected) # Find best sequence of argument, starting with already selected ones
            self.__selected.append(best(self.__current_parameter_index)) # Add the new config value to the selected ones
            self.__current_parameter_index = self.__current_parameter_index + 1 # Increase the index of the focused param
            self.__not_scored = self._all_start_with(self.__not_scored, self.__selected)
            self.__remaining_train = self.__train # Restart the maximal number of train
            if len(self.__not_scored) != 0:
                return self.get_next()
            else:
                return self.__selected
        else:
            res = random.choice(self.__not_scored)
            self.__remaining_train = self.__remaining_train - 1
            return res

    def score(self, config, score):
        if config not in self.__scores:
            self.__scores[config] = list()
        self.__scores[config].append(score)

    def get_score(self, config) -> Metric:
        if config in self.__scores:
            # Shall we implement an operator overload on the Metric class to make sum and len work? (Instead of raw numbers in an array we store Metric.)
            # return sum(self.__scores[config]) / len(self.__scores[config])
            return self.__scores[config]
        else:
            raise KeyError(f'The config {config} have not been tested yet.')

    def get_all_scores_by_config(self):
        return self.__scores

    def has_best(self):
        return self.__selected is not None

    def best(self):
        # TODO (question from Benedek) Why is self.__selected a List not a single Configuration?
        return self.__selected

# if __name__ == '__main__':
#     def get_test_parameters():
#         parameters_dict = dict(
#             param1=["param1_1", "param1_2"],
#             param2=["param2_1", "param2_2"],
#             param3=["param3_1", "param3_2"],
#             param4=["param4_1", "param4_2"]
#         )
#         return [ApplicationParameter(name=key, priority=1, values=values, constraints=[])
#                 for key, values in parameters_dict.items()]


#     parameters = get_test_parameters()
#     sweeper = Sweeper(parameters=parameters, remove_workdir=False)
#     config = sweeper.get_next()
#     while config:
#         try:
#             p1 = config["param1"]
#             p2 = config["param2"]
#             p3 = config["param3"]
#             p4 = config["param4"]
#             print(f"{p1}; {p2}; {p3}; {p4}")
#             sweeper.done(config)
#         except Exception as e:
#             sweeper.skip(config)
#         finally:
#             config = sweeper.get_next()