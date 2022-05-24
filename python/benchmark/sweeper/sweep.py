from execo_engine import ParamSweeper, sweep, HashableDict
from benchmark.data.config import ApplicationParameter, ApplicationParameters, ApplicationParameterConstraint
from benchmark.data.metric import Metric
from benchmark.data.constraint_utils import ConstraintUtil
from typing import List
from pathlib import Path
import shutil, os, random


# https://github.com/lovasoa/execo/blob/master/src/execo_engine/sweep.py
# with tree + heuristic mechanism
# and some monte carlo tree-like properties and features

class Sweeper:

    def __init__(self, application_parameters: ApplicationParameters, train: int, lower: bool = True,
                 remove_workdir: bool = False):
        self.__lower = lower

        # setup workdir
        workdir_path = str(Path("./sweeper_workdir"))
        '''
        Parameter configurations are persisted in the workdir. Remove the workdir if you want to restart the 
        combinations again. See https://github.com/lovasoa/execo/blob/master/src/execo_engine/sweep.py#L197
        '''
        if remove_workdir and os.path.isdir(workdir_path):
            shutil.rmtree(workdir_path)

        # setup parameter_dict
        parameters = application_parameters.parameters
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
        self.__train = train  # maximal number of train before picking one configuration for a parameter
        self.__remaining_train = train
        self.__scores = dict()  # keeping a record of current calculated scores

        # apply constraints
        filtered_after_constraints = sweeps
        constraints = application_parameters.constraints
        if constraints is not None:
            filtered_after_constraints = ConstraintUtil.filter_valid_configs(sweeps, constraints)

        self.__not_scored = filtered_after_constraints

        self.__parameters = self._to_list_of_key(parameters)
        self.__parameter_index = 0
        self.__current_parameter_key = self._get_next_key()

        # the concrete value bindings (configuration) for each parameter, that produce the best metric
        self.__selected = HashableDict()

    @staticmethod
    def _to_list_of_key(parameters: List[ApplicationParameter]):
        parameters.sort(key=lambda ap: ap.priority)
        return list(map(lambda ap: ap.name, parameters))

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

    @staticmethod
    def _start_with(sequence: dict, start: dict):
        for key in start:
            if not key in sequence:
                return False
            if sequence[key] != start[key]:
                return False
        return True

    def _find_best(self, scores: dict, starting_with: dict):
        '''
        scores: dict: dict -> Metric 
        starting_with: dict
        The returned solution must start with starting_with
        '''
        best_config = None
        best_score = None
        for config in scores:
            score = self.get_score(config)
            if self._start_with(config, starting_with):
                if best_config is None:
                    best_config = config
                    best_score = score
                elif self.__lower:
                    if score < best_score:
                        best_config = config
                        best_score = score
                else:
                    if score > best_score:
                        best_config = config
                        best_score = score
        return best_config

    @staticmethod
    def _all_start_with(self, sequences: List[dict], start: dict):
        '''
        return the sequences which starts with start
        '''
        return list(filter(lambda seq: self._start_with(seq, start), sequences))

    def _get_next_key(self):
        '''
        works as an iterator on all ApplicationParameters keys
        '''
        res = self.__parameters[self.__parameter_index]
        self.__parameter_index += 1
        return res

    def get_next(self):
        if len(self.__not_scored) == 0:
            best = self._find_best(self.__scores,
                                   self.__selected)
            self.__selected = best
            return None
        elif self.__remaining_train == 0:
            best = self._find_best(self.__scores,
                                   self.__selected)  # Find best sequence of argument, starting with already selected ones
            self.__selected[self.__current_parameter_key] = best[
                self.__current_parameter_key]  # Add the new config value to the selected ones
            self.__not_scored = self._all_start_with(self, self.__not_scored, self.__selected)
            if len(self.__not_scored) != 0:
                self.__current_parameter_key = self._get_next_key()  # Increase the index of the focused param
                self.__remaining_train = self.__train  # Restart the maximal number of train
                return self.get_next()
        else:
            res = random.choice(self.__not_scored)
            self.__remaining_train = self.__remaining_train - 1
            return res

    def has_next(self):
        return len(self.__not_scored) != 0

    def score(self, config, score):
        if config not in self.__scores:
            self.__scores[config] = list()
        self.__scores[config].append(score)

    def get_score(self, config) -> Metric:
        if config in self.__scores:
            metrics = self.__scores[config]
            length = len(metrics)

            if length == 0:
                raise KeyError(f"No metric was recorded for the config: {config}")

            sum = metrics[0]
            for metric in metrics[1:]:
                sum = sum + metric

            return sum / length
        else:
            raise KeyError(f'The config {config} have not been tested yet.')

    def get_all_scores_by_config(self):
        return self.__scores

    def has_best(self):
        return self.__selected is not None

    def best(self):
        return self.__selected

    def pub_not_scored(self):
        return self.__not_scored

    def __str__(self):
        res = "Parameters fields: " + str(self.__parameters_dict) + "\n"
        res += "Current scored configurations: " + str(self.__scores) + "\n"
        res += "Number of not-scored configurations: " + str(len(self.__not_scored)) + "\n"
        res += "Current best configuration: " + str(self.__selected)
        return res
