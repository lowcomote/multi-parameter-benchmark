# https://github.com/lovasoa/execo/blob/master/src/execo_engine/sweep.py
from execo_engine import sweep, HashableDict
from benchmark.data.config import ApplicationParameter, ApplicationParameters
from benchmark.data.metric import Metric
from benchmark.data.utils import ConstraintUtil, JsonUtil
from typing import List
from pathlib import Path
from dataclasses import dataclass
from abc import ABC
import shutil, os, random


@dataclass
class SweeperState:
    # use lt comparison when searching for the best configuration
    lower: bool

    # maximal number of train before picking one configuration for a parameter
    train: int
    remaining_train: int

    """
    scores :  mapping between a configuration and a list of results 
                (e.g, {config1 -> [2s, 3s, 1.5s], config3 -> [2s, 3s, 1.5s], ...}). 
                Instantiated empty
    """
    scores: dict

    # configurations
    not_scored: List[HashableDict]
    done: List[HashableDict]
    skipped: List[HashableDict]

    parameters: List[str]
    parameter_index: int
    current_parameter_key: str

    # the concrete value bindings (configuration) for each parameter, that produce the best metric
    selected: HashableDict

    def __init__(self, application_parameters: ApplicationParameters, train: int, lower: bool = True):
        # use lt comparison when searching for the best configuration
        self.lower = lower

        # maximal number of train before picking one configuration for a parameter
        self.remaining_train = self.train = train

        """
        __scores :  mapping between a configuration and a list of results 
                    (e.g, {config1 -> [2s, 3s, 1.5s], config3 -> [2s, 3s, 1.5s], ...}). 
                    Instantiated empty
        """
        self.scores = dict()

        # setup parameter_dict
        parameters = application_parameters.parameters
        self.parameters_dict = self._to_parameters_dict(parameters)
        filtered_after_constraints = sweep(self.parameters_dict)

        # apply constraints
        constraints = application_parameters.constraints
        if constraints is not None:
            filtered_after_constraints = ConstraintUtil.filter_valid_configs(filtered_after_constraints, constraints)

        # a list of configuration that have not been scored yet
        self.not_scored = filtered_after_constraints
        self.done = list()
        self.skipped = list()

        # setup parameter names
        self.parameters = self._to_list_of_key(parameters)
        self.parameter_index = 0
        self.current_parameter_key = self.get_next_key()

        # the concrete value bindings (configuration) for each parameter, that produce the best metric
        self.selected = HashableDict()

    def get_next_key(self):
        '''
        works as an iterator on all ApplicationParameters keys
        '''
        res = self.parameters[self.parameter_index]
        self.parameter_index += 1
        return res

    def done(self, config):
        self.not_scored.remove(config)
        self.done.append(config)
        SweeperStatePersistence.persist_state(self)

    def skipped(self, config):
        self.skipped.append(config)
        SweeperStatePersistence.persist_state(self)

    @staticmethod
    def _to_list_of_key(parameters: List[ApplicationParameter]):
        parameters.sort(key=lambda ap: ap.priority)
        return [ap.name for ap in parameters]

    @staticmethod
    def _to_parameters_dict(parameters: List[ApplicationParameter]):
        return {parameter.name: parameter.values for parameter in parameters}


class SweeperStatePersistence(ABC):
    _WORKDIR_PATH = Path("./sweeper_workdir")
    _WORKDIR_PATH_STR = str(_WORKDIR_PATH)

    _STATE_FILE = _WORKDIR_PATH / "sweeper_state.json"
    _STATE_FILE_STR = str(_STATE_FILE)

    @staticmethod
    def remove_workdir():
        if os.path.isdir(SweeperStatePersistence._WORKDIR_PATH_STR):
            shutil.rmtree(SweeperStatePersistence._WORKDIR_PATH_STR)

    @staticmethod
    def create_workdir():
        if not os.path.exists(SweeperStatePersistence._WORKDIR_PATH_STR):
            os.makedirs(SweeperStatePersistence._WORKDIR_PATH_STR)

    @staticmethod
    def load_state() -> SweeperState:
        return JsonUtil.deserialize(SweeperStatePersistence._STATE_FILE_STR, SweeperState)

    @staticmethod
    def persist_state(state: SweeperState):
        SweeperStatePersistence.create_workdir()
        JsonUtil.serialize(SweeperStatePersistence._STATE_FILE_STR, state, SweeperState)

    @staticmethod
    def persisted_sweeper_state_exists() -> bool:
        return os.path.exists(SweeperStatePersistence._STATE_FILE_STR)


class Sweeper:

    def __init__(self, application_parameters: ApplicationParameters, train: int, lower: bool = True,
                 remove_workdir: bool = False):
        if remove_workdir:
            SweeperStatePersistence.remove_workdir()
        elif SweeperStatePersistence.persisted_sweeper_state_exists():
            self.__state = SweeperStatePersistence.load_sweeper_state()
        else:
            self.__state = SweeperState(application_parameters, train, lower)

    def done(self, config):
        self.__state.done(config)

    def skipped(self, config):
        self.__state.skipped(config)

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
            if self._contains_subdictionary(config, starting_with):
                if best_config is None:
                    best_config = config
                    best_score = score
                elif self.__state.lower:
                    if score < best_score:
                        best_config = config
                        best_score = score
                else:
                    if score > best_score:
                        best_config = config
                        best_score = score
        return best_config

    def get_next(self):
        state = self.__state
        if len(state.not_scored) == 0:
            state.selected = self._find_best(state.scores, state.selected)
            return None
        elif state.remaining_train == 0:
            # Find best sequence of argument, starting with already selected ones
            best = self._find_best(state.scores, state.selected)
            # Add the new config value to the selected ones
            state.selected[state.current_parameter_key] = best[state.current_parameter_key]
            state.not_scored = Sweeper._all_start_with(state.not_scored, state.selected)
            if len(state.not_scored) != 0:
                # Increase the index of the focused param
                state.current_parameter_key = state.get_next_key()
                # Restart the maximal number of train
                state.remaining_train = state.train
                return self.get_next()
        else:
            res = random.choice(state.not_scored)
            state.remaining_train -= 1
            return res

    def has_next(self):
        return len(self.__state.not_scored) != 0

    def score(self, config, score):
        if config not in self.__state.scores:
            self.__state.scores[config] = list()
        self.__state.scores[config].append(score)

    def get_score(self, config) -> Metric:
        if config in self.__state.scores:
            metrics = self.__state.scores[config]
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
        return self.__state.scores

    def has_best(self):
        return self.best is not None

    @property
    def best(self):
        return self.__state.selected

    @property
    def skipped(self):
        return self.__state.skipped

    def __str__(self):
        state = self.__state
        res = f"Parameters fields: {state.parameters_dict}\n"
        res += f"Current scored configurations: {state.scores}\n"
        res += f"Number of not-scored configurations: {len(state.not_scored)}\n"
        res += f"Current best configuration: {state.selected}"
        return res

    @staticmethod
    def _contains_subdictionary(this: dict, subdictionary: dict):
        for key, value in subdictionary.items():
            if (key not in this) or (this[key] != value):
                return False
        return True

    @staticmethod
    def _all_start_with(sequences: List[dict], start: dict):
        '''
        return the sequences which starts with start
        '''
        return [seq for seq in sequences if Sweeper._contains_subdictionary(seq, start)]
