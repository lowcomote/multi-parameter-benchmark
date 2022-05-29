# https://github.com/lovasoa/execo/blob/master/src/execo_engine/sweep.py
from execo_engine import sweep, HashableDict
from benchmark.data.config import ApplicationParameter, ApplicationParameters
from benchmark.data.metric import Metric
from benchmark.data.utils import ConstraintUtil, JsonUtil, DictUtil, ListUtil
from marshmallow import fields, Schema, post_load
from typing import List
from pathlib import Path
from abc import ABC
from dataclasses import dataclass
import shutil, os, random


@dataclass
class SweeperState:
    '''
    !!! TODO Should you add any new fields to SweeperState, please extend SweeperStateSchema too!
    '''

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

    # str -> List[int]
    parameters_dict: dict

    # configurations
    remaining_configs: List[HashableDict]
    done_configs: List[HashableDict]
    skipped_configs: List[HashableDict]

    parameters: List[str]
    parameter_index: int
    current_parameter_key: str

    # the concrete value bindings (configuration) for each parameter, that produce the best metric
    selected: HashableDict

    def __init__(self, **kwargs):
        if len(kwargs) == 0:
            pass
        else:
            application_parameters = kwargs.get("application_parameters", None)
            train = kwargs.get("train", 0)
            lower = kwargs.get("lower", True)
            self._normal_init(application_parameters, train, lower)

    def _normal_init(self, application_parameters, train, lower):
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
        self.remaining_configs = filtered_after_constraints
        self.done_configs = set()
        self.skipped_configs = set()

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
        self.remaining_configs.remove(config)
        self.done_configs.add(config)
        SweeperStatePersistence.persist_state(self)

    def skipped(self, config):
        self.skipped_configs.add(config)
        self.remaining_configs.remove(config)
        SweeperStatePersistence.persist_state(self)

    @staticmethod
    def _to_list_of_key(parameters: List[ApplicationParameter]):
        parameters.sort(key=lambda ap: ap.priority)
        return [ap.name for ap in parameters]

    @staticmethod
    def _to_parameters_dict(parameters: List[ApplicationParameter]):
        return {parameter.name: parameter.values for parameter in parameters}

    @staticmethod
    def Schema():
        return SweeperStateSchema()


class Sweeper:

    def __init__(self, application_parameters: ApplicationParameters, train: int, lower: bool = True,
                 remove_workdir: bool = False):
        if remove_workdir:
            SweeperStatePersistence.remove_workdir()
        if SweeperStatePersistence.persisted_state_exists():
            self.__state = SweeperStatePersistence.load_state()
        else:
            self.__state = SweeperState(application_parameters=application_parameters, train=train, lower=lower)

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
            if DictUtil.contains_subdictionary(config, starting_with):
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
        if len(state.remaining_configs) == 0:
            self._finalize_selected()
            return None
        elif state.remaining_train == 0:
            # Find best sequence of argument, starting with already selected ones
            best = self._find_best(state.scores, state.selected)
            # Add the new config value to the selected ones
            state.selected[state.current_parameter_key] = best[state.current_parameter_key]
            state.remaining_configs = Sweeper._all_start_with(state.remaining_configs, state.selected)
            if len(state.remaining_configs) != 0:
                # Increase the index of the focused param
                state.current_parameter_key = state.get_next_key()
                # Restart the maximal number of train
                state.remaining_train = state.train
                return self.get_next()
        else:
            res = random.choice(state.remaining_configs)
            state.remaining_train -= 1
            return res

    def has_next(self):
        has_remaining = len(self.__state.remaining_configs) != 0
        if not has_remaining:
            self._finalize_selected()
        return has_remaining

    def _finalize_selected(self):
        self.__state.selected = self._find_best(self.__state.scores, self.__state.selected)

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
                # do not replace it with += otherwise metrics[0] will be modified!
                sum = sum + metric

            return sum / length
        else:
            raise KeyError(f'The config {config} has not been tested yet.')

    def get_all_scores_by_config(self):
        return self.__state.scores

    def has_best(self):
        return self.best is not None

    @property
    def best(self):
        return self.__state.selected

    @property
    def skipped_configs(self):
        return self.__state.skipped_configs

    def __str__(self):
        state = self.__state
        res = f"Parameters fields: {state.parameters_dict}\n"
        res += f"Current scored configurations: {state.scores}\n"
        res += f"Number of not-scored configurations: {len(state.remaining_configs)}\n"
        res += f"Skipped configurations: {state.skipped_configs}\n"
        res += f"Current best configuration: {state.selected}"
        return res

    @staticmethod
    def _all_start_with(sequences: List[dict], start: dict):
        '''
        return the sequences which starts with start
        '''
        return [seq for seq in sequences if DictUtil.contains_subdictionary(seq, start)]


class SweeperStateSchema(Schema):
    lower = fields.Boolean()
    train = fields.Integer()
    remaining_train = fields.Integer()
    # are serialized as two lists, because marshmallow cannot serialize dicts whose keys are dicts
    scores = fields.Method("serialize_scores", deserialize="deserialize_scores")
    parameters_dict = fields.Dict()
    # fields.Method() is not embedded in fields.List() due to a bug in marshmallow
    remaining_configs = fields.Method("serialize_remaining_configs", deserialize="deserialize_hashable_dict_list")
    # fields.Method() is not embedded in fields.List() due to a bug in marshmallow
    done_configs = fields.Method("serialize_done_configs", deserialize="deserialize_hashable_dict_set")
    # fields.Method() is not embedded in fields.List() due to a bug in marshmallow
    skipped_configs = fields.Method("serialize_skipped_configs", deserialize="deserialize_hashable_dict_set")
    parameters = fields.List(fields.String())
    parameter_index = fields.Integer()
    current_parameter_key = fields.String()
    selected = fields.Method("serialize_selected", deserialize="deserialize_hashable_dict")

    def serialize_scores(self, obj):
        keys = ListUtil.to_list(obj.scores.keys())

        # [serialized_metric_1_1;serialized_metric_1_2;...],[s_m_2_1;s_m_2_2]
        values = [f"[{';'.join(str(metric) for metric in metrics)}]" for metrics in obj.scores.values()]
        return [keys, values]

    def serialize_remaining_configs(self, obj):
        return DictUtil.clone_list_of_dictionaries(obj.remaining_configs)

    def serialize_done_configs(self, obj):
        return DictUtil.clone_list_of_dictionaries(obj.done_configs)

    def serialize_skipped_configs(self, obj):
        return DictUtil.clone_list_of_dictionaries(obj.skipped_configs)

    def serialize_selected(self, obj):
        return DictUtil.clone(obj.selected)

    def deserialize_scores(self, value):
        keys = value[0]
        values_list = value[1]

        deserialized = dict()
        for i in range(len(keys)):
            key = DictUtil.clone_into(keys[i], HashableDict())
            serialized_metrics = values_list[i][1:-1].split(";")
            value = [Metric.from_string(metric) for metric in serialized_metrics]
            deserialized[key] = value

        return deserialized

    def deserialize_hashable_dict(self, value):
        return DictUtil.clone_into(value, HashableDict())

    def deserialize_hashable_dict_list(self, value):
        return [self.deserialize_hashable_dict(item) for item in value]

    def deserialize_hashable_dict_set(self, value):
        return {self.deserialize_hashable_dict(item) for item in value}

    @post_load
    def create_state(self, deserialized, **kwargs):
        res = SweeperState()
        res.lower = deserialized["lower"]
        res.train = deserialized["train"]
        res.remaining_train = deserialized["remaining_train"]
        res.scores = deserialized["scores"]
        res.remaining_configs = deserialized["remaining_configs"]
        res.done_configs = deserialized["done_configs"]
        res.skipped_configs = deserialized["skipped_configs"]
        res.parameters = deserialized["parameters"]
        res.parameter_index = deserialized["parameter_index"]
        res.current_parameter_key = deserialized["current_parameter_key"]
        res.selected = deserialized["selected"]
        res.parameters_dict = deserialized["parameters_dict"]
        return res


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
    def persisted_state_exists() -> bool:
        return os.path.exists(SweeperStatePersistence._STATE_FILE_STR)
