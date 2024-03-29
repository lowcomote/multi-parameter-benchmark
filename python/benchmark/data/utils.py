from abc import ABC
from typing import final, List
from execo_engine import HashableDict
from pathlib import Path
from benchmark.data.config import ApplicationParameterConstraint


@final
class ConstraintUtil(ABC):

    @staticmethod
    def filter_valid_configs(configs: List[HashableDict], constraints: List[ApplicationParameterConstraint]):
        merged_constraints = ConstraintUtil._merge_constraints(constraints)
        return [config for config in configs if ConstraintUtil._is_config_valid(config, merged_constraints)]

    @staticmethod
    def _merge_constraints(constraints: List[ApplicationParameterConstraint]):
        # dict: constraint_source -> {parameter_name: [values]}
        # constraint_source: ParameterBinding (name+value) that triggers the constraint.
        # saved_targets ({parameter_name: [values]}): a dictionary of parameter names and a list of values the
        #                                             corresponding parameter is allowed to have
        merged_constraints = dict()
        for constraint in constraints:
            constraint_source = HashableDict()
            constraint_source["name"] = constraint.source.name
            constraint_source["value"] = constraint.source.value

            merged_targets = {}
            merged_constraints[constraint_source] = merged_targets

            for target in constraint.targets:
                target_name = target.name
                # it's a contradiction if target_name == source_name, because value is already bound to source_value
                if target_name == constraint_source["name"]:
                    continue
                # if we have not seen this parameter name before
                if target_name not in merged_targets:
                    merged_targets[target_name] = list()
                # save the value that is allowed for this parameter
                merged_targets[target_name].append(target.value)
        return merged_constraints

    @staticmethod
    def _is_config_valid(config: HashableDict, constraints: dict):
        for constraint_source, constraint_targets in constraints.items():
            source_name = constraint_source["name"]
            if config[source_name] == constraint_source["value"]:
                for target_name, target_values in constraint_targets.items():
                    if config[target_name] not in target_values:
                        return False
        return True


@final
class JsonUtil(ABC):

    @staticmethod
    def deserialize(path: str, target_type):
        json = Path(path).read_text()
        return target_type.Schema().loads(json)

    @staticmethod
    def serialize(path: str, obj, type):
        json = type.Schema().dumps(obj)
        with open(path, "wt") as file:
            file.write(json)


@final
class DictUtil(ABC):

    @staticmethod
    def clone(this: dict):
        return {key: value for key, value in this.items()}

    @staticmethod
    def clone_list_of_dictionaries(this: List[dict]):
        return [DictUtil.clone(item) for item in this]

    @staticmethod
    def clone_into(source: dict, target: dict):
        for key, value in source.items():
            target[key] = value
        return target

    @staticmethod
    def contains_subdictionary(this: dict, subdictionary: dict):
        for key, value in subdictionary.items():
            if (key not in this) or (this[key] != value):
                return False
        return True


@final
class ListUtil(ABC):

    @staticmethod
    def to_list(iterable):
        return [item for item in iterable]
