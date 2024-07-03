import yaml

from ..exceptions import ConfigNotInitialisedException


class ConfigManager:
    _instance = None

    @classmethod
    def init(cls, config_path):
        config_dict = dict()
        with open(config_path, "r") as config_file:
            config_dict = yaml.safe_load(config_file)
        cls._instance = ConfigManager(config_dict)

    @classmethod
    def get_config(cls):
        if cls._instance is None:
            raise ConfigNotInitialisedException("Not initialised")
        return cls._instance

    def __init__(self, config_dict=None):
        config_dict = dict() if config_dict is None else config_dict
        self.pipeline = config_dict.get("pipeline", dict())
        self.parameters = config_dict.get("parameters", dict())

    def stage_config(self, stage_name):
        return self.parameters.get(stage_name, dict())

    @property
    def stages_to_run(self):
        return [stage for stage in self.pipeline if self.pipeline.get(stage)]
