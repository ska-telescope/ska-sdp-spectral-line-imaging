import os

from ...util import SafeDict


class FlaggingStrategy:
    """
    Strategy builder for AOFlagger.
    """

    TEMPLATES_PATH = (
        f"{os.path.dirname(os.path.abspath(__file__))}/lua_templates"
    )

    FLAG_TEXT = {
        True: "true",
        False: "false",
    }

    def __init__(self, **configurations):
        """
        Initialise StrategyBuilder

        Parameters
        ----------
            configurations: keyword argument
        """
        self.__base_template = self.__read_template("spectral")
        self.__strategy = ""
        self.__config = configurations
        self.__value_keys = [
            "base_threshold",
            "iteration_count",
            "threshold_timestep_rms",
            "threshold_channel_rms",
            "threshold_factor_step",
            "transient_threshold_factor",
            "time_sigma",
            "freq_sigma",
        ]

        self.__flag_keys = [
            "keep_original_flags",
            "keep_outliers",
            "do_low_pass",
        ]

        self.__build()

    def write(self, file_path):
        """
        Write the strategy to file

        Parameters
        ----------
            file_path: str
                File path
        """

        with open(file_path, "w") as st_file:
            st_file.write(self.__strategy)

    def __build(self):

        template_config = {
            key: self.__config[key] for key in self.__value_keys
        }

        template_config = {
            **template_config,
            **{
                key: self.FLAG_TEXT[self.__config.get(key, False)]
                for key in self.__flag_keys
            },
        }

        window_size = self.__config.get("window_size", [11, 21])
        template_config["window_size_str"] = ", ".join(
            str(x) for x in window_size
        )

        self.__strategy = self.__base_template.format_map(
            SafeDict(template_config)
        )

    def __read_template(self, template_name):
        template_file_path = (
            f"{self.TEMPLATES_PATH}/{template_name}.template.lua"
        )

        with open(template_file_path, "r") as template_file:
            return template_file.read()
