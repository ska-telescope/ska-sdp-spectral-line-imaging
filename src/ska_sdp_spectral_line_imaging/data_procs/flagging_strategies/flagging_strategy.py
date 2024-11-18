import os

from ...util import SafeDict


class FlaggingStrategy:
    """
    Strategy builder for AOFlagger.
    """

    TEMPLATES_PATH = (
        f"{os.path.dirname(os.path.abspath(__file__))}/lua_templates"
    )

    def __init__(self, **configurations):
        """
        Initialise StrategyBuilder

        Parameters
        ----------
            configurations: keyword argument
        """
        self.__base_template = self.__read_template("base")
        self.__strategy = ""
        self.__config = configurations

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
        self.__strategy = self.__apply_config(
            self.__base_template, "threshold_timestep_rms"
        )
        self.__setup()
        self.__original_mask()
        self.__iteration()
        self.__scale_invarient()

    def __apply_config(self, template, *keys):
        return template.format_map(
            SafeDict(**{key: self.__config[key] for key in keys})
        )

    def __apply_template(self, **templates):
        self.__strategy = self.__strategy.format_map(SafeDict(**templates))

    def __read_template(self, template_name):
        template_file_path = (
            f"{self.TEMPLATES_PATH}/{template_name}.template.lua"
        )

        with open(template_file_path, "r") as template_file:
            return template_file.read()

    def __setup(self):
        setup = self.__apply_config(
            self.__read_template("setup"),
            "base_threshold",
            "iteration_count",
            "threshold_factor_step",
            "transient_threshold_factor",
        )

        self.__apply_template(setup_template=setup)

    def __original_mask(self):
        if not self.__config.get("exclude_original_flags", False):
            self.__apply_template(
                original_mask=self.__read_template("clear-mask")
            )
        else:
            self.__apply_template(original_mask="")

    def __scale_invarient(self):
        scale_invarient = self.__read_template("scale-invarient")

        if not self.__config.get("exclude_original_flags", False):
            masked = ""
            input_copy = ""
        else:
            masked = "_masked"
            input_copy = "copy_of_input,"

        self.__apply_template(
            scale_invarient=scale_invarient.format(
                masked=masked, input_copy=input_copy
            )
        )

    def __iteration(self):

        iteration_template = self.__read_template("iteration")
        iteration_template = self.__apply_config(
            iteration_template,
            "threshold_timestep_rms",
            "threshold_channel_rms",
        )

        self.__apply_template(iteration_template=iteration_template)

        outlier_text = (
            "false"
            if not self.__config.get("flag_low_outliers", False)
            else "true"
        )

        self.__apply_template(outlier_text=outlier_text)

        self.__sumthr_sumthr_mask()
        self.__base_sumthr_mask()
        self.__join_copy_mask()
        self.__lowpass_filter()

    def __sumthr_sumthr_mask(self):
        sumthr_template = self.__read_template("sumthr-sumthr-masked")

        if not self.__config.get("exclude_original_flags", False):
            masked = ""
            converted_copy = ""
        else:
            masked = "_masked"
            converted_copy = "converted_copy,"

        self.__apply_template(
            sumthreshold_sumthr_masked=sumthr_template.format(
                masked=masked, converted_copy=converted_copy
            )
        )

    def __base_sumthr_mask(self):
        sumthr_template = self.__read_template("base-sumthr-masked")

        if not self.__config.get("exclude_original_flags", False):
            masked = ""
            converted_copy = ""
        else:
            masked = "_masked"
            converted_copy = "converted_copy,"

        self.__apply_template(
            sumthreshold_base_masked=sumthr_template.format(
                masked=masked, converted_copy=converted_copy
            )
        )

    def __join_copy_mask(self):
        if not self.__config.get("exclude_original_flags", False):
            self.__apply_template(converted_join_copy_mask="")
        else:
            self.__apply_template(
                converted_join_copy_mask=self.__read_template("join-copy-mask")
            )

    def __lowpass_filter(self):
        if not self.__config.get("include_low_pass", False):
            self.__apply_template(low_pass_filter_template="")
        else:
            window_size = self.__config["window_size"]
            low_pass_template = self.__read_template("low-pass-filter")
            self.__config["window_size_str"] = ", ".join(
                str(x) for x in window_size
            )
            self.__apply_template(
                low_pass_filter_template=self.__apply_config(
                    low_pass_template,
                    "window_size_str",
                    "time_sigma",
                    "freq_sigma",
                )
            )
