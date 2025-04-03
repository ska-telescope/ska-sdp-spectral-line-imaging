# pragma: exclude file
import importlib
import os
from functools import reduce

import pandas as pd
from sphinx.application import Sphinx
from sphinx.config import Config

from .. import __version__
from ..piper.configurations.config_param import ConfigParam
from ..piper.configurations.nested_config import NestedConfigParam
from ..piper.pipeline import Pipeline
from .sphinx_config.extension_config import (
    SphinxConfiguration,
    SphinxExtensionConfig,
)

table_config = """
Parameters
==========

..  table::
    :width: 100%
    :widths: 15, 10, 10, 45, 10, 10
"""

indent = "    "


def process_config_param(
    prefix: str, config_param: ConfigParam | NestedConfigParam
):
    """
    Process a configuration parameter recursively.
    Acts as a helper function for
    :func:`generate_config_dfs_per_stage`

    Parameters
    ----------
    prefix : str
        The parameter path as a string.
    config_param : configurations.ConfigParam or \
                   configurations.NestedConfigParam
        The configuration parameter to process.

    Returns
    -------
    list of dict
        A list of dictionaries, each containing the configuration
        parameter information. If the parameter is a nested configuration
        parameter, then this function is called recursively on each nested
        parameter.
    """
    if config_param._type is NestedConfigParam:
        return reduce(
            lambda acc, param: [
                *acc,
                *process_config_param(f"{prefix}.{param[0]}", param[1]),
            ],
            config_param._config_params.items(),
            [],
        )

    return [{"param": prefix, **config_param.__dict__}]


def generate_config_dfs_per_stage(pipeline_definition: Pipeline):
    """
    Generate pandas dataframes of configuration parameters
    for each stage in a pipeline.

    Parameters
    ----------
    pipeline_definition : Pipeline
        Pipeline definition to generate configuration documentation for.

    Returns
    -------
    dict of str to pandas.Dataframe
        A dictionary of dataframes, one for each stage in the pipeline.
        Each dataframe contains the configuration parameters for that
        stage.
    """
    dataframes = {}

    for stage in pipeline_definition._stages:
        df = []
        for name, config_param in stage._Stage__config._config_params.items():
            df.extend(process_config_param(name, config_param))

        df = pd.DataFrame(df).fillna("None")
        if df.empty:
            continue

        df = df.rename(columns={"_type": "type"})
        df = df.rename(columns={"_ConfigParam__value": "default"})
        df = df.rename(columns={"allowed_values": "allowed values"})
        df.columns = df.columns.str.capitalize()
        df["Type"] = df["Type"].apply(lambda x: x.__name__)
        df["Allowed values"] = df["Allowed values"].apply(
            lambda x: "" if x == "None" else x
        )
        dataframes[stage.name] = df

    return dataframes


def generate_stage_config(
    header: str, pipeline_definition: Pipeline, stage_config_path: str
):
    """
    Generate stage configuration documentation in RST format.

    This function generates stage configuration documentation for a given
    pipeline. It processes each stage's configuration parameters and writes
    them to an RST file specified by `stage_config_path`.

    Parameters
    ----------
    header : str
        The header content for the stage configuration documentation.
    pipeline_definition : Pipeline
        The pipeline definition object containing stages with configurations.
    stage_config_path : str
        The file path where the generated RST documentation will be written.

    Returns
    -------
    None
    """
    dataframes = generate_config_dfs_per_stage(pipeline_definition)
    # Header first
    output_string = f"{header}\n\n"

    for stage in pipeline_definition._stages:
        name = stage.name
        df = dataframes[name]
        # Assuming that all stages have "Parameters" section
        doc = stage.__doc__.split(sep="Parameters")[0].rstrip()

        output_string += f"{name}\n{'*' * len(name)}\n{doc}\n{table_config}\n"

        # Convert DataFrame to markdown string and write it to file
        markdown = df.to_markdown(
            index=False,
            tablefmt="grid",
            colalign=["left"] * len(df.columns),
            maxcolwidths=[None, None, 40, 80],
        )
        indented_markdown = "\n".join(
            indent + line for line in markdown.splitlines()
        )

        output_string += f"{indented_markdown}\n\n\n"

    with open(stage_config_path, "w") as f:
        f.write(output_string)


class StageConfigGenerator:
    """
    A class to encapsulate the generation of stage configuration documentation
    for a Sphinx-based documentation system.

    This class uses Sphinx configuration values (typically found in conf.py)
    to manage and generate documentation for pipeline stages
    and their configurations. It utilizes `SphinxExtensionConfig` which stores
    instances of `SphinxConfiguration`. Each instance defines the properties
    of seperate sphinx configurations.

    - `stage_config_pipeline_instance`: A string representing the instance
      path of the pipeline. This is used to avoid issues with pickling
      during the Sphinx build process. The default value is an empty string.
      Note: As of now this works only for 1 instance of the pipeline.
      Support for multiple instances will be added in the future.

    - `stage_config_header`: A string representing the header content for the
      stage configuration documentation. This header is included at the top of
      the generated documentation file. The default value is an empty string.

    - `stage_config_output_path`: The file path to the rst file where stage
      config will be written. Default value is empty string, in which case
      the file is written to `stage_config.rst` under `app.confdir`
      (i.e. directory holding the `conf.py` file)

    These configuration values allow for flexible and customizable generation
    of documentation for pipeline stages, making it easier to maintain and
    update the documentation as the pipeline evolves.
    """

    def __init__(self):
        self.__sphinx_config = SphinxExtensionConfig(
            [
                SphinxConfiguration(
                    name="stage_config_pipeline_instance",
                    default="",
                    rebuild="env",
                    types=str,
                ),
                SphinxConfiguration(
                    name="stage_config_header",
                    default="",
                    rebuild="env",
                    types=str,
                ),
                SphinxConfiguration(
                    name="stage_config_output_path",
                    default="",
                    rebuild="env",
                    types=str,
                ),
            ]
        )

    def register(self, app: Sphinx):
        """
        Register configurations and callback with Sphinx app.
        """
        self.__sphinx_config.register(app)
        app.connect(
            "config-inited",
            self.sphinx_generate_stage_config,
            priority=1000,  # lowest priority
        )

    def sphinx_generate_stage_config(self, app: Sphinx, config: Config):
        """
        The callback function which is called by Sphinx app
        after the configuration is initialized.
        """
        extension_config = self.__sphinx_config.get_config(config)

        # Get the header string of the output RST file
        header = extension_config["stage_config_header"]

        # Ensure that pipeline_instance_path is not not empty
        if (
            pipeline_instance_path := extension_config[
                "stage_config_pipeline_instance"
            ]
        ) == "":
            raise AttributeError(
                "Pipeline instance is not specified in sphinx config."
            )

        # Load pipeline instance from the given path
        module, pipeline_name = pipeline_instance_path.rsplit(".", 1)
        pipeline_instance = getattr(
            importlib.import_module(module), pipeline_name
        )

        # Ensure the output path is defined, else default to confdir
        if (output_path := extension_config["stage_config_output_path"]) == "":
            output_path = os.path.join(app.confdir, "stage_config.rst")

        # Ensure output directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            raise FileNotFoundError(
                f"Directory '{output_dir}' not found. "
                "Can not generate stage config file."
            )

        generate_stage_config(
            header,
            pipeline_instance,
            output_path,
        )


def setup(app: Sphinx):
    """
    Setup for Sphinx generate stage config extension
    """
    stage_config_generator = StageConfigGenerator()

    stage_config_generator.register(app)

    return {
        "version": __version__,
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
