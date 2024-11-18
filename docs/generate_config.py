from functools import reduce
import os
import sys

import pandas as pd

module_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(f"{module_dir}/../src")

from ska_sdp_piper.piper.configurations.nested_config import NestedConfigParam
from ska_sdp_spectral_line_imaging import pipeline

############################################
# Creating dictionary of dataframes
# Each dataframe contains Configuration info
############################################

def process_config_param(prefix, config_param):
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

def generate_config_dfs_per_stage(pipeline_definition):

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

stage_name_to_pddf = generate_config_dfs_per_stage(pipeline.spectral_line_imaging_pipeline)

#######################
# Generate the RST file
#######################

header = """Stage Configs
=============

.. This file is generated using scripts/generate_config.py

.. This file is referenced by "imaging" stage docstring by a relative reference
.. to the generated html page.
"""

table_config = """
..  table::
    :width: 100%
    :widths: 15, 10, 10, 45, 10, 10
"""

indent = "    "


def generate_stage_config(dfs, stage_config_path):
    with open(stage_config_path, "w") as f:
        # Write the header first
        output_string = f"{header}\n\n"

        for name, df in dfs.items():
            output_string += f"{name}\n{'*' * len(name)}\n{table_config}\n"

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

        f.write(output_string)

out_rst_path = os.path.join(module_dir, "src/stage_config.rst")

generate_stage_config(stage_name_to_pddf, out_rst_path)
