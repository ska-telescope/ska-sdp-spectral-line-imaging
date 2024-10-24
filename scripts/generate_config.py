import pandas as pd

from ska_sdp_spectral_line_imaging import pipeline

############################################
# Creating dictionary of dataframes
# Each dataframe contains Configuration info
############################################
dfs = {}
for x in pipeline.spectral_line_imaging_pipeline._stages:
    df = pd.DataFrame(
        [
            {"param": k, **v.__dict__}
            for k, v in x._Stage__config._Configuration__config_params.items()
        ]
    ).fillna("None")
    if df.empty:
        continue
    df = df.rename(columns={"_type": "type"})
    df.columns = df.columns.str.capitalize()
    df["Type"] = df["Type"].apply(lambda x: x.__name__)
    dfs[x.name] = df


###############################
# Handing imaging configuration
###############################

# Since imaging stage config contains dictionaries inside each ConfigParam,
# the key value pairs inside dictionary have to be extracted out seperately
image_df = dfs["imaging"]
new_rows = []
params_to_remove = []

# Iterate over the rows of the original DataFrame
for _, row in image_df.iterrows():
    param = row["Param"]
    default = row["Default"]

    if isinstance(default, dict):
        params_to_remove.append(param)
        # Iterate over the dictionary items in the 'Default' column
        for key, value in default.items():
            new_param = f"{param}.{key}"  # Combine the Param and key
            if value is None:
                value = "None"
            new_rows.append(
                {
                    "Param": new_param,
                    "Type": type(value).__name__,
                    "Default": value,
                    "Description": "",
                }
            )

new_df = pd.DataFrame(new_rows)

for name in params_to_remove:
    image_df = image_df[image_df["Param"] != name]

image_df = pd.concat([image_df, new_df], ignore_index=True)

# Add missing data

image_df.loc[
    image_df["Param"] == "gridding_params.cell_size", "Type"
] = "float"
image_df.loc[
    image_df["Param"] == "gridding_params.cell_size", "Description"
] = "Cell size of the image in arcseconds"
image_df.loc[
    image_df["Param"] == "gridding_params.scaling_factor", "Description"
] = "Scaling factor used for esimation of cell size."
image_df.loc[
    image_df["Param"] == "gridding_params.epsilon", "Description"
] = "Floating point accuracy for ducc gridder"
image_df.loc[
    image_df["Param"] == "gridding_params.image_size", "Description"
] = "Spatial size (nx and ny) of the spectral cube"

image_df.loc[
    image_df["Param"] == "deconvolution_params.algorithm", "Description"
] = "Algorithm for deconvolution. If use_radler is False, then options are 'hogbom'|'msclean'. With radler, the options are 'multiscale'|'iuwt'|'more_sane'|'generic_clean'"
image_df.loc[
    image_df["Param"] == "deconvolution_params.gain", "Description"
] = "Loop gain"
image_df.loc[
    image_df["Param"] == "deconvolution_params.threshold", "Description"
] = "Clean threshold"
image_df.loc[
    image_df["Param"] == "deconvolution_params.fractional_threshold",
    "Description",
] = "Fractional threshold"
image_df.loc[
    image_df["Param"] == "deconvolution_params.scales", "Description"
] = "Scales in pixels for multiscale"
image_df.loc[
    image_df["Param"] == "deconvolution_params.niter", "Description"
] = "Maximum number of minor cycle iterations"
image_df.loc[
    image_df["Param"] == "deconvolution_params.use_radler", "Description"
] = "Whether to use radler or not"

image_df.loc[image_df["Param"] == "beam_info.bmaj", "Type"] = "float"
image_df.loc[
    image_df["Param"] == "beam_info.bmaj", "Description"
] = "Beam major axis in radian"
image_df.loc[image_df["Param"] == "beam_info.bmin", "Type"] = "float"
image_df.loc[
    image_df["Param"] == "beam_info.bmin", "Description"
] = "Beam minor axis in radian"
image_df.loc[image_df["Param"] == "beam_info.bpa", "Type"] = "float"
image_df.loc[
    image_df["Param"] == "beam_info.bpa", "Description"
] = "Beam position angle in radian"

dfs["imaging"] = image_df


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
    :widths: 25, 10, 20, 45
"""

indent = "    "


def generate_stage_config(dfs, stage_config_path):
    with open(stage_config_path, "w") as f:
        # Write the header first
        f.write(header)
        f.write("\n\n")

        for name, df in dfs.items():
            f.write(name)
            f.write("\n")
            f.write("*" * len(name))
            f.write("\n")
            f.write(table_config)
            f.write("\n")
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

            f.write(indented_markdown)
            f.write("\n\n\n")  # Add some space between tables


generate_stage_config(dfs, "docs/src/stage_config.rst")
