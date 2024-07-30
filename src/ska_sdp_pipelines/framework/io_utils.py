import os
from datetime import datetime

import yaml
from xradio.vis.read_processing_set import read_processing_set


def read_dataset(infile: str):
    # Dask related setups.
    return read_processing_set(ps_store=infile)


def write_dataset(output, outfile: str):
    pass


def create_output_dir(output_path, prefix_name):
    """
    Creates the root output directory if it doesn't exist already and a
    timestamped folder inside it to store input config and output generated.

    Parameters
    ----------
        output_path: str
            The root output folder where the timestamped folders are created
        prefix_name: str
            Common prefix to be added to output filename

    Returns
    -------
        (str):
            Timestamped folder path
    """
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    timestamped_folder = f"{output_path}/{prefix_name}_{timestamp()}"
    os.makedirs(timestamped_folder)
    return timestamped_folder


def write_yml(output_path, config):
    """
    Writes a config to output path as yaml

    Parameters
    ----------
        output_path: str
            Location of yaml file to write to.
        config: dict
            Data to write
    """
    with open(output_path, "w") as conf_file:
        yaml.dump(config, conf_file)


def timestamp():
    """
    Creates timestamp with predefined format `%Y-%m-%dT%H:%M:%S`

    Returns
    -------
        (str):
            Timestamp.
    """
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
