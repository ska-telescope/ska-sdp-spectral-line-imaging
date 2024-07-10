import os
from datetime import datetime
from pathlib import Path

from xradio.vis.read_processing_set import read_processing_set


def read_dataset(infile: str):
    # Dask related setups.
    return read_processing_set(ps_store=infile)


def write_dataset(output, outfile: str):
    pass


def create_output_name(src_file, prefix_name, create=False):
    """
    Creates the output filename and the relative output directory

    Parameters
    ----------
        src_file: str
            Creates output filename relative to "src_file"
        prefix_name: str
            Common prefix to be added to output filename

    Returns
    -------
        (str):
            Output filename
    """
    src_file_path = Path(src_file)
    parent_path = src_file_path.parent.absolute()
    output_path = f"{parent_path}/output"
    timestampt = datetime.now().strftime("%Y%m%d%H%M%S")
    outfile = f"{output_path}/{prefix_name}_out_{timestampt}"
    if create:
        os.makedirs(output_path)

    return outfile
