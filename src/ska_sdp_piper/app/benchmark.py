import os
import subprocess
import tarfile
import time
from pathlib import Path
from urllib.request import urlretrieve

from ..piper.utils.io_utils import timestamp

MODULE_DIR = os.path.dirname(__file__)

DOOL_VERSION = "1.3.3"
DOOL_URL = f"https://github.com/scottchiefbaker/\
dool/archive/refs/tags/v{DOOL_VERSION}.tar.gz"
DOOL_DIR = os.path.join(MODULE_DIR, f"dool-{DOOL_VERSION}")
DOOL_BIN = os.path.join(DOOL_DIR, "dool")


def setup_dool():
    """
    Downloads, extracts, and sets up the `dool` project
    in the directory of current module.

    Raises
    ------
        Exception
            For all errors like download failures,
            file extraction issues, or if path exists.
    """
    tar_file = "dool.tar.gz"

    try:
        print(f"Downloading {DOOL_URL} to {tar_file}")
        urlretrieve(DOOL_URL, filename=tar_file)
        print("\nDownload completed.")

        # Extract the tar.gz file
        print(f"Extracting {tar_file} to {MODULE_DIR}...")
        with tarfile.open(tar_file, "r:gz") as tar:
            tar.extractall(path=MODULE_DIR)
        print("Extraction completed.")

    except Exception as e:
        raise Exception(f"Error while setting up dool: {e}")

    finally:
        # Cleanup: Remove the downloaded tar.gz file
        if Path(tar_file).exists():
            os.remove(tar_file)
            print(f"Removed temporary file {tar_file}.")


def run_dool(
    output_dir: str,
    file_prefix: str,
    command: list,
    capture_interval: int = 5,
):
    """
    Runs the dool benchmark tool and executes the specified command,
    collecting system metrics.

    Parameters:
    -----------
    output_dir : str
        Directory where the output CSV file will be stored.
    file_prefix : str
        Prefix for the output CSV file name.
    command : list of str
        Command to execute as a list of strings, e.g., ["ls", "-l"].
    capture_interval: int, default=5
        Time interval for dool to capture metrics, in seconds

    Raises:
    -------
    Exception
        If any errors occur during the execution of the benchmark or command.
    """
    # Ensure the output directory exists
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Construct the output file path with timestamp
    file_path = output_path / f"{file_prefix}_{timestamp()}.csv"

    print("Starting Dool ...")
    print(f"Benchmark report is written to {file_path}")

    # Start the dool process
    dool_args = [
        DOOL_BIN,
        "--time",
        "--mem",
        "--swap",
        "--io",
        "--aio",
        "--disk",
        "--fs",
        "--net",
        "--cpu",
        "--cpu-use",
        "--output",
        str(file_path),
        str(capture_interval),
    ]

    try:
        dool_process = subprocess.Popen(dool_args)
        dool_pid = dool_process.pid
        print(f"Running benchmark on: {command}")

        # Wait for 2 seconds before executing the command
        time.sleep(2)
        subprocess.run(
            command,
            check=True,
        )

        # Wait for an additional 2 seconds to capture metrics
        time.sleep(2)

    except Exception as e:
        raise Exception(f"Error during rundool execution: {e}")

    finally:
        # Terminate the dool process
        if "dool_process" in locals() and dool_process.poll() is None:
            dool_process.kill()
            print(f"Terminated dool process (PID: {dool_pid}).")
