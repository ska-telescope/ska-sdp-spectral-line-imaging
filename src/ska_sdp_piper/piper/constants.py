import argparse

from .command.cli_command_parser import CLIArgument


class ConfigRoot:
    PIPELINE = "pipeline"
    PARAMETERS = "parameters"
    GLOBAL_PARAMETERS = "global_parameters"


CONFIG_CLI_ARGS = [
    CLIArgument(
        "--config-install-path",
        dest="config_install_path",
        type=str,
        default="./",
        help="Path to place the default config.",
    ),
    CLIArgument(
        "--set",
        dest="override_defaults",
        action="append",
        nargs=2,
        metavar=("path", "value"),
        help="Overrides for default config",
    ),
]

DEFAULT_CLI_ARGS = [
    CLIArgument(
        "--input",
        dest="input",
        type=str,
        required=True,
        help="Input visibility path",
    ),
    CLIArgument(
        "--config",
        dest="config_path",
        type=str,
        nargs="?",
        help="Path to the pipeline configuration yaml file",
    ),
    CLIArgument(
        "--output",
        dest="output_path",
        type=str,
        nargs="?",
        help="Path to store pipeline outputs",
    ),
    CLIArgument(
        "--stages",
        dest="stages",
        action="append",
        nargs="*",
        help="Pipleline stages to be executed",
    ),
    CLIArgument(
        "--dask-scheduler",
        type=str,
        default=None,
        help=(
            "Optional dask scheduler address to which to submit jobs. "
            "If specified, any eligible pipeline step will be distributed on "
            "the associated Dask cluster."
        ),
    ),
    CLIArgument(
        "--set",
        dest="override_defaults",
        action="append",
        nargs=2,
        metavar=("path", "value"),
        help="Overrides for default config",
    ),
    CLIArgument(
        "--with-report",
        dest="with_report",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Optionaly capture performance report for dask distributed tasks."
            " If the flag is true, the diagnostic report would be saved "
            "as dask_report.html file in the run output folder."
        ),
    ),
    CLIArgument(
        "--verbose",
        "-v",
        dest="verbose",
        action="count",
        default=0,
        help=("Increase pipeline verbosity to debug level."),
    ),
]
