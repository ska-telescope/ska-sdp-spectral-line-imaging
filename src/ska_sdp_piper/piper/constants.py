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
        help="Path to store pipeline output products.",
    ),
    CLIArgument(
        "--stages",
        dest="stages",
        action="extend",
        nargs="*",
        help="""Names of the stages in the pipeline to execute.
            The stage names must be space seperated.
            e.g. "--stages stage1 stage2" """,
    ),
    CLIArgument(
        "--dask-scheduler",
        dest="dask_scheduler",
        type=str,
        help="""Optional dask scheduler address to which to submit jobs.
            If not specified, the pipeline will run locally using
            threads.""",
    ),
    CLIArgument(
        "--set",
        dest="override_defaults",
        action="append",
        nargs=2,
        metavar=("path_to_key", "value"),
        help="""Overrides for default config of the pipeline
            You need to call --set seperately for each parameter
            which you wish to override.
            e.g. "--set parameters.param1 20 --set pipeline.stage False".
            For nested keys, join the keys with '.' to create single string.
            e.g. "--set root_key.child_key.param_key 30"
            """,
    ),
    CLIArgument(
        "--with-report",
        dest="with_report",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="""Optionaly capture performance report for dask
            distributed tasks.
            If the flag is true, the diagnostic report would be saved
            as dask_report.html file in the run output folder.
            Note: This will not work for local execution. This means that
            --dask-scheduler must be specified.""",
    ),
    CLIArgument(
        "--verbose",
        "-V",
        dest="verbose",
        action="count",
        default=0,
        help=("Increase pipeline verbosity to debug level."),
    ),
]
