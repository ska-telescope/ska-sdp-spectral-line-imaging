from .command.cli_command_parser import CLIArgument

CONFIG_CLI_ARGS = [
    CLIArgument(
        "--config-install-path",
        dest="config_install_path",
        type=str,
        required=True,
        help="Path to place the default config.",
    )
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
        "--with-report",
        dest="with_report",
        action="count",
        default=0,
        help=(
            "For distributed dask process optionaly capture performance "
            "report. If specified, the diagnostic report woulr be captured "
            "in report.html file saved into the run output folder."
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
