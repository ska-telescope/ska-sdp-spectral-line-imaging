from ska_sdp_piper.piper.command import CLIArgument

DIAGNOSTIC_CLI_ARGS = [
    CLIArgument(
        "--input",
        dest="input",
        type=str,
        required=True,
        help="Path of the pipeline run output.",
    ),
    CLIArgument(
        "--channel",
        dest="channel",
        type=int,
        required=True,
        help="A line free channel to plot uv distnaces.",
    ),
    CLIArgument(
        "--output",
        dest="output",
        type=str,
        required=False,
        help="Path to place the diagnosis output.",
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
]
