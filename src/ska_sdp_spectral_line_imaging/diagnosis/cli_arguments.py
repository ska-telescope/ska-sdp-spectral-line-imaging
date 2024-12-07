from ska_sdp_piper.piper.command import CLIArgument

DIAGNOSTIC_CLI_ARGS = [
    CLIArgument(
        "--input",
        dest="input",
        type=str,
        required=True,
        help="Path to the directory containing the output "
        "products of the pipeline.",
    ),
    CLIArgument(
        "--channel",
        dest="channel",
        type=int,
        required=True,
        help="A line free channel to plot uv distances.",
    ),
    CLIArgument(
        "--output",
        dest="output",
        type=str,
        required=False,
        help="""Path to place the diagnosis output products. If not
        provided, the output products of the diagnosis will be stored in
        a new directory "diagnosis" in current working directory.""",
    ),
    CLIArgument(
        "--dask-scheduler",
        dest="dask_scheduler",
        type=str,
        default=None,
        help="""Optional dask scheduler address to which to submit jobs.
        If not specified, the diagnostic tasks will run locally using
        threads.""",
    ),
]
