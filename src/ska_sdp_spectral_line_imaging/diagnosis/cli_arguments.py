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
]
