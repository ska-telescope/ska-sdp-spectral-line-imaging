from ska_sdp_piper.piper.model.cli_command_parser import CLIArgument

DIAGNOSTIC_CLI_ARGS = [
    CLIArgument(
        "--input",
        dest="input",
        type=str,
        required=True,
        help="Path of the pipeline run output.",
    ),
    CLIArgument(
        "--output",
        dest="output",
        type=str,
        required=False,
        help="Path to place the diagnosis output.",
    ),
]
