from ska_sdp_piper.piper.command import CLIArgument

CLI_ARGS = [
    CLIArgument(
        "--pipelineargs",
        type=str,
        default=None,
        help="Parameters for pipeline that is run as part of benchmarks",
    ),
    CLIArgument(
        "--output",
        type=str,
        default="./benchmarks",
        help="Output path to store benchmarking artefacts",
    ),
]
