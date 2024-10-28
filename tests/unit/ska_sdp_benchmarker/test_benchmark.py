from mock import Mock, mock

from ska_sdp_benchmarker.benchmark import Benchmark


@mock.patch("ska_sdp_piper.piper.command.command.CLICommandParser")
@mock.patch("ska_sdp_benchmarker.benchmark.subprocess")
def test_should_run_benchmark_setup_from_cli(
    subprocess_mock, cli_command_parser_mock
):

    cli_command_parser_mock.return_value = cli_command_parser_mock
    args = Mock(name="CLI-ARGS")

    cli_command_parser_mock.parse_args.return_value = args

    benchmarker = Benchmark("test_benchmark")
    subprocess_run_mock = Mock(name="run_mock")
    subprocess_mock.run = subprocess_run_mock
    args.sub_command = benchmarker.setup
    benchmarker()

    # benchmark_mock.assert_called_once_with(args)
    subprocess_mock.run.assert_called_once_with(
        ["./scripts/benchmark.sh", "--setup"]
    )


@mock.patch("ska_sdp_piper.piper.command.command.CLICommandParser")
@mock.patch("ska_sdp_benchmarker.benchmark.subprocess")
def test_should_run_benchmarks_on_local_setup(
    subprocess_mock, cli_command_parser_mock
):
    cli_command_parser_mock.return_value = cli_command_parser_mock
    args = Mock(name="CLI-ARGS")
    args.pipelineargs = "some pipeline args"
    args.output = "./output dir"

    cli_command_parser_mock.parse_args.return_value = args

    benchmarker = Benchmark("test_benchmark")
    subprocess_run_mock = Mock(name="run_mock")
    subprocess_mock.run = subprocess_run_mock
    args.sub_command = benchmarker._run
    benchmarker()

    subprocess_mock.run.assert_called_once_with(
        ["./scripts/benchmark.sh", "--local ./output dir some pipeline args"]
    )
