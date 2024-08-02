import pytest
from mock import Mock, mock

from ska_sdp_piper.piper.command import Command


@pytest.fixture(scope="function", autouse=True)
def cli_command_parser():
    with mock.patch(
        "ska_sdp_piper.piper.command.CLICommandParser"
    ) as cli_arguments_mock:
        yield cli_arguments_mock


def test_should_run_the_sub_command(cli_command_parser):
    cli_command_parser.return_value = cli_command_parser
    cli_command_parser.cli_args_dict = {"input": "infile_path"}
    args = Mock(name="CLI_args")
    args.input = "infile_path"
    args.output = "output"

    cli_command_parser.parse_args.return_value = args
    sub_command_mock = Mock(name="subcommand")

    args.sub_command = sub_command_mock
    command = Command()
    command()
    sub_command_mock.assert_called_once_with(args)
