import pytest
from mock import Mock, mock

from ska_sdp_piper.piper.command import Command


@pytest.fixture(scope="function", autouse=True)
def cli_command_parser():
    with mock.patch(
        "ska_sdp_piper.piper.command.command.CLICommandParser"
    ) as cli_arguments_mock:
        cli_arguments_mock.return_value = cli_arguments_mock
        yield cli_arguments_mock


def test_should_add_sub_command(cli_command_parser):
    command = Command()

    @command.sub_command(
        "list of cli args",
        "subcommand",
        "help for subcommand",
    )
    def mock_sub_command():
        pass

    cli_command_parser.create_sub_parser.assert_called_once_with(
        cli_args="list of cli args",
        name="subcommand",
        func=mock_sub_command,
        help="help for subcommand",
    )


def test_should_add_sub_command_with_defaults(cli_command_parser):
    command = Command()

    @command.sub_command(
        "list of cli args",
    )
    def mock_sub_command():
        pass

    cli_command_parser.create_sub_parser.assert_called_once_with(
        cli_args="list of cli args",
        name="mock-sub-command",
        func=mock_sub_command,
        help=None,
    )


def test_should_run_the_sub_command(cli_command_parser):
    sub_command_mock = Mock(name="subcommand")
    cli_command_parser.cli_args_dict = {
        "input": "infile_path",
        "sub_command": sub_command_mock,
    }
    command = Command()
    command()
    sub_command_mock.assert_called_once_with(
        input="infile_path", sub_command=sub_command_mock
    )


def test_should_raise_exception_if_subcommand_not_found(cli_command_parser):
    cli_command_parser.cli_args_dict = {"input": "infile_path"}
    command = Command()

    with pytest.raises(ValueError) as ex:
        command()

    assert str(ex.value) == "Subcommand is missing. Run with '-h' to see help."


@mock.patch("ska_sdp_piper.piper.command.command.logger")
def test_should_capture_the_exception(logger_mock, cli_command_parser):
    ex = Exception("pipeline exception")
    sub_command_mock = Mock(name="subcommand", side_effect=ex)
    cli_command_parser.cli_args_dict = {
        "input": "infile_path",
        "sub_command": sub_command_mock,
    }

    command = Command()

    with pytest.raises(Exception):
        command()

    logger_mock.exception.assert_called_once_with(ex)
