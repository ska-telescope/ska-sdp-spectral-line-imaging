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


@mock.patch("ska_sdp_piper.piper.command.command.logging")
def test_should_capture_the_exception(logging_mock, cli_command_parser):
    mock_logger = Mock(name="GetLogger")
    logging_mock.getLogger.return_value = mock_logger

    ex = Exception("pipeline exception")
    sub_command_mock = Mock(name="subcommand", side_effect=ex)
    cli_command_parser.cli_args_dict = {
        "input": "infile_path",
        "sub_command": sub_command_mock,
    }

    command = Command()

    with pytest.raises(Exception):
        command()

    mock_logger.exception.assert_called_once_with(ex)
