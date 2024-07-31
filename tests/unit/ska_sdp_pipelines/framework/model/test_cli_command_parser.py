import pytest
from mock import Mock, mock

from ska_sdp_pipelines.framework.model.cli_command_parser import (
    CLIArgument,
    CLICommandParser,
)


@pytest.fixture(scope="function")
def arg_parser():
    with mock.patch(
        "ska_sdp_pipelines.framework.model.cli_command_parser"
        ".argparse.ArgumentParser"
    ) as arg_parser_mock:
        arg_parser_mock.return_value = arg_parser_mock
        yield arg_parser_mock


def test_should_create_cli_command_parser(arg_parser):
    CLICommandParser()
    arg_parser.add_subparsers.assert_called_once()


def test_should_create_sub_parser(arg_parser):
    subparser_mock = Mock(name="subparser_mock")
    add_parser_mock = Mock(name="add_parser")

    subparser_mock.add_parser.return_value = add_parser_mock
    arg_parser.add_subparsers.return_value = subparser_mock

    cli_args = CLICommandParser()
    run_additional_cli_args = [
        CLIArgument("arg1", value1="value1", value2="value2"),
        CLIArgument("arg2", key1="key1", key2="key2"),
    ]
    cli_args.create_sub_parser("run", "RUN", run_additional_cli_args)

    add_parser_mock.set_defaults.assert_called_once_with(sub_command="RUN")
    add_parser_mock.add_argument.assert_has_calls(
        [
            mock.call("arg1", value1="value1", value2="value2"),
            mock.call("arg2", key1="key1", key2="key2"),
        ]
    )


@mock.patch("ska_sdp_pipelines.framework.model.cli_command_parser.sys")
def test_should_parse_cli_arguments(sys_mock, arg_parser):
    sys_mock.argv = ["EXEC", "PARSED_ARGS"]
    arg_parser.parse_args.return_value = "PARSED_ARGS"
    cli_args = CLICommandParser()
    expected = cli_args.parse_args()

    assert "PARSED_ARGS" == expected

    arg_parser.parse_args.assert_called_once()


@mock.patch("ska_sdp_pipelines.framework.model.cli_command_parser.sys")
def test_should_exit_with_status_two_if_no_sub_commands_given(
    sys_mock, arg_parser
):
    sys_mock.argv = ["EXEC"]
    cli_args = CLICommandParser()
    cli_args.parse_args()
    sys_mock.stderr.write.assert_called_once_with(
        "EXEC: error: positional arguments missing.\n"
    )
    arg_parser.print_help.assert_called_once()
    sys_mock.exit.assert_called_once_with(2)


def test_should_return_dictionary_of_cli_args(arg_parser):
    parsed_args = mock.Mock(name="parsed_args")
    parsed_args._get_kwargs.return_value = [
        ("key1", "value1"),
        ("key2", "value2"),
    ]

    arg_parser.parse_args.return_value = parsed_args

    cli_args = CLICommandParser()
    expected = cli_args.cli_args_dict

    assert {"key1": "value1", "key2": "value2"} == expected

    arg_parser.parse_args.assert_called_once()


@mock.patch("ska_sdp_pipelines.framework.model.cli_command_parser.write_yml")
def test_should_write_cli_args_to_yaml_file(write_yml_mock, arg_parser):
    parsed_args = mock.Mock(name="parsed_args")
    parsed_args._get_kwargs.return_value = [
        ("key1", "value1"),
        ("key2", "value2"),
    ]

    arg_parser.parse_args.return_value = parsed_args

    cli_args = CLICommandParser()
    cli_args.write_yml("filepath")

    write_yml_mock.assert_called_once_with(
        "filepath", {"key1": "value1", "key2": "value2"}
    )


@mock.patch("ska_sdp_pipelines.framework.model.cli_command_parser.write_yml")
def test_should_write_cli_args_to_yaml_file_without_sub_commands(
    write_yml_mock, arg_parser
):
    parsed_args = mock.Mock(name="parsed_args")
    parsed_args._get_kwargs.return_value = [
        ("key1", "value1"),
        ("key2", "value2"),
        ("sub_command", "function"),
    ]

    arg_parser.parse_args.return_value = parsed_args

    cli_args = CLICommandParser()
    cli_args.write_yml("filepath")

    write_yml_mock.assert_called_once_with(
        "filepath", {"key1": "value1", "key2": "value2"}
    )
