import pytest
from mock import MagicMock, Mock, mock

from ska_sdp_piper.piper.command.cli_command_parser import (
    CLIArgument,
    CLICommandParser,
)


@pytest.fixture(scope="function")
def arg_parser():
    with mock.patch(
        "ska_sdp_piper.piper.command.cli_command_parser"
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


@mock.patch(
    "builtins.vars", return_value={"key1": "value1", "sub_command": "function"}
)
def test_should_return_dictionary_of_cli_args(vars_mock, arg_parser):
    parse_arg_object = MagicMock(name="parsed_arguments_object")
    arg_parser.parse_args.return_value = parse_arg_object

    cli_args = CLICommandParser()
    expected = cli_args.cli_args_dict

    arg_parser.parse_args.assert_called_once()
    vars_mock.assert_called_once_with(parse_arg_object)
    assert {"key1": "value1", "sub_command": "function"} == expected


@mock.patch("builtins.vars", return_value={"key10": "value10"})
@mock.patch("ska_sdp_piper.piper.command.cli_command_parser.write_yaml_util")
def test_should_write_cli_args_to_yaml_file(
    write_yml_mock, vars_mock, arg_parser
):
    parse_arg_object = MagicMock(name="parsed_arguments_object")
    arg_parser.parse_args.return_value = parse_arg_object

    cli_args = CLICommandParser()
    cli_args.write_yml("filepath")

    arg_parser.parse_args.assert_called_once()
    vars_mock.assert_called_once_with(parse_arg_object)
    write_yml_mock.assert_called_once_with("filepath", {"key10": "value10"})


@mock.patch(
    "builtins.vars",
    return_value={"key20": "value200", "sub_command": "function"},
)
@mock.patch("ska_sdp_piper.piper.command.cli_command_parser.write_yaml_util")
def test_should_write_cli_args_to_yaml_file_without_sub_commands(
    write_yml_mock, vars_mock, arg_parser
):
    cli_args = CLICommandParser()
    cli_args.write_yml("filepath")

    write_yml_mock.assert_called_once_with("filepath", {"key20": "value200"})
