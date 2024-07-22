import mock
import pytest

from ska_sdp_pipelines.framework.model.cli_arguments import (
    CLIArgument,
    CLIArguments,
)


@pytest.fixture(scope="function")
def arg_parser():
    with mock.patch(
        "ska_sdp_pipelines.framework.model.cli_arguments"
        ".argparse.ArgumentParser"
    ) as arg_parser_mock:
        arg_parser_mock.return_value = arg_parser_mock
        yield arg_parser_mock


def test_should_create_cli_arguments(arg_parser):
    additional_cli_args = [
        CLIArgument("arg1", value1="value1", value2="value2"),
        CLIArgument("arg2", key1="key1", key2="key2"),
    ]

    CLIArguments(additional_cli_args)

    arg_parser.add_argument.assert_has_calls(
        [
            mock.call("arg1", value1="value1", value2="value2"),
            mock.call("arg2", key1="key1", key2="key2"),
        ]
    )


def test_should_parse_cli_arguments(arg_parser):
    arg_parser.parse_args.return_value = "PARSED_ARGS"
    cli_args = CLIArguments()
    expected = cli_args.parse_args()

    assert "PARSED_ARGS" == expected

    arg_parser.parse_args.assert_called_once()


def test_should_return_dictionary_of_cli_args(arg_parser):
    parsed_args = mock.Mock(name="parsed_args")
    parsed_args._get_kwargs.return_value = [
        ("key1", "value1"),
        ("key2", "value2"),
    ]

    arg_parser.parse_args.return_value = parsed_args

    cli_args = CLIArguments()
    expected = cli_args.get_cli_args()

    assert {"key1": "value1", "key2": "value2"} == expected

    arg_parser.parse_args.assert_called_once()
