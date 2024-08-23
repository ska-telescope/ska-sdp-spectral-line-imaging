import argparse
import sys

from ..utils import write_yml


class CLIArgument:
    """
    Model to hold argparser Namespace schema

    Attributes
    ----------
        args:
            Arguments
        kwargs:
            Key value arguments
    """

    def __init__(self, *args, **kwargs):
        """
        Instantiates CLIArgument

        Parameters
        ----------
            *args:
                Arguments
            **kwargs:
                Key value arguments
        """
        self.args = args
        self.kwargs = kwargs


class CLICommandParser:
    """
    Builds the argparser for CLI application initialisation

    Attributes
    ----------
        parser: argparse.ArgumentParser
            Holds the CLI arguments
    """

    def __init__(self):
        """
        Instantiate CLICommandParser object.
        """

        self.__parser = argparse.ArgumentParser()
        self.__subparser = self.__parser.add_subparsers()

    def parse_args(self):
        """
        Parse CLI args.

        Returns
        -------
            argparser.Namespace
        """
        if len(sys.argv) == 1:
            sys.stderr.write(
                f"{sys.argv[0]}: error: positional arguments missing.\n"
            )
            self.__parser.print_help()
            sys.exit(2)
        return self.__parser.parse_args()

    def create_sub_parser(
        self, subparser_name, sub_command, cli_args, help=None
    ):
        """
        Creates sub parser.

        Parameters
        ----------
            subparser_name: str
                Name of the sub parser.
            sub_command: func
                Function associated with the sub command.
            cli_args: [CLIArgument]
                List of CLI arguments associated with the sub command.
            help: str
                Help text for the sub command.
        """
        sub_p = self.__subparser.add_parser(subparser_name, help=help)
        sub_p.set_defaults(sub_command=sub_command)
        for arg in cli_args:
            sub_p.add_argument(*arg.args, **arg.kwargs)

    @property
    def cli_args_dict(self):
        """
        Return dictionary containing CLI arguments

        Returns
        -------
            dict
        """

        cli_args = self.__parser.parse_args()
        return dict(cli_args._get_kwargs())

    def write_yml(self, path):
        """
        Writes cli arguments to provided path in yaml format.

        Parameters
        ----------
            path: str
                Location of config file to write to.
        """

        cli_args = self.cli_args_dict
        if "sub_command" in cli_args:
            del cli_args["sub_command"]

        write_yml(path, cli_args)
