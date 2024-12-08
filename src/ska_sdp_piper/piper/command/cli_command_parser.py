import argparse

from ..utils import write_yml as write_yaml_util

SUB_COMMAND_KEY = "sub_command"


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
        self.__subparser = self.__parser.add_subparsers(title="Subcommands")

    def create_sub_parser(self, name, func, cli_args, help=None):
        """
        Creates sub parser for a subcommand defined by func, and
        assigns cli arguments to it.

        Parameters
        ----------
            name: str
                Name of the subcommand.
            func: function
                Function associated with the subcommand.
            cli_args: list of CLIArgument
                List of cli arguments associated with the sub command.
            help: str
                Help text for the sub command.
        """
        sub_p = self.__subparser.add_parser(name=name, help=help)
        sub_p.set_defaults(**{SUB_COMMAND_KEY: func})
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
        return vars(self.__parser.parse_args())

    def write_yml(self, path):
        """
        Writes cli arguments to provided path in yaml format.

        Parameters
        ----------
            path: str
                Location of config file to write to.
        """

        cli_args = self.cli_args_dict
        if SUB_COMMAND_KEY in cli_args:
            del cli_args[SUB_COMMAND_KEY]

        write_yaml_util(path, cli_args)
