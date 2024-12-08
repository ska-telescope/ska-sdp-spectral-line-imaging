import builtins
import logging

from .cli_command_parser import SUB_COMMAND_KEY, CLICommandParser

logger = logging.getLogger()


class Command:
    """
    Base class for creating CLI commands

    Attributes
    ----------
        _cli_command_parser: cli_command_parser.CLICommandParser
            CLI command parser
    """

    def __init__(self):
        """
        Instantiate command object
        """
        self._cli_command_parser = CLICommandParser()

    def sub_command(self, cli_args, subcommand_name=None, help=None):
        """
        Decorator for adding sub commands.

        Parameters
        ----------
            cli_args: list of cli_command_parser.CLIArgument
                List of CLI arguments for the sub command
            subcommand_name: str, optional
                Name of the subcommand. If not provided,
                then it is derived from the wrapped function itself,
                by converting snake_case to dash-case.
            help: str, optional
                Help text for the subcommand

        Returns
        -------
            function
        """

        def wrapper(func):
            """
            Wrapper function

            Parameters
            ----------
                func: function
                    Callback function

            Returns
            -------
                function
            """
            name = subcommand_name or func.__name__.replace("_", "-")
            self._cli_command_parser.create_sub_parser(
                name=name,
                func=func,
                cli_args=cli_args,
                help=help,
            )

            return func

        return wrapper

    def __call__(self):
        """
        Run the pipeline as a CLI command
        """
        cli_args = self._cli_command_parser.cli_args_dict

        if SUB_COMMAND_KEY not in cli_args:
            raise ValueError(
                "Subcommand is missing. Run with '-h' to see help."
            )

        try:
            cli_args[SUB_COMMAND_KEY](**cli_args)
        except builtins.BaseException as ex:
            logger.exception(ex)
            raise ex
