from .model.cli_command_parser import CLICommandParser


class Command:
    """
    Base class for creating CLI commands
    Attributes
    ----------
        _cli_command_parser: CLICommandParser
            CLI command parser
    """

    def __init__(self):
        """
        Instantiate command object
        """
        self._cli_command_parser = CLICommandParser()

    def sub_command(self, name, cli_args, help=None):
        """
        Decorator for adding sub commands
        Parameters
        ----------
            name: str
                Name of the sub command
            cli_args: list[CLIArgument]
                List of CLI arguments for the sub command
            help: str
                Help text
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
            self._cli_command_parser.create_sub_parser(
                name, func, cli_args, help=help
            )

            return func

        return wrapper

    def __call__(self):
        """
        Run the pipeline as a CLI command
        """
        cli_args = self._cli_command_parser.parse_args()
        cli_args.sub_command(cli_args)
