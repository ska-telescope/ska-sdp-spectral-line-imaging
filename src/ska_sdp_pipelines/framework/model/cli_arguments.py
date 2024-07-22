import argparse


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


class CLIArguments:
    """
    Builds the argparser for CLI application initialisation
    Attributes
    ----------
      parse_args: argparse.ArgumentParser
        Holds the CLI arguments
    """

    def __init__(self, cli_args=None):
        """
        Instantial CLIArguments object
        Parameters
        ----------
          cli_args: list(CLIArgument)
            cli arguments to instantiate.
        """
        cli_args = [] if cli_args is None else cli_args
        self.parser = argparse.ArgumentParser()
        for arg in cli_args:
            self.parser.add_argument(*arg.args, **arg.kwargs)

        self.parse_args = self.parser.parse_args

    def get_cli_args(self):
        """
        Return dictionary containing CLI arguments
        Retruns
        -------
          dict
        """

        cli_args = self.parser.parse_args()
        return dict(cli_args._get_kwargs())
