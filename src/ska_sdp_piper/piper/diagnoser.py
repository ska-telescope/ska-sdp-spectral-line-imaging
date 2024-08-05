class Diagnoser:
    """
    Abstract class Diagnoser which pipeline defined diagnoser can implement.
    """

    def diagnose(self, cli_args):
        """
        Main method that runs the diagnosis steps.
        Parameters
        ----------
            cli_args: argparse.Namespace
                CLI arguments
        """
        pass
