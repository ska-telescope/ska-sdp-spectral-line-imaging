import subprocess

from ska_sdp_piper.piper.command import Command

from .cli_args import CLI_ARGS


class Benchmark(Command):
    """
    Performs benchmarking with dool

    Attributes:
    -----------
      name: str
          Name of the benchmark

    """

    def __init__(self, name, cli_args=None):
        """
        Initialise the Benchmark object

        Parameters
        ----------
          name: str
              Name of the benchmark
          cli_args: list[CLIArgument]
              Runtime arguments for the benchmark
        """
        super().__init__()
        self.name = name

        self.sub_command("setup", CLI_ARGS, help="setup benchmarks")(
            self.setup
        )

        self.sub_command("run", CLI_ARGS, help="Run benchmarks")(self._run)

    def setup(self, cli_args):
        """
        Subcommand to setup tools required for benchmarking.

        Parameters:
        -----------
          cli_args:
            Runtime arguments for setup subcommand.
        """
        subprocess.run(["./scripts/benchmark.sh", "--setup"])

    def _run(self, cli_args):
        """
        Subcommand to run benchmarks

        Parameters:
        -----------
          cli_args:
            Runtime arguments to run benchmarking
        """
        args = cli_args.pipelineargs

        prefix = f"--local {cli_args.output} "
        subprocess.run(["./scripts/benchmark.sh", prefix + args])


benchmarker = Benchmark("spectral line imaging pipeline benchmarker")
