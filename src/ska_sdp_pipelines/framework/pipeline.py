from .io_utils import create_output_name, read_dataset, write_dataset


class Pipeline:
    """
    Pipeline class allows for defining a pipeline as an ordered list of
    stages, and takes care of executing those stages.
    Attributes:
      name (str): Name of the pipeline
      _stage (Stage): Stage to be executed
    """

    def __init__(self, name, stage):
        """
        Initialise the pipeline object
        Parameters:
          name (str) : Name of the pipeline
          stage (Stage) : Stage to be executed
        """
        self.name = name
        self._stage = stage

    def __call__(self, infile_path):
        """
        Executes the pipeline
        Parameters:
          infile_path (str) : Path to input file
        """
        vis = read_dataset(infile_path)
        output = self._stage(vis)
        outfile = create_output_name(infile_path, self.name)
        write_dataset(output, outfile)
