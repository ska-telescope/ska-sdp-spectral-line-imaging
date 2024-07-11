MAIN_ENTRY_POINT = """


import argparse
import sys


parser = argparse.ArgumentParser()
parser.add_argument(
  '--input',
  dest='input',
  type=str,
  help='Input visibility path'
)
parser.add_argument(
  '--config',
  dest='config_path',
  type=str,
  nargs='?',
  help='Path to the pipeline configuration yaml file'
)
parser.add_argument(
  '--output',
  dest='output_path',
  type=str,
  nargs='?',
  help='Path to store pipeline outputs'
)
parser.add_argument(
  '--stages',
  dest='stages',
  action='append',
  nargs='*',
  help='Pipleline stages to be executed'
)
parser.add_argument(
  "--dask-scheduler",
  type=str,
  default=None,
  help=(
    "Optional dask scheduler address to which to submit jobs. "
    "If specified, any eligible pipeline step will be distributed on "
    "the associated Dask cluster."
  ),
)
args = parser.parse_args()

if __name__ == "__main__":
  pipeline = Pipeline.get_instance()
  stages = [] if args.stages is None else args.stages[0]
  sys.exit(
    pipeline(
      args.input,
      stages,
      args.dask_scheduler,
      args.config_path,
      args.output_path
    )
  )
"""

SHEBANG_HEADER = "#! {executable} \n\n"
