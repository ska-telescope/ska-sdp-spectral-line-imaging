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
parser.add_argument(
  "--verbose",
  "-v",
  dest='verbose',
  action='count',
  default=0,
  help=(
    "Increase pipeline verbosity to debug level."
  ),
)

args = parser.parse_args()

if __name__ == "__main__":
  pipeline = Pipeline('{pipeline_name}', _existing_instance_=True)
  stages = [] if args.stages is None else args.stages[0]
  sys.exit(
    pipeline(
      args.input,
      stages=stages,
      dask_scheduler=args.dask_scheduler,
      config_path=args.config_path,
      verbose=(args.verbose != 0),
      output_path=args.output_path
    )
  )
"""

SHEBANG_HEADER = "#! {executable} \n\n"
