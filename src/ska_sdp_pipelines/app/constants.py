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
  '--stages',
  dest='stages',
  action='append',
  nargs='*',
  help='Pipleline stages to be executed'
)
args = parser.parse_args()

if __name__ == "__main__":
    pipeline = Pipeline.get_instance()
    stages = [] if args.stages is None else args.stages[0]
    sys.exit(pipeline(args.input, stages, args.config_path))
"""

SHEBANG_HEADER = "#! {executable} \n\n"
