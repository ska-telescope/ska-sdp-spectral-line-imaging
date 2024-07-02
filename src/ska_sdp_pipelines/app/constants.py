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
  '--stages',
  dest='stages',
  action='append',
  nargs='+',
  help='Pipleline stages'
)
args = parser.parse_args()

if __name__ == "__main__":
    pipeline = Pipeline.get_instance()
    stages = [] if args.stages is None else args.stages[0]
    sys.exit(pipeline(args.input, stages))
"""

SHEBANG_HEADER = "#! {executable} \n\n"
