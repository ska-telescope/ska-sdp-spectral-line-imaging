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
args = parser.parse_args()

if __name__ == "__main__":
    pipeline = Pipeline.get_instance()
    sys.exit(pipeline(args.input))
"""

SHEBANG_HEADER = "#! {executable} \n\n"
