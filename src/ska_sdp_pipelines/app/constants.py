MAIN_ENTRY_POINT = """
import sys


if __name__ == "__main__":
  pipeline = Pipeline('{pipeline_name}', _existing_instance_=True)
  sys.exit(
    pipeline.run()
  )
"""

SHEBANG_HEADER = "#! {executable} \n\n"
