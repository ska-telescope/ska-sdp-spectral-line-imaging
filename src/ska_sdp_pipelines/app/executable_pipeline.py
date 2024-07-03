import importlib.util
import os
import sys
from pathlib import Path

from ..framework.pipeline import Pipeline
from .constants import MAIN_ENTRY_POINT, SHEBANG_HEADER


class ExecutablePipeline:
    """
    Creates an executable instance from the pipeline definition
    Attributes:
        _script_path(str): Path to the pipeline definition
        installable_pipeline (module): The pipeline definition
                                       loaded as a module
        executable_content (str): The content of the executable file
    """

    def __init__(self, script_path):
        """
        Initialise a executable pipeline object
        Parameter:
            script_path(str): Path to the pipeline definition
        """
        self._script_path = script_path
        self.installable_pipeline = None
        self.executable_content = None

    def validate_pipeline(self):
        """
        Validates if the pipeline definition is syntactically correct python
        code.
        Raises: Compilation error based on python interpretor.
        """
        if not os.path.exists(self._script_path):
            raise FileNotFoundError(self._script_path)

        spec = importlib.util.spec_from_file_location(
            "installable_pipeline", self._script_path
        )
        self.installable_pipeline = importlib.util.module_from_spec(spec)
        sys.modules["installable_pipeline"] = self.installable_pipeline
        spec.loader.exec_module(self.installable_pipeline)

    def prepare_executable(self):
        """
        Prepares the content of the executable script file
        """
        file_content = ""
        with open(self._script_path, "r") as script_file:
            file_content = script_file.readlines()

        self.executable_content = (
            SHEBANG_HEADER.format(executable=sys.executable)
            + "".join(file_content)
            + MAIN_ENTRY_POINT
        )

    def install(self):
        """
        Installs the executable script containing the executable script.
        The path is taken from the executable path obtained from sys.executable
        The script file is created with a execute privilage to user and group
        """
        executable_path = self.__executable_script_path()

        with open(executable_path, "w") as outfile:
            outfile.write(self.executable_content)
        os.chmod(executable_path, 0o750)

    def uninstall(self):
        """
        Removes the executable pipeline from the executable path.
        """
        os.remove(self.__executable_script_path())

    def __executable_script_path(self):
        """
        Returns the absolute path of the executable.
        The path is derived from sys.executable
        """
        executable_root = Path(sys.executable).parent.absolute()
        pipeline = Pipeline.get_instance()
        return f"{executable_root}/{pipeline.name}"
