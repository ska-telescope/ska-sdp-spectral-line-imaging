import builtins


class ArgumentMismatchException(builtins.Exception):
    pass


class VisibilityMissingException(ArgumentMismatchException):
    pass


class StageNotFoundException(builtins.Exception):
    pass


class ConfigNotInitialisedException(builtins.Exception):
    pass


class NoStageToExecuteException(builtins.Exception):
    pass


class PipelineNotFoundException(builtins.Exception):
    pass


__all__ = [
    "ArgumentMismatchException",
    "VisibilityMissingException",
    "StageNotFoundException",
    "ConfigNotInitialisedException",
    "NoStageToExecuteException",
    "PipelineNotFoundException",
]
