import builtins


class ArgumentMismatchException(builtins.Exception):
    pass


class VisibilityMissingException(ArgumentMismatchException):
    pass


class StageNotFoundException(builtins.Exception):
    pass


__all__ = [
    "ArgumentMismatchException",
    "VisibilityMissingException",
    "StageNotFoundException",
]
