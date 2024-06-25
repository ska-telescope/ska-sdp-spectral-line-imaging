import builtins


class ArgumentMismatchException(builtins.Exception):
    pass


class VisibilityMissingException(ArgumentMismatchException):
    pass


__all__ = ["ArgumentMismatchException", "VisibilityMissingException"]
