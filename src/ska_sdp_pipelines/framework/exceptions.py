import builtins


class ArgumentMismatchException(builtins.Exception):
    """
    Raised in the event when the pipeline config parameters and
    stage arguments dont match
    """

    pass


class PipelineMetadataMissingException(ArgumentMismatchException):
    """
    Raised in the event when the mandatory stage argument pipeline metadata
    is missing
    """

    pass


class StageNotFoundException(builtins.Exception):
    """
    Raised if a stage name is provided which is not present in the pipeline
    definition
    """

    pass


class ConfigNotInitialisedException(builtins.Exception):
    """
    Raised for non initialised configurations
    """

    pass


class NoStageToExecuteException(builtins.Exception):
    """
    Raised in the event all stages are toggled off, and no stages are provided
    in the CLI --stages option
    """

    pass


class PipelineNotFoundException(builtins.Exception):
    pass


__all__ = [
    "ArgumentMismatchException",
    "PipelineMetadataMissingException",
    "StageNotFoundException",
    "ConfigNotInitialisedException",
    "NoStageToExecuteException",
    "PipelineNotFoundException",
]
