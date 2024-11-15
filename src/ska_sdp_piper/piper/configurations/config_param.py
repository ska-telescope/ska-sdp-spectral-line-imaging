class ConfigParam:
    """
    Configuration Parameters

    Attributes
    ----------
        _type: type
            Type of the configurable parameter.
        description: str
            Description of the configurable parameter.
        nullable: bool, default: True
            Flag to allow parameter to be None
        allowed_values: list[_type], default: None
            Allowed values for the parameter.
    """

    def __init__(
        self,
        _type,
        default,
        nullable=True,
        description=None,
        allowed_values=None,
    ):
        """
        Initialise a ConfigParam object

        Parameters
        ----------
            _type: type
                Type of the configurable parameter.
            default: _type
                The default value for the configurable parameter.
            description: str
                Description of the configurable parameter.
            nullable: bool, default: True
                Flag to allow parameter to be None
            allowed_values: list[_type], default: None
                Allowed values for the parameter.
        """
        self._type = _type
        self.__value = default
        self.description = description
        self.nullable = nullable
        self.allowed_values = allowed_values

    @property
    def value(self):
        """
        Value property
        """
        return self.__value

    @value.setter
    def value(self, new_value):
        """
        Value setter

        Parameters
        ----------
            new_value: _type
                The new value to be set for the config parameter.

        Raises
        ------
            TypeEror: If type doesnot match, or non nullable parameter set to
            None
            ValueError: If the value not within allowed_values
        """
        if not self.nullable and new_value is None:
            raise TypeError("Non nullable parameter provided with None.")

        if new_value is not None and type(new_value) is not self._type:
            raise TypeError(
                "Parameter type does not match:"
                f" {self._type} expected, {type(new_value)} provided"
            )

        if self.allowed_values and new_value not in self.allowed_values:
            raise ValueError(
                f"Allowed values are {self.allowed_values}. {new_value} "
                "provided."
            )

        self.__value = new_value
