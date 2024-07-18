class NamedInstance(type):
    """
    Metaclass to manage named instances
    Attributes
    ----------
         _instances: dict
            Mapping between cls, name and instance
    """

    _instances = {}

    def __call__(cls, name, existing_instance=False, *args, **kwargs):
        """
        Creates mapping between cls, name and instance of cls
        Parameters
        ----------
            cls: class
                Class to instantiate
            name: str
                Name of the instance.
            existing_instance: bool
                If existing_instance is true, return existing instance,
                else create new
            *args
                Additional args
            **kwargs
                Additional named args
        Returns
        -------
            Instance of cls
        """
        if not existing_instance:
            cls._instances[(cls, name)] = super(NamedInstance, cls).__call__(
                name, *args, **kwargs
            )

        return cls._instances.get((cls, name), None)
