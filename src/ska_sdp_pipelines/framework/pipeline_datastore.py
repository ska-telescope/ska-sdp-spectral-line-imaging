class PipelineDatastore(dict):
    """
    Datastore used storing input and intermediatry values
    during the course of execution of pipeline.
    """

    def __init__(self, input_data, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self["input_data"] = input_data

    def __getattr__(self, key):
        return self.get(key, None)
