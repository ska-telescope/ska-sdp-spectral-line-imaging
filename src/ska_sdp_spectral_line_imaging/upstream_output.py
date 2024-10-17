class UpstreamOutput:
    def __init__(self):
        self.__stage_outputs = {}
        self.__compute_tasks = []

    def __setitem__(self, key, value):
        self.__stage_outputs[key] = value

    def __getitem__(self, key):
        return self.__stage_outputs[key]

    def __getattr__(self, key):
        return self.__stage_outputs[key]

    @property
    def compute_tasks(self):
        return self.__compute_tasks

    def add_compute_tasks(self, *args):
        self.__compute_tasks.extend(args)
