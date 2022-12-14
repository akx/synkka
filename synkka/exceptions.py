class TaskFailed(Exception):
    def __init__(self, message, *, item):
        super().__init__(message)
        self.item = item
