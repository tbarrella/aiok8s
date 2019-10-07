class FIFOClosedError(RuntimeError):
    pass


class RequeueError(RuntimeError):
    pass


class ProcessError(RuntimeError):
    def __init__(self, item):
        self.item = item
