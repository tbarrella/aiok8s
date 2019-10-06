class FIFOClosedError(RuntimeError):
    pass


class RequeueError(RuntimeError):
    def __init__(self, err=None):
        self.err = err


class ProcessError(RuntimeError):
    def __init__(self, item, err):
        self.item = item
        self.err = err
