class FIFOClosedError(RuntimeError):
    pass


class RequeueError(RuntimeError):
    def __init__(self, err=None):
        self.err = err
