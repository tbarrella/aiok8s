class FIFOClosedError(Exception):
    pass


class RequeueError(Exception):
    pass


class ProcessError(Exception):
    def __init__(self, item):
        self.item = item
