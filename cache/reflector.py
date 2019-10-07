class Reflector:
    def __init__(self, lw, expected_type, store, resync_period):
        self.should_resync = None
        self.watch_list_page_size = 0
        # TODO: name
        self._store = store
        self._lister_watcher = lw
        self._period = 1
        self._resync_period = resync_period
        self._set_expected_type(expected_type)

    def run(self, stop_event):
        pass

    def _set_expected_type(self, expected_type):
        self._expected_type = type(expected_type)
        if expected_type is None:
            self._expected_type_name = _DEFAULT_EXPECTED_TYPE_NAME
            return
        self._expected_type_name = str(self._expected_type)
        # TODO: Handle Unstructured


_DEFAULT_EXPECTED_TYPE_NAME = "<unspecified>"
_MIN_WATCH_TIMEOUT = 5 * 60
