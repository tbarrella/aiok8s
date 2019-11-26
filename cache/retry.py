from . import wait

DEFAULT_RETRY = wait.Backoff(steps=5, duration=0.01, factor=1, jitter=0.1)
