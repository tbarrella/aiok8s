import queue
import random
import threading

from . import time_


def until(f, period, stop_event):
    jitter_until(f, period, 0, True, stop_event)


def jitter_until(f, period, jitter_factor, sliding, stop_event):
    select = queue.Queue()

    def stop():
        stop_event.wait()
        select.put(None)

    threading.Thread(target=stop).start()
    while True:
        if stop_event.is_set():
            return
        if jitter_factor > 0:
            jittered_period = jitter(period, jitter_factor)
        else:
            jittered_period = period
        if not sliding:
            # TODO: Reuse
            t = time_.Timer(jittered_period)
        f()
        if sliding:
            t = time_.Timer(jittered_period)

        def time_out():
            t.c.get()
            select.put(None)

        threading.Thread(target=time_out, daemon=True).start()
        select.get()


def jitter(duration, max_factor):
    if max_factor <= 0:
        max_factor = 1
    wait = duration + random.random() * max_factor * duration
    return wait
