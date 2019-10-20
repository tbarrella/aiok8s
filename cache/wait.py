import asyncio
import random

from . import time_

FOREVER_TEST_TIMEOUT = 30


async def until(f, period, stop_event):
    await jitter_until(f, period, 0, True, stop_event)


async def jitter_until(f, period, jitter_factor, sliding, stop_event):
    stop = asyncio.ensure_future(stop_event.wait())
    while not stop_event.is_set():
        if jitter_factor > 0:
            jittered_period = jitter(period, jitter_factor)
        else:
            jittered_period = period
        if not sliding:
            # TODO: Reuse
            t = time_.Timer(jittered_period)
        await f()
        if sliding:
            t = time_.Timer(jittered_period)

        await asyncio.wait(
            [asyncio.ensure_future(t.c.get()), stop],
            return_when=asyncio.FIRST_COMPLETED,
        )


def jitter(duration, max_factor):
    if max_factor <= 0:
        max_factor = 1
    wait = duration + random.random() * max_factor * duration
    return wait
