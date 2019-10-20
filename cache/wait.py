import asyncio
import random

from . import time_

FOREVER_TEST_TIMEOUT = 30
NeverStop = asyncio.Event()


async def until(f, period, stop_event):
    await jitter_until(f, period, 0, True, stop_event)


async def jitter_until(f, period, jitter_factor, sliding, stop_event):
    select = asyncio.Queue()

    async def stop():
        await stop_event.wait()
        await select.put(None)

    asyncio.ensure_future(stop())
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

        async def time_out():
            await t.c.get()
            await select.put(None)

        asyncio.ensure_future(time_out())
        await select.get()


def jitter(duration, max_factor):
    if max_factor <= 0:
        max_factor = 1
    wait = duration + random.random() * max_factor * duration
    return wait
