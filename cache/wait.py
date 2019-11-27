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


class WaitTimeoutError(Exception):
    pass


class Backoff:
    def __init__(self, *, steps, duration=0, factor=0, jitter=0, cap=0):
        self.duration = duration
        self.factor = factor
        self.jitter = jitter
        self.steps = steps
        self.cap = cap

    def step(self):
        if self.steps < 1:
            if self.jitter:
                return jitter(self.duration, self.jitter)
            return self.duration
        self.steps -= 1
        duration = self.duration
        if self.factor:
            self.duration = self.duration * self.factor
            if self.cap and self.duration > self.cap:
                self.duration = self.cap
                self.steps = 0

        if self.jitter:
            duration = jitter(duration, self.jitter)
        return duration


async def exponential_backoff(backoff, condition):
    backoff = Backoff(**backoff.__dict__)
    while backoff.steps:
        if await condition():
            return
        if backoff.steps == 1:
            break
        await asyncio.sleep(backoff.step())
    raise WaitTimeoutError


# TODO: test, rewrite?
async def poll(interval, timeout, condition):
    await asyncio.wait_for(_poll_internal(interval, condition), timeout)


# TODO: test, rewrite?
async def poll_immediate(interval, timeout, condition):
    if condition():
        return
    await poll(interval, timeout, condition)


# TODO: test, rewrite?
async def poll_immediate_until(interval, condition, stop_event):
    poll_task = asyncio.ensure_future(_poll_internal(interval, condition))
    stop_task = asyncio.ensure_future(stop_event.wait())
    _, pending = await asyncio.wait(
        [poll_task, stop_task], return_when=asyncio.FIRST_COMPLETED
    )
    if poll_task in pending:
        poll_task.cancel()


async def _poll_internal(interval, condition):
    while True:
        await asyncio.sleep(interval)
        if condition():
            return
