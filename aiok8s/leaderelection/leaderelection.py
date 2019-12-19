# Copyright 2015 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, NamedTuple

from aiok8s.util import clock, wait

JITTER_FACTOR = 1.2
logger = logging.getLogger(__name__)


class LeaderCallbacks(NamedTuple):
    on_started_leading: Callable[[], Awaitable[None]]
    on_stopped_leading: Callable[[], Awaitable[None]]
    on_new_leader: Callable[[str], Awaitable[None]]


class LeaderElectionConfig(NamedTuple):
    lock: Any
    callbacks: LeaderCallbacks
    name: str
    lease_duration: float = 15
    renew_deadline: float = 10
    retry_period: float = 2
    release_on_cancel: bool = False


class LeaderElector:
    def __init__(self, lec):
        if lec.lease_duration <= lec.renew_deadline:
            raise Exception("lease_duration must be greater than renew_deadline")
        if lec.renew_deadline <= JITTER_FACTOR * lec.retry_period:
            raise Exception(
                "renew_deadline must be greater than retry_period * JITTER_FACTOR"
            )
        if lec.lease_duration <= 0:
            raise Exception("lease_duration must be greater than zero")
        if lec.renew_deadline <= 0:
            raise Exception("renew_deadline must be greater than zero")
        if lec.retry_period <= 0:
            raise Exception("retry_period must be greater than zero")
        if not lec.callbacks.on_started_leading:
            raise Exception("on_started_leading callback must not be None")
        if not lec.callbacks.on_stopped_leading:
            raise Exception("on_stopped_leading callback must not be None")
        if not lec.lock:
            raise Exception("lock must not be None")
        self._config = lec
        self._clock = clock.RealClock()

    async def run(self):
        try:
            if not await self._acquire():
                return
            task = asyncio.ensure_future(self._config.callbacks.on_started_leading())
            await self._renew()
        finally:
            task.cancel()
            await self._config.callbacks.on_stopped_leading()
            await asyncio.gather(task, return_exceptions=True)

    def is_leader(self):
        return self._observed_record.holder_identity == self._config.lock.identity()

    async def _acquire(self):
        succeeded = False
        description = self._config.lock.describe()
        logger.info("attempting to acquire leader lease %s...", description)

        async def f():
            nonlocal task, succeeded
            succeeded = await self._try_acquire_or_renew()
            self._maybe_report_transition()
            if not succeeded:
                logger.debug("failed to acquire lease %s", description)
                return
            logger.info("succesfully acquired lease %s", description)
            task.cancel()

        task = asyncio.ensure_future(
            wait.jitter_loop(f, self._config.retry_period, JITTER_FACTOR, True)
        )
        await asyncio.gather(task, return_exceptions=True)
        return succeeded

    async def _renew(self):
        try:
            while True:
                description = self._config.lock.describe()
                try:
                    await wait.poll_immediate_infinite(
                        self._config.retry_period, self._try_acquire_or_renew
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.info("failed to renew lease %s: %r", description, e)
                    break
                else:
                    logger.debug("successfully renewed lease %s", description)
                finally:
                    self._maybe_report_transition()
                await asyncio.sleep(self._config.retry_period)
        finally:
            if self._config.release_on_cancel:
                await self._release()

    async def _release(self):
        if not self.is_leader():
            return True
        leader_election_record = {
            "leaderTransitions": self._observed_record["leaderTransitions"]
        }
        try:
            await self._config.lock.update(leader_election_record)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("Failed to release lock: %r", e)
            return False
        self._observed_record = leader_election_record
        self._observed_time = self._clock.now()

    async def _try_acquire_or_renew(self):
        now = time.time()
        leader_election_record = {
            "holderIdentity": self._config.lock.identity(),
            "leaseDurationSeconds": int(self._config.lease_duration),
            "renewTime": now,
            "acquireTime": now,
        }
        try:
            (
                old_leader_election_record,
                old_leader_election_raw_record,
            ) = await self._config.lock.get()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(
                "error retrieving resource lock %s: %r", self._config.lock.describe(), e
            )
            return False
        if old_leader_election_record is None:
            try:
                await self._config.lock.create(leader_election_record)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("error initially creating leader election record: %r", e)
                return False
            self._observed_record = leader_election_record
            self._observed_time = self._clock.now()
            return True

        if self._observed_raw_record != old_leader_election_raw_record:
            self._observed_record = dict(old_leader_election_record)
            self._observed_raw_record = old_leader_election_raw_record
            self._observed_time = self._clock.now()
        if (
            old_leader_election_record.get("holderIdentity")
            and self._observed_time + self._config.lease_duration > now
            and not self.is_leader()
        ):
            logger.debug(
                "lock is held by %s and has not yet expired",
                old_leader_election_record["holderIdentity"],
            )
            return False

        if self.is_leader():
            leader_election_record["acquireTime"] = old_leader_election_record[
                "acquireTime"
            ]
            leader_election_record["leaderTransitions"] = old_leader_election_record[
                "leaderTransitions"
            ]
        else:
            leader_election_record["leaderTransitions"] = (
                old_leader_election_record["leaderTransitions"] + 1
            )

        try:
            await self._config.lock.update(leader_election_record)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("Failed to update lock: %r", e)
            return False

        self._observed_record = leader_election_record
        self._observed_time = self._clock.now()
        return True

    def _maybe_report_transition(self):
        if self._observed_record.holder_identity == self._reported_leader:
            return
        self._reported_leader = self._observed_record.holder_identity
        if self._config.callbacks.on_new_leader:
            asyncio.ensure_future(
                self._config.callbacks.on_new_leader(self._reported_leader)
            )
