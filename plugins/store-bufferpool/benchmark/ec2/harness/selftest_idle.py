#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""Self-test without a node: coldbench's wait_prefetch_idle keeps waiting while
the prefetch scheduler holds items or demand reads run, names them on a timeout, and keeps the thread-pool check
on a build without the scheduler."""
import os
import sys
import types

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import coldbench  # noqa: E402

cls = next(c for c in vars(coldbench).values() if isinstance(c, type) and hasattr(c, "wait_prefetch_idle"))


def node(states, with_scheduler=True):
    n = object.__new__(cls)
    it = iter(states)
    cur = {}

    def prefetch_pool(self):
        cur.update(next(it, cur))
        return {"active": 0, "queue": 0}

    def bp_stats(self):
        if not with_scheduler:
            return {"files": {}}
        return {"prefetch_scheduler": {"pending": cur["pending"], "queued": cur["pending"], "active_workers": 0,
                                       "demand_reads_in_flight": cur["demand"]}}

    n.prefetch_pool = types.MethodType(prefetch_pool, n)
    n.bp_stats = types.MethodType(bp_stats, n)
    return n, cur


# held items with no worker: not idle until pending and demand reads are 0
n, cur = node([{"pending": 2, "demand": 1}, {"pending": 1, "demand": 0}, {"pending": 0, "demand": 1},
               {"pending": 0, "demand": 0}])
ms, p = n.wait_prefetch_idle(timeout_s=5)
assert cur == {"pending": 0, "demand": 0}, cur
# a stranded item: the timeout names the scheduler fields
n, cur = node([{"pending": 1, "demand": 0}])
try:
    n.wait_prefetch_idle(timeout_s=0.05)
    raise AssertionError("expected a timeout")
except TimeoutError as e:
    assert "'pending': 1" in str(e) and "demand_reads_in_flight" in str(e), e
# a build without the scheduler: the thread-pool check only
n, cur = node([{"pending": 5, "demand": 5}], with_scheduler=False)
n.wait_prefetch_idle(timeout_s=1)
print("wait_prefetch_idle checks passed")
