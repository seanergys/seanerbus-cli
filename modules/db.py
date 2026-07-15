# SPDX-FileCopyrightText: 2026 SEANERGYS Partners <https://seanergys.eu>
#
# SPDX-License-Identifier: MIT

from seanerbus.client import Connection
from seanerbus.time_series import get_timeline, get_aggregate

import uuid


async def make_history_aggregate(args):
    addr = uuid.UUID(args.addr)
    start_time = int(args.start)
    end_time = int(args.end)
    interval_max = int(args.interval_max)

    conn = await Connection.connect("localhost", 5398)

    aggr = await get_aggregate(conn, addr, start_time, end_time, interval_max)

    print(aggr)


async def make_history_timeline(args):
    addr = uuid.UUID(args.addr)
    start_time = int(args.start)
    end_time = int(args.end)
    interval_max = int(args.interval_max)

    conn = await Connection.connect("localhost", 5398)

    timeline = await get_timeline(conn, addr, start_time, end_time, interval_max)

    if timeline.type == "raw":
        print("time, value")
        for t, v in zip(timeline.time_delta, timeline.raw):
            print(f"{t}, {v}")

    if timeline.type == "aggr":
        print("time, minimum, maximum, sum, count, integral, active_time")
        for t, v in zip(timeline.time_delta, timeline.aggr):
            print(
                f"{t}, {v.minimum}, {v.maximum}, {v.sum}, {v.count}, {v.integral}, {v.active_time}"
            )
