# SPDX-FileCopyrightText: 2026 SEANERGYS Partners <https://seanergys.eu>
#
# SPDX-License-Identifier: MIT

from seanerbus.client import Connection
from seanerbus.time_series import get_timeline, get_aggregate


async def make_history_aggregate(
    entity: str, start_time: int, end_time: int, interval_max: int
):
    conn = Connection("seanerbroker://localhost:5398", "seanerbroker://localhost:5398")

    aggr = await get_aggregate(conn, entity, start_time, end_time, interval_max)

    print(aggr)


async def make_history_timeline(
    entity: str, start_time: int, end_time: int, interval_max: int
):
    conn = Connection("seanerbroker://localhost:5398", "seanerbroker://localhost:5398")
    timeline = await get_timeline(conn, entity, start_time, end_time, interval_max)

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
