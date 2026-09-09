# /usr/bin/env python3

# SPDX-FileCopyrightText: 2026 SEANERGYS Partners <https://seanergys.eu>
#
# SPDX-License-Identifier: MIT

import argparse
import seanerbus.msg
from seanerbus.client import Connection
import seanerbus.naming
import seanerbus.time_series
import asyncio
import capnp

from seanerbus.client import MsgHandle

import seanerbus_cpp_py

from .modules.db import make_history_timeline, make_history_aggregate


# Parse a Cap'n'Proto message through introspection hacks
def parse_msg(payload_type: int, payload: bytes) -> tuple[str, object]:
    parsed_payload_type: str | None = None

    for var in vars(seanerbus.msg.msg_capnp.MessageTypes):
        if getattr(seanerbus.msg.msg_capnp.MessageTypes, var) == payload_type:
            parsed_payload_type = var

    if parsed_payload_type is None:
        raise RuntimeError(f"{payload_type} is no known payload type!")

    payload_class = parsed_payload_type[:1].upper() + parsed_payload_type[1:]

    with getattr(seanerbus.msg.msg_capnp, payload_class).from_bytes(payload) as msg:
        return (payload_class, msg)


async def make_publish(addr: str, capnp_type: str, fields: list[str]):
    payload_content = getattr(seanerbus.msg.msg_capnp, capnp_type).new_message()
    payload_type = getattr(
        seanerbus.msg.msg_capnp.MessageTypes, capnp_type[:1].lower() + capnp_type[1:]
    )

    for kv in fields:
        kv = kv.split("=")
        if len(kv) != 2:
            print(
                f"{kv}: key=value arguments need to consist of two terms separated by a '=' sign!"
            )
            exit(1)
        key: str = kv[0]
        value: str | float = kv[1]
        try:
            value = float(value)
        except ValueError:
            pass
        setattr(payload_content, key, value)

    conn = Connection("demobus://localhost:5398", "demobus://localhost:5398")
    handle = await conn.publish(addr, (payload_type, payload_content.to_bytes()))
    await handle()


async def make_subscribe(addr: str):
    conn = Connection("demobus://localhost:5398", "demobus://localhost:5398")
    handle: MsgHandle = await conn.subscribe(addr)

    while True:
        msg: seanerbus_cpp_py.Message = await handle()
        parsed_msg = parse_msg(msg.payload_type_as_int(), msg.payload())
        print(parsed_msg[0], "==", parsed_msg[1])


async def make_resolve(keyvalue: list[str]):
    resolve = {}
    for kv in keyvalue:
        kv = kv.split("=")
        if len(kv) != 2:
            print(
                f"{kv}: key=value arguments need to consist of two terms separated by a '=' sign!"
            )
            exit(1)
        resolve[kv[0]] = kv[1]
    conn = Connection("demobus://localhost:5398", "demobus://localhost:5398")
    addrs = await seanerbus.naming.resolve(conn, resolve)

    print(f"Resolved {resolve} to:")
    for addr in addrs:
        print(f"\t{addr}")


async def make_rev_resolve(addr: str):
    conn = Connection("demobus://localhost:5398", "demobus://localhost:5398")
    kvs = await seanerbus.naming.reverse_resolve(conn, addr)
    print(f"'{addr} => {kvs}")


async def make_reg_resolve(addr: str, keyvalue: list[str]):
    name: dict[str, str] = {}
    for kv in keyvalue:
        kv = kv.split("=")
        if len(kv) != 2:
            print(
                f"{kv}: key=value arguments need to consist of two terms separated by a '=' sign!"
            )
            exit(1)
        name[kv[0]] = kv[1]

    conn = Connection("demobus://localhost:5398", "demobus://localhost:5398")
    await seanerbus.naming.register_resolve(conn, name, addr)


async def make_unreg_resolve(addr: str):
    conn = Connection("demobus://localhost:5398", "demobus://localhost:5398")
    await seanerbus.naming.unregister_resolve(conn, addr)


def main():
    parser = argparse.ArgumentParser(
        prog="SeanerBUS CLI interface",
        description="Command line tool for interacting with the SeanerBUS",
    )
    subparsers = parser.add_subparsers(dest="cmd")

    subscribe_parser = subparsers.add_parser(
        "subscribe", help="Subscribe to some address"
    )
    _ = subscribe_parser.add_argument(
        "-a", "--addr", help="Address to subscribe to", required=True
    )

    publish_parser = subparsers.add_parser("publish", help="Publish a message")
    _ = publish_parser.add_argument(
        "-t", "--type", help="SeanerBUS payload type to publish", required=True
    )
    _ = publish_parser.add_argument(
        "-a", "--addr", help="UUID address to publish to", required=True
    )
    _ = publish_parser.add_argument(
        "keyvalue", nargs="*", help="key=value pairs to put into the payload"
    )

    resolve_parser = subparsers.add_parser("resolve", help="Resolve a name")
    _ = resolve_parser.add_argument(
        "keyvalue", nargs="*", help="key=value pairs of the name to resolve"
    )

    rev_resolve_parser = subparsers.add_parser(
        "reverse-resolve", help="Reverse resolve address to name"
    )
    _ = rev_resolve_parser.add_argument("addr", help="Address to resolve back")

    reg_resolve_parser = subparsers.add_parser(
        "register-resolve", help="Reverse resolve address to name"
    )

    _ = reg_resolve_parser.add_argument("addr", help="Address to register")
    _ = reg_resolve_parser.add_argument(
        "keyvalue", nargs="*", help="name given to the address as key=value pairs"
    )

    unreg_resolve_parser = subparsers.add_parser(
        "unregister-resolve", help="Reverse resolve address to name"
    )
    _ = unreg_resolve_parser.add_argument(
        "addr", help="Address to deregister from naming service"
    )

    hist_aggr_req_parser = subparsers.add_parser(
        "history-aggregate", help="Request one aggregate of time-series data"
    )
    _ = hist_aggr_req_parser.add_argument(
        "addr", help="Address to deregister from naming service"
    )
    _ = hist_aggr_req_parser.add_argument(
        "-s", "--start", help="Start time for history request", required=True
    )
    _ = hist_aggr_req_parser.add_argument(
        "-e", "--end", help="End time for history request", required=True
    )
    _ = hist_aggr_req_parser.add_argument(
        "-i", "--interval-max", help="Interval Max", required=True
    )

    hist_timeline_req_parser = subparsers.add_parser(
        "history-timeline", help="Request a timeline of  time-series data"
    )
    _ = hist_timeline_req_parser.add_argument(
        "addr", help="Address to deregister from naming service"
    )
    _ = hist_timeline_req_parser.add_argument(
        "-s", "--start", help="Start time for history request"
    )
    _ = hist_timeline_req_parser.add_argument(
        "-e", "--end", help="End time for history request"
    )
    _ = hist_timeline_req_parser.add_argument(
        "-i", "--interval-max", help="Interval Max"
    )

    args = parser.parse_args()

    cmd: str = args.cmd

    # print(vars(seanerbus.msg.msg_capnp.MessageTypes))
    if cmd == "publish":
        asyncio.run(capnp.run(make_publish(args.addr, args.type, args.keyvalue)))

    elif cmd == "subscribe":
        asyncio.run(capnp.run(make_subscribe(args.addr)))

    elif cmd == "resolve":
        asyncio.run(capnp.run(make_resolve(args.keyvalue)))

    elif cmd == "reverse-resolve":
        asyncio.run(capnp.run(make_rev_resolve(args.addr)))

    elif cmd == "register-resolve":
        asyncio.run(capnp.run(make_reg_resolve(args.addr, args.keyvalue)))

    elif cmd == "unregister-resolve":
        asyncio.run(capnp.run(make_unreg_resolve(args.addr)))

    elif cmd == "history-aggregate":
        asyncio.run(
            capnp.run(
                make_history_aggregate(
                    args.addr, int(args.start), int(args.end), int(args.interval_max)
                )
            )
        )

    elif cmd == "history-timeline":
        asyncio.run(
            capnp.run(
                make_history_timeline(
                    args.addr, int(args.start), int(args.end), int(args.interval_max)
                )
            )
        )
    else:
        parser.print_help()
