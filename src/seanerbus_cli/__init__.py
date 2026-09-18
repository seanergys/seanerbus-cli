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


def kv_list_to_dict(kv_list: list[str]) -> dict[str, str]:
    res = {}
    for kv in kv_list:
        kv = kv.split("=")
        if len(kv) != 2:
            print(
                f"{kv}: key=value arguments need to consist of two terms separated by a '=' sign!"
            )
            exit(1)
        key: str = kv[0]
        value: str | float = kv[1]
        res[key] = value
    return res


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


async def make_get(keyvalue: list[str]):
    name = kv_list_to_dict(keyvalue)

    conn = Connection("demobus://localhost:5398", "demobus://localhost:5398")
    entities = await seanerbus.naming.resolve(conn, name)

    if len(entities) != 1:
        raise RuntimeError(f"Could not resolve {name} to a single endpoint!")

    handle = await conn.request(entities[0], seanerbus.msg.GetReqV1().to_capnp())

    msg = await handle()

    if msg.payload_type_as_int() == seanerbus.msg.msg_capnp.MessageTypes.errorResV1:
        err = seanerbus.msg.ErrorResV1.from_capnp(msg)
        raise RuntimeError(f"Getting a value failed: {err.what}")

    if msg.payload_type_as_int() == seanerbus.msg.msg_capnp.MessageTypes.getResV1:
        res = seanerbus.msg.GetResV1.from_capnp(msg)
        print(f"Value of {name} is: {res.value}")
        return

    raise RuntimeError(
        f"Expected ErrorResV1/GetResV1, got {msg.payload_type_as_int()} instead"
    )


async def make_set(keyvalue: list[str], value: int):
    name = kv_list_to_dict(keyvalue)

    conn = Connection("demobus://localhost:5398", "demobus://localhost:5398")
    entities = await seanerbus.naming.resolve(conn, name)

    if len(entities) != 1:
        raise RuntimeError(f"Could not resolve {name} to a single endpoint!")

    handle = await conn.request(entities[0], seanerbus.msg.SetReqV1(value).to_capnp())

    msg = await handle()

    if msg.payload_type_as_int() == seanerbus.msg.msg_capnp.MessageTypes.errorResV1:
        err = seanerbus.msg.ErrorResV1.from_capnp(msg)

        if err.error_code != 0:
            raise RuntimeError(f"Setting a value failed: {err.what}")
        else:
            print("Succesfully set value!")
            return

    raise RuntimeError(f"Expected ErrorResV1 got {msg.payload_type_as_int()} instead")


async def make_publish(entity: str, capnp_type: str, fields: list[str]):
    payload_content = getattr(seanerbus.msg.msg_capnp, capnp_type).new_message()
    payload_type = getattr(
        seanerbus.msg.msg_capnp.MessageTypes, capnp_type[:1].lower() + capnp_type[1:]
    )

    attrs = kv_list_to_dict(fields)

    for key, value in attrs:
        try:
            value = float(value)
        except ValueError:
            pass
        setattr(payload_content, key, value)

    conn = Connection("demobus://localhost:5398", "demobus://localhost:5398")
    handle = await conn.publish(entity, (payload_type, payload_content.to_bytes()))
    await handle()


async def make_subscribe(entities: str):
    conn = Connection("demobus://localhost:5398", "demobus://localhost:5398")
    handle: MsgHandle = await conn.subscribe(entities)

    while True:
        msg: seanerbus_cpp_py.Message = await handle()
        parsed_msg = parse_msg(msg.payload_type_as_int(), msg.payload())
        print(parsed_msg[0], "==", parsed_msg[1])


async def make_resolve(keyvalue: list[str]):
    resolve = kv_list_to_dict(keyvalue)

    conn = Connection("demobus://localhost:5398", "demobus://localhost:5398")
    entities = await seanerbus.naming.resolve(conn, resolve)

    print(f"Resolved {resolve} to:")
    for num, entity in enumerate(entities["entities"]):
        if num < len(entities["resolved_terms"]):
            print(f"\t{entity} (Resolved Terms: {entities['resolved_terms'][num]})")
        else:
            print(f"\t{entity}")


async def make_rev_resolve(entity: str):
    conn = Connection("demobus://localhost:5398", "demobus://localhost:5398")
    kvs = await seanerbus.naming.reverse_resolve(conn, entity)
    print(f"'{entity} => {kvs}")


async def make_reg_resolve(entity: str, keyvalue: list[str]):
    name = kv_list_to_dict(keyvalue)

    conn = Connection("demobus://localhost:5398", "demobus://localhost:5398")
    await seanerbus.naming.register_resolve(conn, name, entity)


async def make_unreg_resolve(entity: str):
    conn = Connection("demobus://localhost:5398", "demobus://localhost:5398")
    await seanerbus.naming.unregister_resolve(conn, entity)


def main():
    parser = argparse.ArgumentParser(
        prog="SeanerBUS CLI interface",
        description="Command line tool for interacting with the SeanerBUS",
    )
    subparsers = parser.add_subparsers(dest="cmd")

    subscribe_parser = subparsers.add_parser(
        "subscribe", help="Subscribe to some entity"
    )
    _ = subscribe_parser.add_argument(
        "-e", "--entity", help="Entity to subscribe to", required=True
    )

    publish_parser = subparsers.add_parser("publish", help="Publish a message")
    _ = publish_parser.add_argument(
        "-t", "--type", help="SeanerBUS payload type to publish", required=True
    )
    _ = publish_parser.add_argument(
        "-e", "--entity", help="Entity to publish for", required=True
    )
    _ = publish_parser.add_argument(
        "keyvalue", nargs="*", help="key=value pairs to put into the payload"
    )

    resolve_parser = subparsers.add_parser("resolve", help="Resolve a name")
    _ = resolve_parser.add_argument(
        "keyvalue", nargs="*", help="key=value pairs of the name to resolve"
    )

    rev_resolve_parser = subparsers.add_parser(
        "reverse-resolve", help="Reverse resolve entity to name"
    )
    _ = rev_resolve_parser.add_argument("entity", help="Entity to resolve back")

    reg_resolve_parser = subparsers.add_parser(
        "register-resolve", help="Reverse resolve entity to name"
    )

    _ = reg_resolve_parser.add_argument("entity", help="Entity to register")
    _ = reg_resolve_parser.add_argument(
        "keyvalue", nargs="*", help="name given to the entity as key=value pairs"
    )

    unreg_resolve_parser = subparsers.add_parser(
        "unregister-resolve", help="Reverse resolve entity to name"
    )
    _ = unreg_resolve_parser.add_argument(
        "entity", help="Entity to deregister from naming service"
    )

    hist_aggr_req_parser = subparsers.add_parser(
        "history-aggregate", help="Request one aggregate of time-series data"
    )
    _ = hist_aggr_req_parser.add_argument(
        "entity", help="Entity to deregister from naming service"
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
        "entity", help="Entity to query historic timeline data from"
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

    get_req_parser = subparsers.add_parser("get", help="Get a metric reading")
    _ = get_req_parser.add_argument(
        "keyvalue", nargs="*", help="name of the metric to query as key=value pairs"
    )

    set_req_parser = subparsers.add_parser("set", help="Set an actuator")
    _ = set_req_parser.add_argument(
        "keyvalue", nargs="*", help="name of the metric to query as as key=value pairs"
    )
    _ = set_req_parser.add_argument("-v", "--value", help="Value to set", required=True)

    args = parser.parse_args()

    cmd: str = args.cmd

    # print(vars(seanerbus.msg.msg_capnp.MessageTypes))
    if cmd == "publish":
        asyncio.run(capnp.run(make_publish(args.entity, args.type, args.keyvalue)))

    elif cmd == "subscribe":
        asyncio.run(capnp.run(make_subscribe(args.entity)))

    elif cmd == "resolve":
        asyncio.run(capnp.run(make_resolve(args.keyvalue)))

    elif cmd == "reverse-resolve":
        asyncio.run(capnp.run(make_rev_resolve(args.entity)))

    elif cmd == "register-resolve":
        asyncio.run(capnp.run(make_reg_resolve(args.entity, args.keyvalue)))

    elif cmd == "unregister-resolve":
        asyncio.run(capnp.run(make_unreg_resolve(args.entity)))

    elif cmd == "history-aggregate":
        asyncio.run(
            capnp.run(
                make_history_aggregate(
                    args.entity, int(args.start), int(args.end), int(args.interval_max)
                )
            )
        )

    elif cmd == "history-timeline":
        asyncio.run(
            capnp.run(
                make_history_timeline(
                    args.entity, int(args.start), int(args.end), int(args.interval_max)
                )
            )
        )
    elif cmd == "get":
        asyncio.run(capnp.run(make_get(args.keyvalue)))
    elif cmd == "set":
        asyncio.run(capnp.run(make_set(args.keyvalue, int(args.value))))
    else:
        parser.print_help()
