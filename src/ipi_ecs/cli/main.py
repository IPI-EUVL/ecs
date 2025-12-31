# src/ipi_ecs/cli/main.py
from __future__ import annotations

import argparse
import os
import time
from pathlib import Path
from typing import Any

from ipi_ecs.logging.journal import resolve_log_dir
from ipi_ecs.logging.logger_server import run_logger_server
from ipi_ecs.logging.reader import JournalReader
from ipi_ecs.dds.server import DDSServer

import ipi_ecs.cli.commands.echo as echo
import ipi_ecs.cli.commands.log_query as log_query
from ipi_ecs.logging.timefmt import parse_time_to_ns, fmt_ns_local
from ipi_ecs.logging.index import DEFAULT_LEVEL_MAP

ENV_LOG_DIR = "IPI_ECS_LOG_DIR"


def build_query_kwargs(args):
    ts_min = parse_time_to_ns(args.since) if args.since else None
    ts_max = parse_time_to_ns(args.until) if args.until else None

    line_min = args.line_from
    line_max = (args.line_to + 1) if args.line_to is not None else None  # inclusive -> exclusive

    min_level_num = None
    if args.min_level is not None:
        if not args.l_type:
            raise SystemExit("--min-level requires --type (ordering is type-specific)")
        min_level_num = DEFAULT_LEVEL_MAP.get(args.l_type, {}).get(args.min_level, 0)

    return dict(
        line_min=line_min,
        line_max=line_max,
        uuid=args.uuid,
        ts_min_ns=ts_min,
        ts_max_ns=ts_max,
        l_type=args.l_type,
        level=args.level,
        min_level_num=min_level_num,
        order_by=args.order_by,
        desc=args.desc,
        limit=args.limit,
    )

def fmt(line: int, rec: dict) -> str:
    origin = rec.get("origin", {}) or {}
    ts = origin.get("ts_ns")
    ou = origin.get("uuid", "?")
    l_type = rec.get("l_type", "?")
    level = rec.get("level", "?")
    msg = rec.get("msg", "")
    return f"{line:>10}  {fmt_ns_local(ts)}  [{l_type}/{level}]  {ou}: {msg}"


def cmd_logger(args: argparse.Namespace) -> int:
    log_dir = resolve_log_dir(args.log_dir, ENV_LOG_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)
    run_logger_server((args.host, args.port), log_dir, rotate_max_bytes=args.rotate_max_mb * 1024 * 1024)
    return 0


def cmd_log_show(args: argparse.Namespace) -> int:
    d = resolve_log_dir(args.log_dir, ENV_LOG_DIR)
    r = JournalReader(d)
    recs = r.read_between(args.start, args.end)
    for rec in recs:
        print(fmt(rec))
    return 0


def cmd_log_follow(args: argparse.Namespace) -> int:
    """
    Tail across segments. This does NOT require prompt_toolkit; it just prints.
    """
    d = resolve_log_dir(args.log_dir, ENV_LOG_DIR)
    r = JournalReader(d)

    # Start position: either user-provided or "near end"
    if args.start is not None:
        pos = args.start
    else:
        # best-effort: jump near the end using manifest's next_line
        pos = max(0, r.next_line - args.tail)

    while True:
        recs = r.read_between(pos, pos + args.batch)
        if recs:
            for rec in recs:
                print(fmt(rec))
            pos += len(recs)
            continue

        # no new records; wait and try again
        time.sleep(args.poll)


def cmd_log_browse(args: argparse.Namespace) -> int:
    """
    Less-like viewer using prompt_toolkit.
    """
    from prompt_toolkit.application import Application
    from prompt_toolkit.key_binding import KeyBindings
    from prompt_toolkit.layout import Layout
    from prompt_toolkit.widgets import TextArea


def cmd_log_tui(args: argparse.Namespace) -> int:
    """Launch the Textual log TUI (archives + filters + details)."""
    from ipi_ecs.logging.viewer import QueryOptions
    from ipi_ecs.cli.logging.tui_app import run_tui

    opts = QueryOptions(
        uuid=args.uuid,
        line_from=args.line_from,
        line_to=args.line_to,
        since=args.since,
        until=args.until,
        l_type=args.l_type,
        level=args.level,
        min_level=args.min_level,
        exclude_types=args.exclude_types if getattr(args, "exclude_types", None) else None,
    )

    # Default exclude REC unless user explicitly included REC or set excludes or type
    if (opts.exclude_types is None) and (opts.l_type is None) and (not getattr(args, "include_rec", False)):
        opts.exclude_types = ["REC"]
    elif getattr(args, "include_rec", False) and opts.exclude_types:
        opts.exclude_types = [x for x in opts.exclude_types if x != "REC"]

    return int(
        run_tui(
            log_dir=args.log_dir,
            env_var=ENV_LOG_DIR,
            archive=getattr(args, "archive", None),
            opts=opts,
            poll=getattr(args, "poll", 1.0),
            follow=not getattr(args, "no_follow", False),
            show_uuids=getattr(args, "show_uuids", False),
            hide_uuids=getattr(args, "never_show_uuids", False),
        )
        or 0
    )

    d = resolve_log_dir(args.log_dir, ENV_LOG_DIR)

    r = JournalReader(d)
    pos = max(0, r.next_line - args.window)

    kb = KeyBindings()
    area = TextArea(text="", read_only=True, scrollbar=True)

    def refresh_view() -> None:
        nonlocal pos
        recs = r.read_between(pos, pos + args.window)
        area.text = "\n".join(fmt(x) for x in recs) if recs else "(no data)"

    @kb.add("q")
    def _(event) -> None:
        event.app.exit()

    @kb.add("down")
    @kb.add("j")
    def _(event) -> None:
        nonlocal pos
        pos += args.step
        refresh_view()

    @kb.add("up")
    @kb.add("k")
    def _(event) -> None:
        nonlocal pos
        pos = max(0, pos - args.step)
        refresh_view()

    @kb.add("g")
    def _(event) -> None:
        nonlocal pos
        pos = 0
        refresh_view()

    @kb.add("G")
    def _(event) -> None:
        nonlocal pos
        r.refresh()
        pos = max(0, r.next_line - args.window)
        refresh_view()

    refresh_view()
    app = Application(layout=Layout(area), key_bindings=kb, full_screen=True)

    if args.follow:
        # crude follow loop integrated via prompt_toolkit's invalidate mechanism
        def follow_tick() -> None:
            nonlocal pos
            r.refresh()
            end = r.next_line
            new_pos = max(0, end - args.window)
            if new_pos != pos:
                pos = new_pos
                refresh_view()
            app.invalidate()

        # background polling
        import threading

        def bg():
            while True:
                time.sleep(args.poll)
                follow_tick()

        t = threading.Thread(target=bg, daemon=True)
        t.start()

    app.run()
    return 0

def cmd_server(args: argparse.Namespace) -> int:
    m_server = DDSServer(args.host, args.port)
    m_server.start()

    time.sleep(0.1)

    try:
        while m_server.ok():
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        m_server.close()

    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="ipi-ecs")
    sub = p.add_subparsers(dest="cmd", required=True)

    # logger
    pl = sub.add_parser("logger", help="Run the log ingestion server.")
    pl.add_argument("--host", default="0.0.0.0")
    pl.add_argument("--port", type=int, default=None)
    pl.add_argument("--log-dir", type=Path, default=None)
    pl.add_argument("--rotate-max-mb", type=int, default=256)
    pl.set_defaults(fn=cmd_logger)

    # log tools
    p_log = sub.add_parser("log", help="Log viewing tools.")
    sub_log = p_log.add_subparsers(dest="logcmd", required=True)

    ps = sub_log.add_parser("show", help="Print a range of global line numbers.")
    ps.add_argument("--log_dir", type=Path, default=None)
    ps.add_argument("--start", type=int, required=True)
    ps.add_argument("--end", type=int, required=True)
    ps.set_defaults(fn=cmd_log_show)

    pf = sub_log.add_parser("follow", help="Live stream logs (tail -F across rollovers).")
    pf.add_argument("--log_dir", type=Path, default=None)
    pf.add_argument("--start", type=int, default=None, help="Start global line (default: tail from end).")
    pf.add_argument("--tail", type=int, default=200, help="If --start not set, start from last N lines.")
    pf.add_argument("--batch", type=int, default=200)
    pf.add_argument("--poll", type=float, default=0.25)
    pf.set_defaults(fn=cmd_log_follow)

    pb = sub_log.add_parser("browse", help="Interactive browser (less-like).")
    pb.add_argument("--log_dir", type=Path, default=None)
    pb.add_argument("--window", type=int, default=10)
    pb.add_argument("--step", type=int, default=1)
    pb.add_argument("--poll", type=float, default=0.25)
    pb.add_argument("--follow", action="store_true", help="Auto-jump to the end as logs arrive.")
    pb.set_defaults(fn=cmd_log_browse)

    pt = sub_log.add_parser("tui", help="Textual TUI (archives + query builder).")
    pt.add_argument("--log_dir", type=Path, default=None)
    pt.add_argument("--archive", default=None, help="Archive name (default: persisted config / current).")

    pt.add_argument("--uuid", default=None, help="Filter by originator UUID.")
    pt.add_argument("--from", dest="line_from", type=int, default=None, help="Inclusive start global line.")
    pt.add_argument("--to", dest="line_to", type=int, default=None, help="Inclusive end global line.")
    pt.add_argument("--since", default=None, help="Start time (local if no timezone).")
    pt.add_argument("--until", default=None, help="End time (local if no timezone).")
    pt.add_argument("--type", dest="l_type", default=None, help="Filter by l_type (e.g. EXP, SOFTW).")
    pt.add_argument("--level", default=None, help="Filter by exact level string.")
    pt.add_argument("--min-level", default=None, help="Filter by minimum level (requires --type ordering rules in your viewer).")

    pt.add_argument("--exclude-type", dest="exclude_types", action="append", default=None,
                    help="Exclude an l_type (repeatable). Default excludes REC unless overridden.")
    pt.add_argument("--include-rec", action="store_true", help="Do not exclude REC by default.")

    pt.add_argument("--poll", type=float, default=1.0, help="Auto-follow polling interval in seconds.")
    pt.add_argument("--no-follow", dest="no_follow", action="store_true", help="Disable auto-follow at tail.")

    # UUID display toggles (match other commands, if present)
    pt.add_argument("--show-uuids", dest="show_uuids", action="store_true", help="Always show UUIDs.")
    pt.add_argument("--hide-uuids", dest="never_show_uuids", action="store_true", help="Never show UUIDs.")

    pt.set_defaults(fn=cmd_log_tui)

    # server
    ps = sub.add_parser("server", help="Run the ECS DDS server.")
    ps.add_argument("--host", default="0.0.0.0")
    ps.add_argument("--port", type=int, default=None)
    ps.set_defaults(fn=cmd_server)

    pe = sub.add_parser("echo", help="Echo a DDS key from a subsystem.")
    pe.add_argument("--sys", type=str)
    pe.add_argument("--hz", type=float, default=None)
    pe.add_argument("name", nargs='?', type=str, default=None)
    pe.add_argument("key", type=str)
    pe.set_defaults(fn=echo.main)

    pq = sub_log.add_parser("query", help="Query logs using the SQLite index.")
    pq.add_argument("log_dir", type=Path)

    pq.add_argument("--uuid", default=None, help="Filter by originator UUID.")

    # line filters (inclusive)
    pq.add_argument("--from", dest="line_from", type=int, default=None,
                help="Inclusive start global line number.")
    pq.add_argument("--to", dest="line_to", type=int, default=None,
                help="Inclusive end global line number.")

    # time filters (human strings, assume local if no tz in string)
    pq.add_argument("--since", default=None, help="Start time (local if no timezone).")
    pq.add_argument("--until", default=None, help="End time (local if no timezone).")

    # type/level filters
    pq.add_argument("--type", dest="l_type", default=None, help="Filter by l_type (e.g. EXP, SOFTW).")
    pq.add_argument("--level", default=None, help="Filter by exact level string.")
    pq.add_argument("--min-level", default=None,
                help="Filter by minimum level (requires --type).")

    # sorting / limiting
    pq.add_argument("--order-by", default="line",
                choices=["line", "origin_ts_ns", "ingest_ts_ns", "level_num"])
    pq.add_argument("--desc", action="store_true")
    pq.add_argument("--limit", type=int, default=None)

    pq.set_defaults(fn=log_query.cmd_log_query)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.fn(args) or 0)
