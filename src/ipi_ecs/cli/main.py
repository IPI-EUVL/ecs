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

ENV_LOG_DIR = "IPI_ECS_LOG_DIR"


def fmt(rec: dict[str, Any]) -> str:
    lvl = rec.get("level", "?")
    msg = rec.get("msg", "")
    origin = rec.get("origin", {})
    ou = origin.get("uuid", "?")
    ts = origin.get("ts_ns", None)
    data = rec.get("data", {})
    # compact one-liner; you can prettify later
    return f"[{lvl}] {ts} {ou}: {msg} | {data}"


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


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="ipi-ecs")
    sub = p.add_subparsers(dest="cmd", required=True)

    # logger
    pl = sub.add_parser("logger", help="Run the log ingestion server.")
    pl.add_argument("--host", default="0.0.0.0")
    pl.add_argument("--port", type=int, default=5556)
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

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.fn(args) or 0)
