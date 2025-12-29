# src/ipi_ecs/cli/main.py
from __future__ import annotations

import argparse
import time
from pathlib import Path

from ipi_ecs.logging.logger_server import run_logger_server
from ipi_ecs.logging.viewer import LogViewer, QueryOptions, format_line
from ipi_ecs.logging.timefmt import fmt_ns_local
from ipi_ecs.dds.server import DDSServer

import ipi_ecs.cli.commands.echo as echo

ENV_LOG_DIR = "IPI_ECS_LOG_DIR"

# ----------------------------
# Commands
# ----------------------------
def cmd_logger(args: argparse.Namespace) -> int:
    """
    Intentionally pass args.log_dir through as-is (can be None).
    Let logger_server.py decide fallback to env var / platformdirs.
    """
    run_logger_server(
        (args.host, args.port),
        args.log_dir,  # may be None
        rotate_max_bytes=args.rotate_max_mb * 1024 * 1024,
    )
    return 0

def cmd_log_show(args: argparse.Namespace) -> int:
    """
    Print logs in [start, end) by absolute global line number.
    """
    viewer = LogViewer(args.log_dir, env_var=ENV_LOG_DIR)
    view = viewer.open_archive(args.archive)
    opts = QueryOptions(line_from=args.start, line_to=args.end - 1)
    for ln in view.query(opts):
        print(format_line(ln))
    return 0

def cmd_log_query(args: argparse.Namespace) -> int:
    viewer = LogViewer(args.log_dir, env_var=ENV_LOG_DIR)
    view = viewer.open_archive(args.archive)
    opts = QueryOptions(
        uuid=args.uuid,
        line_from=args.line_from,
        line_to=args.line_to,
        since=args.since,
        until=args.until,
        l_type=args.l_type,
        level=args.level,
        min_level=args.min_level,
        order_by=args.order_by,
        desc=args.desc,
        limit=args.limit,
    )
    for ln in view.query(opts):
        print(format_line(ln))
    return 0

def cmd_log_follow(args: argparse.Namespace) -> int:
    viewer = LogViewer(args.log_dir, env_var=ENV_LOG_DIR)
    view = viewer.open_archive(args.archive)
    opts = QueryOptions(
        uuid=args.uuid,
        line_from=args.line_from,
        line_to=args.line_to,
        since=args.since,
        until=args.until,
        l_type=args.l_type,
        level=args.level,
        min_level=args.min_level,
    )
    for ln in view.follow(opts, tail=args.tail, batch=args.batch, poll=args.poll):
        print(format_line(ln))
    return 0

def cmd_log_browse(args: argparse.Namespace) -> int:
    """
    Less-like viewer using prompt_toolkit, backed by the SQLite index.
    """
    from prompt_toolkit.application import Application
    from prompt_toolkit.key_binding import KeyBindings
    from prompt_toolkit.layout import Layout
    from prompt_toolkit.widgets import TextArea

    viewer = LogViewer(args.log_dir, env_var=ENV_LOG_DIR)
    view = viewer.open_archive(args.archive)

    base = QueryOptions(
        uuid=args.uuid,
        line_from=args.line_from,
        line_to=args.line_to,
        since=args.since,
        until=args.until,
        l_type=args.l_type,
        level=args.level,
        min_level=args.min_level,
    )

    window = args.window
    poll = args.poll

    # Start at end
    rows = view.window_before(base, line_max_exclusive=view.next_line(), window=window)

    kb = KeyBindings()
    area = TextArea(text="", read_only=True, scrollbar=True)

    cur_first = rows[0].line if rows else 0
    cur_last = rows[-1].line if rows else 0

    def render(rs):
        nonlocal cur_first, cur_last
        if not rs:
            area.text = "(no data)"
            return
        cur_first = rs[0].line
        cur_last = rs[-1].line
        area.text = "".join(format_line(x) for x in rs)

    def refresh_to_end():
        rs = view.window_before(base, line_max_exclusive=view.next_line(), window=window)
        render(rs)

    render(rows)

    @kb.add("q")
    def _(event):
        event.app.exit()

    @kb.add("down")
    @kb.add("j")
    def _(event):
        rs = view.window_after(base, line_min_inclusive=cur_last + 1, window=window)
        if rs:
            render(rs)

    @kb.add("up")
    @kb.add("k")
    def _(event):
        rs = view.window_before(base, line_max_exclusive=cur_first, window=window)
        if rs:
            render(rs)

    @kb.add("g")
    def _(event):
        start = base.line_from or 0
        rs = view.window_after(base, line_min_inclusive=start, window=window)
        if rs:
            render(rs)

    @kb.add("G")
    def _(event):
        refresh_to_end()

    app = Application(layout=Layout(area), key_bindings=kb, full_screen=True)

    if args.follow:
        import threading

        def bg():
            last_seen = view.next_line()
            while True:
                time.sleep(poll)
                now = view.next_line()
                if now != last_seen:
                    last_seen = now
                    refresh_to_end()
                    app.invalidate()

        threading.Thread(target=bg, daemon=True).start()

    app.run()
    return 0

def cmd_log_archive(args: argparse.Namespace) -> int:
    viewer = LogViewer(args.log_dir, env_var=ENV_LOG_DIR)
    info = viewer.archive_current(args.name)
    print(f"Archived to: {info.name}")
    print(f"Range: [{info.start_line}, {info.end_line_exclusive})")
    return 0

def cmd_log_archives(args: argparse.Namespace) -> int:
    viewer = LogViewer(args.log_dir, env_var=ENV_LOG_DIR)
    items = viewer.list_archives(since=args.since, until=args.until)
    if not items:
        print("(no archives)")
        return 0

    print(f"{'ARCHIVE':<20} {'LINES':<24} {'START':<23} {'END':<23}")
    for a in items:
        line_range = f"{a.start_line}-{max(a.start_line, a.end_line_exclusive-1)}"
        print(f"{a.name:<20} {line_range:<24} {fmt_ns_local(a.start_ts_ns):<23} {fmt_ns_local(a.end_ts_ns):<23}")

    return 0

def cmd_log_locate(args: argparse.Namespace) -> int:
    viewer = LogViewer(args.log_dir, env_var=ENV_LOG_DIR)
    info = viewer.locate_line(int(args.line))
    if info is None:
        print("(not found)")
        return 1
    print(f"{args.line} is in archive '{info.name}' ({info.path})  range=[{info.start_line}, {info.end_line_exclusive})")
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


# ----------------------------
# Argparse helpers
# ----------------------------
def add_archive_arg(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--archive",
        default=None,
        help="Archive name under <log_root>/archives (or 'current'). Defaults to current.",
    )


def add_log_dir_arg(p: argparse.ArgumentParser, *, help_text: str = "Log directory (default: env var / platform default).") -> None:
    # positional OR optional, same dest; optional overrides if provided
    #p.add_argument("log_dir", nargs="?", type=Path, default=None)
    p.add_argument("--log_dir", dest="log_dir", type=Path, default=None, help=help_text)


def add_log_filters(p: argparse.ArgumentParser) -> None:
    p.add_argument("--uuid", default=None, help="Filter by originator UUID.")

    # line filters (inclusive in CLI)
    p.add_argument("--from", dest="line_from", type=int, default=None, help="Inclusive start global line number.")
    p.add_argument("--to", dest="line_to", type=int, default=None, help="Inclusive end global line number.")

    # time filters (human strings, assume local if no tz in string)
    p.add_argument("--since", default=None, help="Start time (assume local if no timezone).")
    p.add_argument("--until", default=None, help="End time (assume local if no timezone).")

    # type/level filters
    p.add_argument("--type", dest="l_type", default=None, help="Filter by l_type (e.g. EXP, SOFTW).")
    p.add_argument("--level", default=None, help="Filter by exact level string.")
    p.add_argument("--min-level", default=None, help="Filter by minimum level (requires --type).")


def add_log_sorting(p: argparse.ArgumentParser) -> None:
    p.add_argument("--order-by", default="line", choices=["line", "origin_ts_ns", "ingest_ts_ns", "level_num"])
    p.add_argument("--desc", action="store_true")
    p.add_argument("--limit", type=int, default=None)


# ----------------------------
# Argparse
# ----------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="ipi-ecs")
    sub = p.add_subparsers(dest="cmd", required=True)

    # logger
    pl = sub.add_parser("logger", help="Run the log ingestion server.")
    pl.add_argument("--host", default="0.0.0.0")
    pl.add_argument("--port", type=int, default=None)
    pl.add_argument("--log-dir", "--log_dir", dest="log_dir", type=Path, default=None)
    pl.add_argument("--rotate-max-mb", type=int, default=256)
    pl.set_defaults(fn=cmd_logger)

    # log tools
    p_log = sub.add_parser("log", help="Log viewing tools.")
    sub_log = p_log.add_subparsers(dest="logcmd", required=True)

    ps = sub_log.add_parser("show", help="Print logs in [start, end) global line range.")
    add_log_dir_arg(ps)
    ps.add_argument("--start", type=int, required=True)
    ps.add_argument("--end", type=int, required=True)
    ps.set_defaults(fn=cmd_log_show)

    pq = sub_log.add_parser("query", help="Query logs using the SQLite index.")
    add_archive_arg(pq)
    add_log_dir_arg(pq)
    add_log_filters(pq)
    add_log_sorting(pq)
    pq.set_defaults(fn=cmd_log_query)

    pf = sub_log.add_parser("follow", help="Live stream logs (tail -F across rollovers).")
    add_archive_arg(pf)
    add_log_dir_arg(pf)
    add_log_filters(pf)
    pf.add_argument("--tail", type=int, default=200, help="If --from not set, start from last N lines.")
    pf.add_argument("--batch", type=int, default=200)
    pf.add_argument("--poll", type=float, default=0.25)
    pf.set_defaults(fn=cmd_log_follow)

    pb = sub_log.add_parser("browse", help="Interactive browser (less-like) using prompt_toolkit.")
    add_archive_arg(pb)
    add_log_dir_arg(pb)
    add_log_filters(pb)
    pb.add_argument("--window", type=int, default=200)
    pb.add_argument("--poll", type=float, default=0.25)
    pb.add_argument("--follow", action="store_true", help="Auto-jump to the end as logs arrive.")
    pb.set_defaults(fn=cmd_log_browse)

    pa = sub_log.add_parser("archive", help="Move current/ into archives/ and start a new empty current/.")
    add_log_dir_arg(pa, help_text="Log root directory (default: env var / platform default).")
    pa.add_argument("--name", default=None, help="Archive name (default: YYYY-MM-DD_NNN).")
    pa.set_defaults(fn=cmd_log_archive)

    pls = sub_log.add_parser("archives", help="List available archives under log_root/archives.")
    add_log_dir_arg(pls, help_text="Log root directory (default: env var / platform default).")
    pls.add_argument("--since", default=None, help="Only show archives overlapping start time (human string).")
    pls.add_argument("--until", default=None, help="Only show archives overlapping end time (human string).")
    pls.set_defaults(fn=cmd_log_archives)

    ploc = sub_log.add_parser("locate", help="Find which archive contains a given global line number.")
    add_log_dir_arg(ploc, help_text="Log root directory (default: env var / platform default).")
    ploc.add_argument("--line", type=int, required=True)
    ploc.set_defaults(fn=cmd_log_locate)

    # server
    ps = sub.add_parser("server", help="Run the ECS DDS server.")
    ps.add_argument("--host", default="0.0.0.0")
    ps.add_argument("--port", type=int, default=None)
    ps.set_defaults(fn=cmd_server)

    # echo
    pe = sub.add_parser("echo", help="Echo a DDS key from a subsystem.")
    pe.add_argument("--sys", type=str)
    pe.add_argument("--key", type=str)
    pe.add_argument("--name", type=str, default=None)
    pe.set_defaults(fn=echo.main)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.fn(args) or 0)
