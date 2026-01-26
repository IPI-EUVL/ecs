# src/ipi_ecs/cli/main.py
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from ipi_ecs.logging.logger_server import run_logger_server
from ipi_ecs.logging.viewer import LogViewer, QueryOptions, format_line, PT_STYLE, RICH_TYPE_STYLE, RICH_MESSAGE_STYLE, get_subsystem, resolve_log_dir
from ipi_ecs.logging.timefmt import fmt_ns_local
from ipi_ecs.logging.reader import JournalReader

import ipi_ecs.cli.commands.call_event as call_event
import ipi_ecs.cli.commands.echo as echo
import ipi_ecs.cli.commands.write as write
import ipi_ecs.cli.commands.keyboard_jog as keyboard_jog
import ipi_ecs.cli.commands.server as server

ENV_LOG_DIR = "IPI_ECS_LOG_DIR"



# Optional colored output for non-interactive commands (query/follow/show).
try:
    from rich.console import Console
    from rich.text import Text
except Exception:  # pragma: no cover
    Console = None  # type: ignore
    Text = None  # type: ignore

_RICH_CONSOLE = Console() if Console is not None else None

def _resolve_archive_dir(cli_log_dir: Path | None, archive: str | None) -> Path:
    """Resolve an archive directory.

    - If cli_log_dir points directly at an archive (contains index.sqlite3), honor it.
    - Otherwise treat cli_log_dir as the *log root* and resolve "current" or "archives/<name>".
    """
    root = resolve_log_dir(cli_log_dir, ENV_LOG_DIR)

    # If user points directly at an archive, honor it.
    if (root / "index.sqlite3").exists():
        return root

    name = archive or "current"
    if name == "current":
        return root / "current"

    return root / "archives" / name

def _open_index_for_archive_dir(archive_dir: Path):
    from ipi_ecs.logging.index import SQLiteIndex  # local import keeps CLI startup light
    db_path = archive_dir / "index.sqlite3"
    if not db_path.exists():
        raise SystemExit(f"index.sqlite3 not found in archive dir: {archive_dir}")
    return SQLiteIndex(db_path)


def _use_color(args: argparse.Namespace) -> bool:
    mode = getattr(args, "color", "auto")
    if mode == "never":
        return False
    if mode == "always":
        return True
    # auto
    return bool(sys.stdout.isatty()) and _RICH_CONSOLE is not None


def print_log_line_rich(ln, *, show_uuids: bool, hide_uuids: bool) -> None:
    """
    Print a LogLine with coloring and subsystem display using Rich (Windows-friendly).
    Falls back to plain text if Rich isn't available.
    """
    if _RICH_CONSOLE is None:
        print(format_line(ln))
        return

    rec = ln.record
    origin = rec.get("origin", {}) or {}
    ts_ns = origin.get("ts_ns")
    uuid = origin.get("uuid", "?")
    l_type = rec.get("l_type", "?")
    level = rec.get("level", "?")
    msg = rec.get("msg", "")

    subsystem = get_subsystem(rec)

    sev_style = RICH_TYPE_STYLE.get(level, "")

    t = Text()
    t.append(f"{ln.line:>10}  ", style="dim")
    t.append(f"{fmt_ns_local(ts_ns)}  ", style="dim")
    t.append(f"[{l_type}/{level}]  ", style=sev_style)

    if (show_uuids or subsystem == "(no subsystem)") and not hide_uuids:
        t.append(f"{uuid} ", style="magenta")

    t.append(f"{subsystem}: ", style="cyan" if subsystem != "(no subsystem)" else "magenta")
    t.append(msg, style=RICH_MESSAGE_STYLE.get(level, ""))

    _RICH_CONSOLE.print(t)



def _default_exclude_types(args: argparse.Namespace) -> list[str]:
    # If the user explicitly requests a type, don't apply default exclusions.
    if getattr(args, "l_type", None) is not None:
        return list(getattr(args, "exclude_types", []) or [])
    
    # If user asked to include rec, also don't apply default rec exclusion.
    if getattr(args, "include_rec", False):
        return list(getattr(args, "exclude_types", []) or [])
    
    # If the user provided any explicit exclude-type flags, respect them.
    ex = list(getattr(args, "exclude_types", []) or [])
    if ex:
        if "REC" not in ex:
            ex.append("REC")  # Add default exclude unless specifically included
        return ex
    
    # Default: hide telemetry from human-facing views.
    return ["REC"]


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
        resolve_log_dir(args.log_dir, env_var=ENV_LOG_DIR),
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
    use_color = _use_color(args)
    show_uuids = getattr(args, 'show_uuids', False)
    hide_uuids = getattr(args, 'never_show_uuids', False)


    for ln in view.query(opts):
        print_log_line_rich(ln, show_uuids=show_uuids, hide_uuids=hide_uuids) if use_color else print(format_line(ln))
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
        exclude_types=_default_exclude_types(args),
        level=args.level,
        min_level=args.min_level,
        order_by=args.order_by,
        desc=args.desc,
        limit=args.limit,
    )
    use_color = _use_color(args)
    show_uuids = getattr(args, 'show_uuids', False)
    hide_uuids = getattr(args, 'never_show_uuids', False)


    for ln in view.query(opts):
        print_log_line_rich(ln, show_uuids=show_uuids, hide_uuids=hide_uuids) if use_color else print(format_line(ln))
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
        exclude_types=_default_exclude_types(args),
        level=args.level,
        min_level=args.min_level,
    )
    use_color = _use_color(args)
    show_uuids = getattr(args, 'show_uuids', False)
    hide_uuids = getattr(args, 'never_show_uuids', False)


    for ln in view.follow(opts, tail=args.tail, batch=args.batch, poll=args.poll):
        print_log_line_rich(ln, show_uuids=show_uuids, hide_uuids=hide_uuids) if use_color else print(format_line(ln))
    return 0

def cmd_log_browse(args: argparse.Namespace) -> int:
    """
    Less-like viewer using prompt_toolkit, backed by the SQLite index.

    - Starts at the end (latest logs within the *filtered* range).
    - Up/Down (k/j) scroll by ONE matching log line indefinitely.
    - PageUp/PageDown jump by one window.
    - g jumps to start (first matching line); G jumps to end (last page); q quits.

    Rendering:
      Uses prompt_toolkit formatted text so colors work on Windows (no ANSI hacks).
    """
    from prompt_toolkit.application import Application
    from prompt_toolkit.application.current import get_app
    from prompt_toolkit.formatted_text import FormattedText
    from prompt_toolkit.key_binding import KeyBindings
    from prompt_toolkit.layout import Layout, Window
    from prompt_toolkit.layout.controls import FormattedTextControl
    from prompt_toolkit.layout.margins import ScrollbarMargin
    from prompt_toolkit.styles import Style

    import shutil

    viewer = LogViewer(args.log_dir, env_var=ENV_LOG_DIR)
    view = viewer.open_archive(args.archive)

    base = QueryOptions(
        uuid=args.uuid,
        line_from=args.line_from,
        line_to=args.line_to,
        since=args.since,
        until=args.until,
        l_type=args.l_type,
        exclude_types=_default_exclude_types(args),
        level=args.level,
        min_level=args.min_level,
    )

    kb = KeyBindings()

    # Backing storage for the current page (list[LogLine])
    rows = []
    top_line: int | None = None

    # FormattedText fragments for display
    fragments: list[tuple[str, str]] = []

    always_show_uuids = args.show_uuids
    never_show_uuids = args.never_show_uuids

    def page_size() -> int:
        """
        Number of log lines to render per screen page.
        Prefer prompt_toolkit's size when running, otherwise use OS terminal size.
        """
        try:
            rows_term = get_app().output.get_size().rows
            return max(5, min(args.window, max(5, rows_term - 1)))
        except Exception:
            # fallback for initial load before app is running
            h = getattr(shutil.get_terminal_size(fallback=(80, 24)), "lines", 24)
            return max(5, min(args.window, max(5, h - 1)))

    def end_exclusive() -> int:
        """Exclusive upper bound for browsing, respecting --to if provided."""
        end = view.next_line()
        if base.line_to is not None:
            end = min(end, base.line_to + 1)
        return end

    def _line_fragments(x) -> list[tuple[str, str]]:
        rec = x.record
        origin = rec.get("origin", {}) or {}
        ts = origin.get("ts_ns")
        ou = origin.get("uuid", "?")
        l_type = rec.get("l_type", "?")
        level = rec.get("level", "?")
        msg = rec.get("msg", "")
        s = get_subsystem(rec)

        #print(rec)
        #print(s)

        sev_style = f"class:log.{level}"

        out: list[tuple[str, str]] = []
        out.append(("class:log.lineno", f"{x.line:>10} "))
        out.append(("class:log.ts", f"{fmt_ns_local(ts)} "))
        out.append((sev_style, f"[{l_type}/{level}] "))

        if (always_show_uuids or s == "(no subsystem)") and not never_show_uuids:
            out.append(("class:log.uuid", f"{ou} "))

        out.append(("class:log.subsystem" if s != "(no subsystem)" else "class:log.uuid", f"{s}: "))
        out.append((RICH_MESSAGE_STYLE.get(level, ""), msg))
        out.append(("", "\n"))
        return out

    def render() -> None:
        nonlocal fragments
        if not rows:
            fragments = [("", "(no data)\n")]
        else:
            fr: list[tuple[str, str]] = []
            for x in rows:
                fr.extend(_line_fragments(x))
            fragments = fr
        try:
            get_app().invalidate()
        except Exception:
            pass

    def load_from(line_min: int) -> None:
        """Load a page starting at line_min (inclusive) within the filtered stream."""
        nonlocal rows, top_line
        rows = view.window_after(base, line_min_inclusive=line_min, window=page_size())
        top_line = rows[0].line if rows else None

    def jump_end() -> None:
        nonlocal rows, top_line
        rows = view.window_before(base, line_max_exclusive=end_exclusive(), window=page_size())
        top_line = rows[0].line if rows else None

    def find_first() -> int | None:
        """Find the first matching line in the filtered stream."""
        opts = QueryOptions(**vars(base))
        opts.order_by = "line"
        opts.desc = False
        opts.limit = 1
        if opts.line_from is None:
            opts.line_from = 0
        res = view.query(opts)
        return res[0].line if res else None

    def find_prev(before_line: int) -> int | None:
        """Find previous matching line strictly < before_line, respecting base bounds."""
        opts = QueryOptions(**vars(base))
        opts.order_by = "line"
        opts.desc = True
        opts.limit = 1
        opts.line_to = min(before_line - 1, base.line_to) if base.line_to is not None else (before_line - 1)
        opts.line_from = base.line_from
        res = view.query(opts)
        return res[0].line if res else None

    def find_next(after_line: int) -> int | None:
        """Find next matching line strictly > after_line, respecting base bounds."""
        opts = QueryOptions(**vars(base))
        opts.order_by = "line"
        opts.desc = False
        opts.limit = 1
        opts.line_from = max(after_line + 1, base.line_from) if base.line_from is not None else (after_line + 1)
        opts.line_to = base.line_to
        res = view.query(opts)
        return res[0].line if res else None

    # Initial view: last page within the filtered range.
    jump_end()
    render()

    control = FormattedTextControl(text=lambda: FormattedText(fragments), focusable=False, show_cursor=False)
    window = Window(
        content=control,
        wrap_lines=False,
        right_margins=[ScrollbarMargin(display_arrows=True)],
        always_hide_cursor=True,
    )

    style = Style.from_dict(PT_STYLE)

    @kb.add("q")
    def _(event) -> None:
        event.app.exit()

    @kb.add("up")
    @kb.add("k")
    def _(event) -> None:
        nonlocal top_line
        if top_line is None:
            return
        prev_line = find_prev(top_line)
        if prev_line is None:
            return
        load_from(prev_line)
        render()

    @kb.add("down")
    @kb.add("j")
    def _(event) -> None:
        nonlocal top_line
        if top_line is None:
            return
        nxt_line = find_next(top_line)
        if nxt_line is None:
            return
        load_from(nxt_line)
        render()

    @kb.add("pageup")
    def _(event) -> None:
        nonlocal top_line
        if top_line is None:
            return
        rs = view.window_before(base, line_max_exclusive=top_line, window=page_size())
        if not rs:
            return
        top_line = rs[0].line
        load_from(top_line)
        render()

    @kb.add("pagedown")
    def _(event) -> None:
        nonlocal top_line
        if not rows:
            return
        last = rows[-1].line
        rs = view.window_after(base, line_min_inclusive=last + 1, window=page_size())
        if not rs:
            jump_end()
            render()
            return
        top_line = rs[0].line
        load_from(top_line)
        render()

    @kb.add("g")
    def _(event) -> None:
        first = base.line_from if base.line_from is not None else find_first()
        if first is None:
            return
        load_from(first)
        render()

    @kb.add("G")
    def _(event) -> None:
        jump_end()
        render()

    app = Application(layout=Layout(window), key_bindings=kb, full_screen=True, style=style)
    app.run()
    return 0



def cmd_log_tui(args: argparse.Namespace) -> int:
    """
    Textual TUI for browsing logs (archives + query builder + details).

    CLI flags are used as *initial overrides* on top of the persisted TUI config.
    """
    from ipi_ecs.cli.logging.tui_app import run_tui

    opts = QueryOptions(
        uuid=args.uuid,
        line_from=args.line_from,
        line_to=args.line_to,
        since=args.since,
        until=args.until,
        l_type=args.l_type,
        exclude_types=_default_exclude_types(args),
        level=args.level,
        min_level=args.min_level,
    )

    return int(
        run_tui(
            log_dir=args.log_dir,
            env_var=ENV_LOG_DIR,
            opts=opts,
            archive=getattr(args, "archive", None),
            poll=getattr(args, "poll", 1.0),
            follow=not getattr(args, "no_follow", False),
            show_uuids=getattr(args, "show_uuids", False),
            hide_uuids=getattr(args, "never_show_uuids", False),
        )
        or 0
    )



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


def cmd_log_event_add(args: argparse.Namespace) -> int:
    import uuid as _uuid
    import time as _time

    archive_dir = _resolve_archive_dir(args.log_dir, args.archive)
    archive_dir.mkdir(parents=True, exist_ok=True)
    idx = _open_index_for_archive_dir(archive_dir)

    start = int(args.start_line)
    end = int(args.end_line) if args.end_line is not None else start

    eid = args.event_id or str(_uuid.uuid4())
    now = _time.time_ns()

    idx.begin_event(
        event_id=eid,
        e_type=args.type,
        level=args.level,
        message=args.message or "",
        start_line=start,
        start_ts_ns=now,
        data_start={"source": "cli"},
    )

    if not args.open:
        idx.end_event(event_id=eid, end_line=end, end_ts_ns=now, data_end={"source": "cli"})

    print(eid)
    return 0


def cmd_log_event_list(args: argparse.Namespace) -> int:
    archive_dir = _resolve_archive_dir(args.log_dir, args.archive)
    idx = _open_index_for_archive_dir(archive_dir)

    rows = idx.list_events(
        e_type=args.type,
        open_only=args.open_only,
        limit=args.limit,
        newest_first=not args.oldest_first,
    )

    if not rows:
        print("(no events)")
        return 0

    for ev in rows:
        end = ev.end_line if ev.end_line is not None else ev.start_line
        status = "open" if ev.end_line is None else "closed"
        print(f"{ev.id}  {status:6}  {ev.e_type:<12} {ev.level:<8}  lines {ev.start_line}-{end}  {ev.message}")
    return 0


def cmd_log_event_show(args: argparse.Namespace) -> int:
    archive_dir = _resolve_archive_dir(args.log_dir, args.archive)
    idx = _open_index_for_archive_dir(archive_dir)
    ev = idx.get_event(args.event_id)
    if ev is None:
        raise SystemExit(f"event not found: {args.event_id}")

    end = ev.end_line if ev.end_line is not None else ev.start_line
    status = "open" if ev.end_line is None else "closed"
    print(f"id:        {ev.id}")
    print(f"status:    {status}")
    print(f"type:      {ev.e_type}")
    print(f"level:     {ev.level}")
    print(f"lines:     {ev.start_line}-{end}")
    print(f"start_ns:  {ev.start_ts_ns}")
    print(f"end_ns:    {ev.end_ts_ns}")
    print(f"message:   {ev.message}")
    return 0


def cmd_log_event_query(args: argparse.Namespace) -> int:
    archive_dir = _resolve_archive_dir(args.log_dir, args.archive)
    idx = _open_index_for_archive_dir(archive_dir)
    ev = idx.get_event(args.event_id)
    if ev is None:
        raise SystemExit(f"event not found: {args.event_id}")

    start = ev.start_line
    end = ev.end_line if ev.end_line is not None else ev.start_line

    # Expand range by before/after lines, inclusive.
    line_min = max(0, start - int(args.before))
    line_max = end + int(args.after)

    j = JournalReader(archive_dir)

    # Prefer showing the exact event range + context, with no filters.
    for rec in j.read_between(line_min, line_max):
        print(format_line(rec))

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
    p.add_argument("--exclude-type", dest="exclude_types", action="append", default=[], help="Exclude an l_type (repeatable).")
    p.add_argument("--include-rec", action="store_true", help="Include REC in default views (overrides default exclusion).")
    p.add_argument("--level", default=None, help="Filter by exact level string.")
    p.add_argument("--min-level", default=None, help="Filter by minimum level (requires --type).")

    p.add_argument("--show-uuids", dest="show_uuids", action="store_true", help="Always show UUIDs even if subsystem is known")
    p.add_argument("--hide-uuids", dest="never_show_uuids", action="store_true", help="Never show UUIDs even if subsystem is not known")



def add_color_arg(p: argparse.ArgumentParser) -> None:
    p.add_argument("--color", choices=["auto", "always", "never"], default="auto",
                   help="Colorize output for show/query/follow (requires rich; auto only colors on TTY).")


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
    add_color_arg(ps)
    add_archive_arg(ps)
    ps.add_argument("--start", type=int, required=True)
    ps.add_argument("--end", type=int, required=True)
    ps.add_argument("--show-uuids", dest="show_uuids", action="store_true", help="Always show UUIDs")
    ps.add_argument("--hide-uuids", dest="never_show_uuids", action="store_true", help="Never show UUIDs")
    ps.set_defaults(fn=cmd_log_show)

    pq = sub_log.add_parser("query", help="Query logs using the SQLite index.")
    add_archive_arg(pq)
    add_log_dir_arg(pq)
    add_color_arg(pq)
    add_log_filters(pq)
    add_log_sorting(pq)
    pq.set_defaults(fn=cmd_log_query)

    pf = sub_log.add_parser("follow", help="Live stream logs (tail -F across rollovers).")
    add_archive_arg(pf)
    add_log_dir_arg(pf)
    add_color_arg(pf)
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


    pt = sub_log.add_parser("tui", help="Interactive TUI (Textual) for archives + query building + details.")
    add_log_dir_arg(pt)
    add_archive_arg(pt)
    add_log_filters(pt)
    pt.add_argument("--poll", type=float, default=1.0, help="Polling interval (seconds) for auto-follow at end.")
    pt.add_argument("--no-follow", dest="no_follow", action="store_true", help="Disable auto-follow when at end.")
    pt.set_defaults(fn=cmd_log_tui)

    pe = sub_log.add_parser("event", help="Debug helpers for log events.")
    sub_event = pe.add_subparsers(dest="eventcmd", required=True)

    pea = sub_event.add_parser("add", help="Add an event (debug).")
    pea.add_argument("start_line", type=int)
    pea.add_argument("end_line", type=int, nargs="?")
    pea.add_argument("--type", required=True, help="Event type")
    pea.add_argument("--level", default="INFO")
    pea.add_argument("--message", default="")
    pea.add_argument("--event-id", dest="event_id", default=None)
    pea.add_argument("--open", action="store_true", help="Leave event open (no end_line)")
    pea.add_argument("--log_dir", type=Path, default=None)
    pea.add_argument("--archive", default="current")
    pea.set_defaults(fn=cmd_log_event_add)

    pel = sub_event.add_parser("list", help="List events.")
    pel.add_argument("--type", default=None)
    pel.add_argument("--open-only", action="store_true", dest="open_only")
    pel.add_argument("--limit", type=int, default=200)
    pel.add_argument("--oldest-first", action="store_true", dest="oldest_first")
    pel.add_argument("--log_dir", type=Path, default=None)
    pel.add_argument("--archive", default="current")
    pel.set_defaults(fn=cmd_log_event_list)

    pes = sub_event.add_parser("show", help="Show an event.")
    pes.add_argument("event_id")
    pes.add_argument("--log_dir", type=Path, default=None)
    pes.add_argument("--archive", default="current")
    pes.set_defaults(fn=cmd_log_event_show)

    peq = sub_event.add_parser("query", help="Print log lines around an event.")
    peq.add_argument("event_id")
    peq.add_argument("--before", type=int, default=40)
    peq.add_argument("--after", type=int, default=80)
    peq.add_argument("--limit", type=int, default=500)
    peq.add_argument("--show-uuid", action="store_true", dest="show_uuid")
    peq.add_argument("--log_dir", type=Path, default=None)
    peq.add_argument("--archive", default="current")
    peq.set_defaults(fn=cmd_log_event_query)

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

    p_dds = sub.add_parser("dds", help="Log viewing tools.")
    sub_dds = p_dds.add_subparsers(dest="ddscmd", required=True)

    # server
    ps = sub_dds.add_parser("server", help="Run the ECS DDS server.")
    ps.add_argument("--host", default="0.0.0.0")
    ps.add_argument("--port", type=int, default=None)
    ps.set_defaults(fn=server.cmd_server)

    # echo
    pe = sub_dds.add_parser("echo", help="Echo a DDS key from a subsystem.")
    pe.add_argument("--sys", type=str)
    pe.add_argument("--hz", type=int, default=None)
    pe.add_argument("name", type=str, default=None)
    pe.add_argument("key", type=str)
    pe.set_defaults(fn=echo.main)

    pw = sub_dds.add_parser("write", help="Write a DDS key to a subsystem.")
    pw.add_argument("--sys", type=str)
    pw.add_argument("--hz", type=int, default=None)
    pw.add_argument("--once", action="store_true", help="Write only once then exit.")
    pw.add_argument("name", type=str, default=None)
    pw.add_argument("key", type=str)
    pw.add_argument("value", type=str)
    pw.set_defaults(fn=write.main)

    p_event = sub_dds.add_parser("event", help="Run ECS event tools.")
    sub_event = p_event.add_subparsers(dest="eventcmd", required=True)

    p_call = sub_event.add_parser("call", help="Call an ECS event.")
    p_call.add_argument("event", type=str, help="Event name to call.")
    p_call.add_argument("--data", type=str, default="", help="bytes data payload.")
    p_call.set_defaults(fn=call_event.main)

    p_util = sub.add_parser("util", help="Utility commands.")
    sub_util = p_util.add_subparsers(dest="utilcmd", required=True)

    p_call = sub_util.add_parser("jog", help="Send a jog vector from keyboard input.")
    p_call.add_argument("--name", "-n", type=str, required=False, default=None, help="Target subsystem name to connect to.")
    p_call.add_argument("--key", "-k", type=str, required=False, default="target_jog_vector", help="Key value to write the jog vector to.")
    p_call.add_argument("--sys", "-s", type=str, required=False, default=None, help="Target subsystem UUID to connect to. If both name and sys are given, sys takes precedence.")
    p_call.add_argument("--hz", "-f", type=float, required=False, default=None, help="Frequency to poll the keyboard and write jog values at. Default 2Hz.")
    p_call.set_defaults(fn=keyboard_jog.main)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.fn(args) or 0)
