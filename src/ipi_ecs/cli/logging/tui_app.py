# src/ipi_ecs/cli/logging/tui_app.py
from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any, Optional, Callable

from rich.pretty import Pretty
from rich.text import Text

from ipi_ecs.logging.viewer import (
    LogViewer,
    QueryOptions,
    RICH_TYPE_STYLE,
    RICH_MESSAGE_STYLE,
    get_subsystem,
)
from ipi_ecs.logging.timefmt import fmt_ns_local


# Config persistence
# Prefer CLI-local config module (ipi_ecs/cli/logging/config.py).
# Fall back to a local implementation if unavailable.
try:
    from ipi_ecs.cli.logging.config import load_state, save_state  # type: ignore
except Exception:  # pragma: no cover
    import json
    from platformdirs import user_config_dir

    APP_NAME = "ipi-ecs"
    CONFIG_FILE = "log_tui.json"

    def _cfg_path() -> Path:
        p = Path(user_config_dir(APP_NAME)) / CONFIG_FILE
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    def load_state() -> dict[str, Any]:
        p = _cfg_path()
        if not p.exists():
            return {}
        return json.loads(p.read_text(encoding="utf-8"))

    def save_state(*, archive: str, opts: QueryOptions) -> None:
        p = _cfg_path()
        p.write_text(
            json.dumps({"archive": archive, "filters": asdict(opts)}, indent=2),
            encoding="utf-8",
        )

MAX_W = 170

def _format_logline_rich(ln, *, show_uuids: bool, hide_uuids: bool, range_prefix: Text | None = None, screen_width = MAX_W) -> Text:
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

    if range_prefix is not None:
        t.append_text(range_prefix)

    t.append(f"{fmt_ns_local(ts_ns)}  ", style="dim")
    t.append(f"[{l_type}/{level}]  ", style=sev_style)

    # UUID display policy:
    # show UUID when explicitly requested OR subsystem unknown, unless hide_uuids is set
    if (show_uuids or subsystem == "(no subsystem)") and not hide_uuids:
        t.append(f"{uuid} ", style="magenta")

    # Subsystem label
    t.append(f"{subsystem}: ", style=RICH_TYPE_STYLE.get("subsystem", ""))

    # Message (highlight warnings/errors)

    if len(t) + len(msg) > screen_width - 5:
        msg = msg[:screen_width - (len(t) + 5)] + "..."
    
    t.append(msg, style=RICH_MESSAGE_STYLE.get(level, ""))

    return t

def _build_range_prefix_map(rows, ranges: list[tuple[int, int, str, str | None, str | None]] | None, *, gutter_width: int = 18, max_cols: int = 30, sep: int = 3) -> dict[int, Text]:
    """Build per-visible-line prefix Text for a left gutter showing event ranges."""
    if not ranges or not rows:
        return {}

    # normalize
    norm: list[tuple[int, int, str]] = []
    for a, b, label, style_gutter, style_label in ranges:
        a_i = int(a)

        if b is None:
            b_i = rows[-1].line
        else:
            b_i = int(b)

        if b_i < a_i:
            a_i, b_i = b_i, a_i
        norm.append((a_i, b_i, str(label), b, style_gutter, style_label))
    ranges_n = norm[: max(1, int(max_cols))]
    visible = [int(r.line) for r in rows]
    if not visible:
        return {}

    w = max(6, int(gutter_width))

    # pick a label line for each range (closest visible line to midpoint)
    label_line_for: dict[tuple[int, int, str], int] = {}
    for rng in ranges_n:
        s, e, lbl, end, style_gutter, style_label = rng
        if s == e:
            label_line_for[rng] = s
            continue

        cands = [ln for ln in visible if s <= ln <= e]
        if not cands:
            continue
        mid = (s + e) // 2
        label_line_for[rng] = min(cands, key=lambda ln: abs(ln - mid))

    max_overlap = dict()

    ranges_n.sort(key=lambda v: abs(v[0]-v[1]))
    ranges_n.reverse()

    for i in range(len(ranges_n)): #pylint: disable=consider-using-enumerate
        rng_a = ranges_n[i]
        max_overlap[i] = 0
        s, e, lbl, end, style_gutter, style_label = rng_a
        for j in range(len(ranges_n)): #pylint: disable=consider-using-enumerate
            rng_b = ranges_n[j]
            s2, e2, lbl2, end, style_gutter, style_label = rng_b
            if i <= j:
                break

            if (s2 <= e and e2 >= s) or (s < e2 and e > s2):
                max_overlap[i] = max(max_overlap[j] + 1, max_overlap[i])

    ranges_of_interest = dict()
    for ln in visible:
        ranges_of_interest[ln] = []
        for i in range(len(ranges_n)): #pylint: disable=consider-using-enumerate
            rng = ranges_n[i]
            
            s, e, lbl, end, style_gutter, style_label = rng
            if not (s <= ln <= e):
                continue

            m_position = max_overlap[i]
            while len(ranges_of_interest[ln]) < m_position:
                ranges_of_interest[ln].append(None)
            
            ranges_of_interest[ln].append(rng)

    #raise Exception(max_overlap)

    out: dict[int, Text] = {}
    max_lbl = w - 2
    for ln in visible:
        cols: list[str] = []
        glyphs = ""
        ranges_line = ranges_of_interest[ln]
        for i in range(len(ranges_line)): #pylint: disable=consider-using-enumerate
            rng = ranges_line[i]

            if rng is None: 
                glyph = (" " * sep)
            else:
                s, e, lbl, end, style_gutter, style_label = rng
                if rng is None or not (s <= ln <= e):
                    glyph = (" " * sep)

                # glyphs
                elif s == e:
                    glyph = "●"
                elif ln == s:
                    glyph = "┌"
                elif ln == e:
                    glyph = "└"
                else:
                    glyph = "│"

            glyphs += glyph.ljust(sep)

            if len(glyphs) > max_lbl and not glyphs.isspace():
                glyphs = glyphs[: max(0, max_lbl - 1)] + "…"
        glyphs_text = Text(glyphs.ljust(w), style="dim")

        text = None
        for i in range(len(ranges_line)): #pylint: disable=consider-using-enumerate
            rng = ranges_line[i]

            if rng is None:
                continue

            s, e, lbl, end, style_gutter, style_label = rng

            if label_line_for.get(rng) == ln:
                clean = lbl.replace("\n", " ").strip()
                if text is not None:
                    clean = "(multiple)"

                clean = glyphs[:sep * i] + ("├" if s != e else "●") + " " + clean

                if len(clean) > max_lbl:
                    clean = clean[: max(0, max_lbl - 1)] + "…"
                    
                text = Text(clean.ljust(w), style="dim")
                text.highlight_words([lbl.replace("\n", " ").strip()], "orange1 underline blink" if end is None else "orange1")

            if end is None and label_line_for.get(rng) + 1 == ln:
                max_lbl = w - 2
                clean = lbl.replace("\n", " ").strip()

                clean = glyphs[:sep * i + 1] + " (ONGOING)"

                if len(clean) > max_lbl:
                    clean = clean[: max(0, max_lbl - 1)] + "…"

                text = Text(clean.ljust(w), style="")
                text.highlight_words([lbl.replace("\n", " ").strip()], "orange1 underline blink")

        if text is None:
            out[ln] = glyphs_text
        else:
            out[ln] = text
    return out


def _highlight_line(line: Text) -> Text:
    out = line.copy()
    out.stylize("reverse")
    return out


def run_tui(
    *,
    log_dir: Optional[Path],
    env_var: str,
    archive: str | None,
    opts: QueryOptions,
    poll: float = 1.0,
    follow: bool = True,
    show_uuids: bool = False,
    hide_uuids: bool = False,
) -> int:
    """
    Textual TUI for logs.

    - Uses custom viewport rendering (no ListView scrolling) for smooth navigation.
    - If follow=True, auto-refreshes when you're at the tail and new matching lines arrive.
    - `opts` and `archive` are initial overrides on top of persisted config.
    """
    try:
        from textual.app import App, ComposeResult
        from textual.binding import Binding
        from textual.containers import Horizontal, Vertical, VerticalScroll
        from textual.screen import ModalScreen
        from textual.widgets import Footer, Header, Input, Button, Static, ListView, ListItem
        from textual.widget import Widget
    except Exception:  # pragma: no cover
        print("Textual is not installed. Install it with: pip install textual")
        return 1

    viewer = LogViewer(log_dir, env_var=env_var)

    class DetailScreen(ModalScreen[None]):
        BINDINGS = [
            Binding("escape", "close", "Close", show=False),
            Binding("q", "close", "Close", show=False),
        ]

        def __init__(self, ln, *, on_filter_uuid: Callable[[str], None] | None = None) -> None:
            super().__init__()
            self.ln = ln
            self._on_filter_uuid = on_filter_uuid
            origin = (ln.record.get("origin") or {})
            self._uuid = origin.get("uuid") if isinstance(origin.get("uuid"), str) else None

        def compose(self) -> ComposeResult:
            yield Vertical(
                Static(f"Line {self.ln.line}  UUID={self._uuid or '-'}", id="detail_title"),
                VerticalScroll(
                    Static(Pretty(self.ln.record, expand_all=False), id="detail_body"),
                    id="detail_scroll",
                ),
                Horizontal(
                    Button("Filter UUID", id="filter_uuid"),
                    Button("Close", id="close"),
                ),
                id="detail_modal",
            )

        def action_close(self) -> None:
            self.dismiss(None)

        def on_button_pressed(self, event: Button.Pressed) -> None:
            if event.button.id == "close":
                self.dismiss(None)
                return
            if event.button.id == "filter_uuid":
                if callable(self._on_filter_uuid) and self._uuid:
                    try:
                        self._on_filter_uuid(self._uuid)
                    except Exception:
                        pass
                self.dismiss(None)

    class FilterScreen(ModalScreen[QueryOptions]):
        """
        Scrollable query builder. Buttons are always visible.
        """
        BINDINGS = [
            Binding("escape", "cancel", "Cancel", show=False),
            Binding("q", "cancel", "Cancel", show=False),
            Binding("ctrl+s", "apply", "Apply", show=False),
        ]

        def __init__(self, current: QueryOptions) -> None:
            super().__init__()
            self.current = current

        def compose(self) -> ComposeResult:
            def iv(x: Any) -> str:
                return "" if x is None else str(x)

            ex = ",".join(self.current.exclude_types or [])

            yield Vertical(
                Static("Query builder", id="filter_title"),
                VerticalScroll(
                    Static(
                        "Filters (leave blank for none). Exclude types is comma-separated.",
                        id="filter_help",
                    ),
                    Input(value=iv(self.current.uuid), placeholder="uuid", id="uuid"),
                    Input(value=iv(self.current.l_type), placeholder="type (l_type), e.g. EXP", id="l_type"),
                    Input(value=iv(self.current.level), placeholder="level (exact)", id="level"),
                    Input(value=iv(self.current.min_level), placeholder="min-level", id="min_level"),
                    Input(value=iv(self.current.line_from), placeholder="from line (inclusive)", id="line_from"),
                    Input(value=iv(self.current.line_to), placeholder="to line (inclusive)", id="line_to"),
                    Input(value=iv(self.current.since), placeholder="since (local time)", id="since"),
                    Input(value=iv(self.current.until), placeholder="until (local time)", id="until"),
                    Input(value=ex, placeholder="exclude types (comma list), default REC", id="exclude_types"),
                    id="filter_scroll",
                ),
                Horizontal(
                    Button("Apply", id="apply"),
                    Button("Cancel", id="cancel"),
                    id="filter_buttons",
                ),
                id="filter_modal",
            )

        def action_cancel(self) -> None:
            self.dismiss(self.current)

        def _parse_int(self, s: str) -> int | None:
            s = s.strip()
            if not s:
                return None
            try:
                return int(s)
            except Exception:
                return None

        def action_apply(self) -> None:
            uuid = self.query_one("#uuid", Input).value.strip() or None
            l_type = self.query_one("#l_type", Input).value.strip() or None
            level = self.query_one("#level", Input).value.strip() or None
            min_level = self.query_one("#min_level", Input).value.strip() or None
            line_from = self._parse_int(self.query_one("#line_from", Input).value)
            line_to = self._parse_int(self.query_one("#line_to", Input).value)
            since = self.query_one("#since", Input).value.strip() or None
            until = self.query_one("#until", Input).value.strip() or None

            ex_raw = self.query_one("#exclude_types", Input).value.strip()
            exclude_types = [x.strip() for x in ex_raw.split(",") if x.strip()] if ex_raw else None

            nxt = QueryOptions(
                **{
                    **asdict(self.current),
                    "uuid": uuid,
                    "l_type": l_type,
                    "level": level,
                    "min_level": min_level,
                    "line_from": line_from,
                    "line_to": line_to,
                    "since": since,
                    "until": until,
                    "exclude_types": exclude_types,
                }
            )

            if nxt.exclude_types is None and nxt.l_type is None:
                nxt.exclude_types = ["REC"]

            self.dismiss(nxt)

        def on_button_pressed(self, event: Button.Pressed) -> None:
            if event.button.id == "cancel":
                self.action_cancel()
            elif event.button.id == "apply":
                self.action_apply()

    class LogPane(Widget):
        """
        Single widget that renders the current viewport (no internal scrolling).
        """
        can_focus = True

        def __init__(self) -> None:
            super().__init__()
            self.rows = []
            self.cursor = 0
            self._show_uuids = False
            self._hide_uuids = False

            self.__show_gutter = True

            # Optional range gutter: list[(start_line, end_line, label)]
            self.ranges: list[tuple[int, int, str]] = [(0, 25, "mylabel"), (5, 35, "my other label"), (10, 45, "my other label")]

        def set_ranges(self, ranges: list[tuple[int, int, str]] | None) -> None:
            """Set the range gutter spans. Call with None or [] to clear."""
            self.ranges = ranges or []
            self._render()

        def clear_ranges(self) -> None:
            self.set_ranges([])

        def render(self) -> Text:
            if not self.rows:
                return Text("(no data)")
            
            prefix_map = _build_range_prefix_map(self.rows, self.ranges, gutter_width=35, max_cols=30)
            blank_prefix = Text(' ' * 18, style='dim') if self.ranges else None

            out = Text()
            for i, ln in enumerate(self.rows):
                line = _format_logline_rich(ln, show_uuids=self._show_uuids, hide_uuids=self._hide_uuids, range_prefix=(prefix_map.get(int(ln.line), blank_prefix) if blank_prefix is not None and self.__show_gutter else None), screen_width=self.size.width)
                if i == self.cursor:
                    line = _highlight_line(line)
                out.append_text(line)
                out.append("\n")
            return out

        # Compatibility helper (some older code uses set_id)
        def set_id(self, id):
            self._id = id
            return self
        
        def highlight(self, start, end):
            pass

        def set_rows(self, rows, cursor: int, *, show_uuids: bool, hide_uuids: bool) -> None:
            self.rows = rows
            self.cursor = max(0, min(cursor, max(0, len(rows) - 1)))
            self._show_uuids = show_uuids
            self._hide_uuids = hide_uuids
            self.refresh()

        def toggle_gutter(self):
            self.__show_gutter = not self.__show_gutter
            self.refresh()

    class LogTUI(App):
        TITLE = "ipi-ecs log tui"

        CSS = """
        #topbar { height: 1; padding: 0 1; }
        #main { height: 1fr; }
        #archives_wrap { width: 34; border: round $accent; }
        #archives_title { height: 1; padding: 0 1; }
        #events_wrap { width: 44; border: round $accent; }
        #events_title { height: 1; padding: 0 1; }
        #log_wrap { border: round $primary; height: 1fr; }
        #filter_modal, #detail_modal { border: round $accent; padding: 1; height: 95%; width: 95%; }
        #filter_scroll, #detail_scroll { height: 1fr; }
        #filter_buttons { height: auto; }
        """

        BINDINGS = [
            Binding("q", "quit", "Quit", priority=True),
            Binding("a", "toggle_archives", "Archives", priority=True),
            Binding("e", "toggle_events", "Events", priority=True),
            Binding("/", "filters", "Query", priority=True),
            Binding("enter", "details", "Details", priority=True),

            Binding("f", "toggle_follow", "Follow", priority=True),
            Binding("u", "filter_uuid", "Filter UUID", priority=True),
            Binding("U", "clear_uuid", "Clear UUID", priority=True),

            Binding("j", "down", "Down", priority=True),
            Binding("k", "up", "Up", priority=True),
            Binding("down", "down", show=False, priority=True),
            Binding("up", "up", show=False, priority=True),

            Binding("pagedown", "page_down", show=False, priority=True),
            Binding("pageup", "page_up", show=False, priority=True),

            Binding("g", "home", "Home", priority=True),
            Binding("G", "end", "End", priority=True),

            Binding("[", "event_start", "EvStart", priority=True),
            Binding("]", "event_end", "EvEnd", priority=True),
            Binding("E", "refresh_events", "Refresh Events", show=True, priority=True),
            Binding("i", "toggle_gutter", "Toggle Event Gutter", show=True, priority=True),
        ]

        def __init__(self) -> None:
            super().__init__()

            # persisted state
            st = load_state() or {}
            persisted_archive = st.get("archive") or "current"
            filt = st.get("filters") or {}
            try:
                persisted_opts = QueryOptions(**filt)
            except Exception:
                persisted_opts = QueryOptions()

            # initial overrides from CLI
            self.archive = archive or persisted_archive
            self.opts = persisted_opts

            # Override only fields that CLI set (non-None, non-empty)
            for k, v in asdict(opts).items():
                if v is None:
                    continue
                if isinstance(v, str) and v.strip() == "":
                    continue
                if isinstance(v, list) and len(v) == 0:
                    continue
                setattr(self.opts, k, v)

            if self.opts.exclude_types is None and self.opts.l_type is None:
                self.opts.exclude_types = ["REC"]

            self.show_uuids = bool(show_uuids)
            self.hide_uuids = bool(hide_uuids)

            self.view = None
            self.rows = []
            self.cursor = 0
            self._last_page_size = None

            # tail/follow state
            self._at_end_view = True
            self._follow_enabled = bool(follow)
            self._poll = float(poll) if poll and poll > 0 else 1.0

            # events panel state
            self.events = []
            self._selected_event_id = None
            self.__refreshing = False
            

        def compose(self) -> ComposeResult:
            yield Header()
            yield Static("", id="topbar")
            with Horizontal(id="main"):
                with Vertical(id="archives_wrap"):
                    yield Static("Archives", id="archives_title")
                    yield ListView(id="archives")
                with Vertical(id="events_wrap"):
                    yield Static("Events", id="events_title")
                    yield ListView(id="events")
                with Vertical(id="log_wrap"):
                    yield LogPane().set_id("log")
            yield Footer()

        def on_mount(self) -> None:
            self._load_archives()
            self._open_archive(self.archive)

            # Events panel starts hidden
            try:
                self.query_one("#events_wrap").display = False
            except Exception:
                pass

            # Wait for layout so we know viewport height
            self.call_after_refresh(self._jump_end_and_render)

            try:
                self.query_one("#log", LogPane).focus()
            except Exception:
                pass

            # Auto-follow polling
            if self._poll > 0:
                try:
                    self.set_interval(self._poll, self._poll_new_lines)
                except Exception:
                    raise

        # ----- Layout helpers -----
        def _page_size(self) -> int:
            pane = self.query_one("#log", LogPane)
            h = pane.size.height
            if not h or h <= 1:
                return 20
            return max(5, h)

        def _end_exclusive(self) -> int:
            end = self.view.next_line()
            if self.opts.line_to is not None:
                end = min(end, self.opts.line_to + 1)
            return end

        def _tail_selected(self) -> bool:
            try:
                if not self.rows:
                    return True
                return (self.cursor == len(self.rows) - 1)
            except Exception:
                return False

        def _set_topbar(self) -> None:
            ex = ",".join(self.opts.exclude_types or [])
            flags = []
            flags.append("FOLLOW" if self._follow_enabled else "NOFOLLOW")
            if self._tail_selected():
                flags.append("TAIL")

            uuid_flag = self.opts.uuid if self.opts.uuid else "-"
            self.query_one("#topbar", Static).update(
                f"archive={self.archive}  [{' '.join(flags)}]  uuid={uuid_flag}  type={self.opts.l_type or '-'}  "
                f"level={self.opts.level or '-'}  min={self.opts.min_level or '-'}  exclude=[{ex}]"
            )

        def _load_archives(self) -> None:
            lv = self.query_one("#archives", ListView)
            lv.clear()
            lv.append(ListItem(Static("current"), id="archive_current"))
            try:
                for a in viewer.list_archives():
                    lv.append(ListItem(Static(a.name), id=f"archive_{a.name}"))
            except Exception:
                self.query_one("#archives_wrap").display = False

        # ----- Data access -----
        def _open_archive(self, name: str) -> None:
            if self.view is not None:
                try:
                    self.view.close()
                except Exception:
                    pass
            self.archive = name
            self.view = viewer.open_archive(name)
            save_state(archive=self.archive, opts=self.opts)
            self._refresh_events()

        def _event_label(self, ev) -> str:
            end = ev.end_line if ev.end_line is not None else ev.start_line
            rng = f"{ev.start_line}" if end == ev.start_line else f"{ev.start_line}-{end}"
            lvl = (ev.level or "").upper()
            msg = (ev.message or "").strip()
            if len(msg) > 80:
                msg = msg[:77] + "..."
            suffix = " (open)" if ev.end_line is None else ""
            return f"{rng}  {ev.e_type}:{lvl}{suffix}  {msg}"

        def _refresh_events(self) -> None:
            lv = self.query_one("#events", ListView)

            prev_id = getattr(self, "_selected_event_id", None)

            # IMPORTANT: Don't assign explicit widget IDs to event rows.
            # Textual may keep widgets alive briefly; reusing IDs can raise DuplicateIds.
            lv.clear()

            self.events = []
            if self.view is None:
                return

            try:
                self.events = self.view.list_events(limit=200, desc=True)
            except Exception:
                self.events = []

            ranges = []

            sel_index: int | None = None
            for i, ev in enumerate(self.events):
                lv.append(ListItem(Static(self._event_label(ev))))
                if prev_id and str(ev.event_id) == str(prev_id):
                    sel_index = i

                ranges.append((ev.start_line, ev.end_line, ev.message, "", ""))

            self.query_one("#log", LogPane).set_ranges(ranges)                
            if sel_index is not None:
                try:
                    lv.index = sel_index
                except Exception:
                    pass

        def _selected_event(self):
            if not self.events:
                return None
            if self._selected_event_id:
                for ev in self.events:
                    if ev.event_id == self._selected_event_id:
                        return ev
            try:
                lv = self.query_one("#events", ListView)
                idx = lv.index
            except Exception:
                idx = None
            if idx is None or idx < 0 or idx >= len(self.events):
                return None
            return self.events[idx]

        def _jump_to_abs_line(self, target_line: int) -> None:
            if self.view is None or len(self.rows) == 0:
                return
            
            n = self._page_size()

            self.rows = self.view.window_after(self.opts, line_min_inclusive=int(target_line), window=1)

            if len(self.rows) == 0:
                return

            while len(self.rows) < n:
                before_rows = self.view.window_before(self.opts, line_max_exclusive=self.rows[0].line, window=1)
                after_rows = self.view.window_after(self.opts, line_min_inclusive=self.rows[-1].line + 1, window=1)

                if len(before_rows) == 0 and len(after_rows) == 0:
                    break

                self.rows = before_rows + self.rows + after_rows

            for i in range(len(self.rows)):
                r = self.rows[i]
                if r.line == target_line:
                    self.cursor = i
                    break

            self._render()

        def _jump_end_and_render(self) -> None:
            self._jump_end()
            self._render()

        def _jump_end(self) -> None:
            n = self._page_size()
            self.rows = self.view.window_before(self.opts, line_max_exclusive=self._end_exclusive(), window=n)
            self.cursor = max(0, len(self.rows) - 1)
            self._at_end_view = True

        def _jump_home(self) -> None:
            n = self._page_size()
            start = self.opts.line_from if self.opts.line_from is not None else 0
            self.rows = self.view.window_after(self.opts, line_min_inclusive=start, window=n)
            self.cursor = 0
            self._at_end_view = False

        def _ensure_page_size_consistent(self) -> None:
            n = self._page_size()
            if self._last_page_size != n:
                self._last_page_size = n
                self._jump_end()

        # ----- Rendering -----
        def _render(self) -> None:
            self._ensure_page_size_consistent()
            self._set_topbar()
            self.query_one("#log", LogPane).set_rows(
                self.rows,
                self.cursor,
                show_uuids=self.show_uuids,
                hide_uuids=self.hide_uuids,
            )

        def _poll_new_lines(self) -> None:
            """
            If you're at the tail (cursor on last visible line of the permitted range),
            refresh tail when new matching lines arrive.
            """
            if not self._follow_enabled or self.view is None:
                return
            if not self._tail_selected():
                return
            
            last_line = self.rows[-1].line if self.rows else -1
            n = self._page_size()
            new_rows = self.view.window_before(self.opts, line_max_exclusive=self._end_exclusive(), window=n)
            if not new_rows:
                return
                        
            if new_rows[-1].line != last_line:

                self.rows = new_rows
                self.cursor = len(self.rows) - 1
                self._at_end_view = True
                self._jump_end()
                self._render()
                self._refresh_events()

        # ----- Actions -----
        def action_toggle_archives(self) -> None:
            panel = self.query_one("#archives_wrap")
            panel.display = not panel.display

        def action_toggle_events(self) -> None:
            panel = self.query_one("#events_wrap")
            panel.display = not panel.display

            if panel.display:
                self._refresh_events()

        def action_toggle_gutter(self) -> None:
            panel = self.query_one("#log", LogPane).toggle_gutter()

        def action_refresh_events(self) -> None:
            self._refresh_events()

        def action_event_start(self) -> None:
            ev = self._selected_event()
            if ev is None:
                return
            self._jump_to_abs_line(int(ev.start_line))

        def action_event_end(self) -> None:
            ev = self._selected_event()
            if ev is None:
                return
            end = int(ev.end_line) if ev.end_line is not None else int(ev.start_line)
            self._jump_to_abs_line(end)

        def action_toggle_follow(self) -> None:
            self._follow_enabled = not self._follow_enabled
            if self._follow_enabled:
                self._jump_end()
            self._render()

        def _apply_uuid_filter(self, uuid: str) -> None:
            self.opts.uuid = uuid
            save_state(archive=self.archive, opts=self.opts)
            self._jump_end()
            self._render()

        def action_filter_uuid(self) -> None:
            if not self.rows:
                return
            rec = self.rows[self.cursor].record
            origin = rec.get("origin", {}) or {}
            uuid = origin.get("uuid")
            if isinstance(uuid, str) and uuid.strip():
                self._apply_uuid_filter(uuid)

        def action_clear_uuid(self) -> None:
            self.opts.uuid = None
            save_state(archive=self.archive, opts=self.opts)
            self._jump_end()
            self._render()

        def action_home(self) -> None:
            self._jump_home()
            self._render()

        def action_end(self) -> None:
            self._jump_end()
            self._render()

        def action_up(self) -> None:
            if not self.rows:
                return
            self._at_end_view = False

            if self.cursor > 0:
                self.cursor -= 1
                self._render()
                return

            first_line = self.rows[0].line
            prev = self.view.window_before(self.opts, line_max_exclusive=first_line, window=1)
            if not prev:
                return
            self.rows = prev + self.rows[:-1]
            self.cursor = 0
            self._render()

        def action_down(self) -> None:
            if not self.rows:
                return

            if self.cursor < len(self.rows) - 1:
                self.cursor += 1
                self._render()
                return

            last_line = self.rows[-1].line
            nxt = self.view.window_after(self.opts, line_min_inclusive=last_line + 1, window=1)
            if not nxt:
                self._at_end_view = True
                return
            self._at_end_view = False
            self.rows = self.rows[1:] + nxt
            self.cursor = len(self.rows) - 1
            self._render()

        def action_page_up(self) -> None:
            if not self.rows:
                return
            self._at_end_view = False
            n = self._page_size()
            first_line = self.rows[0].line
            rs = self.view.window_before(self.opts, line_max_exclusive=first_line, window=n)
            if not rs:
                return
            self.rows = rs
            self.cursor = 0
            self._render()

        def action_page_down(self) -> None:
            if not self.rows:
                return
            self._at_end_view = False
            n = self._page_size()
            last_line = self.rows[-1].line
            rs = self.view.window_after(self.opts, line_min_inclusive=last_line + 1, window=n)
            if not rs:
                self._jump_end()
                self._render()
                return
            self.rows = rs
            self.cursor = min(n - 1, len(self.rows) - 1)
            self._render()

        def action_details(self) -> None:
            if not self.rows:
                return
            ln = self.rows[self.cursor]
            self.push_screen(DetailScreen(ln, on_filter_uuid=self._apply_uuid_filter))

        def action_filters(self) -> None:
            def _cb(result: QueryOptions | None = None, *args, **kwargs) -> None:
                ro = None
                if isinstance(result, QueryOptions):
                    ro = result
                else:
                    for x in reversed((result,) + args):
                        if isinstance(x, QueryOptions):
                            ro = x
                            break
                if ro is None:
                    return
                self.opts = ro
                if self.opts.exclude_types is None and self.opts.l_type is None:
                    self.opts.exclude_types = ["REC"]
                save_state(archive=self.archive, opts=self.opts)
                self._jump_end()
                self._render()

            self.push_screen(FilterScreen(self.opts), callback=_cb)

        def on_list_view_selected(self, event: ListView.Selected) -> None:
            lv = event.list_view
            lv_id = str(getattr(lv, "id", "") or "")

            # Archive selection still uses IDs.
            if lv_id == "archives":
                if not event.item or not event.item.id:
                    return
                item_id = str(event.item.id)
                if item_id.startswith("archive_"):
                    name = item_id.replace("archive_", "", 1)
                    self._open_archive(name)
                    self._jump_end()
                    self._render()
                    try:
                        self.query_one("#log", LogPane).focus()
                    except Exception:
                        pass
                return

            # Event selection: items have no IDs; use ListView index -> self.events mapping.
            if lv_id == "events":
                idx = lv.index if lv is not None else None
                if idx is None or idx < 0 or idx >= len(self.events):
                    return

                ev = self.events[idx]
                self._selected_event_id = str(ev.event_id)
                self._jump_to_abs_line(int(ev.start_line))
                try:
                    self.query_one("#log", LogPane).focus()
                except Exception:
                    pass
                return

        def on_mouse_scroll_up(self, event) -> None:
            self.action_up()
            try:
                event.stop()
            except Exception:
                pass

        def on_mouse_scroll_down(self, event) -> None:
            self.action_down()
            try:
                event.stop()
            except Exception:
                pass

        def on_unmount(self) -> None:
            try:
                save_state(archive=self.archive, opts=self.opts)
            except Exception:
                pass
            try:
                if self.view is not None:
                    self.view.close()
            except Exception:
                pass

    LogTUI().run()
    return 0
