from ipi_ecs.logging.db_reader import DBJournalReader
from ipi_ecs.logging.index import DEFAULT_LEVEL_MAP
from ipi_ecs.logging.timefmt import parse_time_to_ns, fmt_ns_local

def fmt(line: int, rec: dict) -> str:
    origin = rec.get("origin", {}) or {}
    ts = origin.get("ts_ns")
    ou = origin.get("uuid", "?")
    l_type = rec.get("l_type", "?")
    level = rec.get("level", "?")
    msg = rec.get("msg", "")
    return f"{line:>10}  {fmt_ns_local(ts)}  [{l_type}/{level}]  {ou}: {msg}"

def cmd_log_query(args) -> int:
    rdr = DBJournalReader(args.log_dir)

    ts_min = parse_time_to_ns(args.since) if args.since else None
    ts_max = parse_time_to_ns(args.until) if args.until else None

    line_min = args.line_from
    line_max = (args.line_to + 1) if args.line_to is not None else None

    min_level_num = None
    if args.min_level is not None:
        if not args.l_type:
            raise SystemExit("--min-level requires --type (because ordering is type-specific)")
        min_level_num = DEFAULT_LEVEL_MAP.get(args.l_type, {}).get(args.min_level, 0)

    q = dict(
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

    for line, rec in rdr.query(**q):
        print(fmt(line, rec))

    return 0
