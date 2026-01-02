from ipi_ecs.logging.viewer import LogViewer, QueryOptions, resolve_log_dir

ENV_LOG_DIR = "IPI_ECS_LOG_DIR"
lv = LogViewer(resolve_log_dir(None, ENV_LOG_DIR))
av = lv.open_archive()

print(av)

# list latest 50 events
events = av.list_events(limit=50)
print(events[0])

# open logs for that event
opts = QueryOptions(exclude_types=["REC"])
opts = av.apply_event_range(opts, events[0])
lines = av.query(opts)
print(lines)
print(len(lines))