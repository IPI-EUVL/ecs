# ipi-ecs
**IPI Experiment Control System (ECS)** — a modular toolkit for building distributed control systems.

ipi-ecs provides a compact, DDS-like communications layer for discovery, key/value state, and event distribution, plus a **networked, journal-like logging system** designed for multi-process / multi-machine research setups.

Primarily built to support our in-house EUV source system, but the core pieces have been intentionally kept generic: `ipi-ecs` aims to be a solid foundation that can support a wide-variety of usage scenarios ranging from experiment control systems to electric vehicle firnware (yes, really!)**

<b><sub><sup> *\*\*Arduino compatible embedded client library is under development!*</sup></sub></b>

---

## What’s included

### Distributed coordination (DDS-like)
- **Subsystem registry** for dynamic discovery (with stable identities).
- **Key/Value data surface** for configuration, commands, and telemetry.
- **Push/subscribe updates** for “published” KV entries.
- **Event distribution** for request/response-style control actions.

### Journaled logging (built for debugging + replay)
- **Network log ingestion server** (`ipi-ecs logger`) that accepts structured records over TCP.
- **Global line numbers**: Every log entry gets a global incremental line number that will always be unique, allowing systems to store the line numbers for regions of interest to retrieve them later.
- **Append-only NDJSON segments** with rotation (size/time) so logs stay manageable.
- **SQLite index** for fast queries by line range, time, subsystem/UUID, type, severity, etc.
- **Event markers**: begin/end “events” that reference a range of log lines (great for experiments, shots, runs, faults, etc.).
- **CLI tooling**:
  - `log show/query/follow` for scripting + terminal use
  - `log browse` (prompt_toolkit) for a “less-like” interactive viewer
  - `log tui` (Textual) for a richer interactive viewer (filters, details, event-range gutter)
- **JSON payload**: Useful to record system state in a machine-readable format and replay it if desired, when combined with...
- **Replay** services for reproducing past system states and debugging.
- **Log type and origin filtering**: Filter out software-originated / state recording messages to get only the data operators want to see.

### Experiment data recording / indexing*
- **Data recording** services for capturing relevant streams/state.
- **Indexing**: Automatically handle file saving / loading / indexing to keep a registry of what data was captured, when, and for what purpose.  
Intended use case is to record high-rate instrument data that cannot neatly fit into the logging system (i.e. exposure UV intensity over time per pulse)

### UI + operator tooling*
- **Terminal-oriented UI** (TUI) options for interactive operation.
- **Tk-based UI** components for GUI apps.

### System structure*
- **Lifecycle management** primitives for starting/stopping subsystems cleanly, and restarting dead systems neatly.
- **Reflection / introspection** hooks to make it easier to build UIs and diagnostics.


\* Limited functionality at this moment. This project is still under active development!

---

## Design goals
- **Modularity:** subsystems stay small and composable.
- **Fault tolerance:** failures should be observable and survivable.
- **Observability by default:** structured logs + queryability.
- **Practical ergonomics:** optimized for real research equipment bring-up and long runs.

- **Minimal user interaction** needed to recover from faults.

---

## Installation
### From the Python Package Index
```bash
python -m pip install ipi-ecs
```

### From source as editable install (recommended for development)
```bash
git clone https://github.com/IPI-EUVL/ecs.git
cd ecs
python -m pip install -e.
```

Optional dev tooling:
```bash
python -m pip install -e ".[dev]"
```
---

## Core concepts

### Subsystems
A **subsystem** is a service/process that:
- registers a name + a UUID
- exposes a set of KV items (readable/writable/published)
- exposes a set of events it can handle (and/or consume)

### KV items
**Key-Value** items are the primary way of sharing data between subsystems.
- **read/write** for configuration and commands
- **read-only or write-only** handlers are supported.
- **Dynamic handlers** can handle arbitrary requests that a subsystem receives
- Can be **published** for streaming telemetry/state updates to **subscribers**
- Local and remote **KV providers** can hide the entire DDS stack and expose a regular variable that can be used in normal Python expressions  
- KVs are **typed**, and subsystems can specifically accept/reject values at runtime (i.e. can enforce the validity of a configuration write request.)

### Events
Events are routed control actions:
- Request/response semantics for “do a thing”
- Asynchronous, with state feedback
- Can be used as a query system as well:
    - i.e. Controller subsystem sends global event "can begin exposure"
    - If a subsystem objects, it can reject the event and provide a reason alongside as well if desired.

---

## Contributing
PRs are welcome, HOWEVER, as this is a project undergoing very fast development at the moment, we'd advise you to wait until things have settled down for a bit and most major features have been implemented.

---

## Status
This project is under active development and is primarily driven by real research-system needs.
APIs may evolve; pin versions for production deployments.
Expect lots, and lots of bugs, we'd like if you let us know by opening an issue ticket!
