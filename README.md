# `ipi-ecs`
IPI Experiment Control System

`ipi-ecs` is a modular, fault-tolerant framework for building control systems. It was developed mainly to support the backbone for our in-house EUV source but is intentionally general-purpose and adaptable for a wide range of applications ranging from experiment setups to electric vehicle firmware (yes, really!)

Key features:

- Lightweight DDS with publish/subscribe key-value and event bus
- Dynamic subsystem registry and lifecycle management
- Networked, continuous journal-style logging with archival and query support
- Data recording and replay services
- TUI and Tk-based user interfaces
- Reflection for runtime and metadata statistics

The project emphasizes modularity, fault-tolerance, and ease of integration, making it suitable for research, automation, and embedded or firmware-level applications.