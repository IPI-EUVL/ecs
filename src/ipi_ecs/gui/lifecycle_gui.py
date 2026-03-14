import argparse
import ast
import os
import time
import uuid
import tkinter as tk
from dataclasses import dataclass
from queue import Empty, Queue
from tkinter import messagebox, ttk

import segment_bytes

from ipi_ecs.core import daemon
from ipi_ecs.dds import client, subsystem
from ipi_ecs.dds import types as dds_types


@dataclass
class RuntimeState:
	managed: bool = False
	started: bool = False
	initializing: bool = False
	connected: bool = False
	warn: bool = False
	error: bool = False
	process_running: bool = False
	name: str = ""


def _uuid6(s_uuid: uuid.UUID) -> str:
	return str(s_uuid).replace("-", "")[-6:]


def _decode_runtime_state(data: bytes) -> RuntimeState:
	if data is None or len(data) < 7:
		return RuntimeState()

	ret = RuntimeState()
	ret.started = bool.from_bytes(bytes([data[0]]), byteorder="big")
	ret.initializing = bool.from_bytes(bytes([data[1]]), byteorder="big")
	ret.connected = bool.from_bytes(bytes([data[2]]), byteorder="big")
	ret.warn = bool.from_bytes(bytes([data[3]]), byteorder="big")
	ret.error = bool.from_bytes(bytes([data[4]]), byteorder="big")
	ret.process_running = bool.from_bytes(bytes([data[5]]), byteorder="big")
	ret.managed = bool.from_bytes(bytes([data[6]]), byteorder="big")

	if len(data) > 7:
		try:
			ret.name = data[7:].decode("utf-8")
		except UnicodeDecodeError:
			ret.name = ""

	return ret


def _decode_runtime_states_blob(data: bytes) -> dict[uuid.UUID, RuntimeState]:
	decoded: dict[uuid.UUID, RuntimeState] = {}
	if data is None or len(data) == 0:
		return decoded

	for item in segment_bytes.decode(data):
		b_uuid, b_state = segment_bytes.decode(item)
		s_uuid = uuid.UUID(bytes=b_uuid)
		decoded[s_uuid] = _decode_runtime_state(b_state)

	return decoded


class LifecycleInterface:
	_PARSE_MODES = (
		"Descriptor",
		"String",
		"Int",
		"Float",
		"Bytes (utf-8)",
		"Bytes (hex)",
		"List (literal)",
	)

	def __init__(self, lifecycle_manager_uuid: uuid.UUID, dds_ip: str = "127.0.0.1"):
		self.__lifecycle_manager_uuid = lifecycle_manager_uuid
		self.__dds_ip = dds_ip

		self.__out_queue: Queue = Queue()
		self.__cmd_queue: Queue = Queue()

		self.__did_config = False
		self.__client: client.DDSClient | None = None
		self.__subsystem_handle: client._RegisteredSubsystemHandle | None = None

		self.__start_event = None
		self.__stop_event = None
		self.__restart_event = None
		self.__start_all_event = None
		self.__stop_all_event = None
		self.__event_providers: dict[bytes, object] = {}

		self.__pending_ops: list[dict] = []
		self.__restart_all_pending = False

		self.__event_consumer = None
		self.__E_SYSTEM_UPDATE = None

		c_uuid = uuid.uuid4()
		s_uuid = uuid.uuid4()
		self.__client = client.DDSClient(c_uuid, ip=dds_ip)

		def _on_ready():
			if self.__did_config:
				return

			self.__did_config = True
			sh = self.__client.register_subsystem(f"__lifecycle_gui_{s_uuid}", s_uuid, temporary=True)
			self.__on_got_subsystem(sh)

		self.__client.when_ready().then(_on_ready)

		import mt_events

		self.__event_consumer = mt_events.EventConsumer()
		self.__E_SYSTEM_UPDATE = self.__client.on_remote_system_update().bind(self.__event_consumer)

		self.__daemon = daemon.Daemon()
		self.__daemon.add(self.__thread)
		self.__daemon.start()

	def __on_got_subsystem(self, handle: client._RegisteredSubsystemHandle):
		self.__subsystem_handle = handle

		self.__start_event = handle.add_event_provider(b"start_subsystem")
		self.__stop_event = handle.add_event_provider(b"stop_subsystem")
		self.__restart_event = handle.add_event_provider(b"restart_subsystem")
		self.__start_all_event = handle.add_event_provider(b"start_all_subsystems")
		self.__stop_all_event = handle.add_event_provider(b"stop_all_subsystems")
		self.__event_providers[b"start_subsystem"] = self.__start_event
		self.__event_providers[b"stop_subsystem"] = self.__stop_event
		self.__event_providers[b"restart_subsystem"] = self.__restart_event
		self.__event_providers[b"start_all_subsystems"] = self.__start_all_event
		self.__event_providers[b"stop_all_subsystems"] = self.__stop_all_event

		self.__emit_snapshot()

	def __await_awaiter(self, awaiter_handle, timeout_s: float = 2.0):
		import mt_events

		done = {"ok": False, "state": None, "reason": None, "value": None, "has_result": False}
		consumer = mt_events.EventConsumer()
		evt = mt_events.Event()

		def _ok(value=None):
			done["ok"] = True
			done["value"] = value
			done["has_result"] = True
			evt.call()

		def _err(*args, **kwargs):
			done["ok"] = False
			done["state"] = kwargs.get("state")
			done["reason"] = kwargs.get("reason")
			if len(args) > 0 and done["state"] is None:
				done["state"] = args[0]
			if len(args) > 1 and done["reason"] is None:
				done["reason"] = args[1]
			done["has_result"] = True
			evt.call()

		awaiter_handle.then(_ok).catch(_err)
		bound = evt.bind(consumer)

		start = time.time()
		while time.time() - start < timeout_s:
			try:
				e = consumer.get(timeout=0.1)
				if e == bound and done["has_result"]:
					return done
			except Empty:
				pass

		return {"ok": False, "state": client.TRANSOP_STATE_REJ, "reason": "Timed out waiting for response.", "value": None, "has_result": False}

	@staticmethod
	def __handle_done(h) -> bool:
		return h.get_state() != client.TRANSOP_STATE_PENDING

	@staticmethod
	def __descriptor_type_name(desc: subsystem.KVDescriptor) -> str:
		t = desc.get_type()
		if t is None:
			return "Unknown"
		return type(t).__name__

	@staticmethod
	def __severity_text(status_items: list[subsystem.StatusItem]) -> str:
		if status_items is None:
			return "OK"

		has_alarm = False
		has_warn = False
		for item in status_items:
			sev = item.get_severity()
			if sev == subsystem.StatusItem.STATE_ALARM:
				has_alarm = True
			elif sev == subsystem.StatusItem.STATE_WARN:
				has_warn = True

		if has_alarm:
			return "ALARM"
		if has_warn:
			return "WARN"
		return "OK"

	def __read_runtime_states(self, lm_remote: client._RemoteSubsystemHandle) -> dict[uuid.UUID, RuntimeState]:
		lm_info = lm_remote.get_info()
		if lm_info is None:
			return {}

		runtime_desc = None
		for desc in lm_info.get_kvs():
			if desc.get_key() == b"lifecycle_manager_runtime_states":
				runtime_desc = desc
				break

		if runtime_desc is None:
			return {}

		# Force manual read behavior: do not subscribe and do not auto-update this KV.
		no_sub_desc = subsystem.KVDescriptor(
			runtime_desc.get_type(),
			runtime_desc.get_key(),
			False,
			runtime_desc.get_readable(),
			runtime_desc.get_writable(),
		)
		kv = self.__subsystem_handle.add_remote_kv(self.__lifecycle_manager_uuid, no_sub_desc)

		try:
			data = kv.value
		except Exception:
			return {}

		if data is None:
			return {}

		return _decode_runtime_states_blob(data)

	def __build_snapshot(self) -> dict:
		if self.__subsystem_handle is None:
			return {
				"rows": [],
				"by_uuid": {},
				"lifecycle_manager_present": False,
				"lifecycle_manager_uuid": self.__lifecycle_manager_uuid,
			}

		all_rows = self.__subsystem_handle.get_all()

		lm_present = False
		lm_remote = None

		enriched_rows = []
		by_uuid = {}

		# Use get_subsystem() per subsystem for enrichment, while get_all() is the source list.
		for remote, state_from_all in all_rows:
			base_info = remote.get_info()
			s_uuid = base_info.get_uuid()

			info = base_info
			state = state_from_all

			h = self.__subsystem_handle.get_subsystem(s_uuid, client.KVP_RET_HANDLE)
			if h is not None:
				start = time.time()
				while not self.__handle_done(h) and time.time() - start < 1.0:
					time.sleep(0.01)
				if self.__handle_done(h) and h.get_state() == client.TRANSOP_STATE_OK and h.get_value() is not None:
					remote = h.get_value()
					info = remote.get_info()

					status_wait = self.__await_awaiter(remote.get_status(), timeout_s=1.0)
					if status_wait["ok"] and status_wait["value"] is not None:
						state = status_wait["value"]

			if s_uuid == self.__lifecycle_manager_uuid:
				lm_present = True
				lm_remote = remote

			row = {
				"uuid": s_uuid,
				"uuid6": _uuid6(s_uuid),
				"name": info.get_name(),
				"temporary": bool(info.get_temporary()),
				"connected": state.get_status() == subsystem.SubsystemStatus.STATE_ALIVE,
				"status_items": list(state.get_status_items()),
				"status_text": self.__severity_text(state.get_status_items()),
				"kvs": list(info.get_kvs()),
				"events_provided": list(info.get_events()[0]),
				"events_handled": list(info.get_events()[1]),
				"managed": False,
				"runtime": RuntimeState(),
			}
			enriched_rows.append(row)
			by_uuid[s_uuid] = row

		runtime_states = {}
		if lm_remote is not None:
			runtime_states = self.__read_runtime_states(lm_remote)

		for s_uuid, runtime_state in runtime_states.items():
			row = by_uuid.get(s_uuid)
			if row is None:
				continue
			row["runtime"] = runtime_state
			row["managed"] = runtime_state.managed

			# Prefer lifecycle runtime connectivity view for managed subsystems.
			row["connected"] = runtime_state.connected

		enriched_rows.sort(key=lambda r: (r["name"] or "", str(r["uuid"])))

		return {
			"rows": enriched_rows,
			"by_uuid": by_uuid,
			"lifecycle_manager_present": lm_present,
			"lifecycle_manager_uuid": self.__lifecycle_manager_uuid,
		}

	def __emit_snapshot(self):
		try:
			snap = self.__build_snapshot()
			self.__out_queue.put(("snapshot", snap))
		except Exception as exc:
			self.__out_queue.put(("error", f"Snapshot failed: {exc}"))

	def __queue_op(self, action: str, s_uuid: uuid.UUID | None):
		if self.__subsystem_handle is None:
			self.__out_queue.put(("error", "DDS client is not ready."))
			return

		if action == "restart_all":
			self.__restart_all_pending = True
			self.__queue_op("stop_all", None)
			return

		provider = None
		if action == "start":
			provider = self.__start_event
		elif action == "stop":
			provider = self.__stop_event
		elif action == "restart":
			provider = self.__restart_event
		elif action == "start_all":
			provider = self.__start_all_event
		elif action == "stop_all":
			provider = self.__stop_all_event

		if provider is None:
			self.__out_queue.put(("error", f"'{action}' provider is not available."))
			return

		payload = bytes() if s_uuid is None else s_uuid.bytes
		handle = provider.call(payload, [self.__lifecycle_manager_uuid])
		if handle is None:
			self.__out_queue.put(("error", f"Failed to send '{action}' request."))
			return

		self.__pending_ops.append({"action": action, "uuid": s_uuid, "handle": handle, "queued": time.time()})
		self.__out_queue.put(("op_started", action, s_uuid))

	def __check_pending_ops(self):
		did_remove = False
		removed = True
		while removed:
			removed = False
			for op in self.__pending_ops:
				h = op["handle"]
				target = self.__lifecycle_manager_uuid if op["action"] != "event" else op["uuid"]
				state = h.get_state(target)
				result = h.get_result(target)

				if h.is_in_progress():
					if state == client.EVENT_IN_PROGRESS:
						feedback = ""
						if isinstance(result, bytes):
							try:
								feedback = result.decode("utf-8")
							except UnicodeDecodeError:
								feedback = str(result)
						elif result is not None:
							feedback = str(result)

						if feedback and feedback != op.get("last_feedback"):
							op["last_feedback"] = feedback
							if op["action"] == "event":
								self.__out_queue.put(("event_feedback", op["uuid"], op.get("event_name", b""), feedback))
							else:
								self.__out_queue.put(("op_feedback", op["action"], op["uuid"], feedback))
					continue

				ok = state == client.EVENT_OK
				message = ""
				if op["action"] == "event":
					e_name = op.get("event_name", b"")
					e_name_str = e_name.decode("utf-8", errors="replace") if isinstance(e_name, bytes) else str(e_name)
					if ok:
						message = f"Event '{e_name_str}' succeeded for ...{_uuid6(op['uuid'])}."
					else:
						if isinstance(result, bytes):
							try:
								message = result.decode("utf-8")
							except UnicodeDecodeError:
								message = str(result)
						else:
							message = str(result)
						if message == "None":
							message = f"Event '{e_name_str}' failed for ...{_uuid6(op['uuid'])}."
					self.__out_queue.put(("event_done", op["uuid"], e_name, ok, message))
				elif ok:
					if op["uuid"] is None:
						message = f"{op['action'].replace('_', ' ').capitalize()} succeeded."
					else:
						message = f"{op['action'].capitalize()} succeeded for ...{_uuid6(op['uuid'])}."
					self.__out_queue.put(("op_done", op["action"], op["uuid"], ok, message))
				else:
					if isinstance(result, bytes):
						try:
							message = result.decode("utf-8")
						except UnicodeDecodeError:
							message = str(result)
					else:
						message = str(result)
					if message == "None":
						if op["uuid"] is None:
							message = f"{op['action'].replace('_', ' ').capitalize()} failed."
						else:
							message = f"{op['action'].capitalize()} failed for ...{_uuid6(op['uuid'])}."

					self.__out_queue.put(("op_done", op["action"], op["uuid"], ok, message))

				if op["action"] == "stop_all" and self.__restart_all_pending:
					if ok:
						self.__restart_all_pending = False
						self.__queue_op("start_all", None)
					else:
						self.__restart_all_pending = False

				self.__pending_ops.remove(op)
				removed = True
				did_remove = True
				break

		if did_remove:
			self.__emit_snapshot()

	@staticmethod
	def __render_value(value) -> str:
		if value is None:
			return "(none)"
		if isinstance(value, bytes):
			if len(value) <= 64:
				return f"bytes[{len(value)}]: {value.hex()}"
			return f"bytes[{len(value)}]: {value[:32].hex()}..."
		return str(value)

	@staticmethod
	def __parse_typed_input(type_spec: dds_types.PropertyTypeSpecifier, raw: str, parse_mode: str = "Descriptor"):
		text = raw.strip()

		if parse_mode == "String":
			return text

		if parse_mode == "Int":
			return int(text)

		if parse_mode == "Float":
			return float(text)

		if parse_mode == "Bytes (utf-8)":
			return text.encode("utf-8")

		if parse_mode == "Bytes (hex)":
			if text.startswith("hex:"):
				text = text[4:].strip()
			return bytes.fromhex(text.replace(" ", ""))

		if parse_mode == "List (literal)":
			v = ast.literal_eval(text)
			if not isinstance(v, list):
				raise ValueError("Input must be a Python list literal.")
			return v

		if isinstance(type_spec, dds_types.IntegerTypeSpecifier):
			return int(text)

		if isinstance(type_spec, dds_types.FloatTypeSpecifier):
			return float(text)

		if isinstance(type_spec, dds_types.ByteTypeSpecifier):
			if text.startswith("hex:"):
				h = text[4:].strip().replace(" ", "")
				return bytes.fromhex(h)
			return text.encode("utf-8")

		if isinstance(type_spec, dds_types.VectorTypeSpecifier):
			v = ast.literal_eval(text)
			if not isinstance(v, list):
				raise ValueError("Vector input must be a Python list literal.")

			e_type = getattr(type_spec, "_VectorTypeSpecifier__element_type", None)
			if e_type is None:
				return v

			parsed = []
			for item in v:
				if isinstance(e_type, dds_types.IntegerTypeSpecifier):
					parsed.append(int(item))
				elif isinstance(e_type, dds_types.FloatTypeSpecifier):
					parsed.append(float(item))
				elif isinstance(e_type, dds_types.ByteTypeSpecifier):
					if isinstance(item, bytes):
						parsed.append(item)
					else:
						parsed.append(str(item).encode("utf-8"))
				else:
					parsed.append(item)
			return parsed

		# Fallback for unknown custom types.
		return text

	def __read_kv(self, s_uuid: uuid.UUID, desc: subsystem.KVDescriptor):
		if self.__subsystem_handle is None:
			self.__out_queue.put(("error", "DDS client is not ready."))
			return

		if not desc.get_readable():
			self.__out_queue.put(("kv_value", s_uuid, desc.get_key(), False, "KV is not readable."))
			return

		no_sub_desc = subsystem.KVDescriptor(desc.get_type(), desc.get_key(), False, desc.get_readable(), desc.get_writable())
		kv = self.__subsystem_handle.add_remote_kv(s_uuid, no_sub_desc)

		try:
			value = kv.value
			self.__out_queue.put(("kv_value", s_uuid, desc.get_key(), True, self.__render_value(value)))
		except Exception as exc:
			self.__out_queue.put(("kv_value", s_uuid, desc.get_key(), False, str(exc)))

	def __write_kv(self, s_uuid: uuid.UUID, desc: subsystem.KVDescriptor, raw_value: str, parse_mode: str = "Descriptor"):
		if self.__subsystem_handle is None:
			self.__out_queue.put(("error", "DDS client is not ready."))
			return

		if not desc.get_writable():
			self.__out_queue.put(("kv_write", s_uuid, desc.get_key(), False, "KV is not writable."))
			return

		try:
			parsed = self.__parse_typed_input(desc.get_type(), raw_value, parse_mode=parse_mode)
		except Exception as exc:
			self.__out_queue.put(("kv_write", s_uuid, desc.get_key(), False, f"Invalid value: {exc}"))
			return

		no_sub_desc = subsystem.KVDescriptor(desc.get_type(), desc.get_key(), False, desc.get_readable(), desc.get_writable())
		kv = self.__subsystem_handle.add_remote_kv(s_uuid, no_sub_desc)

		ret = kv.try_set(parsed, client.KVP_RET_AWAIT)
		if ret is None:
			self.__out_queue.put(("kv_write", s_uuid, desc.get_key(), False, "Set request did not start."))
			return

		result = self.__await_awaiter(ret, timeout_s=2.0)
		if result["ok"]:
			self.__out_queue.put(("kv_write", s_uuid, desc.get_key(), True, "Write successful."))
		else:
			reason = result.get("reason")
			self.__out_queue.put(("kv_write", s_uuid, desc.get_key(), False, str(reason)))

	def __call_event(self, target_uuid: uuid.UUID, desc: subsystem.EventDescriptor, raw_param: str, parse_mode: str = "Descriptor"):
		if self.__subsystem_handle is None:
			self.__out_queue.put(("error", "DDS client is not ready."))
			return

		name = desc.get_name()
		provider = self.__event_providers.get(name)
		if provider is None:
			provider = self.__subsystem_handle.add_event_provider(name)
			self.__event_providers[name] = provider

		try:
			typed_param = self.__parse_typed_input(desc.get_parameter_type(), raw_param, parse_mode=parse_mode)
		except Exception as exc:
			self.__out_queue.put(("event_done", target_uuid, name, False, f"Invalid parameter value: {exc}"))
			return

		h = provider.call(typed_param, [target_uuid])
		if h is None:
			self.__out_queue.put(("event_done", target_uuid, name, False, "Failed to send event request."))
			return

		self.__pending_ops.append({"action": "event", "uuid": target_uuid, "handle": h, "queued": time.time(), "event_name": name})
		self.__out_queue.put(("event_started", target_uuid, name))

	def __handle_cmd(self, cmd):
		kind = cmd[0]

		if kind == "refresh":
			self.__emit_snapshot()
			return

		if kind == "operate":
			_, action, s_uuid = cmd
			self.__queue_op(action, s_uuid)
			return

		if kind == "read_kv":
			_, s_uuid, desc = cmd
			self.__read_kv(s_uuid, desc)
			return

		if kind == "write_kv":
			_, s_uuid, desc, raw_value, parse_mode = cmd
			self.__write_kv(s_uuid, desc, raw_value, parse_mode=parse_mode)
			return

		if kind == "call_event":
			_, target_uuid, desc, raw_param, parse_mode = cmd
			self.__call_event(target_uuid, desc, raw_param, parse_mode=parse_mode)
			return

	def __thread(self, stop_flag: daemon.StopFlag):
		while stop_flag.run():
			try:
				while True:
					cmd = self.__cmd_queue.get_nowait()
					self.__handle_cmd(cmd)
			except Empty:
				pass

			self.__check_pending_ops()

			if self.__subsystem_handle is None:
				time.sleep(0.05)
				continue

			try:
				e = self.__event_consumer.get(timeout=0.2)
				if e == self.__E_SYSTEM_UPDATE:
					self.__emit_snapshot()
			except Empty:
				pass

	def request_refresh(self):
		self.__cmd_queue.put(("refresh",))

	def request_operation(self, action: str, s_uuid: uuid.UUID | None):
		self.__cmd_queue.put(("operate", action, s_uuid))

	def request_read_kv(self, s_uuid: uuid.UUID, desc: subsystem.KVDescriptor):
		self.__cmd_queue.put(("read_kv", s_uuid, desc))

	def request_write_kv(self, s_uuid: uuid.UUID, desc: subsystem.KVDescriptor, raw_value: str, parse_mode: str = "Descriptor"):
		self.__cmd_queue.put(("write_kv", s_uuid, desc, raw_value, parse_mode))

	def request_call_event(self, target_uuid: uuid.UUID, desc: subsystem.EventDescriptor, raw_param: str, parse_mode: str = "Descriptor"):
		self.__cmd_queue.put(("call_event", target_uuid, desc, raw_param, parse_mode))

	def pop_messages(self) -> list:
		msgs = []
		try:
			while True:
				msgs.append(self.__out_queue.get_nowait())
		except Empty:
			pass
		return msgs

	def close(self):
		if self.__client is not None:
			self.__client.close()
		self.__daemon.stop()


class LifecycleGUI:
	_PARSE_MODE_OPTIONS = (
		"Descriptor",
		"String",
		"Int",
		"Float",
		"Bytes (utf-8)",
		"Bytes (hex)",
		"List (literal)",
	)

	def __init__(self, root: tk.Tk, lifecycle_manager_uuid: uuid.UUID):
		self.root = root
		self.__interface = LifecycleInterface(lifecycle_manager_uuid)

		self.__snapshot = {
			"rows": [],
			"by_uuid": {},
			"lifecycle_manager_present": False,
			"lifecycle_manager_uuid": lifecycle_manager_uuid,
		}
		self.__kv_last_values: dict[tuple[uuid.UUID, bytes], str] = {}
		self.__active_operation_count = 0
		self.__request_dialog = None
		self.__request_progress = None
		self.__request_status_var = tk.StringVar(value="")
		self.__request_close_btn = None
		self.__request_auto_close_job = None

		root.title("Lifecycle Manager Browser")
		root.geometry("1280x760")
		root.minsize(980, 620)

		self.__status_var = tk.StringVar(value="Connecting...")

		self.__build()
		self.__refresh_controls()
		self.__interface.request_refresh()

		root.protocol("WM_DELETE_WINDOW", self.on_close)
		self.__updater()

	def __build(self):
		top = ttk.Frame(self.root)
		top.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)

		paned = ttk.PanedWindow(top, orient=tk.HORIZONTAL)
		paned.pack(fill=tk.BOTH, expand=True)

		left = ttk.Frame(paned)
		right = ttk.Frame(paned)

		paned.add(left, weight=3)
		paned.add(right, weight=2)

		self.__build_subsystem_table(left)
		self.__build_detail_panel(right)

		status = ttk.Label(self.root, textvariable=self.__status_var, anchor=tk.W, relief=tk.SUNKEN, padding=(4, 2))
		status.pack(side=tk.BOTTOM, fill=tk.X)

	def __build_subsystem_table(self, parent):
		action_bar = ttk.Frame(parent)
		action_bar.pack(fill=tk.X, pady=(0, 4))

		ttk.Button(action_bar, text="Refresh", command=self.__on_refresh).pack(side=tk.LEFT, padx=2)

		self.__start_btn = ttk.Button(action_bar, text="Start", command=self.__on_start)
		self.__start_btn.pack(side=tk.LEFT, padx=2)

		self.__stop_btn = ttk.Button(action_bar, text="Stop", command=self.__on_stop)
		self.__stop_btn.pack(side=tk.LEFT, padx=2)

		self.__restart_btn = ttk.Button(action_bar, text="Restart", command=self.__on_restart)
		self.__restart_btn.pack(side=tk.LEFT, padx=2)

		self.__start_all_btn = ttk.Button(action_bar, text="Start All", command=self.__on_start_all)
		self.__start_all_btn.pack(side=tk.LEFT, padx=(12, 2))

		self.__stop_all_btn = ttk.Button(action_bar, text="Stop All", command=self.__on_stop_all)
		self.__stop_all_btn.pack(side=tk.LEFT, padx=2)

		self.__restart_all_btn = ttk.Button(action_bar, text="Restart All", command=self.__on_restart_all)
		self.__restart_all_btn.pack(side=tk.LEFT, padx=2)

		cols = ("name", "uuid6", "connected", "managed", "process", "init", "started", "status")
		tree_frame = ttk.Frame(parent)
		tree_frame.pack(fill=tk.BOTH, expand=True)

		self.__tree = ttk.Treeview(tree_frame, columns=cols, show="headings", selectmode="browse")

		self.__tree.heading("name", text="Name")
		self.__tree.heading("uuid6", text="UUID")
		self.__tree.heading("connected", text="Connected")
		self.__tree.heading("managed", text="Managed")
		self.__tree.heading("process", text="Process")
		self.__tree.heading("init", text="Init")
		self.__tree.heading("started", text="Started")
		self.__tree.heading("status", text="Status")

		self.__tree.column("name", width=220, minwidth=120)
		self.__tree.column("uuid6", width=90, minwidth=70, anchor=tk.CENTER)
		self.__tree.column("connected", width=90, minwidth=70, anchor=tk.CENTER)
		self.__tree.column("managed", width=90, minwidth=70, anchor=tk.CENTER)
		self.__tree.column("process", width=90, minwidth=70, anchor=tk.CENTER)
		self.__tree.column("init", width=70, minwidth=55, anchor=tk.CENTER)
		self.__tree.column("started", width=80, minwidth=60, anchor=tk.CENTER)
		self.__tree.column("status", width=100, minwidth=80, anchor=tk.CENTER)

		vsb = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.__tree.yview)
		hsb = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL, command=self.__tree.xview)
		self.__tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

		self.__tree.pack(fill=tk.BOTH, expand=True, side=tk.LEFT)
		vsb.pack(fill=tk.Y, side=tk.RIGHT)
		hsb.pack(fill=tk.X, side=tk.BOTTOM)

		self.__tree.bind("<<TreeviewSelect>>", self.__on_select_subsystem)

		self.__tree_uuid_by_iid: dict[str, uuid.UUID] = {}

		status_bar_lf = ttk.LabelFrame(parent, text="Active Status Items", padding=4)
		status_bar_lf.pack(fill=tk.X, pady=(4, 0))
		self.__active_status_tree = ttk.Treeview(
			status_bar_lf,
			columns=("subsystem", "sev", "code", "message"),
			show="headings",
			height=5,
			selectmode="none",
		)
		self.__active_status_tree.heading("subsystem", text="Subsystem")
		self.__active_status_tree.heading("sev", text="Severity")
		self.__active_status_tree.heading("code", text="Code")
		self.__active_status_tree.heading("message", text="Message")
		self.__active_status_tree.column("subsystem", width=180, minwidth=100)
		self.__active_status_tree.column("sev", width=80, minwidth=65, anchor=tk.CENTER)
		self.__active_status_tree.column("code", width=65, minwidth=50, anchor=tk.CENTER)
		self.__active_status_tree.column("message", width=520, minwidth=150)
		self.__active_status_tree.pack(fill=tk.X, expand=True)

		self.__active_status_tree.tag_configure("alarm", background="#ffd8d8")
		self.__active_status_tree.tag_configure("warn", background="#fff4cc")
		self.__active_status_tree.tag_configure("info", background="#dff1ff")

	def __build_detail_panel(self, parent):
		summary = ttk.LabelFrame(parent, text="Selected Subsystem", padding=8)
		summary.pack(fill=tk.X, pady=(0, 4))

		summary.columnconfigure(1, weight=1)

		ttk.Label(summary, text="Name:").grid(row=0, column=0, sticky=tk.W)
		self.__sel_name = tk.StringVar(value="")
		ttk.Label(summary, textvariable=self.__sel_name).grid(row=0, column=1, sticky=tk.W)

		ttk.Label(summary, text="UUID:").grid(row=1, column=0, sticky=tk.W)
		self.__sel_uuid = tk.StringVar(value="")
		ttk.Label(summary, textvariable=self.__sel_uuid).grid(row=1, column=1, sticky=tk.W)

		ttk.Label(summary, text="Managed:").grid(row=2, column=0, sticky=tk.W)
		self.__sel_managed = tk.StringVar(value="")
		ttk.Label(summary, textvariable=self.__sel_managed).grid(row=2, column=1, sticky=tk.W)

		ttk.Label(summary, text="Connected:").grid(row=3, column=0, sticky=tk.W)
		self.__sel_connected = tk.StringVar(value="")
		ttk.Label(summary, textvariable=self.__sel_connected).grid(row=3, column=1, sticky=tk.W)

		status_lf = ttk.LabelFrame(parent, text="Status Items", padding=6)
		status_lf.pack(fill=tk.BOTH, expand=False, pady=(0, 4))

		self.__status_tree = ttk.Treeview(status_lf, columns=("sev", "code", "message"), show="headings", height=6)
		self.__status_tree.heading("sev", text="Severity")
		self.__status_tree.heading("code", text="Code")
		self.__status_tree.heading("message", text="Message")
		self.__status_tree.column("sev", width=80, minwidth=70, anchor=tk.CENTER)
		self.__status_tree.column("code", width=60, minwidth=50, anchor=tk.CENTER)
		self.__status_tree.column("message", width=340, minwidth=140)
		self.__status_tree.pack(fill=tk.BOTH, expand=True)

		kv_lf = ttk.LabelFrame(parent, text="KVs (Descriptor + Manual Read)", padding=6)
		kv_lf.pack(fill=tk.BOTH, expand=True)

		kv_btn_bar = ttk.Frame(kv_lf)
		kv_btn_bar.pack(fill=tk.X, pady=(0, 4))
		self.__read_kv_btn = ttk.Button(kv_btn_bar, text="Read Selected KV", command=self.__on_read_kv)
		self.__read_kv_btn.pack(side=tk.LEFT)
		self.__write_kv_btn = ttk.Button(kv_btn_bar, text="Write Selected KV", command=self.__on_write_kv)
		self.__write_kv_btn.pack(side=tk.LEFT, padx=(4, 0))

		self.__kv_write_value_var = tk.StringVar(value="")
		self.__kv_parse_mode_var = tk.StringVar(value="Descriptor")
		ttk.Label(kv_btn_bar, text="Value:").pack(side=tk.LEFT, padx=(10, 2))
		self.__kv_write_entry = ttk.Entry(kv_btn_bar, textvariable=self.__kv_write_value_var, width=22)
		self.__kv_write_entry.pack(side=tk.LEFT)
		ttk.Label(kv_btn_bar, text="Type:").pack(side=tk.LEFT, padx=(8, 2))
		self.__kv_parse_mode_combo = ttk.Combobox(
			kv_btn_bar,
			textvariable=self.__kv_parse_mode_var,
			values=self._PARSE_MODE_OPTIONS,
			state="readonly",
			width=14,
		)
		self.__kv_parse_mode_combo.pack(side=tk.LEFT)
		ttk.Label(kv_btn_bar, text="(Descriptor uses KV type)", foreground="gray").pack(side=tk.LEFT, padx=(8, 0))

		self.__kv_tree = ttk.Treeview(
			kv_lf,
			columns=("key", "type", "pub", "read", "write", "value"),
			show="headings",
			selectmode="browse",
			height=12,
		)
		self.__kv_tree.heading("key", text="Key")
		self.__kv_tree.heading("type", text="Type")
		self.__kv_tree.heading("pub", text="Pub")
		self.__kv_tree.heading("read", text="Read")
		self.__kv_tree.heading("write", text="Write")
		self.__kv_tree.heading("value", text="Last Read Value")

		self.__kv_tree.column("key", width=170, minwidth=100)
		self.__kv_tree.column("type", width=120, minwidth=80)
		self.__kv_tree.column("pub", width=52, minwidth=45, anchor=tk.CENTER)
		self.__kv_tree.column("read", width=52, minwidth=45, anchor=tk.CENTER)
		self.__kv_tree.column("write", width=52, minwidth=45, anchor=tk.CENTER)
		self.__kv_tree.column("value", width=280, minwidth=120)

		self.__kv_tree.pack(fill=tk.BOTH, expand=True)
		self.__kv_tree.bind("<<TreeviewSelect>>", lambda _e: self.__refresh_controls())
		self.__kv_desc_by_iid: dict[str, subsystem.KVDescriptor] = {}

		events_lf = ttk.LabelFrame(parent, text="Events", padding=6)
		events_lf.pack(fill=tk.BOTH, expand=True, pady=(4, 0))

		provided_lf = ttk.LabelFrame(events_lf, text="Provided Events", padding=4)
		provided_lf.pack(fill=tk.BOTH, expand=True, pady=(0, 4))
		self.__events_provided_tree = ttk.Treeview(
			provided_lf,
			columns=("name", "param", "ret"),
			show="headings",
			selectmode="browse",
			height=5,
		)
		self.__events_provided_tree.heading("name", text="Name")
		self.__events_provided_tree.heading("param", text="Param Type")
		self.__events_provided_tree.heading("ret", text="Return Type")
		self.__events_provided_tree.column("name", width=170, minwidth=100)
		self.__events_provided_tree.column("param", width=120, minwidth=70)
		self.__events_provided_tree.column("ret", width=120, minwidth=70)
		self.__events_provided_tree.pack(fill=tk.BOTH, expand=True)

		handled_lf = ttk.LabelFrame(events_lf, text="Handled Events", padding=4)
		handled_lf.pack(fill=tk.BOTH, expand=True)
		call_bar = ttk.Frame(handled_lf)
		call_bar.pack(fill=tk.X, pady=(0, 4))
		self.__call_event_btn = ttk.Button(call_bar, text="Call Selected Handled Event", command=self.__on_call_event)
		self.__call_event_btn.pack(side=tk.LEFT)
		self.__event_param_var = tk.StringVar(value="")
		self.__event_parse_mode_var = tk.StringVar(value="Descriptor")
		ttk.Label(call_bar, text="Param:").pack(side=tk.LEFT, padx=(8, 2))
		self.__event_param_entry = ttk.Entry(call_bar, textvariable=self.__event_param_var, width=22)
		self.__event_param_entry.pack(side=tk.LEFT)
		ttk.Label(call_bar, text="Type:").pack(side=tk.LEFT, padx=(8, 2))
		self.__event_parse_mode_combo = ttk.Combobox(
			call_bar,
			textvariable=self.__event_parse_mode_var,
			values=self._PARSE_MODE_OPTIONS,
			state="readonly",
			width=14,
		)
		self.__event_parse_mode_combo.pack(side=tk.LEFT)

		self.__events_handled_tree = ttk.Treeview(
			handled_lf,
			columns=("name", "param", "ret"),
			show="headings",
			selectmode="browse",
			height=6,
		)
		self.__events_handled_tree.heading("name", text="Name")
		self.__events_handled_tree.heading("param", text="Param Type")
		self.__events_handled_tree.heading("ret", text="Return Type")
		self.__events_handled_tree.column("name", width=170, minwidth=100)
		self.__events_handled_tree.column("param", width=120, minwidth=70)
		self.__events_handled_tree.column("ret", width=120, minwidth=70)
		self.__events_handled_tree.pack(fill=tk.BOTH, expand=True)

		self.__events_handled_tree.bind("<<TreeviewSelect>>", lambda _e: self.__refresh_controls())

		self.__event_desc_by_iid: dict[str, subsystem.EventDescriptor] = {}

		activity_lf = ttk.LabelFrame(parent, text="Activity", padding=6)
		activity_lf.pack(fill=tk.BOTH, expand=False, pady=(4, 0))
		self.__activity_list = tk.Listbox(activity_lf, height=6)
		self.__activity_list.pack(fill=tk.BOTH, expand=True)

	def __push_activity(self, text: str):
		self.__activity_list.insert(tk.END, text)
		while self.__activity_list.size() > 100:
			self.__activity_list.delete(0)
		self.__activity_list.yview_moveto(1.0)

	def __ensure_request_dialog(self):
		if self.__request_dialog is not None and self.__request_dialog.winfo_exists():
			return

		dialog = tk.Toplevel(self.root)
		dialog.title("Request Status")
		dialog.geometry("520x140")
		dialog.resizable(False, False)
		dialog.transient(self.root)

		frame = ttk.Frame(dialog, padding=10)
		frame.pack(fill=tk.BOTH, expand=True)

		ttk.Label(frame, textvariable=self.__request_status_var).pack(anchor=tk.W)

		self.__request_progress = ttk.Progressbar(frame, mode="indeterminate")
		self.__request_progress.pack(fill=tk.X, pady=(10, 8))

		btn_row = ttk.Frame(frame)
		btn_row.pack(fill=tk.X)
		self.__request_close_btn = ttk.Button(btn_row, text="Close", command=self.__close_request_dialog)
		self.__request_close_btn.pack(side=tk.RIGHT)
		self.__request_close_btn.config(state=tk.DISABLED)

		dialog.protocol("WM_DELETE_WINDOW", self.__close_request_dialog)
		self.__request_dialog = dialog

	def __close_request_dialog(self):
		if self.__request_auto_close_job is not None:
			self.root.after_cancel(self.__request_auto_close_job)
			self.__request_auto_close_job = None

		if self.__request_dialog is None or not self.__request_dialog.winfo_exists():
			return

		if self.__active_operation_count > 0:
			return

		self.__request_dialog.destroy()
		self.__request_dialog = None

	def __set_request_dialog_active(self, text: str):
		self.__ensure_request_dialog()
		if self.__request_auto_close_job is not None:
			self.root.after_cancel(self.__request_auto_close_job)
			self.__request_auto_close_job = None
		self.__request_status_var.set(text)
		if self.__request_progress is not None:
			self.__request_progress.config(mode="indeterminate")
			self.__request_progress.start(10)
		if self.__request_close_btn is not None and self.__request_close_btn.winfo_exists():
			self.__request_close_btn.config(state=tk.DISABLED, text="Close")

	def __set_request_dialog_done(self, text: str):
		self.__ensure_request_dialog()
		self.__request_status_var.set(text)
		if self.__request_progress is not None:
			self.__request_progress.stop()
		if self.__request_close_btn is not None and self.__request_close_btn.winfo_exists():
			self.__request_close_btn.config(state=tk.NORMAL, text="Close")

	def __schedule_request_dialog_close(self, delay_ms: int = 1200):
		if self.__request_auto_close_job is not None:
			self.root.after_cancel(self.__request_auto_close_job)

		def _close_if_idle():
			self.__request_auto_close_job = None
			if self.__active_operation_count == 0:
				self.__close_request_dialog()

		self.__request_auto_close_job = self.root.after(delay_ms, _close_if_idle)

	def __update_active_status_items_bar(self):
		self.__active_status_tree.delete(*self.__active_status_tree.get_children())

		count = 0
		for row in self.__snapshot.get("rows", []):
			s_name = row.get("name") or f"...{row.get('uuid6', '??????')}"
			for item in row.get("status_items", []):
				sev = item.get_severity()
				sev_str = self.__sev_text(sev)
				tag = "info"
				if sev == subsystem.StatusItem.STATE_ALARM:
					tag = "alarm"
				elif sev == subsystem.StatusItem.STATE_WARN:
					tag = "warn"

				self.__active_status_tree.insert(
					"",
					tk.END,
					values=(s_name, sev_str, item.get_code(), item.get_message()),
					tags=(tag,),
				)
				count += 1

		if count == 0:
			self.__active_status_tree.insert("", tk.END, values=("(none)", "", "", "No active status items."))

	def __selected_uuid(self):
		selection = self.__tree.selection()
		if not selection:
			return None
		return self.__tree_uuid_by_iid.get(selection[0])

	def __selected_row(self):
		s_uuid = self.__selected_uuid()
		if s_uuid is None:
			return None
		return self.__snapshot["by_uuid"].get(s_uuid)

	@staticmethod
	def __sev_text(sev: int) -> str:
		if sev == subsystem.StatusItem.STATE_ALARM:
			return "ALARM"
		if sev == subsystem.StatusItem.STATE_WARN:
			return "WARN"
		return "INFO"

	def __update_detail_panel(self):
		row = self.__selected_row()
		self.__status_tree.delete(*self.__status_tree.get_children())
		self.__kv_tree.delete(*self.__kv_tree.get_children())
		self.__events_provided_tree.delete(*self.__events_provided_tree.get_children())
		self.__events_handled_tree.delete(*self.__events_handled_tree.get_children())
		self.__kv_desc_by_iid.clear()
		self.__event_desc_by_iid.clear()

		if row is None:
			self.__sel_name.set("")
			self.__sel_uuid.set("")
			self.__sel_managed.set("")
			self.__sel_connected.set("")
			return

		self.__sel_name.set(row["name"])
		self.__sel_uuid.set(f"{row['uuid']} (...{row['uuid6']})")
		self.__sel_managed.set("Yes" if row["managed"] else "No")
		self.__sel_connected.set("Yes" if row["connected"] else "No")

		for item in row["status_items"]:
			self.__status_tree.insert(
				"",
				tk.END,
				values=(self.__sev_text(item.get_severity()), item.get_code(), item.get_message()),
			)

		for desc in row["kvs"]:
			key = desc.get_key()
			key_str = key.decode("utf-8", errors="replace")
			value = self.__kv_last_values.get((row["uuid"], key), "")
			iid = self.__kv_tree.insert(
				"",
				tk.END,
				values=(
					key_str,
					type(desc.get_type()).__name__,
					"Y" if desc.get_published() else "N",
					"Y" if desc.get_readable() else "N",
					"Y" if desc.get_writable() else "N",
					value,
				),
			)
			self.__kv_desc_by_iid[iid] = desc

		for e_desc in row.get("events_provided", []):
			e_name = e_desc.get_name().decode("utf-8", errors="replace")
			self.__events_provided_tree.insert(
				"",
				tk.END,
				values=(e_name, type(e_desc.get_parameter_type()).__name__, type(e_desc.get_return_type()).__name__),
			)

		for e_desc in row.get("events_handled", []):
			e_name = e_desc.get_name().decode("utf-8", errors="replace")
			iid = self.__events_handled_tree.insert(
				"",
				tk.END,
				values=(e_name, type(e_desc.get_parameter_type()).__name__, type(e_desc.get_return_type()).__name__),
			)
			self.__event_desc_by_iid[iid] = e_desc

	def __refresh_controls(self):
		row = self.__selected_row()
		managed_selected = row is not None and row.get("managed", False)
		lm_present = self.__snapshot.get("lifecycle_manager_present", False)

		btn_state = tk.NORMAL if (row is not None and managed_selected and lm_present) else tk.DISABLED
		self.__start_btn.config(state=btn_state)
		self.__stop_btn.config(state=btn_state)
		self.__restart_btn.config(state=btn_state)

		all_btn_state = tk.NORMAL if lm_present else tk.DISABLED
		self.__start_all_btn.config(state=all_btn_state)
		self.__stop_all_btn.config(state=all_btn_state)
		self.__restart_all_btn.config(state=all_btn_state)

		kv_sel = self.__kv_tree.selection()
		self.__read_kv_btn.config(state=tk.NORMAL if kv_sel and row is not None else tk.DISABLED)

		write_enabled = False
		if kv_sel and row is not None:
			desc = self.__kv_desc_by_iid.get(kv_sel[0])
			write_enabled = desc is not None and desc.get_writable()
		self.__write_kv_btn.config(state=tk.NORMAL if write_enabled else tk.DISABLED)

		e_sel = self.__events_handled_tree.selection()
		call_enabled = e_sel and row is not None
		self.__call_event_btn.config(state=tk.NORMAL if call_enabled else tk.DISABLED)

	def __rebuild_table(self):
		previous = self.__selected_uuid()

		self.__tree.delete(*self.__tree.get_children())
		self.__tree_uuid_by_iid.clear()

		selected_iid = None
		for row in self.__snapshot["rows"]:
			runtime = row["runtime"]
			iid = self.__tree.insert(
				"",
				tk.END,
				values=(
					row["name"],
					row["uuid6"],
					"Y" if row["connected"] else "N",
					"Y" if row["managed"] else "N",
					"Y" if runtime.process_running else "N",
					"Y" if runtime.initializing else "N",
					"Y" if runtime.started else "N",
					row["status_text"],
				),
			)
			self.__tree_uuid_by_iid[iid] = row["uuid"]
			if previous is not None and row["uuid"] == previous:
				selected_iid = iid

		if selected_iid is not None:
			self.__tree.selection_set(selected_iid)

		self.__update_active_status_items_bar()
		self.__update_detail_panel()
		self.__refresh_controls()

	def __on_select_subsystem(self, _event=None):
		self.__update_detail_panel()
		self.__refresh_controls()

	def __on_refresh(self):
		self.__interface.request_refresh()
		self.__status_var.set("Refreshing subsystem snapshot...")

	def __on_start(self):
		row = self.__selected_row()
		if row is None:
			return
		self.__interface.request_operation("start", row["uuid"])

	def __on_stop(self):
		row = self.__selected_row()
		if row is None:
			return
		self.__interface.request_operation("stop", row["uuid"])

	def __on_restart(self):
		row = self.__selected_row()
		if row is None:
			return
		self.__interface.request_operation("restart", row["uuid"])

	def __on_start_all(self):
		self.__interface.request_operation("start_all", None)

	def __on_stop_all(self):
		self.__interface.request_operation("stop_all", None)

	def __on_restart_all(self):
		self.__interface.request_operation("restart_all", None)

	def __on_read_kv(self):
		row = self.__selected_row()
		if row is None:
			return

		sel = self.__kv_tree.selection()
		if not sel:
			messagebox.showinfo("Read KV", "Select a KV first.")
			return

		desc = self.__kv_desc_by_iid.get(sel[0])
		if desc is None:
			return

		self.__interface.request_read_kv(row["uuid"], desc)
		key_name = desc.get_key().decode("utf-8", errors="replace")
		self.__status_var.set(f"Reading KV '{key_name}' for ...{row['uuid6']}...")

	def __on_write_kv(self):
		row = self.__selected_row()
		if row is None:
			return

		sel = self.__kv_tree.selection()
		if not sel:
			messagebox.showinfo("Write KV", "Select a KV first.")
			return

		desc = self.__kv_desc_by_iid.get(sel[0])
		if desc is None:
			return

		if not desc.get_writable():
			messagebox.showerror("Write KV", "Selected KV is not writable.")
			return

		raw = self.__kv_write_value_var.get()
		parse_mode = self.__kv_parse_mode_var.get() or "Descriptor"
		self.__interface.request_write_kv(row["uuid"], desc, raw, parse_mode=parse_mode)
		key_name = desc.get_key().decode("utf-8", errors="replace")
		self.__status_var.set(f"Writing KV '{key_name}' for ...{row['uuid6']} (type={parse_mode})...")

	def __on_call_event(self):
		row = self.__selected_row()
		if row is None:
			return

		sel = self.__events_handled_tree.selection()
		if not sel:
			messagebox.showinfo("Call Event", "Select a handled event first.")
			return

		e_desc = self.__event_desc_by_iid.get(sel[0])
		if e_desc is None:
			return

		param_text = self.__event_param_var.get()
		parse_mode = self.__event_parse_mode_var.get() or "Descriptor"
		self.__interface.request_call_event(row["uuid"], e_desc, param_text, parse_mode=parse_mode)
		e_name = e_desc.get_name().decode("utf-8", errors="replace")
		self.__status_var.set(f"Calling event '{e_name}' on ...{row['uuid6']} (type={parse_mode})...")

	def __apply_message(self, msg):
		kind = msg[0]

		if kind == "snapshot":
			_, snapshot = msg
			self.__snapshot = snapshot
			n_rows = len(snapshot["rows"])
			lm_text = "found" if snapshot["lifecycle_manager_present"] else "not found"
			if self.__active_operation_count == 0:
				self.__status_var.set(f"System update received. {n_rows} subsystem(s), lifecycle manager {lm_text}.")
			self.__rebuild_table()
			return

		if kind == "op_started":
			_, action, s_uuid = msg
			self.__active_operation_count += 1
			if s_uuid is None:
				text = f"{action.replace('_', ' ').capitalize()} requested."
			else:
				text = f"{action.capitalize()} requested for ...{_uuid6(s_uuid)}."
			self.__status_var.set(text)
			self.__push_activity(text)
			self.__set_request_dialog_active(text)
			return

		if kind == "op_feedback":
			_, action, s_uuid, feedback = msg
			if s_uuid is None:
				text = f"{action.replace('_', ' ').capitalize()}: {feedback}"
			else:
				text = f"{action.capitalize()} ...{_uuid6(s_uuid)}: {feedback}"
			self.__status_var.set(text)
			self.__push_activity(text)
			self.__set_request_dialog_active(text)
			return

		if kind == "op_done":
			_, action, s_uuid, ok, message = msg
			self.__active_operation_count = max(0, self.__active_operation_count - 1)
			if ok:
				self.__status_var.set(message)
				self.__push_activity(message)
				self.__set_request_dialog_done(message)
				if self.__active_operation_count == 0:
					self.__schedule_request_dialog_close()
			else:
				if s_uuid is None:
					text = f"{action.replace('_', ' ').capitalize()} failed: {message}"
				else:
					text = f"{action.capitalize()} failed for ...{_uuid6(s_uuid)}: {message}"
				self.__status_var.set(text)
				self.__push_activity(text)
				self.__set_request_dialog_done(text)
				messagebox.showerror("Lifecycle Operation Failed", message)
			return

		if kind == "kv_value":
			_, s_uuid, key, ok, value_text = msg
			self.__kv_last_values[(s_uuid, key)] = value_text if ok else f"ERROR: {value_text}"
			self.__status_var.set(f"KV read {'succeeded' if ok else 'failed'} for ...{_uuid6(s_uuid)} key={key.decode('utf-8', errors='replace')}")
			self.__update_detail_panel()
			self.__refresh_controls()
			return

		if kind == "kv_write":
			_, s_uuid, key, ok, message = msg
			if ok:
				self.__status_var.set(f"KV write succeeded for ...{_uuid6(s_uuid)} key={key.decode('utf-8', errors='replace')}")
			else:
				self.__status_var.set(f"KV write failed for ...{_uuid6(s_uuid)} key={key.decode('utf-8', errors='replace')}: {message}")
				messagebox.showerror("KV Write Failed", message)
			return

		if kind == "event_started":
			_, s_uuid, e_name = msg
			e_name_str = e_name.decode("utf-8", errors="replace") if isinstance(e_name, bytes) else str(e_name)
			self.__active_operation_count += 1
			self.__status_var.set(f"Event '{e_name_str}' requested for ...{_uuid6(s_uuid)}.")
			self.__push_activity(f"Event '{e_name_str}' requested for ...{_uuid6(s_uuid)}")
			self.__set_request_dialog_active(f"Event '{e_name_str}' requested for ...{_uuid6(s_uuid)}")
			return

		if kind == "event_feedback":
			_, s_uuid, e_name, feedback = msg
			e_name_str = e_name.decode("utf-8", errors="replace") if isinstance(e_name, bytes) else str(e_name)
			self.__status_var.set(f"Event '{e_name_str}' ...{_uuid6(s_uuid)}: {feedback}")
			self.__push_activity(f"Event '{e_name_str}' ...{_uuid6(s_uuid)}: {feedback}")
			self.__set_request_dialog_active(f"Event '{e_name_str}' ...{_uuid6(s_uuid)}: {feedback}")
			return

		if kind == "event_done":
			_, s_uuid, e_name, ok, message = msg
			e_name_str = e_name.decode("utf-8", errors="replace") if isinstance(e_name, bytes) else str(e_name)
			self.__active_operation_count = max(0, self.__active_operation_count - 1)
			if ok:
				self.__status_var.set(f"Event '{e_name_str}' succeeded for ...{_uuid6(s_uuid)}.")
				self.__push_activity(f"Event '{e_name_str}' succeeded for ...{_uuid6(s_uuid)}")
				self.__set_request_dialog_done(f"Event '{e_name_str}' succeeded for ...{_uuid6(s_uuid)}")
				if self.__active_operation_count == 0:
					self.__schedule_request_dialog_close()
			else:
				self.__status_var.set(f"Event '{e_name_str}' failed for ...{_uuid6(s_uuid)}: {message}")
				self.__push_activity(f"Event '{e_name_str}' failed for ...{_uuid6(s_uuid)}: {message}")
				self.__set_request_dialog_done(f"Event '{e_name_str}' failed for ...{_uuid6(s_uuid)}: {message}")
				messagebox.showerror("Event Call Failed", message)
			return

		if kind == "error":
			_, text = msg
			self.__status_var.set(text)

	def __updater(self):
		for msg in self.__interface.pop_messages():
			self.__apply_message(msg)

		self.root.after(150, self.__updater)

	def on_close(self):
		if self.__request_dialog is not None and self.__request_dialog.winfo_exists():
			self.__request_dialog.destroy()
		self.__interface.close()
		self.root.destroy()


def _default_lifecycle_uuid() -> uuid.UUID:
	return uuid.uuid3(uuid.NAMESPACE_OID, "Lifecycle Manager")


def main():
	parser = argparse.ArgumentParser(description="Lifecycle Manager GUI")
	parser.add_argument(
		"--lifecycle-uuid",
		type=str,
		default=os.environ.get("IPI_ECS_LIFECYCLE_UUID"),
		help="Lifecycle manager subsystem UUID.",
	)
	args = parser.parse_args()

	lifecycle_uuid = _default_lifecycle_uuid() if args.lifecycle_uuid is None else uuid.UUID(args.lifecycle_uuid)

	root = tk.Tk()
	LifecycleGUI(root, lifecycle_uuid)
	root.mainloop()


if __name__ == "__main__":
	main()
