import argparse
import os
import time
import tkinter as tk
from queue import Empty, Queue
from tkinter import filedialog, messagebox, ttk

from ipi_ecs.core import daemon
from ipi_ecs.db.db_library import Entry, Library


def _fmt_timestamp(ts) -> str:
	if ts is None:
		return ""
	try:
		return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(ts)))
	except (TypeError, ValueError):
		return str(ts)


class LibraryThread:
	"""Thread-safe access to Library and Entry operations."""

	def __init__(self, data_path: str):
		self.__data_path = data_path
		self.__library: Library | None = None
		self.__in_queue: Queue = Queue()
		self.__daemon = daemon.Daemon()
		self.__daemon.add(self.__lib_thread)
		self.__daemon.start()

	def __lib_thread(self, stop_flag: daemon.StopFlag):
		self.__library = Library(self.__data_path)
		while stop_flag.run():
			try:
				fn, result_queue = self.__in_queue.get(timeout=0.1)
				try:
					result_queue.put(("ok", fn()))
				except Exception as exc:
					result_queue.put(("err", exc))
			except Empty:
				pass

		if self.__library is not None:
			self.__library.close()

	def __enqueue(self, fn) -> Queue:
		result_queue: Queue = Queue()
		self.__in_queue.put((fn, result_queue))
		return result_queue

	def __enqueue_sync(self, fn):
		status, result = self.__enqueue(fn).get()
		if status == "err":
			raise result
		return result

	def query_async(self, filters: dict) -> Queue:
		q_copy = dict(filters)
		return self.__enqueue(lambda: self.__library.query(q_copy, limit=None))

	def list_all_async(self) -> Queue:
		return self.__enqueue(lambda: self.__library.query({}, limit=None))

	def set_name(self, entry: Entry, name: str):
		self.__enqueue_sync(lambda: entry.set_name(name))

	def set_description(self, entry: Entry, description: str):
		self.__enqueue_sync(lambda: entry.set_desc(description))

	def set_tag(self, entry: Entry, key: str, value):
		self.__enqueue_sync(lambda: entry.set_tag(key, value))

	def remove_tag(self, entry: Entry, key: str):
		self.__enqueue_sync(lambda: entry.remove_tag(key))

	def close(self):
		self.__daemon.stop()


class QueryFrame(ttk.LabelFrame):
	def __init__(self, parent, on_search, on_list_all):
		super().__init__(parent, text="Query / Filter", padding=8)
		self.__on_search = on_search
		self.__on_list_all = on_list_all
		self.__build()

	def __build(self):
		row = 0
		ttk.Label(self, text="Name:").grid(row=row, column=0, sticky=tk.W, padx=2, pady=2)
		self.__name_var = tk.StringVar()
		ttk.Entry(self, textvariable=self.__name_var, width=20).grid(row=row, column=1, sticky=tk.EW, padx=2, pady=2)

		ttk.Label(self, text="Description:").grid(row=row, column=2, sticky=tk.W, padx=(10, 2), pady=2)
		self.__desc_var = tk.StringVar()
		ttk.Entry(self, textvariable=self.__desc_var, width=20).grid(row=row, column=3, sticky=tk.EW, padx=2, pady=2)

		row += 1
		ttk.Label(self, text="Date (YYYY-MM-DD):").grid(row=row, column=0, sticky=tk.W, padx=2, pady=2)
		self.__date_var = tk.StringVar()
		ttk.Entry(self, textvariable=self.__date_var, width=14).grid(row=row, column=1, sticky=tk.EW, padx=2, pady=2)
		ttk.Button(self, text="Today", command=self.__set_today).grid(row=row, column=2, sticky=tk.W, padx=2, pady=2)

		row += 1
		ttk.Label(self, text="Tag Key:").grid(row=row, column=0, sticky=tk.W, padx=2, pady=2)
		self.__tag_key_var = tk.StringVar()
		ttk.Entry(self, textvariable=self.__tag_key_var, width=16).grid(row=row, column=1, sticky=tk.EW, padx=2, pady=2)

		ttk.Label(self, text="Tag Value:").grid(row=row, column=2, sticky=tk.W, padx=(10, 2), pady=2)
		self.__tag_value_var = tk.StringVar()
		ttk.Entry(self, textvariable=self.__tag_value_var, width=16).grid(row=row, column=3, sticky=tk.EW, padx=2, pady=2)

		row += 1
		ttk.Label(self, text="Tag Min:").grid(row=row, column=0, sticky=tk.W, padx=2, pady=2)
		self.__tag_min_var = tk.StringVar()
		ttk.Entry(self, textvariable=self.__tag_min_var, width=10).grid(row=row, column=1, sticky=tk.EW, padx=2, pady=2)

		ttk.Label(self, text="Tag Max:").grid(row=row, column=2, sticky=tk.W, padx=(10, 2), pady=2)
		self.__tag_max_var = tk.StringVar()
		ttk.Entry(self, textvariable=self.__tag_max_var, width=10).grid(row=row, column=3, sticky=tk.EW, padx=2, pady=2)

		row += 1
		btn_frame = ttk.Frame(self)
		btn_frame.grid(row=row, column=0, columnspan=4, sticky=tk.W, pady=(6, 2))
		ttk.Button(btn_frame, text="Search", command=self.__do_search).pack(side=tk.LEFT, padx=2)
		ttk.Button(btn_frame, text="List All", command=self.__on_list_all).pack(side=tk.LEFT, padx=2)
		ttk.Button(btn_frame, text="Clear", command=self.__clear_fields).pack(side=tk.LEFT, padx=2)

		self.columnconfigure(1, weight=1)
		self.columnconfigure(3, weight=1)

	def __set_today(self):
		self.__date_var.set(time.strftime("%Y-%m-%d"))

	def __clear_fields(self):
		for var in (
			self.__name_var,
			self.__desc_var,
			self.__date_var,
			self.__tag_key_var,
			self.__tag_value_var,
			self.__tag_min_var,
			self.__tag_max_var,
		):
			var.set("")

	def __do_search(self):
		query = {}

		name = self.__name_var.get().strip()
		if name:
			query["name"] = name

		desc = self.__desc_var.get().strip()
		if desc:
			query["description"] = desc

		date_str = self.__date_var.get().strip()
		if date_str:
			try:
				ts_min = time.mktime(time.strptime(date_str, "%Y-%m-%d"))
				query["created_min"] = ts_min
				query["created_max"] = ts_min + 86400.0
			except ValueError:
				messagebox.showerror("Invalid Input", f"Invalid date: '{date_str}'. Use YYYY-MM-DD.")
				return

		tag_key = self.__tag_key_var.get().strip()
		tag_value = self.__tag_value_var.get().strip()
		tag_min = self.__tag_min_var.get().strip()
		tag_max = self.__tag_max_var.get().strip()

		has_numeric = bool(tag_min or tag_max)
		has_value = bool(tag_value)
		if not tag_key and (has_numeric or has_value):
			messagebox.showerror("Invalid Input", "Tag key is required when filtering by tag value/min/max.")
			return

		if tag_key:
			tags = {}
			if has_numeric and has_value:
				messagebox.showerror("Invalid Input", "Use either Tag Value or Tag Min/Tag Max, not both.")
				return

			if has_numeric:
				numeric_filter = {}
				if tag_min:
					try:
						numeric_filter["min"] = float(tag_min)
					except ValueError:
						messagebox.showerror("Invalid Input", f"Invalid tag min: '{tag_min}'.")
						return
				if tag_max:
					try:
						numeric_filter["max"] = float(tag_max)
					except ValueError:
						messagebox.showerror("Invalid Input", f"Invalid tag max: '{tag_max}'.")
						return
				tags[tag_key] = numeric_filter
			elif has_value:
				tags[tag_key] = tag_value
			else:
				tags[tag_key] = None

			query["tags"] = tags

		self.__on_search(query)


class ResultsFrame(ttk.LabelFrame):
	_COLS = ("name", "description", "created", "uuid", "tag_count")

	def __init__(self, parent, on_selection_changed):
		super().__init__(parent, text="Results", padding=5)
		self.__on_selection_changed = on_selection_changed
		self.__entries: dict = {}
		self.__build()

	def __build(self):
		tree_frame = ttk.Frame(self)
		tree_frame.pack(fill=tk.BOTH, expand=True)
		tree_frame.rowconfigure(0, weight=1)
		tree_frame.columnconfigure(0, weight=1)

		vsb = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL)
		hsb = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL)

		self.__tree = ttk.Treeview(
			tree_frame,
			columns=self._COLS,
			show="headings",
			yscrollcommand=vsb.set,
			xscrollcommand=hsb.set,
			selectmode="browse",
		)
		vsb.config(command=self.__tree.yview)
		hsb.config(command=self.__tree.xview)

		self.__tree.heading("name", text="Name")
		self.__tree.heading("description", text="Description")
		self.__tree.heading("created", text="Created")
		self.__tree.heading("uuid", text="UUID")
		self.__tree.heading("tag_count", text="Tags")

		self.__tree.column("name", width=180, minwidth=100)
		self.__tree.column("description", width=250, minwidth=120)
		self.__tree.column("created", width=150, minwidth=100)
		self.__tree.column("uuid", width=120, minwidth=80)
		self.__tree.column("tag_count", width=80, minwidth=50)

		self.__tree.grid(row=0, column=0, sticky=tk.NSEW)
		vsb.grid(row=0, column=1, sticky=tk.NS)
		hsb.grid(row=1, column=0, sticky=tk.EW)

		self.__tree.bind("<<TreeviewSelect>>", self.__on_select)

		self.__count_label = ttk.Label(self, text="No results.")
		self.__count_label.pack(side=tk.BOTTOM, anchor=tk.W, pady=(4, 0))

	def populate(self, entries: list[Entry]):
		self.__tree.delete(*self.__tree.get_children())
		self.__entries.clear()

		for entry in entries:
			created = _fmt_timestamp(entry.get_timestamp())
			entry_uuid = str(entry.get_uuid())[-8:]
			tags = entry.get_tags() or {}
			desc = entry.get_description() or ""
			if len(desc) > 80:
				desc = desc[:77] + "..."

			iid = self.__tree.insert(
				"",
				tk.END,
				values=(
					entry.get_name() or "",
					desc,
					created,
					entry_uuid,
					len(tags),
				),
			)
			self.__entries[iid] = entry

		count = len(entries)
		self.__count_label.config(text=f"{count} entr{'y' if count == 1 else 'ies'}.")

	def refresh_entry(self, entry: Entry):
		for iid, known in self.__entries.items():
			if known is not entry:
				continue

			created = _fmt_timestamp(entry.get_timestamp())
			entry_uuid = str(entry.get_uuid())[-8:]
			tags = entry.get_tags() or {}
			desc = entry.get_description() or ""
			if len(desc) > 80:
				desc = desc[:77] + "..."

			self.__tree.item(
				iid,
				values=(
					entry.get_name() or "",
					desc,
					created,
					entry_uuid,
					len(tags),
				),
			)
			break

	def get_selected_entry(self) -> Entry | None:
		sel = self.__tree.selection()
		if not sel:
			return None
		return self.__entries.get(sel[0])

	def __on_select(self, _event):
		self.__on_selection_changed(self.get_selected_entry())


class DetailFrame(ttk.LabelFrame):
	def __init__(self, parent, library_thread: LibraryThread, on_saved):
		super().__init__(parent, text="Entry Detail", padding=8)
		self.__library_thread = library_thread
		self.__on_saved = on_saved
		self.__entry: Entry | None = None
		self.__section_bodies: dict[str, ttk.Frame] = {}
		self.__section_buttons: dict[str, ttk.Button] = {}
		self.__scroll_canvas: tk.Canvas | None = None
		self.__scroll_window_id = None
		self.__scroll_container: ttk.Frame | None = None
		self.__build()

	def __build(self):
		content_host = ttk.Frame(self)
		content_host.pack(fill=tk.BOTH, expand=True)

		self.__scroll_canvas = tk.Canvas(content_host, borderwidth=0, highlightthickness=0)
		scrollbar = ttk.Scrollbar(content_host, orient=tk.VERTICAL, command=self.__scroll_canvas.yview)
		self.__scroll_canvas.configure(yscrollcommand=scrollbar.set)
		self.__scroll_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
		scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

		self.__scroll_container = ttk.Frame(self.__scroll_canvas)
		self.__scroll_window_id = self.__scroll_canvas.create_window((0, 0), window=self.__scroll_container, anchor=tk.NW)

		self.__scroll_container.bind("<Configure>", self.__on_scroll_container_configure)
		self.__scroll_canvas.bind("<Configure>", self.__on_scroll_canvas_configure)

		edit_frame = ttk.Frame(self.__scroll_container)
		edit_frame.pack(fill=tk.X, pady=(0, 4))
		edit_frame.columnconfigure(1, weight=1)

		ttk.Label(edit_frame, text="Name:").grid(row=0, column=0, sticky=tk.W, padx=2, pady=2)
		self.__name_var = tk.StringVar()
		self.__name_entry = ttk.Entry(edit_frame, textvariable=self.__name_var, state=tk.DISABLED)
		self.__name_entry.grid(row=0, column=1, sticky=tk.EW, padx=2, pady=2)

		ttk.Label(edit_frame, text="Description:").grid(row=1, column=0, sticky=tk.W, padx=2, pady=2)
		self.__desc_var = tk.StringVar()
		self.__desc_entry = ttk.Entry(edit_frame, textvariable=self.__desc_var, state=tk.DISABLED)
		self.__desc_entry.grid(row=1, column=1, sticky=tk.EW, padx=2, pady=2)

		btn_frame = ttk.Frame(edit_frame)
		btn_frame.grid(row=2, column=0, columnspan=2, pady=5)
		self.__save_btn = ttk.Button(btn_frame, text="Save", command=self.__do_save, state=tk.DISABLED)
		self.__save_btn.pack(side=tk.LEFT, padx=3)
		self.__copy_id_btn = ttk.Button(
			btn_frame,
			text="Copy Entry ID",
			command=self.__copy_entry_id,
			state=tk.DISABLED,
		)
		self.__copy_id_btn.pack(side=tk.LEFT, padx=3)

		ttk.Separator(self.__scroll_container, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=(0, 4))

		self.__tags_lf = self.__make_collapsible_section("Tags")
		self.__tags_lf.pack(fill=tk.X, padx=2, pady=4)
		self.__tags_body = self.__section_bodies["Tags"]
		self.__tags_body.rowconfigure(0, weight=1)
		self.__tags_body.rowconfigure(1, weight=0)
		self.__tags_body.columnconfigure(0, weight=1)

		self.__tags_tree = ttk.Treeview(
			self.__tags_body,
			columns=("key", "value"),
			show="headings",
			height=8,
			selectmode="browse",
		)
		self.__tags_tree.heading("key", text="Key")
		self.__tags_tree.heading("value", text="Value")
		self.__tags_tree.column("key", width=180, minwidth=80)
		self.__tags_tree.column("value", width=260, minwidth=80)
		self.__tags_tree.grid(row=0, column=0, sticky=tk.NSEW, padx=2, pady=(0, 4))

		btns = ttk.Frame(self.__tags_body)
		btns.grid(row=1, column=0, sticky=tk.W)
		ttk.Button(btns, text="Add Tag", command=self.__add_tag_dialog).pack(side=tk.LEFT, padx=2)
		ttk.Button(btns, text="Edit Selected", command=self.__edit_selected_tag).pack(side=tk.LEFT, padx=2)
		ttk.Button(btns, text="Remove Selected", command=self.__remove_selected_tag).pack(side=tk.LEFT, padx=2)
		self.__tags_tree.bind("<Double-1>", lambda _e: self.__edit_selected_tag())

		self.__meta_lf = self.__make_collapsible_section("Metadata")
		self.__meta_lf.pack(fill=tk.X, padx=2, pady=4)
		self.__meta_body = self.__section_bodies["Metadata"]
		self.__meta_body.rowconfigure(0, weight=1)
		self.__meta_body.columnconfigure(0, minsize=120)
		self.__meta_body.columnconfigure(1, weight=1)

		self.__resources_lf = self.__make_collapsible_section("Resources")
		self.__resources_lf.pack(fill=tk.X, padx=2, pady=4)
		self.__resources_body = self.__section_bodies["Resources"]
		self.__resources_body.rowconfigure(0, weight=1)
		self.__resources_body.columnconfigure(0, minsize=160)
		self.__resources_body.columnconfigure(1, weight=1)

		self.__placeholder = ttk.Label(
			self.__scroll_container,
			text="Select a library entry to view details.",
			font=("Arial", 11),
			foreground="gray",
		)
		self.__placeholder.place(relx=0.5, rely=0.5, anchor=tk.CENTER)
		self.__set_section_open("Tags", True)
		self.__set_section_open("Metadata", True)
		self.__set_section_open("Resources", True)
		self.__refresh_scroll_region()

	def __make_collapsible_section(self, title: str) -> ttk.Frame:
		section = ttk.Frame(self.__scroll_container)
		header = ttk.Frame(section)
		header.pack(fill=tk.X)

		button = ttk.Button(header, text=f"[-] {title}", command=lambda: self.__toggle_section(title))
		button.pack(fill=tk.X)

		body = ttk.Frame(section, padding=5)
		body.pack(fill=tk.BOTH, expand=True)

		self.__section_bodies[title] = body
		self.__section_buttons[title] = button
		return section

	def __toggle_section(self, title: str):
		body = self.__section_bodies.get(title)
		button = self.__section_buttons.get(title)
		if body is None or button is None:
			return

		if body.winfo_ismapped():
			body.pack_forget()
			button.config(text=f"[+] {title}")
		else:
			body.pack(fill=tk.BOTH, expand=True)
			button.config(text=f"[-] {title}")

		self.__refresh_scroll_region()

	def __set_section_open(self, title: str, is_open: bool):
		body = self.__section_bodies.get(title)
		button = self.__section_buttons.get(title)
		if body is None or button is None:
			return

		if is_open:
			if not body.winfo_ismapped():
				body.pack(fill=tk.BOTH, expand=True)
			button.config(text=f"[-] {title}")
		else:
			if body.winfo_ismapped():
				body.pack_forget()
			button.config(text=f"[+] {title}")

	def __on_scroll_container_configure(self, _event):
		self.__refresh_scroll_region()

	def __on_scroll_canvas_configure(self, event):
		if self.__scroll_canvas is not None and self.__scroll_window_id is not None:
			self.__scroll_canvas.itemconfigure(self.__scroll_window_id, width=event.width)

	def __refresh_scroll_region(self):
		if self.__scroll_canvas is None:
			return
		self.__scroll_canvas.update_idletasks()
		self.__scroll_canvas.configure(scrollregion=self.__scroll_canvas.bbox("all"))

	@staticmethod
	def __clear_frame(frame: ttk.Frame):
		for child in frame.winfo_children():
			child.destroy()

	@staticmethod
	def __populate_kv_frame(frame: ttk.Frame, items: dict, empty_text="(none)"):
		DetailFrame.__clear_frame(frame)
		if not items:
			ttk.Label(frame, text=empty_text, foreground="gray").grid(row=0, column=0, sticky=tk.W)
			return

		for idx, (key, value) in enumerate(items.items()):
			ttk.Label(frame, text=f"{key}:", font=("TkDefaultFont", 9, "bold")).grid(
				row=idx,
				column=0,
				sticky=tk.W,
				padx=2,
				pady=1,
			)
			ttk.Label(frame, text=str(value)).grid(row=idx, column=1, sticky=tk.W, padx=2, pady=1)

	def load_entry(self, entry: Entry | None):
		self.__entry = entry

		if entry is None:
			self.__placeholder.place(relx=0.5, rely=0.5, anchor=tk.CENTER)
			self.__name_entry.config(state=tk.DISABLED)
			self.__desc_entry.config(state=tk.DISABLED)
			self.__save_btn.config(state=tk.DISABLED)
			self.__copy_id_btn.config(state=tk.DISABLED)
			self.__name_var.set("")
			self.__desc_var.set("")
			self.__tags_tree.delete(*self.__tags_tree.get_children())
			self.__populate_kv_frame(self.__meta_body, {}, empty_text="(none)")
			self.__populate_kv_frame(self.__resources_body, {}, empty_text="(none)")
			self.__refresh_scroll_region()
			return

		self.__placeholder.place_forget()
		self.__name_entry.config(state=tk.NORMAL)
		self.__desc_entry.config(state=tk.NORMAL)
		self.__save_btn.config(state=tk.NORMAL)
		self.__copy_id_btn.config(state=tk.NORMAL)

		self.__name_var.set(entry.get_name() or "")
		self.__desc_var.set(entry.get_description() or "")

		self.__tags_tree.delete(*self.__tags_tree.get_children())
		tags = entry.get_tags() or {}
		for key, value in sorted(tags.items(), key=lambda kv: str(kv[0])):
			self.__tags_tree.insert("", tk.END, values=(str(key), str(value)))

		meta = {
			"UUID": str(entry.get_uuid()),
			"Created": _fmt_timestamp(entry.get_timestamp()),
			"Folder": entry.get_foldername(),
		}
		self.__populate_kv_frame(self.__meta_body, meta)

		resources = {name: r_type for name, r_type in entry.list_resources()}
		self.__populate_kv_frame(self.__resources_body, resources)
		self.__refresh_scroll_region()

	def is_showing_entry(self, entry: Entry | None) -> bool:
		return self.__entry is entry

	def __do_save(self):
		if self.__entry is None:
			return

		try:
			self.__library_thread.set_name(self.__entry, self.__name_var.get().strip())
			self.__library_thread.set_description(self.__entry, self.__desc_var.get().strip())
			self.__on_saved(self.__entry)
		except Exception as exc:
			messagebox.showerror("Save Failed", str(exc))

	def __copy_entry_id(self):
		if self.__entry is None:
			return
		self.clipboard_clear()
		self.clipboard_append(str(self.__entry.get_uuid()))
		self.update_idletasks()

	def __add_tag_dialog(self):
		self.__tag_dialog("Add Or Update Tag")

	def __edit_selected_tag(self):
		if self.__entry is None:
			return
		sel = self.__tags_tree.selection()
		if not sel:
			messagebox.showinfo("Edit Tag", "Select a tag to edit first.")
			return
		key, value = self.__tags_tree.item(sel[0], "values")
		self.__tag_dialog("Edit Tag", key=key, value=value)

	def __remove_selected_tag(self):
		if self.__entry is None:
			return
		sel = self.__tags_tree.selection()
		if not sel:
			messagebox.showinfo("Remove Tag", "Select a tag to remove first.")
			return

		key, _value = self.__tags_tree.item(sel[0], "values")
		if not messagebox.askyesno("Remove Tag", f"Remove tag '{key}'?"):
			return

		try:
			self.__library_thread.remove_tag(self.__entry, key)
			self.load_entry(self.__entry)
			self.__on_saved(self.__entry)
		except Exception as exc:
			messagebox.showerror("Remove Tag", str(exc))

	def __tag_dialog(self, title: str, key: str = "", value: str = ""):
		if self.__entry is None:
			return

		dialog = tk.Toplevel(self)
		dialog.title(title)
		dialog.resizable(False, False)
		dialog.grab_set()

		frame = ttk.Frame(dialog, padding=12)
		frame.pack(fill=tk.BOTH, expand=True)

		ttk.Label(frame, text="Key:").grid(row=0, column=0, sticky=tk.W, padx=2, pady=4)
		key_entry = ttk.Entry(frame, width=24)
		key_entry.grid(row=0, column=1, padx=2, pady=4)
		key_entry.insert(0, key)

		ttk.Label(frame, text="Value:").grid(row=1, column=0, sticky=tk.W, padx=2, pady=4)
		value_entry = ttk.Entry(frame, width=24)
		value_entry.grid(row=1, column=1, padx=2, pady=4)
		value_entry.insert(0, value)

		def _on_ok():
			tag_key = key_entry.get().strip()
			value_str = value_entry.get().strip()
			if not tag_key:
				messagebox.showerror("Invalid Input", "Key cannot be empty.", parent=dialog)
				return

			try:
				typed_val = int(value_str)
			except ValueError:
				try:
					typed_val = float(value_str)
				except ValueError:
					typed_val = value_str

			try:
				self.__library_thread.set_tag(self.__entry, tag_key, typed_val)
				dialog.destroy()
				self.load_entry(self.__entry)
				self.__on_saved(self.__entry)
			except Exception as exc:
				messagebox.showerror("Error", str(exc), parent=dialog)

		btn_frame = ttk.Frame(frame)
		btn_frame.grid(row=2, column=0, columnspan=2, pady=(8, 0))
		ttk.Button(btn_frame, text="OK", command=_on_ok).pack(side=tk.LEFT, padx=5)
		ttk.Button(btn_frame, text="Cancel", command=dialog.destroy).pack(side=tk.LEFT, padx=5)

		key_entry.focus_set()
		dialog.bind("<Return>", lambda _event: _on_ok())


class LibraryBrowserGUI:
	def __init__(self, root: tk.Tk, data_path: str):
		self.root = root
		self.root.title("Library Browser")
		self.root.geometry("1200x700")
		self.root.minsize(850, 550)

		self.__data_path = data_path
		self.__library_thread = LibraryThread(data_path)
		self.__pending_query: Queue | None = None

		self.__loading_dialog = None
		self.__loading_progressbar = None

		self.__status_var = tk.StringVar(value=f"Ready. Path: {data_path}")
		ttk.Label(
			root,
			textvariable=self.__status_var,
			anchor=tk.W,
			relief=tk.SUNKEN,
			padding=(4, 2),
		).pack(side=tk.BOTTOM, fill=tk.X)

		paned = ttk.PanedWindow(root, orient=tk.HORIZONTAL)
		paned.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

		left = ttk.Frame(paned)
		paned.add(left, weight=2)

		self.__query_frame = QueryFrame(left, on_search=self.__on_search, on_list_all=self.__on_list_all)
		self.__query_frame.pack(fill=tk.X, padx=2, pady=(2, 4))

		actions = ttk.Frame(left)
		actions.pack(fill=tk.X, padx=2, pady=(0, 4))
		ttk.Button(actions, text="Choose Library Path...", command=self.__on_choose_library_path).pack(side=tk.LEFT, padx=2)
		ttk.Button(actions, text="Refresh", command=self.__on_list_all).pack(side=tk.LEFT, padx=2)

		self.__results_frame = ResultsFrame(left, on_selection_changed=self.__on_selection_changed)
		self.__results_frame.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)

		right = ttk.Frame(paned)
		paned.add(right, weight=3)

		self.__detail_frame = DetailFrame(right, library_thread=self.__library_thread, on_saved=self.__on_entry_saved)
		self.__detail_frame.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)

		self.root.protocol("WM_DELETE_WINDOW", self.on_close)

		self.__on_list_all()
		self.__updater()

	def __show_loading_dialog(self, message: str):
		if self.__loading_dialog is not None and self.__loading_dialog.winfo_exists():
			self.__hide_loading_dialog()

		dialog = tk.Toplevel(self.root)
		dialog.title("Please Wait")
		dialog.geometry("320x90")
		dialog.resizable(False, False)
		dialog.transient(self.root)
		dialog.grab_set()

		frame = ttk.Frame(dialog, padding=12)
		frame.pack(fill=tk.BOTH, expand=True)

		ttk.Label(frame, text=message).pack(anchor=tk.W, pady=(0, 6))
		self.__loading_progressbar = ttk.Progressbar(frame, mode="indeterminate")
		self.__loading_progressbar.pack(fill=tk.X)
		self.__loading_progressbar.start(10)

		dialog.protocol("WM_DELETE_WINDOW", lambda: None)
		self.__loading_dialog = dialog

	def __hide_loading_dialog(self):
		if self.__loading_progressbar is not None:
			self.__loading_progressbar.stop()
		if self.__loading_dialog is not None and self.__loading_dialog.winfo_exists():
			self.__loading_dialog.grab_release()
			self.__loading_dialog.destroy()

		self.__loading_dialog = None
		self.__loading_progressbar = None

	def __on_choose_library_path(self):
		new_path = filedialog.askdirectory(title="Select Library Path", initialdir=self.__data_path)
		if not new_path:
			return

		messagebox.showinfo(
			"Restart Required",
			"To switch library path safely, restart this browser with:\n"
			f"python -m ipi_ecs.gui.db_gui --data-path \"{new_path}\"",
		)

	def __on_search(self, query: dict):
		self.__pending_query = self.__library_thread.query_async(query)
		self.__show_loading_dialog("Searching entries...")
		self.__status_var.set("Searching entries...")

	def __on_list_all(self):
		self.__pending_query = self.__library_thread.list_all_async()
		self.__show_loading_dialog("Loading all entries...")
		self.__status_var.set("Loading all entries...")

	def __on_selection_changed(self, entry: Entry | None):
		self.__detail_frame.load_entry(entry)

	def __on_entry_saved(self, entry: Entry):
		self.__results_frame.refresh_entry(entry)

	def __updater(self):
		if self.__pending_query is not None:
			try:
				status, result = self.__pending_query.get_nowait()
			except Empty:
				pass
			else:
				self.__pending_query = None
				self.__hide_loading_dialog()

				try:
					if status == "ok":
						self.__results_frame.populate(result)
						count = len(result)
						self.__status_var.set(f"Found {count} entr{'y' if count == 1 else 'ies'}.")
					else:
						self.__status_var.set(f"Error: {result}")
						messagebox.showerror("Query Error", str(result))
				except Exception as exc:
					self.__status_var.set(f"Query result handling failed: {exc}")
					messagebox.showerror("Query Error", str(exc))

		self.root.after(200, self.__updater)

	def on_close(self):
		self.__hide_loading_dialog()
		self.__library_thread.close()
		self.root.destroy()


def _default_data_path() -> str:
	base = os.environ.get("EUVL_PATH")
	if base:
		return os.path.join(base, "datasets")
	return os.path.abspath(".")


def main():
	parser = argparse.ArgumentParser(description="Browse and edit generic library entries.")
	parser.add_argument("--data-path", default=_default_data_path(), help="Path containing library.sqlite3")
	args = parser.parse_args()

	root = tk.Tk()
	LibraryBrowserGUI(root, data_path=args.data_path)
	root.mainloop()


if __name__ == "__main__":
	main()
