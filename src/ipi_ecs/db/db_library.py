import os
import uuid
import time
import sqlite3
from pathlib import Path

from ipi_ecs.db.registry import read_registry, update_registry

# SAVE_PATH = os.path.join(os.environ["EUVL_PATH"], "datasets")
DDL = """
CREATE TABLE IF NOT EXISTS library (
    uuid TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created INTEGER NOT NULL,
    description TEXT,
    foldername TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tags (
    entry_uuid TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT,  -- Nullable for numeric tags
    val_num REAL,  -- Nullable for string tags
    FOREIGN KEY (entry_uuid) REFERENCES library(uuid) ON DELETE CASCADE,
    PRIMARY KEY (entry_uuid, key)  -- Ensures one value per key per entry
);

CREATE INDEX IF NOT EXISTS idx_tags_key ON tags(key);
CREATE INDEX IF NOT EXISTS idx_tags_value ON tags(value);
CREATE INDEX IF NOT EXISTS idx_tags_key_value ON tags(key, value);
CREATE INDEX IF NOT EXISTS idx_tags_val_num ON tags(val_num);  -- For range queries
CREATE INDEX IF NOT EXISTS idx_library_created_uuid ON library(created DESC, uuid DESC);
"""

class Library:
    SQLITE_BUSY_TIMEOUT_MS = 10_000

    def __init__(self, path: str, *, read_only: bool = False):
        self.__path = path
        self.__read_only = read_only

        self.__db_file_path = os.path.join(self.__path, "library.sqlite3")

        #print(f"Initializing library at {self.__db_file_path}...")
        if self.__read_only:
            if not os.path.isfile(self.__db_file_path):
                raise FileNotFoundError(f"Experiment index does not exist: {self.__db_file_path}")
            db_uri = f"{Path(self.__db_file_path).resolve().as_uri()}?mode=ro"
            self.__conn = sqlite3.connect(db_uri, uri=True, timeout=self.SQLITE_BUSY_TIMEOUT_MS / 1000)
            self.__conn.execute("PRAGMA query_only = ON")
        else:
            self.__conn = sqlite3.connect(
                self.__db_file_path,
                timeout=self.SQLITE_BUSY_TIMEOUT_MS / 1000,
            )
            self.__conn.execute(f"PRAGMA busy_timeout = {self.SQLITE_BUSY_TIMEOUT_MS}")
            self.__conn.executescript(DDL)
            self.__conn.commit()

    @property
    def read_only(self) -> bool:
        return self.__read_only

    def _require_writable(self) -> None:
        if self.__read_only:
            raise PermissionError("Library is read-only.")

    def __write_tag_to_db(self, entry_uuid: uuid.UUID, key: str, value: str | float) -> None:
        if type(value) is float:
            self.__conn.execute(
                "INSERT INTO tags (entry_uuid, key, value, val_num) VALUES (?, ?, NULL, ?) "
                "ON CONFLICT(entry_uuid, key) DO UPDATE SET value = NULL, val_num = excluded.val_num",
                (str(entry_uuid), key, value),
            )
        else:
            self.__conn.execute(
                "INSERT INTO tags (entry_uuid, key, value, val_num) VALUES (?, ?, ?, NULL) "
                "ON CONFLICT(entry_uuid, key) DO UPDATE SET value = excluded.value, val_num = NULL",
                (str(entry_uuid), key, value),
            )

    def __save_tags_to_db(self, entry: "Entry") -> None:
        """Save every tag for a newly created entry."""
        self._require_writable()
        for key, value in entry.get_tags().items():
            self.__write_tag_to_db(entry.get_uuid(), key, value)

    def __merge_tags_to_db(self, entry: "Entry") -> None:
        """Apply only this entry's tag changes, preserving tags written by other instances."""
        changed_tags, removed_keys = entry._tag_changes()
        entry_uuid = str(entry.get_uuid())
        for key in removed_keys:
            self.__conn.execute("DELETE FROM tags WHERE entry_uuid = ? AND key = ?", (entry_uuid, key))
        for key, value in changed_tags.items():
            self.__write_tag_to_db(entry.get_uuid(), key, value)

        persisted_tags = {}
        rows = self.__conn.execute(
            "SELECT key, value, val_num FROM tags WHERE entry_uuid = ?", (entry_uuid,)
        ).fetchall()
        for key, value, val_num in rows:
            persisted_tags[key] = str(val_num) if val_num is not None else value
        entry._replace_persisted_tags(persisted_tags, changed_tags)

    def __update_tags(self, entry: "Entry") -> None:
        """Apply only an entry's tag delta to the DB."""
        self._require_writable()
        with self.__conn:
            self.__merge_tags_to_db(entry)

    def _update_name(self, entry: "Entry", name: str) -> None:
        self._require_writable()
        with self.__conn:
            self.__conn.execute(
                "UPDATE library SET name = ? WHERE uuid = ?",
                (name, str(entry.get_uuid())),
            )

    def _update_description(self, entry: "Entry", description: str) -> None:
        self._require_writable()
        with self.__conn:
            self.__conn.execute(
                "UPDATE library SET description = ? WHERE uuid = ?",
                (description, str(entry.get_uuid())),
            )

    @staticmethod
    def __filter_conditions(filters: dict) -> tuple[list[str], list]:
        if not isinstance(filters, dict):
            raise TypeError("Library filters must be a dictionary.")

        conditions = []
        params = []

        if "name" in filters:
            conditions.append("l.name LIKE ?")
            params.append(f"%{filters['name']}%")
        if "description" in filters:
            conditions.append("l.description LIKE ?")
            params.append(f"%{filters['description']}%")
        if "created_min" in filters:
            conditions.append("l.created >= ?")
            params.append(filters["created_min"])
        if "created_max" in filters:
            conditions.append("l.created <= ?")
            params.append(filters["created_max"])

        if "tags" in filters:
            tag_filters = filters["tags"]
            if not isinstance(tag_filters, dict):
                raise TypeError("Library tag filters must be a dictionary.")
            for key, tag_filter in tag_filters.items():
                if tag_filter is None:
                    conditions.append("EXISTS (SELECT 1 FROM tags t WHERE t.entry_uuid = l.uuid AND t.key = ?)")
                    params.append(key)
                elif isinstance(tag_filter, str):
                    conditions.append(
                        "EXISTS (SELECT 1 FROM tags t WHERE t.entry_uuid = l.uuid AND t.key = ? AND t.value = ?)"
                    )
                    params.extend([key, tag_filter])
                elif isinstance(tag_filter, dict):
                    tag_query = (
                        "EXISTS (SELECT 1 FROM tags t WHERE t.entry_uuid = l.uuid "
                        "AND t.key = ? AND t.val_num IS NOT NULL"
                    )
                    tag_params = [key]
                    if "min" in tag_filter:
                        tag_query += " AND t.val_num >= ?"
                        tag_params.append(tag_filter["min"])
                    if "max" in tag_filter:
                        tag_query += " AND t.val_num <= ?"
                        tag_params.append(tag_filter["max"])
                    tag_query += ")"
                    conditions.append(tag_query)
                    params.extend(tag_params)
                else:
                    raise TypeError(f"Unsupported tag filter for {key!r}.")

        return conditions, params

    @staticmethod
    def __normalize_cursor(cursor) -> tuple[int, str]:
        if not isinstance(cursor, (tuple, list)) or len(cursor) != 2:
            raise ValueError("Library cursor must contain (created, uuid).")
        try:
            created = int(cursor[0])
            cursor_uuid = str(uuid.UUID(str(cursor[1])))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("Library cursor must contain a timestamp and UUID.") from exc
        return created, cursor_uuid

    def query(
        self,
        filters: dict,
        limit: int | None = None,
        *,
        offset: int = 0,
        cursor=None,
    ) -> list["Entry"]:
        """
        Unified query function for entries.

        filters: dict with optional keys:
        - 'name': str (substring match, case-insensitive)
        - 'description': str (substring match, case-insensitive)
        - 'created_min': int (timestamp >=)
        - 'created_max': int (timestamp <=)
        - 'tags': dict[str, any] where each value can be:
          - str: exact string match
          - dict with 'min' and/or 'max': numeric range
          - None: check if tag key exists (regardless of value)
        limit: optional int, maximum number of results, ordered by creation date (most recent first)
        """
        if not isinstance(offset, int) or offset < 0:
            raise ValueError("Library offset must be a non-negative integer.")
        if limit is not None and (not isinstance(limit, int) or limit < 0):
            raise ValueError("Library limit must be a non-negative integer or None.")
        if cursor is not None and offset:
            raise ValueError("Library offset and cursor cannot be combined.")

        query = "SELECT l.uuid FROM library l"
        conditions, params = self.__filter_conditions(filters)
        if cursor is not None:
            cursor_created, cursor_uuid = self.__normalize_cursor(cursor)
            conditions.append("(l.created < ? OR (l.created = ? AND l.uuid < ?))")
            params.extend([cursor_created, cursor_created, cursor_uuid])
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY l.created DESC, l.uuid DESC"
        
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        elif offset:
            query += " LIMIT -1"
        if offset:
            query += " OFFSET ?"
            params.append(offset)
        
        rows = self.__conn.execute(query, params).fetchall()
        entries = []
        for row in rows:
            s_uuid = uuid.UUID(row[0])
            try:
                #print(f"Loading entry {s_uuid}...")
                entry = self.__read(s_uuid)
            except Exception as e:
                print(f"Error reading entry {s_uuid}: {e}")
                continue
            entries.append(entry)

        #print(f"Query returned {len(entries)} entries.")
        return entries

    def count(self, filters: dict) -> int:
        query = "SELECT COUNT(*) FROM library l"
        conditions, params = self.__filter_conditions(filters)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        row = self.__conn.execute(query, params).fetchone()
        return int(row[0]) if row is not None else 0

    def __read(self, s_uuid: uuid.UUID) -> "Entry":
        """Load an entry from the DB by UUID."""
        #print("Reading entry with UUID:", s_uuid)
        row = self.__conn.execute(
            "SELECT name, created, description, foldername FROM library WHERE uuid = ?", (str(s_uuid),)
        ).fetchone()
        #print("DB query result for entry:", row)

        if row is None:
            raise ValueError("Entry not found")

        name, created, description, foldername = row
        #print(f"Found foldername '{foldername}' for entry with UUID: {s_uuid}, creating Entry object...")
        entry = Entry(self, s_uuid=s_uuid, name=name, created=created, desc=description, foldername=foldername, quickload=True, tags=dict())

        #print("Reading tags for entry with UUID:", s_uuid)
        tags_rows = self.__conn.execute(
        "SELECT key, value, val_num FROM tags WHERE entry_uuid = ?", (str(s_uuid),)
        ).fetchall()
        #print("DB query result for tags:", tags_rows)

        for key, value, val_num in tags_rows:
            if val_num is not None:
                entry._load_tag(key, str(val_num))
            else:
                entry._load_tag(key, value)

        return entry

    def __save(self, entry: "Entry") -> None:
        """Save new entry to DB."""
        self._require_writable()
        with self.__conn:
            self.__conn.execute(
                "INSERT INTO library (uuid, name, created, description, foldername) VALUES (?, ?, ?, ?, ?)",
                (
                    str(entry.get_uuid()),
                    entry.get_name(),
                    entry.get_timestamp(),
                    entry.get_description(),
                    entry.get_foldername(),
                ),
            )
            self.__save_tags_to_db(entry)
            self.__conn.commit()

    def get_base_path(self) -> str:
        return self.__path

    def update(self, entry: "Entry") -> None:
        self._require_writable()
        self.__update_tags(entry)

    def create_entry(self, name: str, desc: str) -> "Entry":
        self._require_writable()
        entry = Entry(self, name=name, desc=desc)
        self.__save(entry)
        return entry
    
    def read_entry(self, s_uuid: uuid.UUID) -> "Entry":
        return self.__read(s_uuid)
    
    def list_entries(self) -> list[uuid.UUID]:
        rows = self.__conn.execute("SELECT uuid FROM library").fetchall()
        entries = []
        for row in rows:
            s_uuid = uuid.UUID(row[0])
            entries.append(s_uuid)
        return entries

    def close(self) -> None:
        self.__conn.close()


class Entry:
    def __init__(
        self,
        library: Library,
        name: str | None = None,
        desc: str | None = None,
        foldername: str | None = None,
        s_uuid: uuid.UUID | None = None,
        quickload: bool = False,
        tags: dict = dict(),
        created: int | None = None
    ):
        self.__library = library
        self.__uuid = s_uuid or uuid.uuid4()

        self.__name = None
        self.__created = None
        self.__description = None
        self.__foldername = None
        self.__res_path = None
        self.__registry = dict()

        self.__quickload = quickload

        self.__tags = dict()
        self.__persisted_tags = dict()
        self.__stored_tags = dict()
        self.__dirty_tag_keys = set()
        self.__removed_tag_keys = set()
        if quickload:
            self.__name = name
            self.__description = desc
            self.__created = created
            self.__tags = dict(tags)
            self.__persisted_tags = dict(tags)
            self.__stored_tags = dict(tags)
            self.__foldername = foldername
            self.__res_path = os.path.join(
                self.__library.get_base_path(), self.__foldername
                )
        elif foldername is not None:
            self.__read(foldername)
        elif name is not None and desc is not None:
            self.__create(name, desc)

    def get_name(self) -> str:
        return self.__name

    def get_description(self) -> str:
        return self.__description

    def get_timestamp(self) -> int:
        return self.__created

    def get_tags(self) -> dict:
        return self.__tags

    def get_uuid(self) -> uuid.UUID:
        return self.__uuid

    def set_name(self, name):
        self.__library._require_writable()
        if not isinstance(name, str):
            raise TypeError("Entry name must be text.")
        self.__name = name
        self.__write_metadata(name=name)
        self.__library._update_name(self, name)

    def set_desc(self, desc):
        self.__library._require_writable()
        if not isinstance(desc, str):
            raise TypeError("Entry description must be text.")
        self.__description = desc
        self.__write_metadata(description=desc)
        self.__library._update_description(self, desc)

    def get_foldername(self):
        return self.__foldername

    def resource(self, filename, r_type, mode: str | None = "r"):
        mode = "r" if mode is None else mode
        if not isinstance(mode, str) or not mode:
            raise ValueError("Resource mode must be a non-empty string.")
        if any(flag in mode for flag in ("w", "a", "x", "+")):
            self.__library._require_writable()
            self.__add_or_update_registry(filename, r_type)
        return self.__resource(filename, mode)

    def register_existing_resource(self, filename: str, r_type: str) -> None:
        """Register a complete resource file that was atomically published by its caller."""
        self.__library._require_writable()
        if not isinstance(filename, str) or not filename or os.path.basename(filename) != filename:
            raise ValueError("Resource filename must be a non-empty local filename.")
        if not isinstance(r_type, str) or not r_type:
            raise ValueError("Resource type must be non-empty text.")
        path = os.path.join(self.__res_path, filename)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Cannot register missing resource: {filename}")
        self.__add_or_update_registry(filename, r_type)

    def list_resources(self):
        self.__read_data()
        return self.__registry.copy().items()

    def set_tag(self, key: str, value: str | float) -> None:
        self.__library._require_writable()
        self.__tags[key] = value
        self.__dirty_tag_keys.add(key)
        self.__removed_tag_keys.discard(key)
        self.__library.update(self)

    def _load_tag(self, key: str, value: str | float) -> None:
        self.__tags[key] = value
        self.__persisted_tags[key] = value
        self.__stored_tags[key] = value

    def _tag_changes(self) -> tuple[dict[str, str | float], set[str]]:
        changed_keys = self.__dirty_tag_keys | {
            key for key, value in self.__tags.items()
            if key not in self.__persisted_tags or self.__persisted_tags[key] != value
        }
        removed_keys = self.__removed_tag_keys | (self.__persisted_tags.keys() - self.__tags.keys())
        return (
            {key: self.__tags[key] for key in changed_keys if key in self.__tags},
            set(removed_keys),
        )

    def _replace_persisted_tags(
        self,
        tags: dict[str, str | float],
        changed_tags: dict[str, str | float],
    ) -> None:
        previous_tags = self.__tags
        previous_stored_tags = self.__stored_tags
        current_tags = dict(tags)
        for key, value in tags.items():
            if key in previous_tags and previous_stored_tags.get(key) == value:
                current_tags[key] = previous_tags[key]
        current_tags.update(changed_tags)
        self.__tags = current_tags
        self.__persisted_tags = dict(current_tags)
        self.__stored_tags = dict(tags)
        self.__dirty_tag_keys.clear()
        self.__removed_tag_keys.clear()

    def add_tag(self, key: str) -> None:
        self.__library._require_writable()
        if key in self.__tags and self.__tags[key] != "":
            raise ValueError("Tag key already exists")
        
        self.__tags[key] = ""
        self.__dirty_tag_keys.add(key)
        self.__removed_tag_keys.discard(key)
        self.__library.update(self)

    def remove_tag(self, key: str) -> None:
        self.__library._require_writable()
        if key in self.__tags:
            del self.__tags[key]
            self.__dirty_tag_keys.discard(key)
            self.__removed_tag_keys.add(key)
            self.__library.update(self) 

    def __resource(self, filename, mode: str | None = "r"):
        p = os.path.join(self.__res_path, filename)
        file = open(p, mode, encoding=("utf-8" if "b" not in mode else None))

        return file

    def __add_or_update_registry(self, f, t):
        self.__library._require_writable()
        self.__write_metadata(resources={f: t})

    def __read_data(self):
        registry = read_registry(os.path.join(self.__res_path, "registry.dat"))
        if registry.entry_uuid != self.__uuid:
            raise ValueError("Registry UUID does not match entry UUID.")
        self.__name = registry.name
        self.__created = registry.created
        self.__description = registry.description
        self.__registry = dict(registry.resources)
        self.__quickload = False

    def __write_metadata(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        resources: dict[str, str] | None = None,
    ):
        self.__library._require_writable()
        registry_path = os.path.join(self.__res_path, "registry.dat")
        updated = update_registry(
            registry_path,
            entry_uuid=self.__uuid,
            initial_name=self.__name,
            initial_created=self.__created,
            initial_description=self.__description,
            name=name,
            description=description,
            resources=resources,
        )
        self.__name = updated.name
        self.__created = updated.created
        self.__description = updated.description
        self.__registry = dict(updated.resources)
        self.__quickload = False

    def __make_path(self):
        foldername = str(self.__uuid)
        folderpath = os.path.join(self.__library.get_base_path(), foldername)
        os.makedirs(folderpath, exist_ok=True)

        return foldername

    def __create(self, name, desc):
        self.__library._require_writable()
        self.__uuid = uuid.uuid4()
        n_folder = self.__make_path()

        self.__foldername = n_folder
        self.__res_path = os.path.join(
            self.__library.get_base_path(), self.__foldername
        )
        self.__name = name
        self.__description = desc
        self.__created = int(time.time())

        self.__write_metadata()

        return self

    def __read(self, foldername):
        self.__foldername = foldername
        self.__res_path = os.path.join(
            self.__library.get_base_path(), self.__foldername
        )
        #print(f"Reading entry from folder '{self.__foldername}'...")
        self.__read_data()
