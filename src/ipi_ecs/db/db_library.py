import csv
import os
import re
import uuid
import time
import sqlite3
from datetime import date, datetime

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
"""

class Library:
    def __init__(self, path: str):
        self.__path = path

        self.__db_file_path = os.path.join(self.__path, "library.sqlite3")
        self.__conn = sqlite3.connect(self.__db_file_path)
        self.__conn.executescript(DDL)
        self.__conn.commit()

    def __save_tags_to_db(self, entry: "Entry") -> None:
        """Replace all tags for this entry."""
        with self.__conn:
            self.__conn.execute("DELETE FROM tags WHERE entry_uuid = ?", (str(entry.get_uuid()),))
            for key, value in entry.get_tags().items():
                try:
                    # Try to parse as float; if successful, store as numeric
                    val_num = float(value)
                    self.__conn.execute(
                        "INSERT INTO tags (entry_uuid, key, value, val_num) VALUES (?, ?, NULL, ?)",
                        (str(entry.get_uuid()), key, val_num),
                    )
                except ValueError:
                    # Not numeric, store as string
                    self.__conn.execute(
                        "INSERT INTO tags (entry_uuid, key, value, val_num) VALUES (?, ?, ?, NULL)",
                        (str(entry.get_uuid()), key, value),
                    )

    def __update(self, entry: "Entry") -> None:
        """Update an existing entry in the DB."""
        with self.__conn:
            self.__conn.execute(
                "UPDATE library SET name = ?, created = ?, description = ?, foldername = ? WHERE uuid = ?",
                (
                    entry.get_name(),
                    entry.get_timestamp(),
                    entry.get_description(),
                    entry.get_foldername(),
                    str(entry.get_uuid()),
                ),
            )
            self.__save_tags_to_db(entry)

    def query(self, filters: dict, limit: int | None = None) -> list["Entry"]:
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
        query = "SELECT uuid FROM library l"
        params = []
        conditions = []
        
        # Library table filters
        if 'name' in filters:
            conditions.append("l.name LIKE ?")
            params.append(f"%{filters['name']}%")
        if 'description' in filters:
            conditions.append("l.description LIKE ?")
            params.append(f"%{filters['description']}%")
        if 'created_min' in filters:
            conditions.append("l.created >= ?")
            params.append(filters['created_min'])
        if 'created_max' in filters:
            conditions.append("l.created <= ?")
            params.append(filters['created_max'])
        
        # Tag filters
        if 'tags' in filters:
            for key, tag_filter in filters['tags'].items():
                if tag_filter is None:
                    # Check if tag key exists
                    conditions.append("EXISTS (SELECT 1 FROM tags t WHERE t.entry_uuid = l.uuid AND t.key = ?)")
                    params.append(key)
                elif isinstance(tag_filter, str):
                    # String exact match
                    conditions.append("EXISTS (SELECT 1 FROM tags t WHERE t.entry_uuid = l.uuid AND t.key = ? AND t.value = ?)")
                    params.extend([key, tag_filter])
                elif isinstance(tag_filter, dict):
                    # Numeric range
                    tag_query = "EXISTS (SELECT 1 FROM tags t WHERE t.entry_uuid = l.uuid AND t.key = ? AND t.val_num IS NOT NULL"
                    tag_params = [key]
                    if 'min' in tag_filter:
                        tag_query += " AND t.val_num >= ?"
                        tag_params.append(tag_filter['min'])
                    if 'max' in tag_filter:
                        tag_query += " AND t.val_num <= ?"
                        tag_params.append(tag_filter['max'])
                    tag_query += ")"
                    conditions.append(tag_query)
                    params.extend(tag_params)
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY l.created DESC"
        
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        
        rows = self.__conn.execute(query, params).fetchall()
        entries = []
        for row in rows:
            s_uuid = uuid.UUID(row[0])
            entry = self.__read(s_uuid)
            entries.append(entry)
        return entries

    def __read(self, s_uuid: uuid.UUID) -> "Entry":
        """Load an entry from the DB by UUID."""
        row = self.__conn.execute(
            "SELECT foldername FROM library WHERE uuid = ?", (str(s_uuid),)
        ).fetchone()

        if row is None:
            raise ValueError("Entry not found")

        foldername = row[0]
        entry = Entry(self, s_uuid=s_uuid, foldername=foldername)

        tags_rows = self.__conn.execute(
        "SELECT key, value, val_num FROM tags WHERE entry_uuid = ?", (str(s_uuid),)
        ).fetchall()

        for key, value, val_num in tags_rows:
            if val_num is not None:
                entry.set_tag(key, str(val_num))
            else:
                entry.set_tag(key, value)

        return entry

    def __save(self, entry: "Entry") -> None:
        """Save new entry to DB."""
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
        self.__update(entry)

    def create_entry(self, name: str, desc: str) -> "Entry":
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
    ):
        self.__library = library
        self.__uuid = s_uuid or uuid.uuid4()

        self.__name = None
        self.__created = None
        self.__description = None
        self.__foldername = None
        self.__res_path = None
        self.__registry = dict()

        self.__tags = dict()

        if foldername is not None:
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
        self.__name = name
        self.__write_metadata()
        self.__library.update(self)

    def set_desc(self, desc):
        self.__description = desc
        self.__write_metadata()
        self.__library.update(self)

    def get_foldername(self):
        return self.__foldername

    def resource(self, filename, r_type, mode: str | None = "r"):
        self.__add_or_update_registry(filename, r_type)
        return self.__resource(filename, mode)

    def list_resources(self):
        return self.__registry.copy().items()

    def set_tag(self, key: str, value: str | float) -> None:
        self.__tags[key] = value
        self.__library.update(self)

    def add_tag(self, key: str) -> None:
        if key in self.__tags and self.__tags[key] != "":
            raise ValueError("Tag key already exists")
        
        self.__tags[key] = ""
        self.__library.update(self)

    def remove_tag(self, key: str) -> None:
        if key in self.__tags:
            del self.__tags[key]
            self.__library.update(self) 

    def __resource(self, filename, mode: str | None = "r"):
        p = os.path.join(self.__res_path, filename)
        file = open(p, mode, encoding=("utf-8" if "b" not in mode else None))

        return file

    def __add_or_update_registry(self, f, t):
        if not f in self.__registry or self.__registry[f] != t:
            self.__registry[f] = t
            self.__write_metadata()

    def __read_data(self):
        dat_file = self.__resource("registry.dat")

        try:
            read_uuid = uuid.UUID(dat_file.readline().strip())
            assert read_uuid == self.__uuid

            self.__name = dat_file.readline().strip()
            self.__created = int(dat_file.readline().strip())
            self.__description = dat_file.readline().strip()

            for line in dat_file:
                kv = line.strip().split(":")
                assert len(kv) == 2
                self.__registry[kv[0]] = kv[1]

        except (ValueError, IOError, IndexError) as exc:
            raise ValueError("Invalid file") from exc
        finally:
            dat_file.close()

    def __write_metadata(self):
        file = self.__resource("registry.dat", "w")

        if self.__name.find("\n") != -1 or self.__description.find("\n") != -1:
            raise ValueError("Name or description cannot contain newline")

        file.write(f"{self.__uuid}\n")
        file.write(f"{self.__name}\n")
        file.write(f"{str(self.__created)}\n")
        file.write(f"{self.__description}\n")

        if self.__registry is not None:
            for filename, filetype in self.__registry.items():
                file.write(f"{filename}:{filetype}\n")

        file.close()

    def __make_path(self):
        foldername = str(self.__uuid)
        folderpath = os.path.join(self.__library.get_base_path(), foldername)
        os.makedirs(folderpath, exist_ok=True)

        return foldername

    def __create(self, name, desc):
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
        self.__library.update(self)

        return self

    def __read(self, foldername):
        self.__foldername = foldername
        self.__res_path = os.path.join(
            self.__library.get_base_path(), self.__foldername
        )

        self.__read_data()
