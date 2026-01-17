import os
import time
import ipi_ecs.db.db_library as db_library

SAVE_PATH = os.path.join(os.environ["EUVL_PATH"], "datasets")
my_library = db_library.Library(SAVE_PATH)

my_entry = my_library.create_entry(
    name=f"test_db_create_entry at {time.strftime('%Y-%m-%d %H:%M:%S')}",
    desc="Test entry created in db_create.py",
)

print(f"Created entry with UUID: {my_entry.get_uuid()}")
print(f"Found entries: {my_library.list_entries()}")

my_entry.set_tag("experiment", "db_create_test")
my_entry.set_tag("my value", 42)

print("Querying with tag experiment=db_create_test")

entries = my_library.query(
    {
        "tags": {
            "experiment": "db_create_test",  # Numeric range
        }
    }
)

for entry in entries:
    print(f" - Found entry with UUID: {entry.get_uuid()}")

print("Querying with tag my value=42")
entries = my_library.query({"tags": {"my value": None}})
for entry in entries:
    print(f" - Found entry with UUID: {entry.get_uuid()}")


print("Querying with tag my value>40")
entries = my_library.query({"tags": {"my value": {"min": 40}}})

for entry in entries:
    print(f" - Found entry with UUID: {entry.get_uuid()}")
