import os
import time
import uuid
import ipi_ecs.db.db_library as db_library

SAVE_PATH = os.path.join(os.environ["EUVL_PATH"], "datasets")
my_library = db_library.Library(SAVE_PATH)


my_uuid = uuid.UUID("3383f453-6513-4580-90fe-f523c63b2dea")

my_entry = my_library.read_entry(my_uuid)

res = my_entry.resource("my_data.dat", r_type="data", mode="a")
res.write("Adding a new line to my_data.dat\n")
res.close()

res = my_entry.resource("my_data.dat", r_type="data", mode="r")
for line in res:
    print(line.strip())

res.close()

print(f"Tags for entry {my_entry.get_uuid()}: {my_entry.get_tags()}")
