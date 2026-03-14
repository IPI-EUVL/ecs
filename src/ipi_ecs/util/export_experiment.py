import os
import uuid
import argparse
import shutil

from ipi_ecs.subsystems.experiment_controller import ExperimentReader, RunRecord, RunState

DATA_PATH = os.path.join(os.environ["EUVL_PATH"], "datasets")

def export_experiment_data(experiment_uuid: uuid.UUID, output_dir: str, experiment_reader: ExperimentReader, overwrite: bool) -> None:
    record = experiment_reader.locate_run_by_uuid(experiment_uuid)
    entry = record.get_record()

    print(f"Name: {entry.get_name()}")
    print(f"Description: {entry.get_description()}")

    #print(f"Resources: {entry.list_resources()}")

    os.makedirs(output_dir, exist_ok=True)
    output_savedir = os.path.join(output_dir, f"{entry.get_name()} ({entry.get_description()}) ({str(experiment_uuid)[-6:]})")

    if os.path.exists(output_savedir):
        if overwrite:
            shutil.rmtree(output_savedir)
        else:
            raise ValueError(f"Directory {output_savedir} already exists.")

    os.makedirs(output_savedir, exist_ok=True)

    for resource, type in entry.list_resources():
        resource_data = entry.resource(resource, type, "rb")
        resource_path = os.path.join(output_savedir, resource)
        print(f"Copying {resource} to {resource_path}")
        with open(resource_path, "wb") as f:
            f.write(resource_data.read())

def main():
    parser = argparse.ArgumentParser(description="Export data for a given experiment UUID.")
    parser.add_argument("experiment_uuid", type=str, help="UUID of the experiment to export data for.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output directory.")
    args = parser.parse_args()

    experiment_reader = ExperimentReader(DATA_PATH, "exposure")

    print(f"Exporting data for experiment UUID: {args.experiment_uuid}")
    print(f"Using path: {DATA_PATH}")

    export_experiment_data(uuid.UUID(args.experiment_uuid), os.getcwd(), experiment_reader, args.overwrite)


if __name__ == "__main__":
    main()
