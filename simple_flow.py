"""This simple flow will aim to test watchdog"""

import os.path as op, os
import pickle
from watchdog.utils.dirsnapshot import DirectorySnapshot, DirectorySnapshotDiff
from datetime import timedelta
from prefect.schedules import IntervalSchedule
import prefect
from prefect import task, Flow
from prefect.schedules import IntervalSchedule

WATCH_DIR = "/home/yguan/work/e2e_test/out/simset1"
SNAP_PATH = ".watchdog"

@task
def get_new_files(watch_dir, snap_path=SNAP_PATH):
    logger = prefect.context.get("logger")
    dirname_transformer = lambda name: op.basename(name).replace("/","_").replace("-","_")
    snap = DirectorySnapshot(watch_dir, recursive=True)
    if not op.exists(snap_path): os.makedirs(snap_path)
    snap_file = op.join(snap_path, dirname_transformer(watch_dir))
    if op.exists(snap_file):
        logger.info(f"Found snapshot: {snap_file}")
        with open(snap_file, "rb") as f:
            snap_prev = pickle.load(f)
        # compute diff
        diff = DirectorySnapshotDiff(snap_prev, snap)
        logger.info("Files created since last snapshot:")
        logger.info(f"{diff.files_created}")
        return diff.files_created
    else:
        logger.info(f"Creating snapshot: {snap_file}")
        with open(snap_file, "wb") as f:
            pickle.dump(snap, f)
        logger.info("Nothing else to do")
        return []

schedule = IntervalSchedule(interval=timedelta(minutes=1))

with Flow("file-watcher", schedule) as flow:
    files = get_new_files(WATCH_DIR)

flow.run()
