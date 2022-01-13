"""This simple flow will aim to test watchdog"""

from datetime import timedelta

import prefect
from prefect import task, Flow
from prefect.schedules import IntervalSchedule

WATCH_DIR = "/home/yguan/work/e2e_test/out/simset1"
SNAP_PATH = ".watchdog"

@task
def watch_files(watch_dir, snap_path=SNAP_PATH):
    import pickle, os, os.path as op
    from watchdog.utils.dirsnapshot import DirectorySnapshot, DirectorySnapshotDiff

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

@task
def process_smurf(files):
    from sotodlib.io.load_smurf import load_file
    from sotodlib.tod_ops.fft_ops import calc_psd

    logger = prefect.context.get("logger")
    # find smurf files
    smurf_files = [f for f in files if "timestreams" in f]
    for smurf_file in smurf_files:
        aman = load_file(smurf_file, load_primary=False, load_biases=False)
        # do something to these data, such as calculating psd
        freq, psd = calc_psd(aman)
        logger.info(f"freq: {freq}")
        logger.info(f"psd: {psd}")

    return smurf_files

schedule = IntervalSchedule(interval=timedelta(minutes=1))

with Flow("file-watcher", schedule) as flow:
    files = watch_files(WATCH_DIR)
    process_smurf(files)

flow.run()
