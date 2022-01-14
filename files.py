"""Files related tasks"""
import prefect
from prefect import task

WATCH_DIR = "/home/yguan/work/e2e_test/out/simset1"
SNAP_PATH = ".watchdog"
dirname_transformer = lambda name: op.basename(name).replace("/","_").replace("-","_")

@task
def watch_files(watch_dir=WATCH_DIR, snap_path=SNAP_DIR):
    """task to watch any directory for new files"""
    import pickle, os, os.path as op
    from watchdog.utils.dirsnapshot import DirectorySnapshot, DirectorySnapshotDiff

    logger = prefect.context.get("logger")
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
