"""This script will test watchdog's directory snapshot functionality."""
import os, os.path as op
import pickle
from watchdog.utils.dirsnapshot import DirectorySnapshot, DirectorySnapshotDiff


# parameters
snap_path = ".watchdog"
watch_dir = "/mnt/so1/data/chicago-latrt"
recursive = True
dirname_transformer = lambda name: op.basename(name).replace("/","_").replace("-","_")

snap = DirectorySnapshot(watch_dir, recursive=recursive)

if not op.exists(snap_path): os.makedirs(snap_path)
snap_file = op.join(snap_path, dirname_transformer(watch_dir))
if op.exists(snap_file):
    print(f"Found snapshot: {snap_file}")
    with open(snap_file, "rb") as f:
        snap_prev = pickle.load(f)
    # compute diff
    diff = DirectorySnapshotDiff(snap, snap_prev)
    print("Files created since last snapshot:")
    print(diff.files_created)
    
else:
    print(f"Creating snapshot: {snap_file}")
    with open(snap_file, "wb") as f:
        pickle.dump(snap, f)
    print("Nothing else to do")
