"""This script will test watchdog's directory snapshot functionality."""
import os, os.path as op
import pickle
from watchdog.utils.dirsnapshot import DirectorySnapshot, DirectorySnapshotDiff
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--watch", default="/mnt/so1/data/chicago-latrt")
parser.add_argument("--snap-path", default=".watchdog")
args = parser.parse_args()
dirname_transformer = lambda name: op.basename(name).replace("/","_").replace("-","_")

snap = DirectorySnapshot(args.watch, recursive=True)
if not op.exists(args.snap_path): os.makedirs(args.snap_path)
snap_file = op.join(args.snap_path, dirname_transformer(args.watch))
if op.exists(snap_file):
    print(f"Found snapshot: {snap_file}")
    with open(snap_file, "rb") as f:
        snap_prev = pickle.load(f)
    # compute diff
    diff = DirectorySnapshotDiff(snap_prev, snap)
    print("Files created since last snapshot:")
    print(diff.files_created)
else:
    print(f"Creating snapshot: {snap_file}")
    with open(snap_file, "wb") as f:
        pickle.dump(snap, f)
    print("Nothing else to do")
