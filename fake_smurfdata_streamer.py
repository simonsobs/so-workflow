"""This script aims to generate a smurf data that change based on time, so it could be managed by a cron job to simulate a preliminary data collection cycle. """

import numpy as np, time
import os.path as op, os, argparse

from spt3g import core
from sosmurf.SessionManager import SessionManager

# import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-o", "--odir",default="out")
parser.add_argument("--tag", default="simset1")
parser.add_argument("--srate", type=float, help="per sample time in sec", default=0.005)
parser.add_argument("--stream-id", default="crate1slot3")
parser.add_argument("--nchan", type=int, default=10)
parser.add_argument("--nsamps-per-frame", type=int, default=1000)
parser.add_argument("--nframes", type=int, default=10)
args = parser.parse_args()
if not op.exists(args.odir): os.makedirs(args.odir)

# Create session
ctime = int(time.time())  # utc
session = SessionManager(stream_id=args.stream_id)
session.session_id = ctime

# Create an output file and write the initial frame
oname = op.join(args.odir, f"{args.tag}_smurfdata_{args.stream_id}_{ctime}.g3")
writer = core.G3Writer(oname)
writer.Process(session.start_session())
# Create a status frame
writer.Process(session.status_frame())

# Sampling parameters
frame_time = ctime
dt = args.srate  # seconds
n = args.nsamps_per_frame    # number of samples <- YG: should be optimized
Nchan = args.nchan  # number of channels

# Placeholder data: a linear sweep from 0 to 1
sim_data = np.linspace(0., 1., n)

# Create a scan frame
for i in range(args.nframes):
    # Vector of unix timestamps
    t = frame_time + dt * np.arange(n)

    # Construct the G3TimesampleMap containing the data
    data = core.G3TimesampleMap()
    data.times = core.G3VectorTime([core.G3Time(_t * core.G3Units.s) for _t in t])
    for c in range(Nchan):
        data['r{:04d}'.format(c)] = core.G3Timestream(sim_data)

    # Create an output data frame and insert the data
    frame = core.G3Frame(core.G3FrameType.Scan)
    frame['data'] = data

    # Write the frame
    frame['timing_paradigm'] = "Low Precision"
    writer.Process(session(frame)[0])

    # For next iteration
    frame_time += n * dt

print("Writing:", oname)    
