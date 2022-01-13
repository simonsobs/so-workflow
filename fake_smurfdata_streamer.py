"""This script aims to generate a smurf data that change based on time, so it could be managed by a cron job to simulate a preliminary data collection cycle. """

import numpy as np, time
import os.path as op, os, argparse

import so3g
from spt3g import core
from sosmurf.SessionManager import SessionManager

# import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-o", "--odir",default="out")
parser.add_argument("--dataset", default="simset1")
parser.add_argument("--srate", type=float, help="per sample time in sec", default=0.005)
parser.add_argument("--stream-id", default="crate1slot3")
parser.add_argument("--nchan", type=int, default=10)
parser.add_argument("--nsamps-per-frame", type=int, default=1000)
parser.add_argument("--nframes", type=int, default=120)
args = parser.parse_args()

# Create session
ctime = int(time.time())  # utc
first5 = str(ctime)[:5]
session = SessionManager(stream_id=args.stream_id)
session.session_id = ctime
mask = str(list(np.arange(args.nchan).astype(int)))
session.status = {
    "AMCc.SmurfProcessor.ChannelMapper.NumChannels": args.nchan,
    "AMCc.SmurfProcessor.ChannelMapper.Mask": mask
}
# Create an output file and write the initial frame
odir = op.join(args.odir, args.dataset, "timestreams", first5, args.stream_id)
if not op.exists(odir): os.makedirs(odir)
oname = op.join(odir, f"{ctime}_{0:03d}.g3")

writer = core.G3Writer(oname)
writer.Process(session.start_session())
# Create a status frame
writer.Process(session.status_frame())

# Sampling parameters
frame_time = ctime
dt = args.srate  # seconds
nsamps = args.nsamps_per_frame
Nchan = args.nchan  # number of channels  # for reference, ufm has 882 chans

# Placeholder data: a linear sweep from 0 to 1
# sim_data = np.linspace(0., 1., n)
sim_data = np.random.rand(Nchan, nsamps)
cal = np.ones(Nchan, dtype=np.double)
# Create a scan frame
for i in range(args.nframes):
    # Vector of unix timestamps
    t = frame_time + dt * np.arange(nsamps)

    # Construct the G3TimesampleMap containing the data
    data = so3g.G3SuperTimestream()
    data.times = core.G3VectorTime([core.G3Time(_t * core.G3Units.s) for _t in t])
    data.names = [f'r{c:04d}' for c in range(Nchan)]
    # a scale factor for each detector
    data.quanta = cal
    data.data = sim_data

    # Create an output data frame and insert the data
    frame = core.G3Frame(core.G3FrameType.Scan)
    frame['data'] = data

    # Write the frame
    frame['timing_paradigm'] = "Low Precision"
    writer.Process(session(frame)[0])
    print(f"Writing frame {i}")

    # For next iteration
    frame_time += nsamps * dt

print("Writing:", oname)
