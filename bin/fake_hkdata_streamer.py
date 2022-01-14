#!/usr/bin/env python3

# Generates a G3 file containing simulated telescope pointing data
# in the "SO HK" format.

import numpy as np, time, os.path as op, os
from spt3g import core
from so3g import hk
from utils import *
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-o", "--odir", default="out")
parser.add_argument("--dataset", default="simdata1")
parser.add_argument("--srate", type=float, default=0.005)
parser.add_argument("--v-az", type=float, default=1)
parser.add_argument("--turntime", type=float, default=0.5)
parser.add_argument("--halfscan", type=float, default=9.75)
parser.add_argument("--nsamps-per-frame", type=int, default=10000)
args = parser.parse_args()
if not op.exists(args.odir): os.makedirs(args.odir)

ctime_float = time.time()
ctime_int = int(ctime_float)
first5 = str(ctime_int)[:5]

########################################
# Generate the simulated scan pattern
########################################

# Initial telescope location
az0 = 0.
el0 = 0.

v_az = args.v_az  # degrees/second
dt = args.srate  # sampling cadence
turntime = args.turntime     # turnaround time
halfscan = args.halfscan    # degrees (excl turnaround)

# Part 1: Do 10 scans
az = scanwave(az0, v_az, dt, turntime, halfscan, start=True)
az = np.append(az, np.concatenate([scanwave(az0, v_az, dt, turntime, halfscan) for i in range(8)]))
az = np.append(az, scanwave(az0, v_az, dt, turntime, halfscan, end=True))

# Part 2: Stop for 2 minutes
az = np.append(az, np.zeros(int(120./dt)))

# Part 3: Do another 10 scans
az = np.append(az, scanwave(az0, v_az, dt, turntime, halfscan, start=True))
az = np.append(az, np.concatenate([scanwave(az0, v_az, dt, turntime, halfscan) for i in range(8)]))
az = np.append(az, scanwave(az0, v_az, dt, turntime, halfscan, end=True))

# Part 4: Stop scanning and slew to new az/el
az1 = -10. # new az
el1 = 5.   # new el

az = np.append(az, np.zeros(int(60./dt)))
el = el0 * np.ones(len(az))

az = np.append(az, slew(v_az, dt, az0, az1))
el = np.append(el, slew(v_az, dt, el0, el1))

az = np.append(az, az1*np.ones(int(60./dt)))

# Part 5: Do another 10 scans at new az/el
az = np.append(az, scanwave(az1, v_az, dt, turntime, halfscan, start=True))
az = np.append(az, np.concatenate([scanwave(az1, v_az, dt, turntime, halfscan) for i in range(8)]))
az = np.append(az, scanwave(az1, v_az, dt, turntime, halfscan, end=True))

el = np.append(el, el1 * np.ones(len(az)-len(el)))

########################################
# Organize scan data into frames and
# write to G3 file.
########################################

# Start a "Session" to help generate template frames.
session = hk.HKSessionHelper(hkagg_version=2)

# Create an output file and write the initial "session" frame.
odir = op.join(args.odir, args.dataset, "hk", first5)
if not op.exists(odir): os.makedirs(odir)
oname = op.join(odir, f"{ctime_int}.g3")
writer = core.G3Writer(oname)
writer.Process(session.session_frame())

# Create a new data "provider".
provider = 'observatory.acu1.feeds.acu_udp_stream'
prov_id = session.add_provider(provider)

# Whenever there is a change in the active "providers", write a
# "status" frame.
writer.Process(session.status_frame())

# Parameters
frame_time = ctime_int  # 2021-09-01-00:00:00
n = args.nsamps_per_frame  # length of each frame (samples)
start = 0

# Main loop
for i in range(len(az)//n + 1):
    end = min(start+n, len(az))
    sli = slice(start, end)
    # Vector of unix timestamps
    t = frame_time + dt * np.arange(min(n, end-start))

    # Construct a "block"
    block = core.G3TimesampleMap()
    block.times = core.G3VectorTime([core.G3Time(_t * core.G3Units.s) for _t in t])
    block['Azimuth_Corrected'] = core.G3VectorDouble(az[sli])
    block['Elevation_Corrected'] = core.G3VectorDouble(el[sli])

    # Create an output data frame template associated with this provider.
    frame = session.data_frame(prov_id)

    # Add the block and block name to the frame, and write it.
    frame['block_names'].append('ACU_position')
    frame['blocks'].append(block)
    frame['address'] = provider
    frame['provider_session_id'] = str(ctime_float) 
    writer.Process(frame)

    # For next iteration.
    start += n
    frame_time += n * dt

print("Writing:", oname)
