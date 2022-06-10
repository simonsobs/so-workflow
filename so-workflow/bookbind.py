import os, os.path as op
import argparse
import numpy as np
import datetime as dt

from collections import OrderedDict
from sqlalchemy import or_, and_, not_

from sotodlib.io.load_smurf import G3tSmurf, Observations
from bookbinder import Bookbinder
from spt3g import core

from prefect import task, Flow

# emulate argparse
class args: pass
args.data_prefix = '/mnt/so1/data/pton-rd/'
args.db_path = '/mnt/so1/users/kmharrin/smurf_context/pton-rd_v1.db'
# args.min_ctime = None
args.min_ctime = 1647117381
args.max_ctime = None
args.stream_ids = "crate1slot2,crate1slot3,crate1slot4,crate1slot5"
args.timestream_folder = None
args.smurf_folder = None
args.ignore_singles = True
# args.ignore_singles = False
args.min_overlap = 0
args.odir = "out"

#####################
# utility functions #
#####################

def contains(obs_tuple, stream_id):
    """check whether an observation book has a given stream id"""
    for obs in obs_tuple:
        if stream_id in obs: return True
    return False

def stream_timestamp(obs):
    """parse observation id to obtain a (stream, timestamp) pair"""
    return obs.split("-")[0], obs.split("_")[1]

def get_book_id(**kwargs):
    """build a compliant book id based on observation. This will also be
    called by prefect service to name tasks by book id (required using
    kwargs)

    """
    obs = kwargs.get('obs')
    slot_flags = ''
    slot_flags += '1' if contains(obs, 'crate1slot2') else '0'
    slot_flags += '1' if contains(obs, 'crate1slot3') else '0'
    slot_flags += '1' if contains(obs, 'crate1slot4') else '0'
    slot_flags += '1' if contains(obs, 'crate1slot5') else '0'
    # add more here

    # parse observation time and name the book with the first timestamp
    stream_id, ts = stream_timestamp(obs[0])
    book_id = f'obs_{ts}_sat1_{slot_flags}'
    return book_id

def find_start_end_times(file_list):
    """Loop through a list of g3 files to find the start and end time"""
    if not isinstance(file_list, list):
        file_list = list(file_list)

    start_times = []
    end_times = []
    for f in file_list:
        sfile = core.G3File(f)
        scanframes = [sf for sf in sfile if sf.type == core.G3FrameType.Scan]
        start_times.append(scanframes[0]['data'].times[0])
        end_times.append(scanframes[-1]['data'].times[-1])
    return np.min(start_times), np.max(end_times)

def find_book_start_end(d):
    """Given an observation dictionary that specifies which g3 files are
    associated with a given wafer, it produces the overlapping start and
    end time that should be used across wafers.

    """
    all_start_times = []
    all_end_times = []
    for k in d.keys():
        st, et = find_start_end_times(d[k])
        all_start_times.append(st)
        all_end_times.append(et)
    return np.max(all_start_times), np.min(all_end_times)

####################
# database related #
####################

def create_smurf_session():
    """create a connection session to g3tsmurf database"""
    if args.timestream_folder is None:
        args.timestream_folder = os.path.join( args.data_prefix, 'timestreams/')

    if args.smurf_folder is None:
        args.smurf_folder = os.path.join(args.data_prefix, 'smurf/')

    SMURF = G3tSmurf(args.timestream_folder,
                     db_path=args.db_path,
                     meta_path=args.smurf_folder)
    session = SMURF.Session()

    return session

class SmurfSession:
    """manage singleton connection to SMURF database"""
    _session = None
    @classmethod
    def load_smurf_session(cls):
        if cls._session is None:
            cls._session = create_smurf_session()
        return cls._session

def get_obs_files(obs):
    """product an obs dictionary with stream_id as the keys and the smurf files as values"""
    session = SmurfSession.load_smurf_session()
    out = {}
    for obs_id in obs:
        stream_id, ts = stream_timestamp(obs_id)
        q = session.query(Observations).filter(Observations.obs_id==obs_id).all()
        if len(q) == 0: raise RuntimeError(f"Unexpected: {obs_id} not found")
        if len(q)  > 1: raise RuntimeError(f"Unexpected: {obs_id} has more than one observations")
        out[stream_id] = [f.name for f in q[0].files]
    return out

#################
# prefect tasks #
#################

# @task
def imprinter():
    """produce a list of observation bundles (obs book)"""
    session = SmurfSession.load_smurf_session()
    # set sensible ctime range is none is given
    if args.min_ctime is None:
        args.min_ctime = session.query(Observations.timestamp).order_by(Observations.timestamp).first()[0]
    if args.max_ctime is None:
        args.max_ctime = dt.datetime.now().timestamp()
    # find all complete observations that start within the time range
    obs_q = session.query(Observations).filter(Observations.timestamp >= args.min_ctime,
                                               Observations.timestamp <= args.max_ctime,
                                               not_(Observations.stop==None))
    # get wafers
    if args.stream_ids is None:
        # find all unique stream ids during the time range
        streams = session.query(Observations.stream_id).filter(
            Observations.timestamp >= args.min_ctime,
            Observations.timestamp <= args.max_ctime
        ).distinct().all()
        streams = [s[0] for s in streams]
    else:
        # if user input is present, format it like the query response
        streams = args.stream_ids.split(',')
        streams = [s.strip() for s in streams]

    # restrict to given stream ids (wafers)
    stream_filt = or_(*[Observations.stream_id == s for s in streams])

    output = []
    for stream in streams:
        # loop through all observations for this particular stream_id
        for str_obs in obs_q.filter(Observations.stream_id == stream).all():
            # query for all possible types of overlapping observations from other streams
            q = obs_q.filter(
                Observations.stream_id != str_obs.stream_id,
                stream_filt,
                or_(
                    and_(Observations.start <= str_obs.start, Observations.stop >= str_obs.start),
                    and_(Observations.start <= str_obs.stop, Observations.start >= str_obs.start),
                    and_(Observations.start >= str_obs.start, Observations.stop <= str_obs.stop),
                    and_(Observations.start <= str_obs.start, Observations.stop >= str_obs.stop)
                ))
            if q.count()==0 and not args.ignore_singles:
                # observation has no overlapping segments
                output.append((str_obs.obs_id,))

            elif q.count() > 0:
                obs_list = q.all()
                obs_list.append(str_obs)
                # check to make sure ALL observations overlap all others
                if np.max([o.start for o in obs_list]) > np.min([o.stop for o in obs_list]):
                    continue
                overlap_time = np.min([o.stop for o in obs_list]) - np.max([o.start for o in obs_list])
                if overlap_time.total_seconds() < args.min_overlap:
                    continue
                # add all of the possible overlaps
                id_list = [obs.obs_id for obs in obs_list]
                output.append(tuple(sorted(id_list)))

    # remove exact duplicates
    output = list(OrderedDict.fromkeys(output))
    return output

# bind observation bundle (obs book) with bookbinder
# Note: the use of task_run_name is to use a book id as the task name
# @task(task_run_name=get_book_id)
def bookbind(obs):
    book_id = get_book_id(obs=obs)
    # parse observation time and name the book with the first timestamp
    print(f"binding: {book_id}")
    bdir = op.join(args.odir, book_id)
    if not op.exists(bdir): os.makedirs(bdir)
    obs_files = get_obs_files(obs)
    # determine overlapping start and end time for the given observation book
    start_t, end_t = find_book_start_end(obs_files)
    print(f"Trimming to: start={start_t}, end={end_t}")
    for stream_id, files in obs_files.items():
        Bookbinder([], files, out_root=args.odir, start_time=start_t, end_time=end_t,
                   stream_id=stream_id, book_id=book_id)()

######################
# flow registeration #
######################

# debug
# obs_list = imprinter()
# bookbind(obs_list[1])
# with Flow("bookbinder-flow") as flow:
#     obs_list = imprinter()
#     bookbind.map(obs_list)

# if __name__ == '__main__':
    # flow.run()
    # flow.register(project_name='level2-run')
    # obs_list = imprinter()

    # for obs in obs_list:
    #     bookbind(obs)
