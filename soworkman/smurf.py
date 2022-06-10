"""Smurf related tasks"""
import prefect
from prefect import task
import os.path as op

@task
def update_g3tsmurf_database(data_prefix, db_path, timestream_folder=None, smurf_folder=None,
                             update_delay=2, from_scratch=False):
    import datetime as dt
    from sotodlib.io.load_smurf import G3tSmurf, Observations, dump_DetDb

    logger = prefect.context.get("logger")
    if timestream_folder is None: timestream_folder = op.join(data_prefix, 'timestreams/')
    if smurf_folder is None: smurf_folder = op.join(data_prefix, 'smurf/')

    SMURF = G3tSmurf(timestream_folder,
                     db_path=db_path,
                     meta_path=smurf_folder)
    if from_scratch:
        logger.info("Building Database from Scratch, May take awhile")
        min_time = dt.datetime.utcfromtimestamp(int(1.6e9))
    else:
        min_time = dt.datetime.now() - dt.timedelta(days=update_delay)

    new_files = SMURF.index_archive(min_ctime=min_time.timestamp())
    logger.info(f"new files found: {new_files}")
    SMURF.index_metadata(min_ctime=min_time.timestamp())

    session = SMURF.Session()

    new_obs = session.query(Observations).filter(Observations.start >= min_time,
                                                 Observations.stop == None).all()
    for obs in new_obs:
        SMURF.update_observation_files(obs, session, force=True)

    return new_files

@task
def calculate_white_noise(files):
    import numpy as np
    from qds import qds
    from sotodlib.io.load_smurf import load_file
    from sotodlib.tod_ops.fft_ops import calc_psd, calc_wn

    logger = prefect.context.get("logger")

    monitor = qds.Monitor(host='grumpy.physics.yale.edu',
                          port=443,
                          username=u'qdsuser',
                          password=u'SO Pipeline Working Groups QDS',
                          path='influxdb',
                          ssl=True)

    logger.info("connected to qds monitor")

    for f in files:
        stream_id = f.split("/")[-2]
        session_id = f.split("/")[-1].split("_")[1]
        obs_id = f"{stream_id}_{session_id}"
        if monitor.check('white_noise_level', obs_id, {'filename': f}):
            logger.info("Record already exists in monitor, finishing")
            return

        aman = load_file(f)
        phase_to_pA = 9e6/(2*np.pi)
        aman.signal *= phase_to_pA
        nsamps = len(aman.timestamps)
        nsamps_per_slice = 200000
        dets = aman.dets.count
        wn_arr = np.zeros((dets, nsamps//nsamps_per_slice+1))
        times = np.zeros(nsamps//nsamps_per_slice+1)

        # calculate psd and wn from signal (at 100 sec intervals)
        for (i, n) in enumerate(range(0, nsamps, nsamps_per_slice)):
            sl = slice(n,min(n+nsamps_per_slice, nsamps))
            freqs, Pxx = calc_psd(aman, signal=aman.signal[:,sl], nperseg=2**18)
            wn_arr[:, i] = np.abs(calc_wn(aman, Pxx, freqs=freqs))
            times[i] = aman.timestamps[n]

        # publish wn data to qds monitor
        chans = aman.ch_info['channel']
        bands = aman.ch_info['band']
        wlvs  = wn_arr.ravel()
        timestamps = np.tile(times, dets)
        base_tags = {'telescope':'LATRt', 'slot': stream_id}
        tag_list = []
        for det in range(dets):
            det_tag = dict(base_tags)
            det_tag['detector'] = bands[det]*512 + chans[det]
            tag_list.extend([det_tag]*len(times))
        log_tags = ({'observation': obs_id, 'filename': f})
        monitor.record('white_noise_level', wlvs, timestamps, tag_list, 'detector_stats', log_tags=log_tags)
        logger.info(f"{f}: white noise sent to qds monitor")
        monitor.write()
