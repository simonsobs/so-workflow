import numpy as np
from sotodlib.io.load_smurf import G3tSmurf, Files
from sotodlib.tod_ops.fft_ops import calc_psd
from sotodlib.tod_ops.fft_ops import calc_wn
from sotodlib.io.load_smurf import load_file
from qds import qds

# import warnings
# warnings.filterwarnings("error", category=RuntimeWarning)

SMURF = G3tSmurf(archive_path='/mnt/so1/data/chicago-latrt/timestreams/',
                 meta_path='/mnt/so1/data/chicago-latrt/smurf/',
                 db_path='/mnt/so1/users/kmharrin/smurf_context/latrt_db_v3.db')
session = SMURF.Session()
g3_table = "/home/remyg/wn_calculation/wn_texts/latrt_texts/checked_g3s.txt"
bad_g3 = "/home/remyg/wn_calculation/wn_texts/latrt_texts/bad_g3s.txt"

# open text file that contains last run g3 file
with open(g3_table, 'r') as g3_file:
    cur_file = g3_file.readline()

# default g3 file is earliest file since 6/1/2021...first file from cooldown 9, sat1
default = '1628893158_000.g3'

#handle errors in loading upcoming data. If g3_table has no file, upload data starting from default file
if cur_file:
    q = session.query(Files).filter(Files.name == cur_file)
    assert q.count() == 1, "Last file not found"
else:
    q = session.query(Files).filter(Files.name == default)
q = session.query(Files).filter(Files.start > q[0].start)
assert q.count() > 0, "No files found later than most recent file"
q = q.order_by(Files.start.asc())

# set up monitor
monitor = qds.Monitor(host='grumpy.physics.yale.edu',
                      port=443,
                      password=u'##########',
                      username=u'##########',
                      # username=u'qdsuser',
                      # password=u'SO Pipeline Working Groups QDS',
                      path='influxdb',
                      ssl=True)

for file in q:
        try:
            file_check = {'filename': file.name}
            # I recognize this if-else statement is not hyper efficient, but I think it's easier to understand this way.
            if monitor.check('white_noise_level', file.obs_id, file_check) and monitor.check('biasband_mean_wn', file.obs_id, file_check):
                with open(g3_table, 'w') as g3_file:
                    g3_file.seek(0)
                    g3_file.write(file.name)

            else:
                # load in data
                aman = SMURF.load_data(file.start, file.stop)
                if np.size(aman.timestamps) == 0:
                    with open(bad_g3, 'a') as bad_g3s:
                        error = f"Restricted all timestamps: {file.name}\n"
                        bad_g3s.write(error)
                    # Update current file!
                    with open(g3_table, 'w') as g3_file:
                        g3_file.seek(0)
                        g3_file.write(file.name)
                    continue

                phase_to_pA = 9e6/(2*np.pi)
                aman.signal *= phase_to_pA

                # set up to collect data at 100 sec intervals - minute is when "100.0"s were "60.0s
                minute = 0
                start = aman.timestamps[0]
                stop = start + 100.0
                end = aman.timestamps[-1]
                file_mins = int((end - start)/100.0) + 1
                dets = aman.dets.count
                time = []
                wn_arr = np.zeros((dets,file_mins))

                # calculate psd and wn from signal (at 100 sec intervals)
                for i in range(file_mins):
                    tmsk = np.all([aman.timestamps >= start, aman.timestamps <= stop], axis=0)
                    freqs, Pxx = calc_psd(aman, signal=aman.signal[:,tmsk], nperseg=2**18)
                    wn = calc_wn(aman, Pxx, freqs=freqs)
                    for det in range(dets):
                        wn_arr[det][minute] = np.abs(wn[det])

                    time.append(stop)
                    start = stop
                    stop += 100.0
                    minute += 1

                # publish wn data to qds monitor
                chans = aman.ch_info['channel']
                bands = aman.ch_info['band']
                wlvs = wn_arr.ravel()
                time_tile = np.tile(time, (dets, 1))
                timestamps = time_tile.ravel()
                base_tags = {'telescope':'LATRt', 'slot': stream_id}
                tag_list = []
                for det in range(dets):
                    det_tag = dict(base_tags)
                    det_tag['detector'] = bands[det]*512 + chans[det]
                    for i in range(np.size(time)):
                        tag_list.append(det_tag)

                log_tags = ({'observation': obs_id, 'filename': f})
                monitor.record('white_noise_level', wlvs, timestamps, tag_list, 'detector_stats', log_tags=log_tags)
                monitor.write()

                # Update current file!
                with open(g3_table, 'w') as g3_file:
                    g3_file.seek(0)
                    g3_file.write(file.name)


        except KeyError:
            with open(bad_g3, 'a') as bad_g3s:
                error = f"KeyError: {file.name}\n"
                bad_g3s.write(error)
            # Update current file!
            with open(g3_table, 'w') as g3_file:
                g3_file.seek(0)
                g3_file.write(file.name)

        except TypeError:
            with open(bad_g3, 'a') as bad_g3s:
                error = f"TypeError: {file.name}\n"
                bad_g3s.write(error)
            # Update current file!
            with open(g3_table, 'w') as g3_file:
                g3_file.seek(0)
                g3_file.write(file.name)

        except ValueError:
            with open(bad_g3, 'a') as bad_g3s:
                error = f"ValueError: {file.name}\n"
                bad_g3s.write(error)
            # Update current file!
            with open(g3_table, 'w') as g3_file:
                g3_file.seek(0)
                g3_file.write(file.name)

        except RuntimeWarning:
            with open(bad_g3, 'a') as bad_g3s:
                error = f"RuntimeWarning: {file.name}\n"
                bad_g3s.write(error)
            # Update current file!
            with open(g3_table, 'w') as g3_file:
                g3_file.seek(0)
                g3_file.write(file.name)
