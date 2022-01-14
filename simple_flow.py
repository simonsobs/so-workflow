"""This simple flow will aim to test watchdog"""

from datetime import timedelta

import prefect
from prefect import task, Flow, case
from prefect.schedules import IntervalSchedule
from smurf import update_g3tsmurf_database, calculate_white_noise

DATA_PREFIX = "/mnt/so1/data/chicago-latrt"
DB_PATH = "/mnt/so1/users/yguan/smurf_context/latrt_db_v4.db"

@task
def has_newfiles(new_files):
    return len(new_files) > 0

# # debug
# new_files = [  
#     "/mnt/so1/data/chicago-latrt/timestreams/16414/ufm_cv4/1641406521_000.g3"
# ]
schedule = IntervalSchedule(interval=timedelta(minutes=60))
with Flow("simple-flow", schedule) as flow:
    new_files = update_g3tsmurf_database(DATA_PREFIX, DB_PATH)
    cond = has_newfiles(new_files)
    with case(cond, True):
        calculate_white_noise(new_files)

flow.run()
