import json

import pandas as pd
from conftest import RESOURCES

from hpc_funcs.schedulers.uge import has_uge, qstat
from hpc_funcs.schedulers.uge.constants import TAGS_PENDING, TAGS_RUNNING

pd.set_option("display.max_columns", None)


def test_parse_job():

    filename = RESOURCES / "uge/qstat_jobid.json"

    with open(filename, "r") as f:
        stdout = f.read()

    jdata = json.loads(stdout)

    print(jdata)

    data = pd.DataFrame(jdata)


def test_parse_jobs_file():

    filename = RESOURCES / "uge/qstat_all.json"

    with open(filename, "r") as f:
        stdout = f.read()

    list_jobs = qstat.parse_joblist_json(stdout)

    df = pd.DataFrame(list_jobs)
    assert len(df) == 7

    print(df["state"])

    # Filter for active jobs
    running_jobs = df[df["state"].isin(TAGS_RUNNING)].copy()
    assert len(running_jobs) == 4

    pending_jobs = df[df["state"].isin(TAGS_PENDING)].copy()
    assert len(pending_jobs) == 3


if not has_uge():
    pytest.skip("Could not find UGE executable", allow_module_level=True)


def test_all():
    df = qstat.get_all_jobs()
    assert len(df) > 1
