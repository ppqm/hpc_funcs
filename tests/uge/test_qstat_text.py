import pandas as pd
import pytest
from conftest import RESOURCES

from hpc_funcs.schedulers.uge import has_uge
from hpc_funcs.schedulers.uge.qstat_text import parse_joblist_text, parse_jobinfo_text, COLUMN_INFO_JOBID, COLUMN_INFO_NAME, COLUMN_INFO_USER, COLUMN_INFO_ARRAY, COLUMN_ARRAY

pd.set_option("display.max_columns", None)

def test_parse_jobinfo():

    filename = RESOURCES / "uge" / "qstat_jobinfo_pending.txt"

    with open(filename, "r") as f:
        stdout = f.read()

    jobs = parse_jobinfo_text(stdout)

    print(jobs)

    # Should parse exactly one job
    assert len(jobs) == 1

    job = jobs[0]

    # Test basic fields
    assert job[COLUMN_INFO_JOBID] == "29903814"
    assert job[COLUMN_INFO_NAME] == "TestJob"
    assert job[COLUMN_INFO_USER] == "kromaji1"

    # Test job array structure
    assert job[COLUMN_INFO_ARRAY]


def test_parse_joblist():

    filename = RESOURCES / "uge/qstat_list.txt"

    with open(filename, "r") as f:
        stdout = f.read()

    job_list = parse_joblist_text(stdout)

    # print(list_jobs)

    pdf = pd.DataFrame(job_list)
    assert len(pdf)

    print(COLUMN_ARRAY)
    print(pdf.columns.tolist())
    assert COLUMN_ARRAY in pdf

    print(pdf)

