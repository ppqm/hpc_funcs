import json

import pandas as pd
from conftest import RESOURCES

from hpc_funcs.schedulers.uge import has_uge, qstat
from hpc_funcs.schedulers.uge.constants import TAGS_PENDING, TAGS_RUNNING
from hpc_funcs.schedulers.uge.qstat import parse_jobinfo_xml

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


def test_parse_jobinfo_xml():
    """Test parsing of qstat -j -xml output."""

    filename = RESOURCES / "uge" / "qstat_jobinfo_pending.xml"

    with open(filename, "r") as f:
        xml_str = f.read()

    jobs = parse_jobinfo_xml(xml_str)

    # Should parse exactly one job
    assert len(jobs) == 1

    job = jobs[0]

    # Test basic fields
    assert job["JB_job_number"] == "29903814"
    assert job["JB_job_name"] == "TestJob"
    assert job["JB_owner"] == "kromaji1"

    # Test job array structure
    assert len(job["JB_ja_structure"]) == 1
    assert job["JB_ja_structure"][0]["RN_min"] == "1"
    assert job["JB_ja_structure"][0]["RN_max"] == "2"
    assert job["JB_ja_structure"][0]["RN_step"] == "1"

    # Test job array tasks
    assert len(job["JB_ja_tasks"]) == 1
    task = job["JB_ja_tasks"][0]
    assert task["JAT_task_number"] == "1"
    assert task["JAT_status"] == "65536"


if not has_uge():
    pytest.skip("Could not find UGE executable", allow_module_level=True)


def test_all():
    df = qstat.get_all_jobs()
    assert len(df) > 1
