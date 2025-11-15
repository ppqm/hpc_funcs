import pandas as pd
import pytest
from conftest import RESOURCES

from hpc_funcs.schedulers.uge import has_uge
from hpc_funcs.schedulers.uge.qstat import get_all_jobs_json, get_all_jobs_text
from hpc_funcs.schedulers.uge.qstat_text import COLUMN_ARRAY, parse_joblist_text
from hpc_funcs.schedulers.uge.qstat_xml import get_qstat_job_xml, parse_jobinfo_xml

pd.set_option("display.max_columns", None)


def test_parse_joblist_text():

    filename = RESOURCES / "uge/qstat_list.txt"

    with open(filename, "r") as f:
        stdout = f.read()

    list_jobs = parse_joblist_text(stdout)

    # print(list_jobs)

    pdf = pd.DataFrame(list_jobs)
    assert len(pdf)

    print(COLUMN_ARRAY)
    print(pdf.columns.tolist())
    assert COLUMN_ARRAY in pdf

    print(pdf)


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

    print()

    df = get_all_jobs_json()
    print(df)
    assert len(df) > 1

    df = get_all_jobs_text()
    print(df)
    assert len(df) > 1

    assert COLUMN_ARRAY in df


def test_jobid_from_all_text():

    df = get_all_jobs_text()
    print(df)
    assert len(df) > 1

    job_id = df["job_number"].values[0]

    job_infos, job_errors = get_qstat_job_xml(job_id)

    print(job_infos)

    assert job_infos[0]
    assert job_infos[0]["JB_job_number"]

    return
