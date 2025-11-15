import pandas as pd
import pytest
from conftest import RESOURCES

from hpc_funcs.schedulers.uge import has_uge
from hpc_funcs.schedulers.uge.qstat import get_all_jobs_json, get_all_jobs_text
from hpc_funcs.schedulers.uge.qstat_text import COLUMN_ARRAY, COLUMN_JOBID, get_qstat_job_text, COLUMN_INFO_USER
from hpc_funcs.schedulers.uge.qstat_xml import get_qstat_job_xml, parse_jobinfo_xml

pd.set_option("display.max_columns", None)


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


def test_jobinfo_from_joblist_text():

    df = get_all_jobs_text()
    print(df)
    assert len(df) > 1

    job_id = df[COLUMN_JOBID].values[0]

    # Get specific job info
    job_infos, job_erros = get_qstat_job_text(job_id)

    print(job_infos)
    print(job_erros)

    assert len(job_infos)
    assert job_infos[0][COLUMN_INFO_USER]
