import copy
import logging
import time
from typing import Any, Dict, Iterator, List

import pandas as pd
from pandas import DataFrame, Series

from hpc_funcs.schedulers.uge.constants import TAGS_RUNNING
from hpc_funcs.schedulers.uge.qstat import get_all_jobs_text
from hpc_funcs.schedulers.uge.qstat_json import get_qstat_job_json
from hpc_funcs.schedulers.uge.qstat_text import COLUMN_SLOTS, COLUMN_USER

logger = logging.getLogger(__name__)


def get_cluster_usage() -> Series:
    """Get cluster usage information, grouped by users

    To get totla cores in use `pdf["slots"].sum()`
    """

    pdf = get_all_jobs_text()

    # filter to running
    pdf = pdf[pdf.state.isin(TAGS_RUNNING)]

    pdf[COLUMN_SLOTS] = pdf[COLUMN_SLOTS].astype("int64")

    counts = pdf.groupby([COLUMN_USER])[COLUMN_SLOTS].agg("sum")
    counts = counts.sort_values()  # type: ignore

    return counts


def wait_for_jobs(jobs: List[str], sleep: int = 60) -> Iterator[str]:
    """ """

    logger.info(f"Waiting for {len(jobs)} job(s) on UGE...")

    start_time = time.time()

    jobs = copy.deepcopy(jobs)

    while len(jobs):
        logger.info(
            f"... and breathe for {sleep} sec, still waiting for {len(jobs)} job(s) to finish..."
        )

        time.sleep(sleep)

        rm_list = []

        for job_id in jobs:
            if is_job_done(job_id):
                yield job_id
                rm_list.append(job_id)

        for job_id in rm_list:
            jobs.remove(job_id)

    end_time = time.time()
    diff_time = end_time - start_time
    logger.info(f"All jobs finished and took {diff_time / 60 / 60 :.2f}h")


def is_job_done(
    job_id: str,
) -> bool:
    """
    If unable to find job info, assume job is done.
    """

    job_info, job_errors = get_qstat_job_json(job_id)

    if len(job_errors):
        logger.error(f"qstat error: {job_errors[0]}")

    # If there still is some qstat information, the job is not done
    if job_info:
        return False

    # TODO Check qacct -j information

    return True
