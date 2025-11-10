# def get_cluster_usage() -> DataFrame:
#     """Get cluster usage information, grouped by users

#     To get totla cores in use `pdf["slots"].sum()`
#     """

#     stdout, _ = execute("qstat -u \\*")  # noqa: W605
#     pdf = parse_qstat(stdout)

#     # filter to running
#     pdf = pdf[pdf.state.isin(running_tags)]

#     counts = pdf.groupby(["user"])["slots"].agg("sum")
#     counts = counts.sort_values()  # type: ignore

#     return counts
import logging
import time
from typing import Any, Dict, Iterator, List

logger = logging.getLogger(__name__)

from hpc_funcs.schedulers.uge.constants import TAGS_PENDING, TAGS_RUNNING
from hpc_funcs.schedulers.uge.qstat import get_qstat, get_qstat_job

# from hpc_funcs.schedulers.uge.qacct import get_qacct


def wait_for_jobs(jobs: List[str], respiratory: int = 60) -> Iterator[str]:
    """ """

    logger.info(f"Waiting for {len(jobs)} job(s) on UGE...")

    start_time = time.time()

    while len(jobs):
        logger.info(
            f"... and breathe for {respiratory} sec, still waiting for {len(jobs)} job(s) to finish..."
        )

        time.sleep(respiratory)

        for job_id in jobs:
            if is_job_done(job_id):
                yield job_id
                jobs.remove(job_id)

    end_time = time.time()
    diff_time = end_time - start_time
    logger.info(f"All jobs finished and took {diff_time/60/60:.2f}h")


def is_job_done(
    job_id: str,
) -> bool:
    """
    If unable to find job info, assume job is done.
    """

    job_info = get_qstat_job(job_id)

    # If there still is some qstat information, the job is not done
    if job_info:
        return False

    # TODO Check qacct -j information

    return True
