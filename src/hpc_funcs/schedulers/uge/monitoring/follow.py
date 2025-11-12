import logging
from pandas import DataFrame
import time
import datetime
import pandas as pd
from typing import Dict, List, Union, Tuple, Optional
import tqdm

from ..constants import COLUMN_JOB, COLUMN_JOB_ID, COLUMN_SUBMISSION_TIME, COLUMN_TASKARRAY
from ..qstat import get_all_jobs, get_qstat_job_json

DATE_FORMAT = "%m/%d/%Y %H:%M:%S"
TQDM_LENGTH = 95
TQDM_OPTIONS = {
    "ncols": TQDM_LENGTH,
}

logger = logging.getLogger(__name__)


def get_time_from_ugestr(time_str: str):
    _time = ".".join(time_str.split(".")[:-1])
    time_ = datetime.datetime.strptime(_time, DATE_FORMAT)
    return time.mktime(time_.timetuple())


class TaskarrayProgress:
    def __init__(
        self,
        job_id: str,
        position: int = 0,
    ) -> None:
        self.position = position
        self.job_id = str(job_id)

        # Get info

        all_stats = get_all_jobs()
        job_info, job_errors = get_qstat_job_json(job_id)

        # TODO Check if qstat job actually gives the content needed

        print(all_stats)

        # Only about this job
        _job_pdf = all_stats[all_stats[COLUMN_JOB_ID] == self.job_id]
        job_status = dict(_job_pdf.iloc[0])
        self.init_bar(job_info, job_status)

    def init_bar(self, job_info: dict, job_status: dict) -> None:

        # Job Info
        n_pending = job_info.get("pending_tasks", 0)
        n_running = len(job_info.get("job_array_tasks", []))
        n_concurrent = job_info.get("task_concurrency", 1)

        time_start = job_info.get(COLUMN_SUBMISSION_TIME)
        assert time_start is not None
        time_start = get_time_from_ugestr(time_start)

        # From overview
        job_id = job_info[COLUMN_JOB_ID]
        # job_name = job_info["job_name"]

        # Count total tasks
        n_total_ = array_info.split(":")[0].split("-")[-1]
        self.n_total = int(n_total_)

        # Set title
        self.title = job_id

        self.pbar = tqdm.tqdm(
            total=self.n_total,
            desc=f"{self.title}",
            position=self.position,
            ncols=TQDM_LENGTH,
        )

        # Reset time
        self.pbar.last_print_t = self.pbar.start_t = start_time__

        # Set finished and running
        self.update(job_status)

    def update(self, status: dict) -> None:

        n_running = status.get("running", 0)
        n_pending = status.get("pending", 0)
        n_error = status.get("error", 0)
        n_finished = self.n_total - n_pending - n_running

        postfix = dict()

        if n_error > 0:
            postfix["err"] = n_error

        self.pbar.set_postfix(postfix)

        self.pbar.set_description(f"{self.title} ({n_running})", refresh=False)
        self.pbar.n = n_finished
        self.pbar.refresh()

    def finish(self) -> None:
        n_total = self.n_total
        self.pbar.set_postfix({})
        self.pbar.set_description(f"{self.title} (0)", refresh=False)
        self.pbar.n = n_total
        self.pbar.refresh()

    def log_errors(self) -> None:

        qstatj, _ = get_qstatj(self.job_id)
        errors = _get_errors_from_qstatj(qstatj)

        for error in errors:
            logger.error(f"uge {self.job_id}: {error.strip()}")

    def is_finished(self) -> bool:
        return self.pbar.n >= self.n_total

    def close(self) -> None:
        self.pbar.close()
