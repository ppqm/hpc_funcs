import datetime
import logging
import time
from typing import Dict

import tqdm
from pandas import DataFrame

from ..qstat import get_all_jobs_text
from ..qstat_xml import get_qstat_job_xml

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

    @staticmethod
    def by_jobid(job_id: str, position: int = 0, file=None):

        job_id = str(job_id)
        job_infos, job_errors = get_qstat_job_xml(job_id)
        assert len(job_infos)
        job_info = job_infos[0]

        return TaskarrayProgress(job_info, position=position, file=file)

    def __init__(
        self,
        job_info: Dict,
        position: int = 0,
        file=None,
    ) -> None:
        self.position = position
        self.file = file
        self.job_id: str
        self.init_bar(job_info, {})

    def init_bar(self, job_info: dict, job_status: dict) -> None:

        job_id = job_info.get("JB_job_number")
        assert job_id is not None
        self.job_id = job_id

        timestamp = job_info.get(
            "JB_submission_time"
        )  # in UNIX timestamp, but with too many digits
        assert timestamp is not None
        time_start = datetime.datetime.fromtimestamp(int(timestamp) / 1000)
        time_start = time.mktime(time_start.timetuple())

        print("time start", time_start)

        job_array_info = job_info.get("JB_ja_structure", [])
        assert len(job_array_info)

        job_array_start = int(job_array_info[0].get("RN_min", 1))
        job_array_stop = int(job_array_info[0].get("RN_max", 1))
        job_array_concurrent = int(job_array_info[0].get("RN_step", 1))

        self.n_total = job_array_stop

        # Set title
        self.title = job_id

        self.pbar = tqdm.tqdm(
            total=self.n_total,
            desc=f"{self.title}",
            position=self.position,
            unit="task",
            ncols=TQDM_LENGTH,
            file=self.file,
        )

        # Reset time
        self.pbar.last_print_t = self.pbar.start_t = time_start

    def update(self, joblist: None | DataFrame = None) -> None:

        if joblist is None:
            joblist = get_all_jobs_text()

        # Get status
        assert joblist is not None
        joblist = joblist[joblist["job_number"] == self.job_id]

        print(joblist)

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

    def is_finished(self) -> bool:
        return self.pbar.n >= self.n_total

    def close(self) -> None:
        self.pbar.close()
