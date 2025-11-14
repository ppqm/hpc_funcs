import logging
from typing import Dict, List, Optional

import pandas as pd

from hpc_funcs.shell import execute

logger = logging.getLogger(__name__)


def get_qstat_text(
    users: Optional[List[str]] = None,
    queues: Optional[List[str]] = None,
    resource_filter: Optional[str] = None,
) -> pd.DataFrame:
    """Get job status information from UGE using qstat -json.

    Args:
        users: List of usernames to filter jobs by. If None, shows your jobs.
        queues: List of queue names to filter by.
        resource_filter: Resource filter string in format "attr=val,..." (e.g., "arch=lx-amd64").

    Returns:
        DataFrame with job information. Each row represents a job (or task) with columns for
        job properties, state, owner, queue, slots, etc.

    Raises:
        json.JSONDecodeError: If the JSON output from qstat is malformed.

    Examples:
        >>> # Get all jobs for current user
        >>> df = get_qstat(users=["testuser1"])

        >>> # Get specific jobs
        >>> df = get_qstat(job_ids=[12345, 12346])

        >>> # Get jobs in specific queues
        >>> df = get_qstat(queues=["gpu.q", "default.q"])

        >>> # Get all running and pending jobs with full details
        >>> df = get_qstat(show_full=True)
    """

    cmd = "qstat"

    if users is not None and len(users):
        user_list = ",".join(users)
        cmd += f" -u {user_list}"

    if queues:
        queue_list = ",".join(queues)
        cmd += f" -q {queue_list}"

    if resource_filter:
        cmd += f" -l {resource_filter}"

    # Execute command
    logger.debug(f"Executing: {cmd}")
    stdout, stderr = execute(cmd)

    if stderr:
        logger.warning(f"qstat stderr: {stderr}")

    # Convert to DataFrame
    df = parse_joblist_text(stdout)

    return df


def parse_joblist_text(stdout: str) -> pd.DataFrame:
    """
    Parse UGE qstat text output (list format).

    Args:
        stdout: String output from qstat command

    Returns:
        List of dictionaries containing job information
    """
    jobs = []
    lines = stdout.strip().split("\n")

    if len(lines) < 3:
        return pd.DataFrame([])

    # First line is the header with column names
    header_line = lines[0]
    # Second line is the separator (dashes)
    # Data starts from third line

    # Parse the header to find column positions
    # The columns are: job-ID, prior, name, user, state, submit/start at, queue, jclass, slots, ja-task-ID
    # We'll use the header line to identify where each column starts

    # Find column start positions based on header
    col_positions = {
        "job-ID": header_line.find("job-ID"),
        "prior": header_line.find("prior"),
        "name": header_line.find("name"),
        "user": header_line.find("user"),
        "state": header_line.find("state"),
        "submit/start at": header_line.find("submit/start at"),
        "queue": header_line.find("queue"),
        "jclass": header_line.find("jclass"),
        "slots": header_line.find("slots"),
        "ja-task-ID": header_line.find("ja-task-ID"),
    }

    # TODO assert check if all headers are there

    # Process each data line
    for line in lines[2:]:
        if not line.strip():
            continue

        # Extract fields based on column positions
        # We'll use the positions to slice the line
        job = {}

        # job-ID: from start to prior column
        job_id_end = col_positions["prior"]
        job["job_number"] = line[col_positions["job-ID"] : job_id_end].strip()

        # prior: from prior to name column
        prior_end = col_positions["name"]
        job["priority"] = line[col_positions["prior"] : prior_end].strip()

        # name: from name to user column
        name_end = col_positions["user"]
        job["name"] = line[col_positions["name"] : name_end].strip()

        # user: from user to state column
        user_end = col_positions["state"]
        job["owner"] = line[col_positions["user"] : user_end].strip()

        # state: from state to submit/start at column
        state_end = col_positions["submit/start at"]
        job["state"] = line[col_positions["state"] : state_end].strip()

        # submit/start at: from submit/start at to queue column
        datetime_end = col_positions["queue"]
        job["submission_time"] = line[col_positions["submit/start at"] : datetime_end].strip()

        # queue: from queue to jclass column
        queue_end = col_positions["jclass"]
        job["queue"] = line[col_positions["queue"] : queue_end].strip()

        # jclass: from jclass to slots column
        jclass_end = col_positions["slots"]
        jclass_val = line[col_positions["jclass"] : jclass_end].strip()
        if jclass_val:
            job["jclass"] = jclass_val

        # slots: from slots to ja-task-ID column
        slots_end = col_positions["ja-task-ID"]
        job["slots"] = line[col_positions["slots"] : slots_end].strip()

        # ja-task-ID: from ja-task-ID to end of line
        ja_task_id = line[col_positions["ja-task-ID"] :].strip()

        if ja_task_id:
            job["ja_task_id"] = ja_task_id

        jobs.append(job)

    return pd.DataFrame(jobs)


def parse_text_jobinfo(stdout: str) -> List[Dict[str, str]]:
    """
    Output is column-length based and sections split by "=".
    Returns list key-value dict per section.
    """

    output: List[Dict[str, str]] = [dict()]

    lines = stdout.split("\n")

    for line in lines:
        if "===========" in line:
            if len(output[-1]) > 1:
                output += [dict()]
            continue

        # Format: pe_taskid     NONE
        key = line[:COL_SPLIT].strip()
        value = line[COL_SPLIT:].strip()

        if len(key) == 0:
            continue

        key = key.strip()
        value = value.strip()

        output[-1][key] = value

    return output
