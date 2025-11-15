import logging
import re
import subprocess
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from pandas import DataFrame

from .constants import TAGS_ERROR, TAGS_PENDING, TAGS_RUNNING

logger = logging.getLogger(__name__)

# Joblist columns
COLUMN_JOBID = "job-ID"
COLUMN_PRIORITY = "prior"
COLUMN_NAME = "name"
COLUMN_USER = "user"
COLUMN_STATE = "state"
COLUMN_TIME = "submit/start at"
COLUMN_QUEUE = "queue"
COLUMN_JCLASS = "jclass"
COLUMN_SLOTS = "slots"
COLUMN_ARRAY = "ja-task-ID"

# Ordered qstat text joblist columns
COLUMNS_TEXT = [
    COLUMN_JOBID,
    COLUMN_PRIORITY,
    COLUMN_NAME,
    COLUMN_USER,
    COLUMN_STATE,
    COLUMN_TIME,
    COLUMN_QUEUE,
    COLUMN_JCLASS,
    COLUMN_SLOTS,
    COLUMN_ARRAY,
]

# joblist summary table columns
COLUMN_RUNNING = "running"
COLUMN_PENDING = "pending"
COLUMN_ERROR = "error"

# jobinfo columns
COLUMN_INFO_JOBID = "job_number"
COLUMN_INFO_USER = "owner"
COLUMN_INFO_ARRAY = "job-array tasks"
COLUMN_INFO_CONCURRENCY = "task_concurrency"
COLUMN_INFO_NAME = "job_name"


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
    process = subprocess.run(
        cmd,
        encoding="utf-8",
        capture_output=True,
        shell=True,
    )

    stdout = process.stdout
    stderr = process.stderr

    if stderr:
        logger.warning(f"qstat stderr: {stderr}")

    # Convert to DataFrame
    df = parse_joblist_text(stdout)

    return df


def get_qstat_job_text(
    job_id: Union[str, int],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Get detailed information for a specific job using qstat -j -xml.

    This returns comprehensive information about a single job, including
    resource requests, submission details, and task status in XML format,
    which is then parsed into a Python dictionary.

    Args:
        job_id: The job ID to query.

    Returns:
        Tuple containing:
            - List of dictionaries with detailed job information (typically one job)
            - List of error messages (if any)

    Examples:
        >>> # Get detailed info for a specific job
        >>> jobs, errors = get_qstat_job_xml(12345)
        >>> if jobs:
        ...     job = jobs[0]
        ...     print(job["JB_job_name"])
        ...     print(job["JB_owner"])
    """

    cmd = f"qstat -j {job_id} -nenv"

    logger.debug(f"Executing: {cmd}")

    process = subprocess.run(
        cmd,
        encoding="utf-8",
        capture_output=True,
        shell=True,
    )

    stdout = process.stdout
    stderr = process.stderr

    if stderr:
        logger.warning(f"qstat stderr: {stderr}")

    # Parse error lines that might appear before XML
    lines = stdout.splitlines()
    errors = []
    xml_lines = []

    for line in lines:
        if line.startswith("error reason"):
            errors.append(line)
        else:
            xml_lines.append(line)

    del lines
    stdout = "\n".join(xml_lines)

    # Parse the XML
    jobs = parse_jobinfo_text(stdout)

    return jobs, errors


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
    # use the header line to identify where each column starts

    # Find column start positions based on header
    column_positions = {column_name: header_line.find(column_name) for column_name in COLUMNS_TEXT}

    missing = [c for c, pos in column_positions.items() if pos == -1]
    if missing:
        raise KeyError(f"Missing expected header(s): {missing}")

    # Ensure columns are processed in left-to-right order in the header
    ordered_cols = sorted(COLUMNS_TEXT, key=lambda c: column_positions[c])

    # Process each data line
    for line in lines[2:]:
        if not line.strip():
            continue

        # Extract fields based on column positions
        # We'll use the positions to slice the line
        job = {}

        for i, col in enumerate(ordered_cols):

            start = column_positions[col]
            end = column_positions[ordered_cols[i + 1]] if i + 1 < len(ordered_cols) else None
            value = line[start:end].strip()

            job[col] = value

        jobs.append(job)

    return pd.DataFrame(jobs)


def parse_jobinfo_text(stdout: str) -> List[Dict[str, str]]:
    """
    Output is column-length based and sections split by "=".
    Returns list key-value dict per section.
    """

    COL_VALUE_START = 28

    output: List[Dict[str, str]] = [dict()]

    lines = stdout.split("\n")

    for line in lines:
        if "=" * 5 in line:
            if len(output[-1]) > 1:
                output += [dict()]
            continue

        # Format: pe_taskid     NONE
        key = line[:COL_VALUE_START].strip()
        value = line[COL_VALUE_START:].strip()

        if key.endswith(":"):
            key = key[:-1]

        if len(key) == 0:
            continue

        key = key.strip()
        value = value.strip()

        output[-1][key] = value

    return output


def parse_qstat_text(stdout: str) -> pd.DataFrame:
    stdout = stdout.strip()
    lines = stdout.split("\n")

    header = lines[0].split()
    header.remove("at")
    header_indicies = []

    rows = []

    for line in header[1:]:
        idx = lines[0].index(line)
        header_indicies.append(idx)

    def split_qstat_line(line):
        idx = 0

        for ind in header_indicies:
            yield line[idx:ind].strip()
            idx = ind

        yield line[idx:]

    for line in lines[2:]:
        if not line.strip():
            continue

        line_ = split_qstat_line(line)
        line_ = list(line_)

        row = {key: value for key, value in zip(header, line_)}
        rows.append(row)

    pdf = pd.DataFrame(rows)
    pdf["slots"] = pdf["slots"].astype(int)

    return pdf


def parse_taskarray(pdf: DataFrame) -> pd.DataFrame:

    # for unique job-ids
    job_ids = pdf[COLUMN_JOBID].unique()

    def _parse(line):
        count = 0

        lines = line.split(",")
        for task in lines:
            if "-" not in task:
                count += 1
                continue

            start, stop, _ = re.split(",|:|-|!", task)
            count += int(stop) - int(start)

        return count

    rows = []

    for job_id in job_ids:
        jobs = pdf[pdf[COLUMN_JOBID] == job_id]

        pending_jobs = jobs[jobs[COLUMN_STATE].isin(TAGS_PENDING)]
        running_jobs = jobs[jobs[COLUMN_STATE].isin(TAGS_RUNNING)]
        error_jobs = jobs[jobs[COLUMN_STATE].isin(TAGS_ERROR)]
        # deleted_jobs = jobs[jobs[COLUMN_STATE].isin(deleted_tags)]

        pending_count = pending_jobs[COLUMN_ARRAY].apply(_parse)
        error_count = error_jobs[COLUMN_ARRAY].apply(_parse)

        n_running = len(running_jobs)
        n_pending = pending_count.values.sum()
        n_error = error_count.values.sum()

        row = {
            COLUMN_JOBID: job_id,
            COLUMN_RUNNING: n_running,
            COLUMN_PENDING: n_pending,
            COLUMN_ERROR: n_error,
        }

        rows.append(row)

    return pd.DataFrame(rows)
