import json
import logging
import subprocess
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

from hpc_funcs.shell import execute

from .constants import TAGS_PENDING, TAGS_RUNNING

COL_SPLIT = 5

logger = logging.getLogger(__name__)


def get_qstat_args():

    return


def get_qstat_json(
    users: Optional[List[str]] = None,
    job_ids: Optional[List[Union[str, int]]] = None,
    queues: Optional[List[str]] = None,
    resource_filter: Optional[str] = None,
) -> pd.DataFrame:
    """Get job status information from UGE using qstat -json.

    Args:
        users: List of usernames to filter jobs by. If None, shows your jobs.
        job_ids: List of specific job IDs to query. If None, queries all jobs.
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

    cmd = "qstat -json"

    if users is not None and len(users):
        user_list = ",".join(users)
        cmd += f" -u {user_list}"

    if job_ids:
        job_id_list = ",".join(str(jid) for jid in job_ids)
        cmd += f" -j {job_id_list}"

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
    df = parse_joblist_json(stdout)

    return df


def get_qstat_job_json(
    job_id: Union[str, int],
) -> Tuple[Dict[str, Any], list[str]]:
    """Get detailed information for a specific job using qstat -j -json.

    This returns comprehensive information about a single job, including
    resource requests, environment, submission details, and task status.

    Args:
        job_id: The job ID to query.
        max_retries: Maximum number of retries for the qstat command.
        update_interval: Seconds to wait between retries.

    Returns:
        Dictionary containing detailed job information from qstat -j.

    Raises:
        json.JSONDecodeError: If the JSON output from qstat is malformed.

    Examples:
        >>> # Get detailed info for a specific job
        >>> job_info = get_qstat_job(12345)
        >>> print(job_info["job_name"])
        >>> print(job_info["slots"])
        >>> print(job_info["state"])
    """

    cmd = f"qstat -j {job_id} -json -nenv"

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

    # Find error lines

    lines = stdout.splitlines()
    if len(lines) == 1:
        # Valid json
        data = json.loads(stdout)
        if "job_info" in data:
            return data["job_info"][0], []
        return {}, []

    # If there are errors on job submission, uge will return the errors as lines before json output
    # error reason   1: this is the reason it failed
    # We need to filter them out to read the json

    errors = []
    stdout_lines = []
    # Filter error lines to errors, and json lines to stdout_lines
    for line in lines:
        (errors if line.startswith("error reason") else stdout_lines).append(line)

    del lines

    stdout = "\n".join(stdout_lines)
    data = json.loads(stdout)

    del stdout_lines
    del stdout

    # If no jobs found, qstat returns
    # {"unknown jobs":["29878954"]}
    # We should return empty dict, but use logger to warn the job doesn't exist

    # Extract job info (should be first item in job_info list)
    if "job_info" in data and len(data["job_info"]) > 0:
        return data["job_info"][0], errors

    return {}, errors


def get_qstat_text(
    job_id: Union[str, int],
) -> Tuple[Dict[str, Any], list[str]]:
    """Get detailed information for a specific job using qstat -j.

    This returns comprehensive information about a single job, including
    resource requests, environment, submission details, and task status.

    Args:
        job_id: The job ID to query.
        max_retries: Maximum number of retries for the qstat command.
        update_interval: Seconds to wait between retries.

    Returns:
        Dictionary containing detailed job information from qstat -j.

    Raises:
        json.JSONDecodeError: If the JSON output from qstat is malformed.

    Examples:
        >>> # Get detailed info for a specific job
        >>> job_info = get_qstat_job(12345)
        >>> print(job_info["job_name"])
        >>> print(job_info["slots"])
        >>> print(job_info["state"])
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

    # Find error lines

    lines = stdout.splitlines()
    if len(lines) == 1:
        # Valid json
        data = json.loads(stdout)
        if "job_info" in data:
            return data["job_info"][0], []
        return {}, []

    # If there are errors on job submission, uge will return the errors as lines before json output
    # error reason   1: this is the reason it failed
    # We need to filter them out to read the json

    errors = []
    stdout_lines = []
    # Filter error lines to errors, and json lines to stdout_lines
    for line in lines:
        (errors if line.startswith("error reason") else stdout_lines).append(line)

    del lines

    stdout = "\n".join(stdout_lines)
    data = json.loads(stdout)

    del stdout_lines
    del stdout

    # If no jobs found, qstat returns
    # {"unknown jobs":["29878954"]}
    # We should return empty dict, but use logger to warn the job doesn't exist

    # Extract job info (should be first item in job_info list)
    if "job_info" in data and len(data["job_info"]) > 0:
        return data["job_info"][0], errors

    return {}, errors



def parse_joblist_json(stdout) -> pd.DataFrame:
    """Parse qstat JSON output into a pandas DataFrame.

    Args:
        data: Raw JSON from qstat -json.

    Returns:
        DataFrame with one row per job/task, columns for job properties.
    """

    data = json.loads(stdout)

    rows = []

    # Parse running jobs from queue_info
    if "queue_info" in data:
        for queue_section in data["queue_info"]:
            if "running jobs" not in queue_section:
                continue
            for job in queue_section["running jobs"]:
                row = _extract_job_row(job, job_type="running")
                rows.append(row)

    # Parse pending jobs from job_info
    if "job_info" in data:
        for job_section in data["job_info"]:
            if "pending jobs" not in job_section:
                continue
            for job in job_section["pending jobs"]:
                row = _extract_job_row(job, job_type="pending")
                rows.append(row)

    if not rows:
        logger.debug("No jobs found in qstat output")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    return df


def _extract_job_row(job: Dict[str, Any], job_type: str) -> Dict[str, Any]:
    """Extract relevant fields from a job dictionary.

    Args:
        job: Dictionary containing job information.
        job_type: Type of job ("running" or "pending").

    Returns:
        Dictionary with standardized job fields.
    """
    row = {
        "job_number": job.get("JB_job_number", ""),
        "priority": job.get("JAT_prio", 0.0),
        "name": job.get("JB_name", ""),
        "owner": job.get("JB_owner", ""),
        "state": job.get("state", ""),
        "slots": job.get("slots", 0),
        "queue_name": job.get("queue_name", ""),
        # "jclass_name": job.get("jclass_name", ""),
        "job_type": job_type,
    }

    # Add start time for running jobs
    if "JAT_start_time" in job:
        row["start_time"] = job["JAT_start_time"]

    # Add submission time for pending jobs
    if "JB_submission_time" in job:
        row["submission_time"] = job["JB_submission_time"]

    # Add task ID if present (for array jobs)
    if "JAT_task_number" in job:
        row["task_id"] = job["JAT_task_number"]

    return row


def get_all_jobs():
    """Get all jobs for all users"""
    all_users = "\\*"
    all_users = '"*"'
    df = get_qstat_json(users=[all_users])
    return df


def get_running_jobs(
    users: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Get only running jobs.

    Convenience function to filter for running jobs only.

    Args:
        users: List of usernames to filter jobs by.
        max_retries: Maximum number of retries for the qstat command.
        update_interval: Seconds to wait between retries.

    Returns:
        DataFrame with running jobs only.

    Examples:
        >>> # Get all running jobs for a user
        >>> df = get_running_jobs(users=["testuser1"])
    """

    df = get_qstat_json(users=users)

    if df.empty:
        return df

    # Filter to running state
    df_running = df[df["state"].isin(TAGS_RUNNING)]

    return df_running


def get_pending_jobs(
    users: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Get only pending jobs.

    Convenience function to filter for pending jobs only.

    Args:
        users: List of usernames to filter jobs by.
        max_retries: Maximum number of retries for the qstat command.
        update_interval: Seconds to wait between retries.

    Returns:
        DataFrame with pending jobs only.

    Examples:
        >>> # Get all pending jobs for a user
        >>> df = get_pending_jobs(users=["testuser1"])
    """
    df = get_qstat_json(users=users)

    if df.empty:
        return df

    # Filter to pending states
    df_pending = df[df["state"].isin(TAGS_PENDING)]

    return df_pending



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

