import logging
import subprocess
from typing import Dict, List

logger = logging.getLogger(__name__)

COL_SPLIT = 13


def get_job_accounting(job_id: str) -> List[Dict[str, str]]:

    cmd = f"qacct -j {job_id}"

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

    data = parse_qacct(stdout)

    return data


def parse_qacct(stdout: str) -> List[Dict[str, str]]:
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
