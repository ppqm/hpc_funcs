import logging
import subprocess
from typing import Optional

logger = logging.getLogger(__name__)


def delete_job(job_id: Optional[str]) -> None:

    cmd = f"qdel {job_id}"
    logger.debug(cmd)

    process = subprocess.run(
        cmd,
        encoding="utf-8",
        capture_output=True,
        shell=True,
    )

    stdout = process.stdout.strip()
    stderr = process.stderr.strip()

    for line in stderr.split("\n"):
        logger.error(line)

    for line in stdout.split("\n"):
        logger.error(line)
