import logging
import subprocess

logger = logging.getLogger(__name__)


def delete_job(job_id: str) -> None:
    """Delete a UGE job.

    Args:
        job_id: The job ID to delete.

    Raises:
        RuntimeError: If qdel command fails.
    """

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

    # Check for errors
    if process.returncode != 0:
        raise RuntimeError(f"qdel failed for job {job_id}: {stderr or stdout}")

    # Log output for debugging
    if stderr:
        logger.warning(f"qdel stderr: {stderr}")
    if stdout:
        logger.debug(f"qdel stdout: {stdout}")
