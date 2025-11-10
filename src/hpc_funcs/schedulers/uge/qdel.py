def delete_job(job_id: Optional[str]) -> None:

    cmd = f"qdel {job_id}"
    logger.debug(cmd)

    stdout, stderr = execute(cmd)
    stdout = stdout.strip()
    stderr = stderr.strip()

    for line in stderr.split("\n"):
        logger.error(line)

    for line in stdout.split("\n"):
        logger.error(line)
