import logging
import subprocess
from pathlib import Path
from typing import Optional, Tuple, Union

from hpc_funcs.files import generate_name

logger = logging.getLogger(__name__)

# TODO Support for sync -y


# pylint: disable=dangerous-default-value
def submit_script(
    bash_script: str,
    scr: Optional[Union[str, Path]] = None,
) -> Tuple[Optional[str], Optional[Path]]:
    """Submit script and return UGE Job ID

    return:
        job_id - The UGE Job ID
        script_path - The path to the generated script
    """

    cmd = "qsub"

    filename = f"tmp_uge.{generate_name()}.sh"

    if scr is None:
        scr = "./"

    scr = Path(scr)
    scr.mkdir(parents=True, exist_ok=True)

    assert scr.is_dir()

    with open(scr / filename, "w") as f:
        f.write(bash_script)

    logger.debug(f"Writing {filename} for UGE on {scr}")

    cmd = f"{cmd} {filename}"
    logger.debug(cmd)
    logger.debug(scr)

    process = subprocess.run(
        cmd,
        cwd=scr,
        encoding="utf-8",
        capture_output=True,
        shell=True,
    )

    stdout = process.stdout
    stderr = process.stderr

    if stderr:
        for line in stderr.split("\n"):
            logger.error(line)
        return None, scr / filename

    if not stdout:
        logger.error("Unable to fetch qsub job id from stdout")
        return None, scr / filename

    # Successful submission
    # find id
    logger.info(f"submit stdout: {stdout.strip().rstrip()}")

    #
    # Your job JOB_ID ("JOB_NAME") has been submitted
    uge_id = stdout.strip().rstrip().split("\n")[-1]
    if "has been submitted" not in uge_id:
        raise RuntimeError(f"Could not find UGE Job ID in: '{uge_id}'")
    uge_id = uge_id.split()[2]
    uge_id = uge_id.split(".")[0]

    # Test format of job_id
    try:
        int(uge_id)
    except ValueError:
        raise ValueError("UGE Job ID is not correct format")

    logger.info(f"got job_id: {uge_id}")

    return uge_id, scr / filename
