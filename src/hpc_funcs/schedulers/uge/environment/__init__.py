import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

from hpc_funcs.shell import execute

from ..constants import UGE_ENVIRONMENT_VARIABLES

COMMAND_SUBMIT = "qsub"


def has_uge() -> bool:
    """Check if cluster has UGE setup"""

    cmd = shutil.which(COMMAND_SUBMIT)

    if cmd is not None:
        return True

    return False


def is_job() -> bool:
    """Check if runtime is a UGE queue environment"""

    name = os.getenv("SGE_TASK_ID")

    if name is None:
        return False

    return True


def get_env() -> Dict[str, Optional[str]]:
    """
    Get all UGE related environmental variables.

    important keywords are
        NSLOTS - Number of cores in current job
        TMPDIR - Node specific tmpdir

    """

    properties = {}

    for key in UGE_ENVIRONMENT_VARIABLES:
        properties[key] = os.getenv(key)

    return properties


def get_tmpdir() -> Path:
    """From UGE environment, get scratch directory"""

    tmpdir = os.getenv("TMPDIR")
    assert tmpdir is not None

    path = Path(tmpdir)
    assert path.is_dir()

    return path


def get_config() -> Dict[str, Any]:
    """Get UGE configuration

    - Number of cores avaiable on node
    - Scratch directory on node
    - Hostname of current node
    """

    n_cores = os.getenv("NSLOTS")
    scr = os.getenv("TMPDIR")
    hostname = os.getenv("HOSTNAME")

    assert n_cores is not None
    assert scr is not None
    assert hostname is not None

    config = {
        "n_cores": int(n_cores),
        "scr": scr,
        "hostname": hostname,
    }

    return config


def get_cores() -> int:
    """Get avaiable cores in current environment"""

    key = "NSLOTS"
    n_cores = os.getenv(key)
    assert n_cores is not None

    n_cores_ = int(n_cores)

    return n_cores_


def is_interactive():
    """Check if job is run via interactive shell (e.i. qrsh), or submission"""

    uge_type = os.getenv("REQUEST", None)

    # Not UGE
    if not uge_type:
        return False

    # if request is qrlogin, then qrsh was used
    if uge_type == "QRLOGIN":
        return True

    return False


def source(bashfile):
    """
    Return resulting environment variables from sourceing a bashfile

    usage:
        env_dict = source("/path/to/aws_cred_ang")
        os.environ.update(env_dict)

    :returns: dict of variables
    """

    cmd = f'env -i sh -c "source {bashfile} && env"'
    stdout, _ = execute(cmd)
    lines = stdout.split("\n")

    variables = dict()

    for line in lines:

        line = line.split("=")

        # Ignore wrong lines
        # - empty
        # - multiple =
        if len(line) != 2:
            continue

        key, var = line

        if key == "PWD":
            continue

        if key == "_":
            continue

        if key == "SHLVL":
            continue

        variables[key] = var

    return variables
