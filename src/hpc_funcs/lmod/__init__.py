import logging
import os
import re
import subprocess
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from hpc_funcs.shell import which

logger = logging.getLogger("lmod")


@lru_cache(maxsize=None)
def get_lmod_executable() -> Optional[Path]:
    _dir = os.environ.get("LMOD_DIR", None)
    dir = Path(_dir) if _dir is not None else None
    exe = dir / "lmod" if dir is not None else None

    # TODO Raise exception if cannot find executable

    if exe is None:
        logger.error("Could not find LMOD executable in environment")
        return None

    if not which(exe):
        logger.error(f"Could not find {exe} executable in environment")
        return None

    return exe


# pylint: disable=too-many-locals
def module(
    command: str, arguments: str, cmd: Optional[Path] = get_lmod_executable()
) -> Tuple[Dict[str, str], Optional[str]]:
    """Use lmod to execute enviromental changes

    returns dictionary of LMOD returns, stderr
    """

    logger.info(f"module {command} {arguments}")

    execution: Any = [cmd, "python", command, arguments]

    logger.debug(execution)

    with subprocess.Popen(
        execution,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) as popen:

        bstdout, bstderr = popen.communicate()

        stdout = bstdout.decode("utf-8")
        stderr = bstderr.decode("utf-8")

    if "error" in stderr:
        assert False, stderr

    # pylint: disable=too-many-return-statements
    def _filter(line: str) -> bool:
        """
        I just want to remove the noise, so I can see what changes
        """

        if "import" in line:
            return False

        if "os.environ" not in line:
            return False

        if "__LM" in line:
            return False

        if "__LMFILES__" in line:
            return False

        if "_LMFILES_" in line:
            return False

        if "_ModuleTable" in line:
            return False

        return True

    def _split_line(line: str) -> Tuple[str, str]:

        # format:
        # os.environ["key"] = "value:value"

        _line = line.split("=")

        value = _line[-1]
        value = value.strip()

        if value[-1] == ";":
            value = value[:-1]

        value = value[1:-1]

        key = _line[0]
        key = key.strip()
        key = key.replace("os.environ", "")
        key = key[2:-2]

        return key, value

    # Filter some of the lines
    lines = stdout.split("\n")
    lines = [line for line in lines if _filter(line)]
    keyvalues = [_split_line(line) for line in lines]

    environment_update = dict(keyvalues)

    return environment_update, stderr


def update_environment(update_dict: Dict[str, str]) -> None:

    pythonpath = update_dict.get("PYTHONPATH", None)

    os.environ.update(update_dict)

    for key, value in update_dict.items():
        logger.debug(f"{key} = {value}")

    if pythonpath is None:
        return

    pythonpaths = pythonpath.split(":")

    for path in pythonpaths:
        if path in sys.path:
            continue
        sys.path.append(path)

    return


def purge() -> None:
    """Warning: This will break stuff"""
    raise NotImplementedError
    module("purge", "")


def load(module_name: str) -> None:
    """use `module load` to overload your environment"""
    update_dict, _ = module("load", module_name)
    update_environment(update_dict)


def get_load_environment(module_name: str) -> Dict[str, str]:
    """use `module load` to overload your environment"""
    update_dict, _ = module("load", module_name)
    return update_dict


def use(path: Optional[Path]) -> None:
    """Use path in MODULEPATH"""
    update_dict, _ = module("use", str(path))
    update_environment(update_dict)


def get_modules() -> Dict[int, str]:
    """Return all active LMOD modules.

    Hidden modules are ignored.

    returns:
        dict[number, modulename/version]

    """

    _, stderr = module("list", "")

    assert stderr is not None

    lines = stderr.split("\n")

    def _filter(line: str):
        # Format: 1) name/version     10) name/version

        if not len(line.strip()):
            return False

        if line[0] != " ":
            return False

        if ")" not in line:
            return False

        return True

    # Filter to only lines with modules
    lines = [line for line in lines if _filter(line)]

    modules = dict()
    for line in lines:

        # Standardize the line
        line = " ".join(line.strip().split())

        pattern = r"(\d+\))"
        mods = re.split(pattern, line)
        mods = [x.strip() for x in mods if len(x)]

        # The delimiters are kept, so select every second
        for key, mod in zip(mods[::2], mods[1::2]):

            if "(H)" in mod:
                continue

            key = key.replace(")", "")
            _key = int(key)
            modules[_key] = mod

    return modules


def get_paths() -> List[str]:
    """Return all LMOD paths in use"""
    paths = os.environ.get("MODULEPATH", "")
    paths_ = paths.split(":")
    return paths_
