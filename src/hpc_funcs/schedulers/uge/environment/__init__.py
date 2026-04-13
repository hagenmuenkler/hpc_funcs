import os
import shutil
from pathlib import Path
from typing import Any, Optional

from hpc_funcs.shell import execute

from ..constants import UGE_ENVIRONMENT_VARIABLES

COMMAND_SUBMIT = "qsub"


def has_uge() -> bool:
    """Check if cluster has UGE setup"""

    cmd = shutil.which(COMMAND_SUBMIT)

    return cmd is not None


def is_job() -> bool:
    """Check if runtime is a UGE queue environment"""

    name = os.getenv("SGE_TASK_ID")

    return name is not None


def get_env() -> dict[str, str | None]:
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
    """From UGE environment, get scratch directory.

    Raises:
        RuntimeError: If not running in a UGE job environment.
        ValueError: If TMPDIR is not a valid directory.
    """

    tmpdir = os.getenv("TMPDIR")
    if tmpdir is None:
        raise RuntimeError("TMPDIR not set - not running in UGE job environment")

    path = Path(tmpdir)
    if not path.is_dir():
        raise ValueError(f"TMPDIR is not a directory: {tmpdir}")

    return path


def get_config() -> dict[str, Any]:
    """Get UGE configuration

    - Number of cores available on node
    - Scratch directory on node
    - Hostname of current node

    Raises:
        RuntimeError: If required UGE environment variables are not set.
    """

    n_cores = os.getenv("NSLOTS")
    scr = os.getenv("TMPDIR")
    hostname = os.getenv("HOSTNAME")

    if n_cores is None:
        raise RuntimeError("NSLOTS not set - not running in UGE job environment")
    if scr is None:
        raise RuntimeError("TMPDIR not set - not running in UGE job environment")
    if hostname is None:
        raise RuntimeError("HOSTNAME not set - not running in UGE job environment")

    config = {
        "n_cores": int(n_cores),
        "scr": scr,
        "hostname": hostname,
    }

    return config


def get_cores() -> int:
    """Get available cores in current environment.

    Raises:
        RuntimeError: If NSLOTS is not set.
    """

    key = "NSLOTS"
    n_cores = os.getenv(key)
    if n_cores is None:
        raise RuntimeError("NSLOTS not set - not running in UGE job environment")

    n_cores_ = int(n_cores)

    return n_cores_


def is_interactive():
    """Check if job is run via interactive shell (e.i. qrsh), or submission"""

    uge_type = os.getenv("REQUEST", None)

    # Not UGE
    if not uge_type:
        return False

    # if request is qrlogin, then qrsh was used
    return uge_type == "QRLOGIN"


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

    variables = {}

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
