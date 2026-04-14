import logging
import subprocess
from pathlib import Path

from hpc_funcs.files import generate_name

logger = logging.getLogger(__name__)

# TODO Support for sync -y


def write_script(
    content: str,
    directory: Path | str | None = None,
    filename: str | None = None,
) -> Path:
    """Write a bash script to disk for later submission.

    Args:
        content: The bash script content to write.
        directory: Directory to write the script to. Defaults to current directory.
        filename: Optional filename. If not provided, generates a unique name.

    Returns:
        Path to the written script file.

    Raises:
        ValueError: If directory exists but is not a directory.
    """
    directory = Path("./") if directory is None else Path(directory)

    directory.mkdir(parents=True, exist_ok=True)

    if not directory.is_dir():
        raise ValueError(f"Script directory is not a directory: {directory}")

    if filename is None:
        filename = f"tmp_uge.{generate_name()}.sh"

    script_path = directory / filename

    with open(script_path, "w") as f:
        f.write(content)

    logger.debug(f"Wrote script to {script_path}")

    return script_path


def submit_script(script_path: Path | str) -> str:
    """Submit a script to UGE and return the job ID.

    Args:
        script_path: Path to the script file to submit.

    Returns:
        The UGE job ID as a string.

    Raises:
        RuntimeError: If the qsub command fails or job ID cannot be parsed.
        FileNotFoundError: If script_path does not exist.
    """
    script_path = Path(script_path)

    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    cmd = f"qsub -terse {script_path.name}"
    logger.debug(f"Running: {cmd} in {script_path.parent}")

    try:
        process = subprocess.run(
            cmd,
            cwd=script_path.parent,
            encoding="utf-8",
            capture_output=True,
            shell=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        logger.error(f"qsub failed with return code {e.returncode}")
        logger.error(f"qsub stderr: {e.stderr.strip()}")
        raise RuntimeError(f"Failed to submit job with qsub: {e.stderr.strip()}") from e

    uge_id = process.stdout.strip()

    if not uge_id:
        raise RuntimeError("qsub returned no output - unable to get job ID")

    # Validate format of job_id
    try:
        int(uge_id)
    except ValueError as err:
        raise RuntimeError(f"UGE Job ID is not a valid number: '{uge_id}'") from err

    logger.info(f"Submitted job: {uge_id}")

    return uge_id
