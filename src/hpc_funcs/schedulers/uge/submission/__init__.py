import logging
from collections import defaultdict
from pathlib import Path

from jinja2 import Template

DEFAULT_LOG_DIR = Path("./ugelogs/")
MASTER_TEMPLATE = Path(__file__).parent / "templates" / "submit_template.jinja"
logger = logging.getLogger(__name__)

LMOD_LINES = [
    "have been reloaded with a version change",
    "=>",
]


def generate_script(
    cmd: str,
    name: str = "UGEJob",
    cores: int = 1,
    mem: int = 4,
    hours: int = 7,
    mins: int = 0,
    log_dir: Path | None = DEFAULT_LOG_DIR,
    cwd: Path | None = None,
    environ: dict[str, str] | None = None,
    gpu: str | None = None,
    task_start: int = 1,
    task_stop: int | None = None,
    task_step: int = 1,
    task_concurrent: int | None = None,
    user_email: str | None = None,
    hold_job_id: str | None = None,
    module_purge: bool = False,
    module_use: list[Path] | None = None,
    module_load: list[str] | None = None,
    generate_dirs: bool = True,
) -> str:
    """Generate a UGE job submission script with explicit parameters.

    This function generates a UGE (Univa Grid Engine) job submission script by
    rendering the master Jinja2 template with the provided configuration parameters.
    It provides an alternative to JobScript.generate_script() for scenarios where
    you want to work with explicit function parameters rather than a model instance.

    The generated script can be used to submit jobs to a UGE cluster, with support for:
    - Single jobs and task arrays
    - GPU specifications
    - Module loading and purging
    - Email notifications
    - Job dependencies
    - Custom environment variables
    - Working directories and logging

    Args:
        cmd: Command to execute in the job.
        name: Job name (default: "UGEJob").
        cores: Number of CPU cores to request (default: 1, minimum: 1).
        mem: Memory in GB to request (default: 4, minimum: 1).
        hours: Hours for job runtime limit (default: 7, minimum: 0).
        mins: Minutes for job runtime limit (default: 0, range: 0-59).
        log_dir: Directory for job output logs. If None, no logs are created
            (default: "./ugelogs/").
        cwd: Working directory for the job. If None, uses submission directory
            (default: None).
        environ: Dictionary of environment variables to set for the job
            (default: None, treated as empty dict).
        gpu: GPU specification string (e.g., "nvidia_h100:4" for 4 GPU cards).
            If None, no GPU requested (default: None).
        task_start: Starting index for task arrays (default: 1, minimum: 1).
        task_stop: Ending index for task arrays. If None, job is not a task array
            (default: None, minimum: 1 if specified).
        task_step: Step size for iterating through task array indices
            (default: 1, minimum: 1).
        task_concurrent: Maximum number of concurrent tasks in a task array.
            If None, no limit is applied (default: None, minimum: 1 if specified).
        user_email: Email address for job notifications. If None, no notifications
            are sent (default: None).
        hold_job_id: Job ID(s) to wait for before starting this job. Multiple IDs
            can be comma-separated. If None, no job dependency (default: None).
        module_purge: Whether to purge all loaded modules before loading new ones
            (default: False).
        module_use: List of module search paths to add before loading modules
            (default: None, treated as empty list).
        module_load: List of module names/versions to load
            (default: None, treated as empty list).
        generate_dirs: Whether to create the log directory if it doesn't exist
            (default: True).

    Returns:
        str: The rendered UGE job submission script as a string, ready to be
            written to a file or piped to qsub.

    Side Effects:
        If generate_dirs is True and log_dir is not None, creates the log
        directory (and parent directories) if it doesn't already exist.

    Example:
        >>> script = generate_script(
        ...     cmd="python my_script.py",
        ...     name="analysis_job",
        ...     cores=8,
        ...     mem=4,
        ...     hours=2,
        ...     task_start=1,
        ...     task_stop=100,
        ...     module_load=["python/3.11"],
        ... )
        >>> with open("job.sh", "w") as f:
        ...     f.write(script)
    """
    if generate_dirs and log_dir is not None:
        generate_log_dir(log_dir)

    # Prepare context dictionary for template rendering, using defaults for None values
    context = {
        "cmd": cmd,
        "name": name,
        "cores": cores,
        "mem": mem,
        "hours": hours,
        "mins": mins,
        "log_dir": log_dir,
        "cwd": cwd,
        "environ": environ if environ is not None else {},
        "gpu": gpu,
        "task_start": task_start,
        "task_stop": task_stop,
        "task_step": task_step,
        "task_concurrent": task_concurrent,
        "user_email": user_email,
        "hold_job_id": hold_job_id,
        "module_purge": module_purge,
        "module_use": module_use if module_use is not None else [],
        "module_load": module_load if module_load is not None else [],
    }

    with open(MASTER_TEMPLATE, encoding="utf-8") as file_:
        template = Template(file_.read())

    script = template.render(context)

    return script


def generate_log_dir(log_dir: Path | None) -> str | None:
    if log_dir is not None:
        if not log_dir.exists():
            log_dir.mkdir(parents=True)

        if log_dir.is_dir():
            _log_dir = str(log_dir.resolve() / "_")[:-1]  # Added a trailing slash
            return _log_dir

        return str(log_dir.resolve())

    return None


def read_logfiles(
    log_path: Path,
    job_id: str,
    ignore_stdout: bool = True,
    filter_lmod: bool = False,
) -> tuple[dict[Path, list[str]], dict[Path, list[str]]]:
    """Read logfiles produced by UGE task array. Ignore empty log files"""
    logger.debug("Looking for finished log files in %s", log_path)
    stderr_log_filenames = list(log_path.glob(f"*.e{job_id}*"))

    stderr = {}
    for filename in stderr_log_filenames:
        if filename.stat().st_size == 0:
            continue
        stderr[filename] = parse_logfile(filename)

    if filter_lmod:
        stderr = filter_stderr_for_lmod(stderr)

    if ignore_stdout:
        return {}, stderr

    stdout_log_filenames = log_path.glob(f"*.o{job_id}*")
    stdout = {}
    for filename in stdout_log_filenames:
        if filename.stat().st_size == 0:
            continue
        stdout[filename] = parse_logfile(filename)

    return stdout, stderr


def filter_stderr_for_lmod(stderr_dict: dict[Path, list[str]]) -> dict[Path, list[str]]:
    """Filter stderr for lmod lines"""

    stderr_filtered = defaultdict(list)
    for filename, lines in stderr_dict.items():
        for line in lines:
            if len(line) == 0 or any(lmod_line in line for lmod_line in LMOD_LINES):
                continue
            stderr_filtered[filename].append(line)

    return dict(stderr_filtered)


def parse_logfile(filename: Path) -> list[str]:
    """Read logfile, without line-breaks"""
    # TODO Maybe find exceptions and raise them?
    with open(filename, encoding="utf-8") as f:
        lines = f.read().split("\n")
    return lines
