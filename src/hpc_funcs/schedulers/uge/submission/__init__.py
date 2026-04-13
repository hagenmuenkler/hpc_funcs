import logging
from collections import defaultdict
from pathlib import Path
from typing import Optional

from jinja2 import Template

DEFAULT_LOG_DIR = Path("./ugelogs/")
TEMPLATE_TASKARRAY = Path(__file__).parent / "templates" / "submit-task-array.jinja"
TEMPLATE_SINGLE = Path(__file__).parent / "templates" / "submit-normal.jinja"
TEMPLATE_HOLDING = Path(__file__).parent / "templates" / "submit-holding.jinja"
logger = logging.getLogger(__name__)

LMOD_LINES = [
    "have been reloaded with a version change",
    "=>",
]


# pylint: disable=too-many-arguments,too-many-locals
def generate_single_script(
    cmd: str,
    cores: int = 1,
    cwd: Path | None = None,
    environ: dict[str, str] | None = None,
    hours: int = 7,
    mins: int | None = None,
    log_dir: Path | None = DEFAULT_LOG_DIR,
    mem: int = 4,
    name: str = "UGEJob",
    hold_job_id: str | None = None,
    user_email: str | None = None,
    generate_dirs: bool = True,
) -> str:
    """
    Remember:
      - To set core restrictive env variables
    """

    if not isinstance(cores, int) or cores < 1:
        raise ValueError(
            "Cannot submit with invalid cores set. Needs to be a integer greater than 0."
        )

    if environ is None:
        environ = {}

    kwargs = locals()

    if generate_dirs:
        kwargs["log_dir"] = generate_log_dir(log_dir)

    with open(TEMPLATE_SINGLE) as file_:
        template = Template(file_.read())

    script = template.render(**kwargs)

    return script


# pylint: disable=too-many-arguments,too-many-locals
def generate_taskarray_script(
    cmd: str,
    cores: int = 1,
    cwd: Path | None = None,
    environ: dict[str, str] | None = None,
    hours: int = 7,
    mins: int | None = None,
    log_dir: Path | None = DEFAULT_LOG_DIR,
    mem: int = 4,
    name: str = "UGEJob",
    task_concurrent: int = 100,
    task_start: int = 1,
    task_step: int = 1,
    task_stop: int | None = None,
    hold_job_id: str | None = None,
    user_email: str | None = None,
    generate_dirs: bool = True,
) -> str:
    """

    If task_stop is not set, job will not be submitted as jobarray

    Remember:
      - To set core restrictive env variables
    """

    if not isinstance(cores, int) or cores < 1:
        raise ValueError(
            "Cannot submit with invalid cores set. Needs to be a integer greater than 0."
        )

    if environ is None:
        environ = {}

    kwargs = locals()

    if generate_dirs:
        kwargs["log_dir"] = generate_log_dir(log_dir)

    with open(TEMPLATE_TASKARRAY) as file_:
        template = Template(file_.read())

    script = template.render(**kwargs)

    return script


def generate_hold_script(
    hold_job_id: str,
    user_email: str | None = None,
    cmd: str = "sleep 1",
    name: str = "UGEHoldJob",
    log_dir: Path | None = DEFAULT_LOG_DIR,
    generate_dirs: bool = True,
) -> str:
    if generate_dirs:
        log_dir_str = generate_log_dir(log_dir)
    else:
        log_dir_str = str(log_dir.resolve()) if log_dir is not None else None

    with open(TEMPLATE_HOLDING) as file_:
        template = Template(file_.read())

    script = template.render(
        hold_job_id=hold_job_id,
        user_email=user_email,
        cmd=cmd,
        name=name,
        log_dir=log_dir_str,
    )

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
    logger.debug(f"Looking for finished log files in {log_path}")
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
    with open(filename) as f:
        lines = f.read().split("\n")
    return lines
