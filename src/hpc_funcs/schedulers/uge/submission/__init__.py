import logging
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from jinja2 import Template
from pydantic import BaseModel, Field

DEFAULT_LOG_DIR = Path("./ugelogs/")
MASTER_TEMPLATE = Path(__file__).parent / "templates" / "submit_template.jinja"
logger = logging.getLogger(__name__)

LMOD_LINES = [
    "have been reloaded with a version change",
    "=>",
]


@dataclass
class JobScript(BaseModel):
    """Configuration for UGE job submission.

    To set up a task array, set task_stop to a value greater than task_start (1 by default).
    If task_stop is not set, job will not be submitted as job array.

    To limit the number of concurrent tasks in a task array,
    set task_concurrent to a value greater than 0.
    By default, no such limit is set.
    """

    cmd: str = Field(..., description="Command to execute")
    name: str = Field(default="UGEJob", description="Job name")
    cores: int = Field(default=1, ge=1, description="Number of cores")
    mem: int = Field(default=4, ge=1, description="Memory in GB")
    hours: int = Field(default=7, ge=0, description="Hours for runtime")
    mins: int = Field(default=0, ge=0, le=59, description="Minutes for runtime")
    log_dir: Path | None = Field(default=DEFAULT_LOG_DIR, description="Log directory")
    cwd: Path | None = Field(default=None, description="Working directory")
    environ: dict[str, str] = Field(default_factory=dict, description="Environment variables")

    # GPU support
    gpu: str | None = Field(default=None, description="GPU card specification")

    # Task array support
    task_start: int = Field(default=1, ge=1, description="Task array start index")
    task_stop: int | None = Field(default=None, ge=1, description="Task array stop index")
    task_step: int = Field(default=1, ge=1, description="Task array step")
    task_concurrent: int | None = Field(default=None, ge=1, description="Concurrent tasks")

    # Email notifications
    user_email: str | None = Field(default=None, description="User email for notifications")

    # Job dependencies
    hold_job_id: str | None = Field(
        default=None,
        description="Hold job ID for dependencies. Several job IDs can be separated by commas.",
    )

    # Modules
    module_purge: bool = Field(default=False, description="Whether to purge modules")
    module_use: list[Path] = Field(default_factory=list, description="Module use paths")
    module_load: list[str] = Field(default_factory=list, description="Modules to load")

    def generate_script(
        self,
        generate_dirs: bool = True,
    ) -> str:
        """
        Generate a script to submit a job to UGE based on the provided configuration.
        """

        if generate_dirs:
            generate_log_dir(self.log_dir)

        with open(MASTER_TEMPLATE, encoding="utf-8") as file_:
            template = Template(file_.read())

        script = template.render(self.model_dump())

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
