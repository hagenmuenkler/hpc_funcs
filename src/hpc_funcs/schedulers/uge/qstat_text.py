import logging
import re
import subprocess
from typing import Any

from .constants import TAGS_ERROR, TAGS_PENDING, TAGS_RUNNING

logger = logging.getLogger(__name__)

# Joblist columns
COLUMN_JOBID = "job-ID"
COLUMN_PRIORITY = "prior"
COLUMN_NAME = "name"
COLUMN_USER = "user"
COLUMN_STATE = "state"
COLUMN_TIME = "submit/start at"
COLUMN_QUEUE = "queue"
COLUMN_JCLASS = "jclass"
COLUMN_SLOTS = "slots"
COLUMN_ARRAY = "ja-task-ID"

# Ordered qstat text joblist columns
COLUMNS_TEXT = [
    COLUMN_JOBID,
    COLUMN_PRIORITY,
    COLUMN_NAME,
    COLUMN_USER,
    COLUMN_STATE,
    COLUMN_TIME,
    COLUMN_QUEUE,
    COLUMN_JCLASS,
    COLUMN_SLOTS,
    COLUMN_ARRAY,
]

# joblist summary table columns
COLUMN_RUNNING = "running"
COLUMN_PENDING = "pending"
COLUMN_ERROR = "error"

# jobinfo columns
COLUMN_INFO_JOBID = "job_number"
COLUMN_INFO_USER = "owner"
COLUMN_INFO_ARRAY = "job-array tasks"
COLUMN_INFO_CONCURRENCY = "task_concurrency"
COLUMN_INFO_NAME = "job_name"


def get_qstat_text(
    users: list[str] | None = None,
    queues: list[str] | None = None,
    resource_filter: str | None = None,
) -> list[dict[str, Any]]:
    """Get job status information from UGE using qstat text output.

    Args:
        users: List of usernames to filter jobs by. If None, shows your jobs.
        queues: List of queue names to filter by.
        resource_filter: Resource filter string in format "attr=val,..." (e.g., "arch=lx-amd64").

    Returns:
        List of dicts with job information. Each dict represents a job (or task) with keys for
        job properties, state, owner, queue, slots, etc.

    Examples:
        >>> # Get all jobs for current user
        >>> jobs = get_qstat_text(users=["testuser1"])

        >>> # Get jobs in specific queues
        >>> jobs = get_qstat_text(queues=["gpu.q", "default.q"])
    """

    cmd = "qstat"

    if users is not None and len(users):
        user_list = ",".join(users)
        cmd += f" -u {user_list}"

    if queues:
        queue_list = ",".join(queues)
        cmd += f" -q {queue_list}"

    if resource_filter:
        cmd += f" -l {resource_filter}"

    # Execute command
    logger.debug(f"Executing: {cmd}")
    process = subprocess.run(
        cmd,
        encoding="utf-8",
        capture_output=True,
        shell=True,
    )

    stdout = process.stdout
    stderr = process.stderr

    if stderr:
        logger.warning(f"qstat stderr: {stderr}")

    # Parse and return list of dicts
    jobs = parse_joblist_text(stdout)

    return jobs


def get_qstat_job_text(
    job_id: str | int,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Get detailed information for a specific job using qstat -j (text format).

    This returns comprehensive information about a single job, including
    resource requests, submission details, and task status.

    Args:
        job_id: The job ID to query.

    Returns:
        Tuple of (job_info_list, error_list):
        - job_info_list: List of job info dicts. Empty list if job not found.
        - error_list: List of error reason strings (e.g., permission errors).

    Examples:
        >>> # Get detailed info for a specific job
        >>> jobs, errors = get_qstat_job_text(12345)
        >>> if jobs:
        ...     job = jobs[0]
        ...     print(job["job_name"])
        >>> if errors:
        ...     print(f"Errors: {errors}")
    """

    cmd = f"qstat -j {job_id} -nenv"

    logger.debug(f"Executing: {cmd}")

    process = subprocess.run(
        cmd,
        encoding="utf-8",
        capture_output=True,
        shell=True,
    )

    stdout = process.stdout
    stderr = process.stderr

    if stderr:
        logger.warning(f"qstat stderr: {stderr}")

    # Separate error lines from content
    lines = stdout.splitlines()
    text_lines: list[str] = []
    errors: list[str] = []

    for line in lines:
        if line.startswith("error reason"):
            errors.append(line)
        else:
            text_lines.append(line)

    stdout = "\n".join(text_lines)

    # Parse the text output
    jobs = parse_jobinfo_text(stdout)

    return jobs, errors


def parse_joblist_text(stdout: str) -> list[dict[str, Any]]:
    """
    Parse UGE qstat text output (list format).

    Args:
        stdout: String output from qstat command

    Returns:
        List of dictionaries containing job information
    """
    jobs: list[dict[str, Any]] = []
    lines = stdout.strip().split("\n")

    if len(lines) < 3:
        return []

    # First line is the header with column names
    header_line = lines[0]
    # Second line is the separator (dashes)
    # Data starts from third line

    # Parse the header to find column positions
    # use the header line to identify where each column starts

    # Find column start positions based on header
    column_positions = {column_name: header_line.find(column_name) for column_name in COLUMNS_TEXT}

    missing = [c for c, pos in column_positions.items() if pos == -1]
    if missing:
        raise KeyError(f"Missing expected header(s): {missing}")

    # Ensure columns are processed in left-to-right order in the header
    ordered_cols = sorted(COLUMNS_TEXT, key=lambda c: column_positions[c])

    # Process each data line
    for line in lines[2:]:
        if not line.strip():
            continue

        # Extract fields based on column positions
        # We'll use the positions to slice the line
        job: dict[str, Any] = {}

        for i, col in enumerate(ordered_cols):
            start = column_positions[col]
            end = column_positions[ordered_cols[i + 1]] if i + 1 < len(ordered_cols) else None
            value = line[start:end].strip()

            job[col] = value

        jobs.append(job)

    return jobs


def parse_jobinfo_text(stdout: str) -> list[dict[str, str]]:
    """
    Output is column-length based and sections split by "=".
    Returns list key-value dict per section.
    """

    COL_VALUE_START = 28

    output: list[dict[str, str]] = [{}]

    lines = stdout.split("\n")

    for line in lines:
        if "=" * 5 in line:
            if len(output[-1]) > 1:
                output += [{}]
            continue

        # Format: pe_taskid     NONE
        key = line[:COL_VALUE_START].strip()
        value = line[COL_VALUE_START:].strip()

        if key.endswith(":"):
            key = key[:-1]

        if len(key) == 0:
            continue

        key = key.strip()
        value = value.strip()

        output[-1][key] = value

    return output


def parse_qstat_text(stdout: str) -> list[dict[str, Any]]:
    """Parse qstat text output into list of job dicts."""
    stdout = stdout.strip()
    lines = stdout.split("\n")

    header = lines[0].split()
    header.remove("at")
    header_indicies = []

    rows: list[dict[str, Any]] = []

    for line in header[1:]:
        idx = lines[0].index(line)
        header_indicies.append(idx)

    def split_qstat_line(line):
        idx = 0

        for ind in header_indicies:
            yield line[idx:ind].strip()
            idx = ind

        yield line[idx:]

    for line in lines[2:]:
        if not line.strip():
            continue

        line_ = split_qstat_line(line)
        line_ = list(line_)

        row: dict[str, Any] = dict(zip(header, line_, strict=False))
        # Convert slots to int
        if "slots" in row:
            row["slots"] = int(row["slots"])
        rows.append(row)

    return rows


def parse_taskarray(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Parse task array information from job list.

    Args:
        jobs: List of job dicts from get_qstat_text or parse_joblist_text

    Returns:
        List of dicts with job_id, running, pending, error counts
    """

    def _parse_task_count(line: str) -> int:
        """Parse task array string like '1-100:1' into task count."""
        count = 0

        parts = line.split(",")
        for task in parts:
            if "-" not in task:
                count += 1
                continue

            start, stop, _ = re.split(r",|:|-|!", task)
            count += int(stop) - int(start)

        return count

    # Get unique job IDs
    job_ids = {job[COLUMN_JOBID] for job in jobs}

    rows: list[dict[str, Any]] = []

    for job_id in job_ids:
        # Filter jobs by job_id
        job_subset = [j for j in jobs if j[COLUMN_JOBID] == job_id]

        # Filter by state
        pending_jobs = [j for j in job_subset if j.get(COLUMN_STATE) in TAGS_PENDING]
        running_jobs = [j for j in job_subset if j.get(COLUMN_STATE) in TAGS_RUNNING]
        error_jobs = [j for j in job_subset if j.get(COLUMN_STATE) in TAGS_ERROR]

        # Count tasks
        n_pending = sum(_parse_task_count(j.get(COLUMN_ARRAY, "")) for j in pending_jobs)
        n_running = len(running_jobs)
        n_error = sum(_parse_task_count(j.get(COLUMN_ARRAY, "")) for j in error_jobs)

        row = {
            COLUMN_JOBID: job_id,
            COLUMN_RUNNING: n_running,
            COLUMN_PENDING: n_pending,
            COLUMN_ERROR: n_error,
        }

        rows.append(row)

    return rows
