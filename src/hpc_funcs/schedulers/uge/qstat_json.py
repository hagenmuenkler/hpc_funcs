import json
import logging
import subprocess
from typing import Any

from hpc_funcs.shell import execute

logger = logging.getLogger(__name__)


def get_qstat_job_json(
    job_id: str | int,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Get detailed information for a specific job using qstat -j.

    This returns comprehensive information about a single job, including
    resource requests, environment, submission details, and task status.

    Args:
        job_id: The job ID to query.

    Returns:
        Tuple of (job_info_list, error_list):
        - job_info_list: List of job info dicts. Empty list if job not found.
        - error_list: List of error reason strings (e.g., permission errors).

    Raises:
        json.JSONDecodeError: If the JSON output from qstat is malformed.
        RuntimeError: If qstat command fails unexpectedly.

    Examples:
        >>> # Get detailed info for a specific job
        >>> job_info, errors = get_qstat_job_json(12345)
        >>> if job_info:
        ...     print(job_info[0]["job_name"])
        >>> if errors:
        ...     print(f"Job errors: {errors}")
    """

    cmd = f"qstat -j {job_id} -nenv -json"

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

    rows, errors = parse_jobinfo_json(stdout)

    return rows, errors


def get_qstat_json(
    users: list[str] | None = None,
    queues: list[str] | None = None,
    resource_filter: str | None = None,
) -> list[dict[str, Any]]:
    """Get job status information from UGE using qstat -json.

    Args:
        users: List of usernames to filter jobs by. If None, shows your jobs.
        queues: List of queue names to filter by.
        resource_filter: Resource filter string in format "attr=val,..." (e.g., "arch=lx-amd64").

    Returns:
        List of dicts with job information. Each dict represents a job (or task) with keys for
        job properties, state, owner, queue, slots, etc.

    Raises:
        json.JSONDecodeError: If the JSON output from qstat is malformed.

    Examples:
        >>> # Get all jobs for current user
        >>> jobs = get_qstat_json(users=["testuser1"])

        >>> # Get jobs in specific queues
        >>> jobs = get_qstat_json(queues=["gpu.q", "default.q"])
    """

    cmd = "qstat -json"

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
    stdout, stderr = execute(cmd)

    if stderr:
        logger.warning(f"qstat stderr: {stderr}")

    # Parse and return list of dicts
    rows = parse_joblist_json(stdout)

    return rows


def parse_jobinfo_json(stdout: str) -> tuple[list[dict[str, Any]], list[str]]:
    """Parse job info JSON output.

    Args:
        stdout: Raw output from qstat -j command.

    Returns:
        Tuple of (job_info_list, error_list):
        - job_info_list: List of job info dicts. Empty list if job not found.
        - error_list: List of error reason strings (e.g., permission errors).

    Raises:
        json.JSONDecodeError: If JSON is malformed.
    """

    KEY = "job_info"

    stdout_lines: list[str] = []
    errors: list[str] = []

    # Separate error lines from JSON content
    for line in stdout.splitlines():
        if line.startswith("error reason"):
            errors.append(line)
        else:
            stdout_lines.append(line)

    stdout = "\n".join(stdout_lines)

    if not stdout.strip():
        return [], errors

    data = json.loads(stdout)

    if KEY not in data:
        return [], errors

    rows = data[KEY]

    return rows, errors


def parse_joblist_json(stdout: str) -> list[dict[str, str]]:
    """Parse qstat JSON output into a list of job dictionaries.

    Args:
        stdout: Raw JSON string from qstat -json.

    Returns:
        List of dicts, one per job/task, with job properties.
    """

    data = json.loads(stdout)

    rows = []

    # Parse running jobs from queue_info
    if "queue_info" in data:
        for queue_section in data["queue_info"]:
            if "running jobs" not in queue_section:
                continue
            for job in queue_section["running jobs"]:
                row = _extract_job_row(job, job_type="running")
                rows.append(row)

    # Parse pending jobs from job_info
    if "job_info" in data:
        for job_section in data["job_info"]:
            if "pending jobs" not in job_section:
                continue
            for job in job_section["pending jobs"]:
                row = _extract_job_row(job, job_type="pending")
                rows.append(row)

    if not rows:
        logger.debug("No jobs found in qstat output")
        return rows

    return rows


def _extract_job_row(job: dict[str, Any], job_type: str) -> dict[str, Any]:
    """Extract relevant fields from a job dictionary.

    Args:
        job: Dictionary containing job information.
        job_type: Type of job ("running" or "pending").

    Returns:
        Dictionary with standardized job fields.
    """
    row = {
        "job_number": job.get("JB_job_number", ""),
        "priority": job.get("JAT_prio", 0.0),
        "name": job.get("JB_name", ""),
        "owner": job.get("JB_owner", ""),
        "state": job.get("state", ""),
        "slots": job.get("slots", 0),
        "queue_name": job.get("queue_name", ""),
        # "jclass_name": job.get("jclass_name", ""),
        "job_type": job_type,
    }

    # Add start time for running jobs
    if "JAT_start_time" in job:
        row["start_time"] = job["JAT_start_time"]

    # Add submission time for pending jobs
    if "JB_submission_time" in job:
        row["submission_time"] = job["JB_submission_time"]

    # Add task ID if present (for array jobs)
    if "JAT_task_number" in job:
        row["task_id"] = job["JAT_task_number"]

    return row
