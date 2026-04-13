import copy
import logging
import time
from collections import defaultdict
from collections.abc import Iterator
from typing import Any

from hpc_funcs.schedulers.uge.constants import TAGS_RUNNING
from hpc_funcs.schedulers.uge.qstat import get_all_jobs_text
from hpc_funcs.schedulers.uge.qstat_json import get_qstat_job_json
from hpc_funcs.schedulers.uge.qstat_text import COLUMN_SLOTS, COLUMN_STATE, COLUMN_USER

logger = logging.getLogger(__name__)


def get_cluster_usage() -> dict[str, int]:
    """Get cluster usage information, grouped by users.

    Returns:
        Dict mapping username to total slots in use.

    Example:
        >>> usage = get_cluster_usage()
        >>> total_cores = sum(usage.values())
    """

    jobs = get_all_jobs_text()

    # Filter to running jobs only
    running_jobs = [j for j in jobs if j.get(COLUMN_STATE) in TAGS_RUNNING]

    # Group by user and sum slots
    counts: dict[str, int] = defaultdict(int)
    for job in running_jobs:
        user = job.get(COLUMN_USER, "unknown")
        slots = int(job.get(COLUMN_SLOTS, 0))
        counts[user] += slots

    # Sort by count and return as regular dict
    return dict(sorted(counts.items(), key=lambda x: x[1]))


def wait_for_jobs(jobs: list[str], sleep: int = 60) -> Iterator[str]:
    """ """

    logger.info(f"Waiting for {len(jobs)} job(s) on UGE...")

    start_time = time.time()

    jobs = copy.deepcopy(jobs)

    while len(jobs):
        logger.info(
            f"... and breathe for {sleep} sec, still waiting for {len(jobs)} job(s) to finish..."
        )

        time.sleep(sleep)

        rm_list = []

        for job_id in jobs:
            if is_job_done(job_id):
                yield job_id
                rm_list.append(job_id)

        for job_id in rm_list:
            jobs.remove(job_id)

    end_time = time.time()
    diff_time = end_time - start_time
    logger.info(f"All jobs finished and took {diff_time / 60 / 60:.2f}h")


def is_job_done(
    job_id: str,
) -> bool:
    """Check if a job is done (no longer in queue).

    Returns True if job is not found in qstat, meaning it has completed.
    """

    job_info, _ = get_qstat_job_json(job_id)

    # If there still is some qstat information, the job is not done
    # TODO Check qacct -j information

    return not job_info
