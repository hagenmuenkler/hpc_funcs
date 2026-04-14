import logging
from typing import Any

from .qstat_json import get_qstat_json
from .qstat_text import get_qstat_text

logger = logging.getLogger(__name__)


def get_all_jobs_json() -> list[dict[str, Any]]:
    """Get all jobs for all users (JSON format)."""
    all_users = '"*"'
    jobs = get_qstat_json(users=[all_users])
    return jobs


def get_all_jobs_text() -> list[dict[str, Any]]:
    """Get all jobs for all users (text format)."""
    all_users = '"*"'
    jobs = get_qstat_text(users=[all_users])
    return jobs
