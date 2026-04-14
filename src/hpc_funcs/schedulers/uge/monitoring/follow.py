import datetime
import logging
import time
from io import StringIO
from typing import Any

import tqdm

from hpc_funcs.schedulers.uge.qstat_text import (
    COLUMN_ERROR,
    COLUMN_JOBID,
    COLUMN_PENDING,
    COLUMN_RUNNING,
    parse_taskarray,
)

from ..qstat import get_all_jobs_text
from ..qstat_text import get_qstat_job_text

DATE_FORMAT = "%m/%d/%Y %H:%M:%S"
TQDM_LENGTH = 95
TQDM_OPTIONS = {
    "ncols": TQDM_LENGTH,
}

logger = logging.getLogger(__name__)


def get_time_from_ugestr(time_str: str) -> float:
    _time = ".".join(time_str.split(".")[:-1])
    time_ = datetime.datetime.strptime(_time, DATE_FORMAT)
    return time.mktime(time_.timetuple())


class TaskarrayProgress:
    @staticmethod
    def by_jobid(
        job_id: str, position: int = 0, file: StringIO | None = None
    ) -> "TaskarrayProgress":
        job_id = str(job_id)
        job_infos, _ = get_qstat_job_text(job_id)
        if not job_infos:
            raise ValueError(f"Job not found: {job_id}")
        job_info = job_infos[0]

        return TaskarrayProgress(job_info, position=position, file=file)

    def __init__(
        self,
        job_info: dict,
        position: int = 0,
        file: StringIO | None = None,
    ) -> None:
        self.position = position
        self.file = file
        self.job_id: str
        self.init_bar(job_info, {})

    @staticmethod
    def _read_time(timestamp):
        time_start = datetime.datetime.fromtimestamp(int(timestamp) / 1000)
        time_start = time.mktime(time_start.timetuple())
        return time_start

    def init_bar(self, job_info: dict, job_status: dict) -> None:
        is_xml = job_info.get("JB_ja_structure") is not None
        is_json = job_info.get("submission_time") is not None
        # is_text = not is_xml and not is_json

        job_id = (
            job_info.get("job_number")
            or job_info.get("JB_job_number")
            or job_info.get(COLUMN_JOBID)
        )

        if job_id is None:
            raise ValueError("Could not extract job_id from job_info")
        self.job_id = job_id

        # TODO Move submission_time to constant from format

        timestamp: str = (
            job_info.get("submission_time") if is_json else job_info.get("JB_submission_time")
        )  # type: ignore
        if timestamp is None:
            raise ValueError("Could not extract timestamp from job_info")
        time_start = get_time_from_ugestr(timestamp) if is_json else self._read_time(timestamp)

        # Handle both XML format (JB_ja_structure) and text format (job-array tasks)
        if is_xml:
            # XML format: list of dicts with RN_min, RN_max, RN_step
            job_array_info = job_info.get("JB_ja_structure")
            if job_array_info is None:
                raise ValueError("JB_ja_structure not found in XML job_info")
            job_array_stop = int(job_array_info[0].get("RN_max", 1))
        else:
            # Text format: "start-stop:step" string like "1-2:1"
            job_array_str = job_info.get("job-array tasks")
            if job_array_str is None:
                raise ValueError("job-array tasks not found in text job_info")
            # Parse "1-2:1" format
            parts = job_array_str.split(":")
            range_parts = parts[0].split("-")
            job_array_stop = int(range_parts[1]) if len(range_parts) > 1 else int(range_parts[0])

        self.n_total = job_array_stop

        # Set title
        self.title = job_id

        self.pbar = tqdm.tqdm(
            total=self.n_total,
            desc=f"{self.title}",
            position=self.position,
            unit="task",
            ncols=TQDM_LENGTH,
            file=self.file,
        )

        # Reset time
        self.pbar.last_print_t = self.pbar.start_t = time_start

    def update(self, joblist: list[dict[str, Any]] | None = None) -> None:
        if joblist is None:
            jobs = get_all_jobs_text()
            joblist = parse_taskarray(jobs)

        # Get status - find matching job in the list
        matching_jobs = [j for j in joblist if j.get(COLUMN_JOBID) == self.job_id]

        n_running: int
        n_pending: int
        n_error: int
        n_finished: int

        if not len(matching_jobs):
            n_running = 0
            n_pending = 0
            n_error = 0
            n_finished = self.n_total
        else:
            job = matching_jobs[0]
            n_running = int(job.get(COLUMN_RUNNING, 0))
            n_pending = int(job.get(COLUMN_PENDING, 0))
            n_error = int(job.get(COLUMN_ERROR, 0))
            n_finished = self.n_total - n_pending - n_running

        postfix = {}

        if n_error > 0:
            postfix["err"] = n_error

        self.pbar.set_postfix(postfix)

        self.pbar.set_description(f"{self.title} ({n_running})", refresh=False)
        self.pbar.n = n_finished
        self.pbar.refresh()

    def finish(self) -> None:
        n_total = self.n_total
        self.pbar.set_postfix({})
        self.pbar.set_description(f"{self.title} (0)", refresh=False)
        self.pbar.n = n_total
        self.pbar.refresh()

    def is_finished(self) -> bool:
        return self.pbar.n >= self.n_total

    def close(self) -> None:
        self.pbar.close()
