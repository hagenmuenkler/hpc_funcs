import io
from pathlib import Path

import pytest

from hpc_funcs.schedulers.uge import has_uge
from hpc_funcs.schedulers.uge.constants import TASK_ENVIRONMENT_VARIABLE

if not has_uge():
    pytest.skip("Could not find UGE executable", allow_module_level=True)
from hpc_funcs.schedulers.uge.monitoring import wait_for_jobs
from hpc_funcs.schedulers.uge.monitoring.follow import TaskarrayProgress
from hpc_funcs.schedulers.uge.qsub import submit_script, write_script
from hpc_funcs.schedulers.uge.submission import generate_script


def test_array_progressbar(global_tmp_path: Path):
    """Create a task-array job"""

    # Need network accessible folder
    tmp_path = global_tmp_path

    # Generate bash script
    command = f'sleep {TASK_ENVIRONMENT_VARIABLE}0"'  # Sleep for 10, 20, 30 etc
    log_dir = tmp_path / "uge_testlogs"

    script: str = generate_script(
        cmd=command,
        cwd=tmp_path,
        log_dir=log_dir,
        name="TestJob",
        task_concurrent=2,
        task_stop=3,
    )
    print(script)
    assert command in script

    # Submit UGE job
    print("scratch:", tmp_path)
    script_path = write_script(script, directory=tmp_path)
    job_id = submit_script(script_path)
    print(job_id)
    assert job_id is not None

    # Get the progress
    buf = io.StringIO()
    progress_bar = TaskarrayProgress.by_jobid(job_id, file=buf)
    meter = buf.getvalue()
    print(meter)
    assert job_id in meter

    progress_bar.update()

    for fjob_id in wait_for_jobs([job_id], sleep=5):
        print(f"finished jobid {fjob_id}")

    progress_bar.update()

    meter = buf.getvalue()
    print(meter)
    assert job_id in meter
    assert "100%" in meter
