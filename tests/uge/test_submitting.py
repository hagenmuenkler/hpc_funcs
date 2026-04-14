import time
from pathlib import Path

import pandas as pd
import pytest

from hpc_funcs.schedulers.uge import has_uge
from hpc_funcs.schedulers.uge.constants import TASK_ENVIRONMENT_VARIABLE

if not has_uge():
    pytest.skip("Could not find UGE executable", allow_module_level=True)
from hpc_funcs.schedulers.uge.monitoring import wait_for_jobs
from hpc_funcs.schedulers.uge.qacct import get_job_accounting
from hpc_funcs.schedulers.uge.qdel import delete_job
from hpc_funcs.schedulers.uge.qstat_text import get_qstat_job_text
from hpc_funcs.schedulers.uge.qsub import submit_script, write_script
from hpc_funcs.schedulers.uge.submission import generate_script, read_logfiles


def test_generate_submit_script():
    command = "which python"
    script: str = generate_script(cmd=command)
    print(script)
    assert command in script


def test_single(global_tmp_path: Path):
    """Create a single job"""

    # Need network accessible folder
    tmp_path = global_tmp_path

    # Generate bash script
    success_string = "finished work"
    command = f"echo 'before'\nsleep 5\n\necho '{success_string}'"
    log_dir = tmp_path / "uge_testlogs"

    script: str = generate_script(
        cmd=command,
        cores=1,
        cwd=tmp_path,
        log_dir=log_dir,
        name="TestJob",
    )
    print(script)
    assert command in script

    # Submit UGE job
    print("scratch:", tmp_path)
    script_path = write_script(script, directory=tmp_path)
    job_id = submit_script(script_path)
    print(job_id)
    assert job_id is not None

    # Wait
    finished_job_id: str | None = None

    for fjob_id in wait_for_jobs([job_id], sleep=5):
        print(f"job {fjob_id} finished")
        finished_job_id = fjob_id

    assert finished_job_id is not None

    # Wait for UGE to flush output files to disk and for network filesystem to sync
    time.sleep(20)

    stdout, stderr = read_logfiles(log_dir, finished_job_id, ignore_stdout=False, filter_lmod=True)

    assert log_dir.is_dir(), "Something wrong with the network drive"
    print(log_dir)

    print(stdout)
    print(stderr)

    assert len(stdout) == 1
    assert len(stderr) == 0

    # Parse output
    for _, lines in stdout.items():
        assert success_string in lines


def test_taskarray(global_tmp_path: Path):
    """Create a task-array job"""

    # Need network accessible folder
    tmp_path = global_tmp_path

    # Generate bash script
    success_string = "finished work"
    command = f'sleep 5 & echo "{success_string} {TASK_ENVIRONMENT_VARIABLE}"'
    log_dir = tmp_path / "uge_testlogs"

    script: str = generate_script(
        cmd=command,
        cores=1,
        cwd=tmp_path,
        log_dir=log_dir,
        name="TestJob",
        task_concurrent=1,
        task_stop=2,
    )
    print(script)
    assert command in script

    # Submit UGE job
    print("scratch:", tmp_path)
    script_path = write_script(script, directory=tmp_path)
    job_id = submit_script(script_path)
    print(job_id)
    assert job_id is not None

    # Wait
    finished_job_id: str | None = None

    for fjob_id in wait_for_jobs([job_id], sleep=2):
        print(f"job {fjob_id} finished")
        finished_job_id = fjob_id

    assert finished_job_id is not None

    # Wait for UGE to flush output files to disk and for network filesystem to sync
    time.sleep(20)

    stdout, stderr = read_logfiles(log_dir, finished_job_id, ignore_stdout=False, filter_lmod=True)

    print(stdout)
    print(stderr)

    assert len(stdout) == 2
    assert len(stderr) == 0

    # Parse output
    for _, lines in stdout.items():
        for line in lines:
            if not line.strip():
                continue
            assert success_string in line
    return


def test_failed_command(global_tmp_path: Path):
    """
    Submit a task array, but one of the tasks will fail, so we neec to collect the exit_codes and check
    """

    # Need network accessible folder
    tmp_path = global_tmp_path

    # Generate script
    # Command fails on the 2nd task
    command = f'set -e; sleep 1; if test "{TASK_ENVIRONMENT_VARIABLE}" == 2; then command_does_not_exist; else pwd; fi'
    log_dir = tmp_path / "logs"
    n_tasks = 2
    script: str = generate_script(
        cmd=command,
        cores=1,
        log_dir=log_dir,
        name="TestJob",
        task_concurrent=1,
        task_stop=n_tasks,
    )
    print(script)
    assert command in script

    # Submit script
    print("scratch:", tmp_path)
    script_path = write_script(script, directory=tmp_path)
    job_id = submit_script(script_path)
    print(job_id)
    assert job_id is not None

    # Wait
    finished_job_id: str | None = None
    for finished_job_id in wait_for_jobs([job_id], sleep=10):
        print(f"job {finished_job_id} finished")
        assert finished_job_id is not None

    # Parse results
    time.sleep(30)  # Wait for UGE to collect the accounting result for all tasks

    # Overview of the tasks
    accounting = get_job_accounting(job_id)
    assert len(accounting) == n_tasks, "Length of qacct output should be length of task array"

    pdf_report = pd.DataFrame(accounting)
    print(pdf_report)

    pdf_report["exit_status"] = pdf_report["exit_status"].astype(int)
    assert max(pdf_report["exit_status"]) == 127, "Exit code mismatch"
    assert min(pdf_report["exit_status"]) == 0, "Exit code mismatch"

    # Get the log files
    stdout, stderr = read_logfiles(log_dir, job_id, ignore_stdout=False, filter_lmod=True)
    print("stdout:", stdout)
    print("stderr:", stderr)
    assert len(stdout) == 1
    assert len(stderr) == 1


def test_failed_uge_submit(tmp_path: Path):
    """
    test failed job where uge cannot write to the log folder
    Expect the error to be in the uge logger
    """

    # Set a log path that does not exist, so the job fails
    log_dir = Path("/this/path/does/not/exist/")
    command = "echo Hello"
    n_tasks = 2

    script: str = generate_script(
        cmd=command,
        log_dir=log_dir,
        name="TestJob",
        task_concurrent=1,
        task_stop=n_tasks,
        generate_dirs=False,
    )

    script_path = write_script(script, directory=tmp_path)
    job_id = submit_script(script_path)

    # Wait for UGE to attempt to run the job and report the error
    time.sleep(20)

    assert job_id is not None
    print(job_id)

    job_info, job_errors = get_qstat_job_text(job_id)

    assert len(job_errors) >= 1

    error_line = 'can\'t make directory "/this/path/does/not" as stdout_path: Permission denied'
    assert error_line in job_errors[0]

    # Clean the bad job
    delete_job(job_id)
