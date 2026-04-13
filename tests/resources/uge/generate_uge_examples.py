import time
from pathlib import Path

from hpc_funcs.schedulers.uge.constants import TASK_ENVIRONMENT_VARIABLE
from hpc_funcs.schedulers.uge.monitoring import wait_for_jobs
from hpc_funcs.schedulers.uge.qdel import delete_job
from hpc_funcs.schedulers.uge.qsub import submit_script, write_script
from hpc_funcs.schedulers.uge.submission import generate_taskarray_script
from hpc_funcs.shell import execute


def generate_taskarray_log(global_tmp_path: Path):
    # Need network accessible folder
    tmp_path = global_tmp_path

    # Generate bash script
    command = f'set -e; sleep 1; if test "{TASK_ENVIRONMENT_VARIABLE}" == 2; then command_does_not_exist; else pwd; fi'
    log_dir = tmp_path / "uge_testlogs"

    script: str = generate_taskarray_script(
        command,
        cores=1,
        cwd=tmp_path,
        log_dir=log_dir,
        name="TestJob",
        task_concurrent=1,
        task_stop=2,
    )

    script_path = write_script(script, directory=tmp_path)
    job_id = submit_script(script_path)

    assert job_id is not None

    time.sleep(3)

    execute(f"qstat -nenv -j {job_id} > {tmp_path}/qstat_jobinfo_array.txt")
    execute(f"qstat -nenv -j {job_id} -xml > {tmp_path}/qstat_jobinfo_array.xml")
    execute(f"qstat -nenv -j {job_id} -json > {tmp_path}/qstat_jobinfo_array.json")

    for fjob_id in wait_for_jobs([job_id], sleep=5):
        print(f"job {fjob_id} finished")

    execute(f"qacct -j {job_id} > {tmp_path}/qacct_array.txt")


def generate_errorjob_log(global_tmp_path: Path):
    # Need network accessible folder
    tmp_path = global_tmp_path

    # Generate bash script
    command = f'set -e; sleep 1; if test "{TASK_ENVIRONMENT_VARIABLE}" == 2; then command_does_not_exist; else pwd; fi'
    log_dir = Path("/log/dir/does/not/exist")

    script: str = generate_taskarray_script(
        command,
        cores=1,
        cwd=tmp_path,
        log_dir=log_dir,
        name="TestJob",
        task_concurrent=1,
        task_stop=2,
        generate_dirs=False,
    )

    script_path = write_script(script, directory=tmp_path)
    job_id = submit_script(script_path)

    time.sleep(10)

    execute(f"qstat -nenv -j {job_id} > {tmp_path}/qstat_jobinfo_error.txt")
    execute(f"qstat -nenv -j {job_id} -xml > {tmp_path}/qstat_jobinfo_error.xml")
    execute(f"qstat -nenv -j {job_id} -json > {tmp_path}/qstat_jobinfo_error.json")

    delete_job(job_id)


def generate_joblists():
    execute(f"qstat -u '*' > {tmp_path}/qstat_joblist.txt")
    execute(f"qstat -u '*' -xml > {tmp_path}/qstat_joblist.xml")
    execute(f"qstat -u '*' -json > {tmp_path}/qstat_joblist.json")


if __name__ == "__main__":
    tmp_path = Path("./examples")
    tmp_path.mkdir()

    generate_taskarray_log(tmp_path)
    generate_errorjob_log(tmp_path)
    generate_joblists()
