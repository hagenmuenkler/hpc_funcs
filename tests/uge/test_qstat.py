import pandas as pd
import pytest

from hpc_funcs.schedulers.uge import has_uge
from hpc_funcs.schedulers.uge.qstat import get_all_jobs_json, get_all_jobs_text
from hpc_funcs.schedulers.uge.qstat_text import (
    COLUMN_ARRAY,
    COLUMN_INFO_USER,
    COLUMN_JOBID,
    get_qstat_job_text,
)

pd.set_option("display.max_columns", None)


if not has_uge():
    pytest.skip("Could not find UGE executable", allow_module_level=True)


def test_all():
    print()

    jobs_json = get_all_jobs_json()
    df = pd.DataFrame(jobs_json)
    print(df)
    assert len(df) > 1

    jobs_text = get_all_jobs_text()
    df = pd.DataFrame(jobs_text)
    print(df)
    assert len(df) > 1

    assert COLUMN_ARRAY in df


def test_jobinfo_from_joblist_text():
    jobs = get_all_jobs_text()
    df = pd.DataFrame(jobs)
    print(df)
    assert len(df) > 1

    job_id = df[COLUMN_JOBID].values[0]

    # Get specific job info
    job_infos, job_erros = get_qstat_job_text(job_id)

    print(job_infos)
    print(job_erros)

    assert len(job_infos)
    assert job_infos[0][COLUMN_INFO_USER]
