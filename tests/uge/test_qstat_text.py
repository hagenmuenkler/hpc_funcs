import pandas as pd
from conftest import RESOURCES  # type: ignore

from hpc_funcs.schedulers.uge.qstat_text import (
    COLUMN_ARRAY,
    COLUMN_INFO_ARRAY,
    COLUMN_INFO_JOBID,
    COLUMN_INFO_NAME,
    COLUMN_INFO_USER,
    parse_jobinfo_text,
    parse_joblist_text,
)

pd.set_option("display.max_columns", None)


def test_parse_jobinfo():
    filename = RESOURCES / "uge" / "qstat_jobinfo_array.txt"

    with open(filename) as f:
        stdout = f.read()

    jobs = parse_jobinfo_text(stdout)

    print(jobs)

    # Should parse exactly one job
    assert len(jobs) == 1

    job = jobs[0]

    # Test basic fields
    assert job[COLUMN_INFO_JOBID] == "30017751"
    assert job[COLUMN_INFO_NAME] == "TestJob"
    assert job[COLUMN_INFO_USER] == "user01"

    # Test job array structure
    assert job[COLUMN_INFO_ARRAY]


def test_parse_joblist():
    filename = RESOURCES / "uge/qstat_joblist.txt"

    with open(filename) as f:
        stdout = f.read()

    job_list = parse_joblist_text(stdout)

    # print(list_jobs)

    pdf = pd.DataFrame(job_list)
    assert len(pdf)

    print(COLUMN_ARRAY)
    print(pdf.columns.tolist())
    assert COLUMN_ARRAY in pdf

    print(pdf)
