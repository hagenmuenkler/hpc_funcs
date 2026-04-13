from conftest import RESOURCES  # type: ignore

from hpc_funcs.schedulers.uge.qstat_xml import parse_jobinfo_xml


def test_parse_jobinfo_xml():
    """Test parsing of qstat -j -xml -nenv output."""

    filename = RESOURCES / "uge" / "qstat_jobinfo_array.xml"

    with open(filename) as f:
        xml_str = f.read()

    jobs = parse_jobinfo_xml(xml_str)

    # Should parse exactly one job
    assert len(jobs) == 1

    job = jobs[0]

    # Test basic fields
    assert job["JB_job_number"] == "30017751"
    assert job["JB_job_name"] == "TestJob"
    assert job["JB_owner"] == "user01"

    # Test job array structure
    assert len(job["JB_ja_structure"]) == 1
    assert job["JB_ja_structure"][0]["RN_min"] == "1"
    assert job["JB_ja_structure"][0]["RN_max"] == "2"
    assert job["JB_ja_structure"][0]["RN_step"] == "1"

    # Test job array tasks (optional - only present when tasks are running)
    # JB_ja_tasks is not present in the XML when job is queued/pending
    if "JB_ja_tasks" in job:
        assert len(job["JB_ja_tasks"]) >= 1
        task = job["JB_ja_tasks"][0]
        assert "JAT_task_number" in task
        assert "JAT_status" in task
