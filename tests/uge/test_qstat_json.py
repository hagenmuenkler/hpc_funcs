from conftest import RESOURCES  # type: ignore

from hpc_funcs.schedulers.uge.qstat_json import parse_jobinfo_json, parse_joblist_json


def test_parse_joblist_json():
    """Test parsing of qstat -json output (job list format)."""

    filename = RESOURCES / "uge" / "qstat_joblist.json"

    with open(filename) as f:
        stdout = f.read()

    rows = parse_joblist_json(stdout)

    print(f"Parsed {len(rows)} jobs")
    print(rows)

    # Should have parsed jobs
    assert len(rows) > 0, "No jobs parsed from JSON"
    assert isinstance(rows, list), "Expected list of dicts"
    assert isinstance(rows[0], dict), "Expected dict elements"

    # Check expected keys exist in first job
    expected_keys = [
        "job_number",
        "priority",
        "name",
        "owner",
        "state",
        "slots",
        "queue_name",
        "job_type",
    ]

    for key in expected_keys:
        assert key in rows[0], f"Missing expected key: {key}"

    # Check we have running jobs
    running_jobs = [job for job in rows if job["job_type"] == "running"]
    assert len(running_jobs) > 0, "No running jobs found"

    # Verify first job has sanitized data
    first_job = rows[0]
    assert first_job["owner"].startswith("user"), (
        f"Expected sanitized username, got: {first_job['owner']}"
    )

    # Check queue names are sanitized
    assert "example.com" in first_job["queue_name"], (
        f"Expected sanitized hostname in queue, got: {first_job['queue_name']}"
    )


def test_parse_jobinfo_json():
    """Test parsing of qstat -j -json output (single job info format)."""

    filename = RESOURCES / "uge" / "qstat_jobinfo_array.json"

    with open(filename) as f:
        stdout = f.read()

    rows, errors = parse_jobinfo_json(stdout)

    # Should have no errors for this file
    assert len(errors) == 0, f"Expected no errors, got: {errors}"

    # Verify structure
    assert len(rows) > 0, "No jobs returned"
    assert isinstance(rows, list), "Expected list of jobs"

    job = rows[0]

    # Test basic fields with sanitized data
    assert job["job_number"] == 30017751, f"Unexpected job number: {job['job_number']}"
    assert job["job_name"] == "TestJob", f"Unexpected job name: {job['job_name']}"
    assert job["owner"] == "user01", f"Expected sanitized owner 'user01', got: {job['owner']}"

    # Check that paths are sanitized
    assert "cwd" in job, "Missing cwd field"
    assert "/home/user01/" in job["cwd"], f"Expected sanitized path, got: {job['cwd']}"

    # Check supplementary groups are sanitized
    if "supplementary group" in job:
        groups = job["supplementary group"]
        # At least one group should be sanitized (group1, group2, etc.)
        has_sanitized = any(g.startswith("group") for g in groups)
        assert has_sanitized, f"Expected sanitized groups, got: {groups}"


def test_parse_jobinfo_error_json():
    """Test parsing of qstat -j -json for job with error state."""

    filename = RESOURCES / "uge" / "qstat_jobinfo_error.json"

    with open(filename) as f:
        stdout = f.read()

    rows, errors = parse_jobinfo_json(stdout)

    # Verify error lines were extracted
    assert len(errors) > 0, "Expected error lines to be extracted"
    assert any("error reason" in err for err in errors), (
        f"Expected 'error reason' in errors, got: {errors}"
    )

    # Verify the error contains the expected information
    first_error = errors[0]
    assert "Permission denied" in first_error, (
        f"Expected 'Permission denied' in error, got: {first_error}"
    )

    # Verify structure
    assert len(rows) > 0, "No jobs returned"
    assert isinstance(rows, list), "Expected list of jobs"

    job = rows[0]

    # Should have owner field with sanitized data
    assert "owner" in job, "Missing owner field"
    assert job["owner"] == "user01", f"Expected sanitized owner 'user01', got: {job['owner']}"

    # Should have job_name
    assert "job_name" in job, "Missing job_name field"
    assert job["job_name"] == "TestJob", f"Expected job_name 'TestJob', got: {job['job_name']}"

    # Verify job number
    assert job["job_number"] == 30017756, f"Expected job_number 30017756, got: {job['job_number']}"
