import os
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest

RESOURCES = Path("tests/resources/")


@pytest.fixture(scope="module")
def global_tmp_path() -> Generator[Path, None, None]:
    """Make a temporary directory in home

    Check GLOBAL_SCRATCH

    Home is a globally mounted directory and therefore safe for distributed scheduler usage.
    """

    global_scratch_str = os.environ.get("GLOBAL_SCRATCH", None)

    if global_scratch_str is None:
        global_scratch_str = str(Path.home() / "tmp")

    global_scratch_path = Path(global_scratch_str)

    random_name = next(tempfile._get_candidate_names())  # type: ignore[attr-defined]

    tmp_path = global_scratch_path / f"pytest_{random_name}"
    tmp_path.mkdir(parents=True, exist_ok=True)

    yield tmp_path

    # Force clean
    # shutil.rmtree(tmp_path)
    # assert not tmp_path.is_dir()
