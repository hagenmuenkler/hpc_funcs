import os

import pytest
from conftest import RESOURCES

from hpc_funcs import lmod

try:
    lmod.get_lmod_executable()
except RuntimeError:
    pytest.skip("Could not find LMOD executable", allow_module_level=True)


MODULE_PATH = RESOURCES.absolute() / "lmod" / "modules"
MODULE_NAME = "test"


def test_use() -> None:
    lmod.use(MODULE_PATH)

    paths = os.environ.get("MODULEPATH")
    print(paths)

    assert os.environ.get("MODULEPATH") is not None, "No module path to be found"
    assert str(MODULE_PATH) in os.environ.get("MODULEPATH", ""), (
        "Unable to find loaded module path"
    )


def test_load_os() -> None:
    lmod.use(MODULE_PATH)

    print(os.environ.get("MODULEPATH"))

    # Check use
    assert str(MODULE_PATH) in os.environ.get("MODULEPATH", ""), (
        "Unable to find loaded module path"
    )

    lmod.load(MODULE_NAME)

    # Check env is set
    assert os.environ.get("TESTLMODMODULE") == "THIS IS A TEST"

    # Check path is updated
    binpaths = os.environ.get("PATH", "").split(":")
    print(binpaths)
    assert "/does/not/exist/bin" in binpaths

    # Check module loaded is in list
    # NOTE Does not work, because module load only updates the python environ
    # modules_loaded = lmod.get_modules()
    # print(modules_loaded)
    # assert MODULE_NAME in list(modules_loaded.values())


def test_load_return() -> None:
    lmod.use(MODULE_PATH)

    print(os.environ.get("MODULEPATH"))

    # Check use
    assert str(MODULE_PATH) in os.environ.get("MODULEPATH", ""), (
        "Unable to find loaded module path"
    )

    update_dict = lmod.get_load_environment(MODULE_NAME)

    # Check env is set
    assert update_dict.get("TESTLMODMODULE") == "THIS IS A TEST"

    # Check path is updated
    binpaths = update_dict.get("PATH", "").split(":")
    print(binpaths)
    assert "/does/not/exist/bin" in binpaths
