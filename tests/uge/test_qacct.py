import pandas as pd
from conftest import RESOURCES  # type: ignore

from hpc_funcs.schedulers.uge import qacct


def test_parse_text():
    filename = RESOURCES / "uge" / "qacct_array.txt"

    with open(filename) as f:
        stdout = f.read()

    data = qacct.parse_qacct(stdout)

    print(data)

    assert len(data) == 2
    assert "owner" in data[0]
    assert "taskid" in data[0]
    assert "exit_status" in data[0]

    # also you can read it as a dataframe

    pdf = pd.DataFrame(data)
    assert len(pdf) == 2
