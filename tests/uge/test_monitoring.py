import pandas as pd
import pytest
from pandas import Series

from hpc_funcs.schedulers.uge import has_uge
from hpc_funcs.schedulers.uge.monitoring import get_cluster_usage

pd.set_option("display.max_columns", None)

if not has_uge():
    pytest.skip("Could not find UGE executable", allow_module_level=True)


def test_cluster_usage():

    pdf_counts = get_cluster_usage()

    assert isinstance(pdf_counts, Series)

    return
