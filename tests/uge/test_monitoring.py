import pandas as pd

from hpc_funcs.schedulers.uge import has_uge

pd.set_option("display.max_columns", None)

if not has_uge():
    pytest.skip("Could not find UGE executable", allow_module_level=True)


def test_all_jobs():

    return
