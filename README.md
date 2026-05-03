# hpc_funcs

Python utilities for interacting with HPC clusters — shell execution, environment management, LMOD module system, and UGE scheduler.

## Install

```bash
pip install hpc_funcs
```

## Usage

### LMOD

Want to use LMOD to load modules within a python environment? For example in a notebook?

```python
from hpc_funcs import lmod

# Load a module and apply env changes to current process
lmod.use("/folder/with/modules")
lmod.load("program")
```

### UGE (Univa Grid Engine)

UGE is not the most popular scheduler, so having pythonic interface is very convenient.

```python
from hpc_funcs.schedulers.uge import submission, qstat, qacct, qdel

# Generate and submit a job script
script = submission.generate_script(
    cmd="python run.py",
    name="myjob",
    cores=8,
    mem="16G",
    hours=4,
)
script_path = submission.write_script(script, directory="/tmp/jobs")
job_id = submission.submit_script(script_path)

# Monitor status
jobs = qstat.get_qstat_json()

# Fetch accounting info after completion
info = qacct.get_job_accounting(job_id)
```
