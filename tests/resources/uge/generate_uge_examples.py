import time
from pathlib import Path

from hpc_funcs.schedulers.uge.constants import TASK_ENVIRONMENT_VARIABLE
from hpc_funcs.schedulers.uge.qdel import delete_job
from hpc_funcs.schedulers.uge.qsub import submit_script
from hpc_funcs.schedulers.uge.submission import generate_taskarray_script
from hpc_funcs.shell import execute


def generate_taskarray_log(global_tmp_path: Path):

    # Need network accessible folder
    tmp_path = global_tmp_path

    # Generate bash script
    success_string = "finished work"
    command = f'sleep 5 & echo "{success_string} {TASK_ENVIRONMENT_VARIABLE}"'
    log_dir = tmp_path / "uge_testlogs"

    script: str = generate_taskarray_script(
        command,
        cores=1,
        cwd=tmp_path,
        log_dir=log_dir,
        name="TestJob",
        task_concurrent=1,
        task_stop=2,
    )

    job_id, _ = submit_script(script, scr=tmp_path)

    time.sleep(3)

    execute(f"qstat -nenv -j {job_id} > {tmp_path}/qstat_jobinfo_array.txt")
    execute(f"qstat -nenv -j {job_id} -xml > {tmp_path}/qstat_jobinfo_array.xml")
    execute(f"qstat -nenv -j {job_id} -json > {tmp_path}/qstat_jobinfo_array.json")

    delete_job(job_id)


def generate_errorjob_log(global_tmp_path: Path):

    # Need network accessible folder
    tmp_path = global_tmp_path

    # Generate bash script
    command = f'set -e; sleep 1; if test "{TASK_ENVIRONMENT_VARIABLE}" == 2; then command_does_not_exist; else pwd; fi'
    log_dir = tmp_path / "uge_testlogs"

    script: str = generate_taskarray_script(
        command,
        cores=1,
        cwd=tmp_path,
        log_dir=log_dir,
        name="TestJob",
        task_concurrent=1,
        task_stop=2,
    )

    job_id, _ = submit_script(script, scr=tmp_path)

    time.sleep(20)

    execute(f"qstat -nenv -j {job_id} > {tmp_path}/qstat_jobinfo_error.txt")
    execute(f"qstat -nenv -j {job_id} -xml > {tmp_path}/qstat_jobinfo_error.xml")
    execute(f"qstat -nenv -j {job_id} -json > {tmp_path}/qstat_jobinfo_error.json")

    delete_job(job_id)


if __name__ == "__main__":

    tmp_path = Path("./examples")
    tmp_path.mkdir()

    generate_taskarray_log(tmp_path)
    generate_errorjob_log(tmp_path)
