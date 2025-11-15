
# UGE Wrapper

Functions that wraps they key functionality of UGE, to get queue info and submit jobs, from a pythonic interface.

# TODO

First version should be able to

- [x] WaitForJobToFinish
- [ ] WaitForJobToFinish-HoldJob
- [x] Task Array TDQM
- [x] Submit task-array
- [x] get qacctj job information
- [x] delete job
- [x] failed jobs exit_codes
- [x] get cluster information
- [ ] script in test/resources/uge to generate example output files (don't use jq)
- [ ] can json load support, if name exist already create a new?

- [ ] Tests
- [ ] types

- [ ] Check molpipe usage
- [ ] Check molpipejob usage

# Extra

- [ ] qmod / qaliter to change concurrent tasks for uge

# Examples

- [ ] split pandas dataframe into chunks and make task-array with python workers


# Commands

Overview of which commands has been wrapped

- [x] `qstat`
- [ ] `qhost`
- [ ] `qmod`
- [ ] `qsh`
- [x] `qsub`
- [x] `qacct`
- [ ] `qalter`
- [ ] `qconf`
- [x] `qdel`
- [ ] `qhold`
- [ ] `qhost`
- [ ] ~~`qlogin`~~
- [ ] `qmake`
- [ ] `qmod`
- [ ] `qmon`
- [ ] `qping`
- [ ] `qquota`
- [ ] `qrdel`
- [ ] `qresub`
- [ ] `qrls`
- [ ] `qrsh`
- [ ] `qrstat`
- [ ] `qrsub`
- [ ] `qselect`
- [ ] `qsh`
- [ ] `qstat`
- [x] `qsub`
- [ ] `qtcsh`

# Job states

| **Category**  | **State**                                      | **UGE Letter Code** |
| ------------- |:-----------------------------------------------| :------------------ |
| Pending       | pending                                        | qw                  |
| Pending       | pending, user hold                             | qw                  |
| Pending       | pending, system hold                           | hqw                 |
| Pending       | pending, user and system hold                  | hqw                 |
| Pending       | pending, user hold, re-queue                   | hRwq                |
| Pending       | pending, system hold, re-queue                 | hRwq                |
| Pending       | pending, user and system hold, re-queue        | hRwq                |
| Pending       | pending, user hold                             | qw                  |
| Pending       | pending, user hold                             | qw                  |
| Running       | running                                        | r                   |
| Running       | transferring                                   | t                   |
| Running       | running, re-submit                             | Rr                  |
| Running       | transferring, re-submit                        | Rt                  |
| Suspended     | obsuspended                                    | s,  ts              |
| Suspended     | queue suspended                                | S, tS               |
| Suspended     | queue suspended by alarm                       | T, tT               |
| Suspended     | allsuspended withre-submit                     | Rs,Rts,RS, RtS, RT, RtT |
| Error         | allpending states with error                   | Eqw, Ehqw, EhRqw        |
| Deleted       | all running and suspended states with deletion | dr,dt,dRr,dRt,ds, dS, dT,dRs, dRS, dRT |


# Environment Variables

Environment Variables found in UGE jobs

| **Variable** | **Description** |
| ------------ | --------------- |
| ARC | The architecture name of the node on which the job is running. The name is compiled into the sge_execd binary. |
| SGE_ROOT | The root directory of the grid engine system as set for sge_execd before startup, or the default /usr/SGE directory. |
| SGE_BINARY_PATH | The directory in which the grid engine system binaries are installed. |
| SGE_CELL | The cell in which the job runs. |
| SGE_JOB_SPOOL_DIR | The directory used by sge_shepherd to store job-related data while the job runs. |
| SGE_O_HOME | The path to the home directory of the job owner on the host from which the job was submitted. |
| SGE_O_HOST | The host from which the job was submitted. |
| SGE_O_LOGNAME | The login name of the job owner on the host from which the job was submitted. |
| SGE_O_MAIL | The content of the MAIL environment variable in the context of the job submission command. |
| SGE_O_PATH | The content of the PATH environment variable in the context of the job submission command. |
| SGE_O_SHELL | The content of the SHELL environment variable in the context of the job submission command. |
| SGE_O_TZ | The content of the TZ environment variable in the context of the job submission command. |
| SGE_O_WORKDIR | The working directory of the job submission command. |
| SGE_CKPT_ENV | The checkpointing environment under which a checkpointing job runs. The checkpointing environment is selected with the qsub -ckpt command. |
| SGE_CKPT_DIR | The path ckpt_dir of the checkpoint interface. Set only for checkpointing jobs. For more information, see the checkpoint(5) man page. |
| SGE_STDERR_PATH | The path name of the file to which the standard error stream of the job is diverted. This file is commonly used for enhancing the output with error messages from prolog, epilog, parallel environment start and stop scripts, or checkpointing scripts. |
| SGE_STDOUT_PATH | The path name of the file to which the standard output stream of the job is diverted. This file is commonly used for enhancing the output with messages from prolog, epilog, parallel environment start and stop scripts, or checkpointing scripts. |
| SGE_TASK_ID | The task identifier in the array job represented by this task. |
| ENVIRONMENT | Always set to BATCH. This variable indicates that the script is run in batch mode. |
| HOME | The user's home directory path as taken from the passwd file. |
| HOSTNAME | The host name of the node on which the job is running. |
| JOB_ID | A unique identifier assigned by the sge_qmaster daemon when the job was submitted. The job ID is a decimal integer from 1 through 9,999,999. |
| JOB_NAME | The job name, which is built from the file name provided with the qsub command, a period, and the digits of the job ID. You can override this default with qsub -N. |
| LOGNAME | The user's login name as taken from the passwd file. |
| NHOSTS | The number of hosts in use by a parallel job. |
| NQUEUES | The number of queues that are allocated for the job. This number is always 1 for serial jobs. |
| NSLOTS | The number of queue slots in use by a parallel job. |
| PATH | A default shell search path of: /usr/local/bin:/usr/ucb:/bin:/usr/bin. |
| PE | The parallel environment under which the job runs. This variable is for parallel jobs only. |
| PE_HOSTFILE | The path of a file that contains the definition of the virtual parallel machine that is assigned to a parallel job by the grid engine system. This variable is used for parallel jobs only. See the description of the $pe_hostfile parameter in sge_pe for details on the format of this file. |
| QUEUE | The name of the queue in which the job is running. |
| REQUEST | The request name of the job. The name is either the job script file name or is explicitly assigned to the job by the qsub -N command. |
| RESTARTED | Indicates whether a checkpointing job was restarted. If set to value 1, the job was interrupted at least once. The job is therefore restarted. |
| SHELL | The user's login shell as taken from the passwd file. |
| TMPDIR | The absolute path to the job's temporary working directory. |
| TMP | The same as TMPDIR. This variable is provided for compatibility with NQS. |
| TZ | The time zone variable imported from sge_execd, if set. |
| USER | The user's login name as taken from the passwd file. |
| OMP_NUM_THREADS | Number of OpenMP threads |
| OPENBLAS_NUM_THREADS | Number of BLAS threads |
| VECLIB_MAXIMUM_THREADS | Number Vectorlib threads |
| MKL_NUM_THREADS | Number of Intel MathKernel threads |
| NUMEXPR_NUM_THREADS | Fast numerical expression evaluator for NumPy |


# Other UGE parsers

- https://github.com/relleums/qstat
