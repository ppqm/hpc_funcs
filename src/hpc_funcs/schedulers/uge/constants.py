COMMAND_SUBMIT = "qsub"
COMMAND_INTERACTIVE = "qrsh"

FLAG_SYNC = "-sync y"

TAGS_PENDING = ["qw", "hqw", "hRwq"]
TAGS_RUNNING = ["r", "t", "Rr", "Rt", "x"]
TAGS_SUSPENDED = ["s", "ts", "S", "tS", "T", "tT", "Rs", "Rts", "RS", "RtS", "RT", "RtT"]
TAGS_ERROR = ["Eqw", "Ehqw", "EhRqw"]
TAGS_DELETED = ["dr", "dt", "dRr", "dRt", "ds", "dS", "dT", "dRs", "dRS", "dRT"]

TASK_IDENTIFIER = "$SGE_TASK_ID"

UGE_TMPDIR = "TMPDIR"
UGE_CORES = "NSLOTS"

UGE_ENVIRONMENT_VARIABLES = [
    "ARC",
    "SGE_ROOT",
    "SGE_BINARY_PATH",
    "SGE_CELL",
    "SGE_JOB_SPOOL_DIR",
    "SGE_O_HOME",
    "SGE_O_HOST",
    "SGE_O_LOGNAME",
    "SGE_O_MAIL",
    "SGE_O_PATH",
    "SGE_O_SHELL",
    "SGE_O_TZ",
    "SGE_O_WORKDIR",
    "SGE_CKPT_ENV",
    "SGE_CKPT_DIR",
    "SGE_STDERR_PATH",
    "SGE_STDOUT_PATH",
    "SGE_TASK_ID",
    "ENVIRONMENT",
    "HOME",
    "HOSTNAME",
    "JOB_ID",
    "JOB_NAME",
    "LOGNAME",
    "NHOSTS",
    "NQUEUES",
    "NSLOTS",
    "PATH",
    "PE",
    "PE_HOSTFILE",
    "QUEUE",
    "REQUEST",
    "RESTARTED",
    "SHELL",
    "TMPDIR",
    "TMP",
    "TZ",
    "USER",
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "HOSTNAME",
]

UGE_COMMANDS = [
    "qacct",
    "qalter",
    "qconf",
    "qdel",
    "qhold",
    "qhost",
    "qlogin",
    "qmake",
    "qmod",
    "qmon",
    "qping",
    "qquota",
    "qrdel",
    "qresub",
    "qrls",
    "qrsh",
    "qrstat",
    "qrsub",
    "qselect",
    "qsh",
    "qstat",
    "qsub",
    "qtcsh",
]
