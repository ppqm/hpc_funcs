import logging

from .qstat_json import get_qstat_json
from .qstat_text import get_qstat_text

logger = logging.getLogger(__name__)


def get_all_jobs_json():
    """Get all jobs for all users"""
    all_users = "\\*"
    all_users = '"*"'
    df = get_qstat_json(users=[all_users])
    return df


def get_all_jobs_text():
    """Get all jobs for all users"""
    all_users = "\\*"
    all_users = '"*"'
    df = get_qstat_text(users=[all_users])
    return df
