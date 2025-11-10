"""UGE monitoring functions for querying host and job status."""

import json
import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from hpce_utils.shell import execute

logger = logging.getLogger(__name__)


def get_qhost(
    hostnames: Optional[List[str]] = None,
    resource_attributes: Optional[List[str]] = None,
    users: Optional[List[str]] = None,
    show_jobs: bool = False,
    show_queues: bool = False,
    show_totals: bool = False,
    resource_filter: Optional[str] = None,
) -> pd.DataFrame:
    """Get host information from UGE using qhost -json.

    This is a wrapper around the UGE qhost command that returns host information in a pandas DataFrame format.

    Args:
        hostnames: List of specific hostnames to query. If None, queries all hosts.
        resource_attributes: List of resource attributes to display (e.g., ["gpu", "mem_free"]).
        users: List of users to filter jobs by (only used if show_jobs=True).
        show_jobs: Include job information hosted by each host.
        show_queues: Include queue information hosted by each host.
        show_totals: Show total amount of resources (only with resource_attributes).
        resource_filter: Resource filter string in format "attr=val,..." (e.g., "arch=lx-amd64").

    Returns:
        DataFrame with host information. Each row represents a host with columns for host properties, resource values, and optionally queue/job information.

    Raises:
        json.JSONDecodeError: If the JSON output from qhost is malformed.

    Examples:
        >>> # Get all hosts
        >>> df = get_qhost()

        >>> # Get specific hosts with GPU information
        >>> df = get_qhost(hostnames=["node01", "node02"], resource_attributes=["gpu", "gpu_arch"])

        >>> # Get hosts with jobs for specific users
        >>> df = get_qhost(users=["user1", "user2"], show_jobs=True)

        >>> # Filter to hosts with specific architecture
        >>> df = get_qhost(resource_filter="arch=lx-amd64")
    """

    # Build qhost command
    cmd = "qhost -json"

    # Add hostname filter
    if hostnames:
        hostname_list = ",".join(hostnames)
        cmd += f" -h {hostname_list}"

    # Add resource attributes to display
    if resource_attributes:
        attr_list = ",".join(resource_attributes)
        cmd += f" -F {attr_list}"

    # Add resource filter
    if resource_filter:
        cmd += f" -l {resource_filter}"

    # Add user filter (only works with -j)
    if users:
        user_list = ",".join(users)
        cmd += f" -u {user_list}"
        show_jobs = True  # Implicitly enable job display

    # Add flags
    if show_jobs:
        cmd += " -j"

    if show_queues:
        cmd += " -q"

    if show_totals and resource_attributes:
        cmd += " -st"

    # Execute command
    logger.debug(f"Executing: {cmd}")
    stdout, stderr = execute(
        cmd,
    )

    if stderr:
        logger.warning(f"qhost stderr: {stderr}")

    # Parse JSON output
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        logger.error(f"Failed to parse qhost JSON output: {stdout[:500]}")
        raise exc

    # Convert to DataFrame
    df = _parse_qhost_json(data, include_jobs=show_jobs, include_queues=show_queues)

    return df


def _parse_qhost_json(
    data: Dict[str, Any],
    include_jobs: bool = False,
    include_queues: bool = False,
) -> pd.DataFrame:
    """Parse qhost JSON output into a pandas DataFrame.

    Args:
        data: Raw JSON dictionary from qhost -json.
        include_jobs: Whether to include job information columns.
        include_queues: Whether to include queue information columns.

    Returns:
        DataFrame with one row per host, columns for host values and resources.
    """
    if "qhost" not in data:
        logger.warning("No 'qhost' key in JSON data")
        return pd.DataFrame()

    hosts = data["qhost"]

    if not hosts:
        logger.warning("Empty qhost data")
        return pd.DataFrame()

    rows = []

    for host in hosts:
        row = {"hostname": host.get("name", "")}

        # Parse host_values (arch, num_proc, load, memory, etc.)
        if "host_values" in host:
            for item in host["host_values"]:
                name = item.get("name", "")
                value = item.get("value", "")
                if name:
                    row[name] = value

        # Parse resource_values (custom resources like GPU, etc.)
        if "resource_values" in host:
            for item in host["resource_values"]:
                name = item.get("name", "")
                value = item.get("value", "")
                dominance = item.get("dominance", "")
                if name:
                    row[f"resource_{name}"] = value
                    if dominance:
                        row[f"resource_{name}_dominance"] = dominance

        # Parse queues if requested
        if include_queues and "queues" in host:
            queues = host["queues"]
            row["num_queues"] = len(queues)
            if queues:
                # Store queue names and info
                queue_names = [q.get("name", "") for q in queues]
                row["queue_names"] = ",".join(queue_names)

                # Sum up slots if available
                total_slots = sum(q.get("slots_total", 0) for q in queues)
                used_slots = sum(q.get("slots_used", 0) for q in queues)
                row["queue_slots_total"] = total_slots
                row["queue_slots_used"] = used_slots

        # Parse jobs if requested
        if include_jobs and "jobs" in host:
            jobs = host["jobs"]
            row["num_jobs"] = len(jobs)
            if jobs:
                # Store job IDs
                job_ids = [str(j.get("job_number", "")) for j in jobs]
                row["job_ids"] = ",".join(job_ids)

                # Sum up slots used by jobs
                total_job_slots = sum(j.get("slots", 0) for j in jobs)
                row["job_slots_used"] = total_job_slots

        rows.append(row)

    df = pd.DataFrame(rows)

    # Skip the "global" summary row if present
    if not df.empty and df.iloc[0]["hostname"] == "global":
        df = df.iloc[1:].reset_index(drop=True)

    return df


def get_gpu_nodes(
    gpu_resource_name: str = "gpu",
    min_gpus: int = 1,
    max_retries: int = 3,
    update_interval: int = 5,
) -> pd.DataFrame:
    """Get information about GPU-enabled nodes.

    Convenience function to query hosts with GPU resources.

    Args:
        gpu_resource_name: Name of the GPU resource in UGE (default: "gpu").
        min_gpus: Minimum number of GPUs required (default: 1).
        max_retries: Maximum number of retries for the qhost command.
        update_interval: Seconds to wait between retries.

    Returns:
        DataFrame with GPU node information.

    Examples:
        >>> # Get all nodes with GPUs
        >>> df = get_gpu_nodes()

        >>> # Get nodes with at least 4 GPUs
        >>> df = get_gpu_nodes(min_gpus=4)
    """
    # Query with GPU resource attributes
    df = get_qhost(
        resource_attributes=[gpu_resource_name, "gpu_arch", "gpu_type"],
        max_retries=max_retries,
        update_interval=update_interval,
    )

    # Filter to hosts with GPUs
    gpu_col = f"resource_{gpu_resource_name}"
    if gpu_col in df.columns:
        # Convert to numeric, treating "-" as 0
        df[gpu_col] = pd.to_numeric(df[gpu_col], errors="coerce").fillna(0)
        df = df[df[gpu_col] >= min_gpus]

    return df


def get_cluster_resources_summary() -> Dict[str, Any]:
    """Get a summary of total cluster resources.

    Returns:
        Dictionary with aggregate statistics about the cluster.

    Examples:
        >>> summary = get_cluster_resources_summary()
        >>> print(f"Total cores: {summary['total_cores']}")
        >>> print(f"Average load: {summary['avg_load']:.2f}")
    """
    df = get_qhost()

    summary = {}

    # Convert numeric columns
    if "num_proc" in df.columns:
        num_proc = pd.to_numeric(df["num_proc"], errors="coerce")
        summary["total_cores"] = int(num_proc.sum())
        summary["num_hosts"] = len(df)

    if "np_load_avg" in df.columns:
        load_avg = pd.to_numeric(df["np_load_avg"], errors="coerce")
        summary["avg_load"] = float(load_avg.mean())
        summary["total_load"] = float(load_avg.sum())

    # Parse memory (convert from "503.6G" format to GB)
    if "mem_total" in df.columns:
        mem_total_gb = df["mem_total"].apply(_parse_memory_value)
        mem_used_gb = df["mem_used"].apply(_parse_memory_value) if "mem_used" in df.columns else 0
        summary["total_memory_gb"] = float(mem_total_gb.sum())
        summary["used_memory_gb"] = float(mem_used_gb.sum())
        summary["free_memory_gb"] = summary["total_memory_gb"] - summary["used_memory_gb"]

    return summary


def _parse_memory_value(value: str) -> float:
    """Parse memory value from UGE format (e.g., '503.6G', '32M') to GB.

    Args:
        value: Memory string from UGE (e.g., "503.6G", "1024M", "2T").

    Returns:
        Memory value in gigabytes.
    """
    if not isinstance(value, str) or value == "-" or not value:
        return 0.0

    # Extract number and unit
    value = value.strip()
    if value[-1].isalpha():
        unit = value[-1].upper()
        number = float(value[:-1])
    else:
        return float(value)  # Assume GB if no unit

    # Convert to GB
    if unit == "K":
        return number / (1024 * 1024)
    elif unit == "M":
        return number / 1024
    elif unit == "G":
        return number
    elif unit == "T":
        return number * 1024
    else:
        return number
