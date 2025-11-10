# def get_cluster_usage() -> DataFrame:
#     """Get cluster usage information, grouped by users

#     To get totla cores in use `pdf["slots"].sum()`
#     """

#     stdout, _ = execute("qstat -u \\*")  # noqa: W605
#     pdf = parse_qstat(stdout)

#     # filter to running
#     pdf = pdf[pdf.state.isin(running_tags)]

#     counts = pdf.groupby(["user"])["slots"].agg("sum")
#     counts = counts.sort_values()  # type: ignore

#     return counts
