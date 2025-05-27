import pandas as pd
import numpy as np
from collections import defaultdict

from ergo_round import date_ranges

# Construct a lookup mapping each study name to an ordered list of visit prefixes
# Ordered by visit number. Depends on: data_ranges(dict).
cohort_prefix_order_lookup = defaultdict(list)
for (start_date, end_date, study_name), (prefix, visit_number) in sorted(
    date_ranges.items(), key=lambda item: (item[0][2], item[1][1])
):
    cohort_prefix_order_lookup[study_name].append(prefix)


def get_visit_number(row: pd.Series) -> int | None:
    """
    Retrieve the visit number for a given row based on the study ID and prefix.

    Parameters:
    ----------
    row : pd.Series
        A row from a DataFrame containing at least 'study_id' and 'prefix'.

    Returns:
    -------
    int | None
        The visit number (1-based index) corresponding to the prefix within the study,
        or None if the prefix is not found.
    """
    study_prefixes = cohort_prefix_order_lookup.get(row['study_id'], [])
    try:
        return study_prefixes.index(row['prefix']) + 1
    except ValueError:
        return None
