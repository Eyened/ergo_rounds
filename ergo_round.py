import pickle 
import pandas as pd
import numpy as np
from dataclasses import dataclass

date_ranges = {('1989-07-01', '1993-09-01', 'Rotterdam Study 1'): ('e1', 1),  # Value = ergo nr. , follow-up nr.
               ('1993-09-02', '1995-12-31', 'Rotterdam Study 1'): ('e2', 2), 
               ('1997-03-01', '1999-12-31', 'Rotterdam Study 1'): ('e3', 3),
               ('2000-02-01', '2001-12-31', 'Rotterdam Study 2'): ('ep', 1),  
               ('2002-01-01', '2004-07-30', 'Rotterdam Study 1'): ('e4', 4),  
               ('2004-07-01', '2005-12-31', 'Rotterdam Study 2'): ('e4', 2),  
               ('2006-02-01', '2008-12-31', 'Rotterdam Study 3'): ('ej', 1),
               ('2009-03-01', '2011-01-31', 'Rotterdam Study 1'): ('e5', 5),
               ('2011-02-01', '2012-02-28', 'Rotterdam Study 2'): ('e5', 3),
               ('2012-03-01', '2014-06-30', 'Rotterdam Study 3'): ('e5', 2),
               ('2014-05-01', '2015-07-30', 'Rotterdam Study 1'): ('e6', 6),
               ('2015-04-01', '2016-05-31', 'Rotterdam Study 2'): ('e6', 4),
               ('2016-06-01', '2020-12-31', 'Rotterdam Study 4'): ('ex', 1),
               ('2018-04-01', '2019-12-31', 'Rotterdam Study 1'): ('e7', 7),
               ('2021-03-01', '2024-09-30', 'Rotterdam Study 2'): ('e8', 5),
               ('2021-03-01', '2024-09-30', 'Rotterdam Study 3'): ('e8', 2)}

# Creates a lookup per project_name, visit_nr
visit_order_lookup = {
    (project_name, visit_nr): prefix
    for (start, end, project_name), (prefix, visit_nr) in date_ranges.items()
}

# Convert dates to datetime
date_ranges = {(pd.to_datetime(start), pd.to_datetime(end), project_name): value for (start, end, project_name), value in date_ranges.items()}


@dataclass
class ColumnConfig:
    id_column: str = 'ergo_id'
    date_column: str = 'visit_date'
    study_column: str = 'study_id'
    visit_number_column: str = 'visit_nr'
    round_column: str = 'round'
    
    

def choose_prefix(row, config: ColumnConfig):
    '''
    Determines the appropriate round prefix for a participant's visit based on visit date, study cohort,
    and visit number using predefined `date_ranges`.

    Args:
        row (pd.Series): A single row from a DataFrame.
        config (ColumnConfig): Configuration class with column names for flexible usage.

    Returns:
        str: The corresponding prefix (e.g., 'e4', 'ep', etc.), or the closest match if no exact range match is found.

    Logic:
        1. Attempt exact match on date range, cohort name, and visit number.
        2. If no exact match:
            a. Check distance to midpoint of the current visit range.
            b. If still unmatched, check distance to midpoint of the next visit.
            c. If still unmatched and above a 3-year threshold, search all midpoints within the same cohort.
        
    Usage Example:
        df['round'] = df.apply(lambda row: choose_prefix(row, config), axis=1)
    '''
    # Load ColumnConfig
    date_column = config.date_column
    study_column = config.study_column
    visit_number_column = config.visit_number_column
    
    closest_distance = float('inf')
    closest_distance_threshold = 1096 # 3 years 
    closest_prefix = None

    # 1. Direct match. If no visits are missing, this will yield a match
    for (start_date, end_date, project_name), (prefix, visit_number) in date_ranges.items():
        
        if (start_date <= row[date_column] <= end_date) and \
           (row[study_column] == project_name) and \
           (row[visit_number_column] == visit_number):
            return prefix

    # Fallback: no direct match
    visit_date = row[date_column]
    study_id = row[study_column]
    visit_nr = row[visit_number_column]

    # 2. Check distances to midpoint of current visit
    for (start_date, end_date, project_name), (prefix, visit_number) in date_ranges.items():
        if project_name == study_id and visit_number == visit_nr:
            midpoint = start_date + (end_date - start_date) / 2
            distance = abs((visit_date - midpoint).days)
            if distance < closest_distance:
                closest_distance = distance
                closest_prefix = prefix

    # 3. Check distances to midpoint of next visit
    for (start_date, end_date, project_name), (prefix, visit_number) in date_ranges.items():
        if project_name == study_id and visit_number == visit_nr + 1:
            midpoint = start_date + (end_date - start_date) / 2
            distance = abs((visit_date - midpoint).days)
            if distance < closest_distance:
                closest_distance = distance
                closest_prefix = prefix 

    # 4. Check distances to midpoint for all visits
    if closest_distance > closest_distance_threshold:  # 5 years in days
        for (start_date, end_date, project_name), (prefix, visit_number) in date_ranges.items():
            if project_name == study_id:
                midpoint = start_date + (end_date - start_date) / 2
                distance = abs((visit_date - midpoint).days)
                if distance < closest_distance:
                    closest_distance = distance
                    closest_prefix = prefix
    return closest_prefix


def correct_rounds_from_lookup(df, config: ColumnConfig):
    '''
    Resolves duplicated round prefixes for individual participants by correcting them based on expected visit order.

    This is useful when choose_prefix() falls back to midpoint-based assignment, which may cause duplicates
    for certain cases like RS-I-1, RS-II-2, RS-II-3, etc.

    Args:
        df (pd.DataFrame): DataFrame containing participant visit information.
        config (ColumnConfig): Configuration class specifying column names.

    Returns:
        pd.DataFrame: DataFrame with corrected round values where duplicates are resolved.

    Logic:
        - Sort visits by ID, study, and date.
        - For each participant/study combination, detect duplicated rounds.
        - For each duplicated round, correct the first occurrence using the (study, visit_nr) -> prefix mapping.

    Usage Example:
        df_corrected = correct_rounds_from_lookup(df, config)
    '''
    # Load config
    id_column = config.id_column
    round_column = config.round_column
    date_column = config.date_column
    study_column = config.study_column
    visit_number_column = config.visit_number_column

    
    df_ = df.copy()
    df_ = df_.sort_values([id_column, study_column, date_column])

    for (ergo_id, study_id), group in df_.groupby([id_column, study_column]):
        round_counts = group[round_column].value_counts()
        duplicated_rounds = round_counts[round_counts > 1].index.tolist()

        # Get all rows with duplicated round
        # Sort them by visit_nr to find the one to correct
        for dup_round in duplicated_rounds:
            round_rows = group[group[round_column] == dup_round]
            round_rows = round_rows.sort_values(visit_number_column)

            first_idx = round_rows.index[0]
            visit_nr = df_.at[first_idx, visit_number_column]

            # Look up what the correct round should have been
            corrected = visit_order_lookup.get((study_id, visit_nr))
            if corrected:
                df_.at[first_idx, round_column] = corrected

    return df_
