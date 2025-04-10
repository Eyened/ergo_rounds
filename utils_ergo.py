import pickle 
import pandas as pd
import numpy as np
    

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
               ('2021-03-01', '2028-01-01', 'Rotterdam Study 2'): ('e8', 1),
               ('2021-03-01', '2028-01-01', 'Rotterdam Study 3'): ('e8', 2)}

# Creates a lookup per project_name, visit_nr
visit_order_lookup = {
    (project_name, visit_nr): prefix
    for (start, end, project_name), (prefix, visit_nr) in date_ranges.items()
}

# Convert dates to datetime
date_ranges = {(pd.to_datetime(start), pd.to_datetime(end), project_name): value for (start, end, project_name), value in date_ranges.items()}


def choose_prefix(row, 
                  date_column='visit_date', 
                  study_column='study_id',
                  visit_number_column='visit_nr'):
    
    '''
    Choose the correct round prefix from the date_ranges (dct) -ergo wiki-, based on the visit number and the cohort name.
    If no match is found for the visit, then determine the minimum distance to the mid point for other visits.
    '''
    
    closest_distance = float('inf')
    closest_prefix = None

    for (start_date, end_date, project_name), (prefix, visit_number) in date_ranges.items():
        # Direct match
        if (start_date <= row[date_column] <= end_date) and \
           (row[study_column] == project_name) and \
           (row[visit_number_column] == visit_number):
            return prefix

    # Fallback: no direct match
    visit_date = row[date_column]
    study_id = row[study_column]
    visit_nr = row[visit_number_column]

    # Check distances to midpoint of current visit_nr
    for (start_date, end_date, project_name), (prefix, visit_number) in date_ranges.items():
        if project_name == study_id and visit_number == visit_nr:
            midpoint = start_date + (end_date - start_date) / 2
            distance = abs((visit_date - midpoint).days)
            if distance < closest_distance:
                closest_distance = distance
                closest_prefix = prefix

    # Check distances to midpoint of next visit_nr ranges
    for (start_date, end_date, project_name), (prefix, visit_number) in date_ranges.items():
        if project_name == study_id and visit_number == visit_nr + 1:
            midpoint = start_date + (end_date - start_date) / 2
            distance = abs((visit_date - midpoint).days)
            if distance < closest_distance:
                closest_distance = distance
                closest_prefix = prefix 

    return closest_prefix


def correct_rounds_from_lookup(df, visit_order_lookup, round_column='round'):
    '''
    If no exact match is found in choose_prefix() and the midpoints are used to determine the closest visit,
    it is recommended to check the visit order and correct possible mistakes. This could be the case, especially for: RS-I-1, RS-II-2, RS-II-3. 
    '''
    df_ = df.copy()
    df_ = df_.sort_values(['ergo_id', 'study_id', 'visit_date'])

    for (ergo_id, study_id), group in df_.groupby(['ergo_id', 'study_id']):
        round_counts = group[round_column].value_counts()
        duplicated_rounds = round_counts[round_counts > 1].index.tolist()

        # Get all rows with duplicated round
        # Sort them by visit_nr to find the one to correct
        for dup_round in duplicated_rounds:
            round_rows = group[group[round_column] == dup_round]
            round_rows = round_rows.sort_values('visit_nr')

            first_idx = round_rows.index[0]
            visit_nr = df_.at[first_idx, 'visit_nr']

            # Look up what the correct round should have been
            corrected = visit_order_lookup.get((study_id, visit_nr))
            if corrected:
                df_.at[first_idx, round_column] = corrected

    return df_
