import pandas as pd
import numpy as np
from Levenshtein import distance as lev_distance
from collections import defaultdict



import pandas as pd
from datetime import timedelta
import pandas as pd
from datetime import timedelta
from collections import defaultdict

def lev_win(data, window_days, rep_threshold,  similarity_threshold):
    # Sort the data by HASH and DATE
    data_sorted = data.sort_values(['HASH', 'SURVEY_DATE'])
    
    # Create a fraud DataFrame to store duplications
    fraud_records = []
    
    # Group by HASH to process each individual's records
    for hash_val, group in data_sorted.groupby('HASH'):
        # Iterate through each record in the group
        for i, current_row in group.iterrows():
            current_date = current_row['SURVEY_DATE']
            current_address = str(current_row['ADDRESS'])
            
            # Define the sliding window (prior records)
            window_start = current_date - timedelta(days=window_days)
            prior_window = group[
                (group['SURVEY_DATE'] >= window_start) & 
                (group['SURVEY_DATE'] < current_date)
            ]
            
            # Track similar addresses in the window
            similar_addresses = defaultdict(list)
            
            # Check each record in the prior window
            for _, prior_row in prior_window.iterrows():
                prior_address = str(prior_row['ADDRESS'])
                
                # Calculate Levenshtein distance
                try:
                    distance = lev_distance(current_address, prior_address)
                    
                    # If addresses are similar enough, record the similarity
                    if distance <= similarity_threshold:
                        similar_addresses[current_address].append(prior_address)
                
                except Exception as e:
                    print(f"Error calculating distance between {current_address} and {prior_address}: {e}")
            
            # Check if the current address triggers the fraud condition
            for prior_addr, similar_prior_addrs in similar_addresses.items():
                if len(similar_prior_addrs) > rep_threshold:  # 3rd or more similar address
                    fraud_record = current_row.to_dict()
                    fraud_record['FRAUD_TYPE'] = 'MULTIPLE_ADDRESS_DUPLICATIONS'
                    fraud_record['SIMILAR_PRIOR_ADDRESS'] = prior_addr
                    fraud_record['SIMILAR_ADDRESSES_COUNT'] = len(similar_prior_addrs) + 1
                    
                    fraud_records.append(fraud_record)
    
    # Convert fraud records to DataFrame
    fraud_df = pd.DataFrame(fraud_records)
    
    return fraud_df


def lev_win_multi(data, window_days,  addsim_threshold, possim_threshold, rep_threshold):
    # Sort the data by HASH and DATE
    data_sorted = data.sort_values(['HASH', 'SURVEY_DATE'])
    
    # Create a fraud DataFrame to store duplications
    fraud_records = []
    
    # Group by HASH to process each individual's records
    for hash_val, group in data_sorted.groupby('HASH'):
        # Iterate through each record in the group
        for i, current_row in group.iterrows():
            current_date = current_row['SURVEY_DATE']
            current_address = str(current_row['ADDRESS'])
            current_pos = str(current_row['POSITION'])
            current_ind = str(current_row['INDUSTRY'])

            # Define the sliding window (prior records)
            window_start = current_date - timedelta(days=window_days)
            prior_window = group[
                (group['SURVEY_DATE'] >= window_start) & 
                (group['SURVEY_DATE'] < current_date)
            ]
            
            # Track similar addresses in the window
            similar_addresses = defaultdict(list)
            
            # Check each record in the prior window
            for _, prior_row in prior_window.iterrows():
                prior_address = str(prior_row['ADDRESS'])
                prior_pos = str(prior_row['POSITION'])
                prior_ind = str(prior_row['INDUSTRY'])
                
                # Calculate Levenshtein distance
                try:
                    add_distance = lev_distance(current_address, prior_address)
                    pos_distance = lev_distance(current_pos, prior_pos)
                    ind_distance =  lev_distance(current_ind, prior_ind)

                    
                    # If addresses are similar enough, record the similarity
                    if add_distance <= addsim_threshold & pos_distance <= possim_threshold & ind_distance <=1:
                        similar_addresses[current_address].append(prior_address)
                
                except Exception as e:
                    print(f"Error calculating distance between {current_address} and {prior_address}: {e}")
            
            # Check if the current address triggers the fraud condition
            for prior_addr, similar_prior_addrs in similar_addresses.items():
                if len(similar_prior_addrs) > rep_threshold:  # 3rd or more similar address
                    fraud_record = current_row.to_dict()
                    fraud_record['FRAUD_TYPE'] = 'MULTIPLE_ADD_IND_POS_DUPS'
                    fraud_record['SIMILAR_PRIOR_ADDRESS'] = prior_addr
                    fraud_record['SIMILAR_SURVEY_COUNT'] = len(similar_prior_addrs) + 1
                    
                    fraud_records.append(fraud_record)
    
    # Convert fraud records to DataFrame
    fraud_df = pd.DataFrame(fraud_records)
    
    return fraud_df


# def lev_dist_with_fields(data, lev_distance, period, bool_columns=None, text_columns=None):
#     if bool_columns is None:
#         bool_columns = ['DISBURSED', 'DR1']
#     if text_columns is None:
#         text_columns = ['INDUSTRY', 'POSITION']

#     result_rows = []

#     # Ensure boolean columns exist and are properly formatted
#     data_copy = data.copy()
#     for col in bool_columns:
#         if col in data_copy.columns:
#             data_copy[col] = pd.to_numeric(data_copy[col], errors='coerce').fillna(0).astype(int)
#         else:
#             print(f"Warning: Column '{col}' not found in the data")

#     # Group by HASH and period
#     grouped = data_copy.groupby(['HASH', period])

#     for (hash_val, week), group_df in grouped:
#         unique_combinations = defaultdict(lambda: {
#             'COUNT': 0,
#             **{col: 0 for col in bool_columns}
#         })

#         for _, row in group_df.iterrows():
#             address = str(row['ADDRESS'])
#             industry = str(row['INDUSTRY']) if 'INDUSTRY' in row and pd.notna(row['INDUSTRY']) else "UNKNOWN"
#             position = str(row['POSITION']) if 'POSITION' in row and pd.notna(row['POSITION']) else "UNKNOWN"

#             rep_address = None
#             rep_position = None

#             # Find closest existing address + industry
#             for (existing_addr, existing_industry, existing_pos) in unique_combinations:
#                 try:
#                     if lev_distance(address, existing_addr) <= 3 and industry == existing_industry:
#                         rep_address = (existing_addr, existing_industry)
#                         break
#                 except Exception as e:
#                     print(f"Error in lev_distance between '{address}' and '{existing_addr}': {e}")

#             if rep_address is None:
#                 rep_address = (address, industry)

#             # Find closest position in the matched address group
#             for (addr, ind, pos) in unique_combinations:
#                 if addr == rep_address[0] and ind == rep_address[1]:
#                     try:
#                         if lev_distance(position, pos) <= 5:
#                             rep_position = pos
#                             break
#                     except Exception as e:
#                         print(f"Error in lev_distance between '{position}' and '{pos}': {e}")

#             if rep_position is None:
#                 rep_position = position

#             # Use (Address, Industry, Position) as key
#             rep_key = (rep_address[0], rep_address[1], rep_position)

#             unique_combinations[rep_key]['COUNT'] += 1

#             # Aggregate boolean columns
#             for col in bool_columns:
#                 unique_combinations[rep_key][col] += int(row[col]) if pd.notna(row[col]) else 0

#         # Convert results to rows
#         for (address, industry, position), values in unique_combinations.items():
#             row = {
#                 'HASH': hash_val,
#                 period: week,
#                 'ADDRESS': address,
#                 'INDUSTRY': industry,
#                 'POSITION': position,
#                 'COUNT': values['COUNT'],
#                 **{col: values[col] for col in bool_columns}
#             }
#             result_rows.append(row)

#     return pd.DataFrame(result_rows)

