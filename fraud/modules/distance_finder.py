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


def lev_win_opt(data, window_days, rep_threshold,  similarity_threshold):
    # Sort the data by HASH and DATE
    data_sorted = data.sort_values(['HASH', 'SURVEY_DATE'])
    group = data_sorted.groupby('HASH')
    current_date = group['SURVEY_DATE']
    current_address  = group['ADDRESS']
    current_row = group.iloc(i)
    
    # Create a fraud DataFrame to store duplications
    fraud_records = []
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
            distance = lev_distance(group['SURVEY_DATE'], prior_address)
            
            # If addresses are similar enough, record the similarity
            if distance <= similarity_threshold:
                similar_addresses[current_address].append(prior_address)
        
        except Exception as e:
            print(f"Error calculating distance between {current_address} and {prior_address}: {e}")
    
    # Check if the current address triggers the fraud condition
    # for prior_addr, similar_prior_addrs in similar_addresses.items():
        if len(similar_prior_addrs) > rep_threshold:  # 3rd or more similar address
            fraud_record = current_row.to_dict()
            fraud_record['FRAUD_TYPE'] = 'MULTIPLE_ADDRESS_DUPLICATIONS'
            fraud_record['SIMILAR_PRIOR_ADDRESS'] = prior_addr
            fraud_record['SIMILAR_ADDRESSES_COUNT'] = len(similar_prior_addrs) + 1
            
            fraud_records.append(fraud_record)
    
    # Group by HASH to process each individual's records
    # for hash_val, group in data_sorted.groupby('HASH'):
    #     # # Iterate through each record in the group
        # for i, current_row in group.iterrows():
        #     current_date = current_row['SURVEY_DATE']
        #     current_address = str(current_row['ADDRESS'])
            
            # Define the sliding window (prior records)
            
    
    # Convert fraud records to DataFrame
    fraud_df = pd.DataFrame(fraud_records)
    
    return fraud_df

