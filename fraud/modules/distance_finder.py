import pandas as pd
import numpy as np
from Levenshtein import distance as lev_distance
from collections import defaultdict



import pandas as pd
from datetime import timedelta
import pandas as pd
from datetime import timedelta
from collections import defaultdict

def lev_win(data, window_days, rep_threshold, similarity_threshold):
    # Sort the data by HASH and DATE
    data_sorted = data.sort_values(['HASH', 'SURVEY_DATE'])
    
    # Ensure SURVEY_DATE is datetime
    if not pd.api.types.is_datetime64_any_dtype(data_sorted['SURVEY_DATE']):
        data_sorted['SURVEY_DATE'] = pd.to_datetime(data_sorted['SURVEY_DATE'])
    
    # Create a fraud DataFrame to store duplications
    fraud_records = []
    
    # Group by HASH to process each individual's records
    for hash_val, group in data_sorted.groupby('HASH'):
        # Convert addresses to strings once
        addresses = group['ADDRESS'].astype(str).values
        dates = pd.to_datetime(group['SURVEY_DATE']).values  # Ensure dates are datetime64
        
        # Pre-calculate window indices for each record
        window_indices = []
        for i, current_date in enumerate(dates):
            window_start = pd.Timestamp(current_date) - pd.Timedelta(days=window_days)
            window_mask = (dates >= window_start) & (dates < current_date)
            window_indices.append(np.where(window_mask)[0])
        
        # Process each record
        for i, (current_address, current_row) in enumerate(zip(addresses, group.itertuples())):
            # Get indices of records in the window
            prior_indices = window_indices[i]
            
            if len(prior_indices) == 0:
                continue
                
            # Calculate distances for all prior addresses at once
            prior_addresses = addresses[prior_indices]
            distances = np.array([lev_distance(current_address, addr) for addr in prior_addresses])
            
            # Find similar addresses
            similar_mask = distances <= similarity_threshold
            similar_count = np.sum(similar_mask)
            
            if similar_count > rep_threshold:
                # Get the most similar prior address
                most_similar_idx = prior_indices[np.argmin(distances)]
                most_similar_addr = addresses[most_similar_idx]
                
                fraud_record = {
                    'HASH': current_row.HASH,
                    'SURVEY_DATE': current_row.SURVEY_DATE,
                    'ADDRESS': current_row.ADDRESS,
                    'FRAUD_TYPE': 'MULTIPLE_ADDRESS_DUPLICATIONS',
                    'SIMILAR_PRIOR_ADDRESS': most_similar_addr,
                    'SIMILAR_ADDRESSES_COUNT': similar_count + 1
                }
                
                # Add any additional columns from the original data
                for col in group.columns:
                    if col not in fraud_record:
                        fraud_record[col] = getattr(current_row, col)
                
                fraud_records.append(fraud_record)
    
    # Convert fraud records to DataFrame
    fraud_df = pd.DataFrame(fraud_records)
    
    return fraud_df


def lev_win_multi(data, window_days, addsim_threshold, possim_threshold, rep_threshold):
    # Sort the data by HASH and DATE
    data_sorted = data.sort_values(['HASH', 'SURVEY_DATE'])
    
    # Ensure SURVEY_DATE is datetime
    if not pd.api.types.is_datetime64_any_dtype(data_sorted['SURVEY_DATE']):
        data_sorted['SURVEY_DATE'] = pd.to_datetime(data_sorted['SURVEY_DATE'])
    
    # Create a fraud DataFrame to store duplications
    fraud_records = []
    
    # Group by HASH to process each individual's records
    for hash_val, group in data_sorted.groupby('HASH'):
        # Convert all relevant fields to strings once
        addresses = group['ADDRESS'].astype(str).values
        positions = group['POSITION'].astype(str).values
        industries = group['INDUSTRY'].astype(str).values
        dates = pd.to_datetime(group['SURVEY_DATE']).values
        
        # Pre-calculate window indices for each record
        window_indices = []
        for i, current_date in enumerate(dates):
            window_start = pd.Timestamp(current_date) - pd.Timedelta(days=window_days)
            window_mask = (dates >= window_start) & (dates < current_date)
            window_indices.append(np.where(window_mask)[0])
        
        # Process each record
        for i, (current_address, current_pos, current_ind, current_row) in enumerate(
            zip(addresses, positions, industries, group.itertuples())
        ):
            # Get indices of records in the window
            prior_indices = window_indices[i]
            
            if len(prior_indices) == 0:
                continue
                
            # Get prior records' data
            prior_addresses = addresses[prior_indices]
            prior_positions = positions[prior_indices]
            prior_industries = industries[prior_indices]
            
            # Calculate distances for all fields at once
            add_distances = np.array([lev_distance(current_address, addr) for addr in prior_addresses])
            pos_distances = np.array([lev_distance(current_pos, pos) for pos in prior_positions])
            ind_distances = np.array([lev_distance(current_ind, ind) for ind in prior_industries])
            
            # Find similar records based on all criteria
            similar_mask = (
                (add_distances <= addsim_threshold) & 
                (pos_distances <= possim_threshold) & 
                (ind_distances <= 1)
            )
            similar_count = np.sum(similar_mask)
            
            if similar_count > rep_threshold:
                # Get the most similar prior record
                most_similar_idx = prior_indices[np.argmin(add_distances + pos_distances + ind_distances)]
                most_similar_addr = addresses[most_similar_idx]
                
                fraud_record = {
                    'HASH': current_row.HASH,
                    'SURVEY_DATE': current_row.SURVEY_DATE,
                    'ADDRESS': current_row.ADDRESS,
                    'POSITION': current_row.POSITION,
                    'INDUSTRY': current_row.INDUSTRY,
                    'FRAUD_TYPE': 'MULTIPLE_ADD_IND_POS_DUPS',
                    'SIMILAR_PRIOR_ADDRESS': most_similar_addr,
                    'SIMILAR_SURVEY_COUNT': similar_count + 1
                }
                
                # Add all original columns from the current row
                for col in group.columns:
                    if col not in fraud_record:
                        fraud_record[col] = getattr(current_row, col)
                
                fraud_records.append(fraud_record)
    
    # Convert fraud records to DataFrame
    if fraud_records:
        fraud_df = pd.DataFrame(fraud_records)
        # Ensure all original columns are present, even if empty
        for col in data.columns:
            if col not in fraud_df.columns:
                fraud_df[col] = None
    else:
        # If no fraud records found, create empty DataFrame with all original columns
        fraud_df = pd.DataFrame(columns=data.columns)
        fraud_df['FRAUD_TYPE'] = None
        fraud_df['SIMILAR_PRIOR_ADDRESS'] = None
        fraud_df['SIMILAR_SURVEY_COUNT'] = None
    
    # Ensure DISBURSED and DR1 columns exist and are numeric
    if 'DISBURSED' not in fraud_df.columns:
        fraud_df['DISBURSED'] = 0
    if 'DR1' not in fraud_df.columns:
        fraud_df['DR1'] = 0
    
    fraud_df['DISBURSED'] = pd.to_numeric(fraud_df['DISBURSED'], errors='coerce').fillna(0)
    fraud_df['DR1'] = pd.to_numeric(fraud_df['DR1'], errors='coerce').fillna(0)
    
    return fraud_df


def lev_pos_win(data, window_days, rep_threshold, pos_sim_threshold):
    # Sort the data by HASH and DATE
    data_sorted = data.sort_values(['HASH', 'SURVEY_DATE'])
    
    # Ensure SURVEY_DATE is datetime
    if not pd.api.types.is_datetime64_any_dtype(data_sorted['SURVEY_DATE']):
        data_sorted['SURVEY_DATE'] = pd.to_datetime(data_sorted['SURVEY_DATE'])
    
    # Create a fraud DataFrame to store duplications
    fraud_records = []
    
    # Group by HASH to process each individual's records
    for hash_val, group in data_sorted.groupby('HASH'):
        # Convert addresses to strings once
        positions = group['POSITION'].astype(str).values
        dates = pd.to_datetime(group['SURVEY_DATE']).values  # Ensure dates are datetime64
        
        # Pre-calculate window indices for each record
        window_indices = []
        for i, current_date in enumerate(dates):
            window_start = pd.Timestamp(current_date) - pd.Timedelta(days=window_days)
            window_mask = (dates >= window_start) & (dates < current_date)
            window_indices.append(np.where(window_mask)[0])
        
        # Process each record
        for i, (current_pos, current_row) in enumerate(zip(positions, group.itertuples())):
            # Get indices of records in the window
            prior_indices = window_indices[i]
            
            if len(prior_indices) == 0:
                continue
                
            # Calculate distances for all prior addresses at once
            prior_positions = positions[prior_indices]
            distances = np.array([lev_distance(current_pos, addr) for addr in prior_positions])
            
            # Find similar addresses
            similar_mask = distances <= pos_sim_threshold
            similar_count = np.sum(similar_mask)
            
            if similar_count > rep_threshold:
                # Get the most similar prior address
                most_similar_idx = prior_indices[np.argmin(distances)]
                most_similar_pos = positions[most_similar_idx]
                
                fraud_record = {
                    'HASH': current_row.HASH,
                    'SURVEY_DATE': current_row.SURVEY_DATE,
                    'ADDRESS': current_row.ADDRESS,
                    'POSITION': current_row.POSITION,
                    'FRAUD_TYPE': 'MULTIPLE_POSITION_DUPLICATIONS',
                    'SIMILAR_PRIOR_POSITION': most_similar_pos,
                    'SIMILAR_POSITIONS_COUNT': similar_count + 1
                }
                
                # Add any additional columns from the original data
                for col in group.columns:
                    if col not in fraud_record:
                        fraud_record[col] = getattr(current_row, col)
                
                fraud_records.append(fraud_record)
    
    # Convert fraud records to DataFrame
    fraud_df = pd.DataFrame(fraud_records)
    
    return fraud_df


def lev_win_multi_pos(data, window_days, possim_threshold, addsim_threshold, rep_threshold):
    # Sort the data by HASH and DATE
    data_sorted = data.sort_values(['HASH', 'SURVEY_DATE'])
    
    # Ensure SURVEY_DATE is datetime
    if not pd.api.types.is_datetime64_any_dtype(data_sorted['SURVEY_DATE']):
        data_sorted['SURVEY_DATE'] = pd.to_datetime(data_sorted['SURVEY_DATE'])
    
    # Create a fraud DataFrame to store duplications
    fraud_records = []
    
    # Group by HASH to process each individual's records
    for hash_val, group in data_sorted.groupby('HASH'):
        # Convert all relevant fields to strings once
        positions = group['POSITION'].astype(str).values
        addresses = group['ADDRESS'].astype(str).values
        industries = group['INDUSTRY'].astype(str).values
        dates = pd.to_datetime(group['SURVEY_DATE']).values
        
        # Pre-calculate window indices for each record
        window_indices = []
        for i, current_date in enumerate(dates):
            window_start = pd.Timestamp(current_date) - pd.Timedelta(days=window_days)
            window_mask = (dates >= window_start) & (dates < current_date)
            window_indices.append(np.where(window_mask)[0])
        
        # Process each record
        for i, (current_pos, current_addr, current_ind, current_row) in enumerate(
            zip(positions, addresses, industries, group.itertuples())
        ):
            # Get indices of records in the window
            prior_indices = window_indices[i]
            
            if len(prior_indices) == 0:
                continue
                
            # Get prior records' data
            prior_positions = positions[prior_indices]
            prior_addresses = addresses[prior_indices]
            prior_industries = industries[prior_indices]
            
            # Calculate distances for all fields at once
            pos_distances = np.array([lev_distance(current_pos, pos) for pos in prior_positions])
            add_distances = np.array([lev_distance(current_addr, addr) for addr in prior_addresses])
            ind_distances = np.array([lev_distance(current_ind, ind) for ind in prior_industries])
            
            # Find similar records based on all criteria, with position check first
            similar_mask = (
                (pos_distances <= possim_threshold) & 
                (add_distances <= addsim_threshold) & 
                (ind_distances <= 1)
            )
            similar_count = np.sum(similar_mask)
            
            if similar_count > rep_threshold:
                # Get the most similar prior record (weighting position more heavily)
                combined_distances = pos_distances * 2 + add_distances + ind_distances
                most_similar_idx = prior_indices[np.argmin(combined_distances)]
                most_similar_pos = positions[most_similar_idx]
                most_similar_addr = addresses[most_similar_idx]
                
                fraud_record = {
                    'HASH': current_row.HASH,
                    'SURVEY_DATE': current_row.SURVEY_DATE,
                    'ADDRESS': current_row.ADDRESS,
                    'POSITION': current_row.POSITION,
                    'INDUSTRY': current_row.INDUSTRY,
                    'FRAUD_TYPE': 'MULTIPLE_POS_ADD_IND_DUPS',
                    'SIMILAR_PRIOR_POSITION': most_similar_pos,
                    'SIMILAR_PRIOR_ADDRESS': most_similar_addr,
                    'SIMILAR_SURVEY_COUNT': similar_count + 1
                }
                
                # Add any additional columns from the original data
                for col in group.columns:
                    if col not in fraud_record:
                        fraud_record[col] = getattr(current_row, col)
                
                fraud_records.append(fraud_record)
    
    # Convert fraud records to DataFrame
    fraud_df = pd.DataFrame(fraud_records)
    
    return fraud_df

def lev_win_pos_ind(data, window_days, possim_threshold,  rep_threshold):
    # Sort the data by HASH and DATE
    data_sorted = data.sort_values(['HASH', 'SURVEY_DATE'])
    
    # Ensure SURVEY_DATE is datetime
    if not pd.api.types.is_datetime64_any_dtype(data_sorted['SURVEY_DATE']):
        data_sorted['SURVEY_DATE'] = pd.to_datetime(data_sorted['SURVEY_DATE'])
    
    # Create a fraud DataFrame to store duplications
    fraud_records = []
    
    # Group by HASH to process each individual's records
    for hash_val, group in data_sorted.groupby('HASH'):
        # Convert all relevant fields to strings once
        positions = group['POSITION'].astype(str).values
        addresses = group['ADDRESS'].astype(str).values
        industries = group['INDUSTRY'].astype(str).values
        dates = pd.to_datetime(group['SURVEY_DATE']).values
        
        # Pre-calculate window indices for each record
        window_indices = []
        for i, current_date in enumerate(dates):
            window_start = pd.Timestamp(current_date) - pd.Timedelta(days=window_days)
            window_mask = (dates >= window_start) & (dates < current_date)
            window_indices.append(np.where(window_mask)[0])
        
        # Process each record
        for i, (current_pos, current_addr, current_ind, current_row) in enumerate(
            zip(positions, addresses, industries, group.itertuples())
        ):
            # Get indices of records in the window
            prior_indices = window_indices[i]
            
            if len(prior_indices) == 0:
                continue
                
            # Get prior records' data
            prior_positions = positions[prior_indices]
            prior_addresses = addresses[prior_indices]
            prior_industries = industries[prior_indices]
            
            # Calculate distances for all fields at once
            pos_distances = np.array([lev_distance(current_pos, pos) for pos in prior_positions])
            add_distances = np.array([lev_distance(current_addr, addr) for addr in prior_addresses])
            ind_distances = np.array([lev_distance(current_ind, ind) for ind in prior_industries])
            
            # Find similar records based on all criteria, with position check first
            similar_mask = (
                (pos_distances <= possim_threshold) & 
                #(add_distances <= addsim_threshold) & 
                (ind_distances <= 1)
            )
            similar_count = np.sum(similar_mask)
            
            if similar_count > rep_threshold:
                # Get the most similar prior record (weighting position more heavily)
                combined_distances = pos_distances * 2 + add_distances + ind_distances
                most_similar_idx = prior_indices[np.argmin(combined_distances)]
                most_similar_pos = positions[most_similar_idx]
                most_similar_addr = addresses[most_similar_idx]
                
                fraud_record = {
                    'HASH': current_row.HASH,
                    'SURVEY_DATE': current_row.SURVEY_DATE,
                    'ADDRESS': current_row.ADDRESS,
                    'POSITION': current_row.POSITION,
                    'INDUSTRY': current_row.INDUSTRY,
                    'FRAUD_TYPE': 'MULTIPLE_POS_ADD_IND_DUPS',
                    'SIMILAR_PRIOR_POSITION': most_similar_pos,
                    'SIMILAR_PRIOR_ADDRESS': most_similar_addr,
                    'SIMILAR_SURVEY_COUNT': similar_count + 1
                }
                
                # Add any additional columns from the original data
                for col in group.columns:
                    if col not in fraud_record:
                        fraud_record[col] = getattr(current_row, col)
                
                fraud_records.append(fraud_record)
    
    # Convert fraud records to DataFrame
    fraud_df = pd.DataFrame(fraud_records)
    
    return fraud_df

