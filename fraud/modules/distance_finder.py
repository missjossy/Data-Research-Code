import pandas as pd
import numpy as np
from Levenshtein import distance as lev_distance
from collections import defaultdict
from jellyfish import jaro_winkler_similarity
from sklearn.metrics.pairwise import cosine_similarity
from datetime import timedelta


def check_whitelist(text):
    """Simple whitelist check with SQL LIKE pattern matching"""
    if not text:
        return False
        
    # Convert to lowercase and add spaces for word boundaries
    text = f' {str(text).lower()} '
    
    # Terms with SQL LIKE patterns
    patterns = [
        '%street%', #'%st %', '%st.%',
        '%hospital%',
        '%camp%', '%burma camp%',
        '%university%', '%university of ghana%', '%university of cape coast%',
        '%street po%', '%p o%' , '%near the%', '%po box%','%market%', '%town%',
        '%tema newtown%', '%tema new town%', '%road%'
    ]
    
    # Convert pattern to regex and check
    import re
    for pattern in patterns:
        # Convert SQL LIKE pattern to regex
        regex_pattern = pattern.lower().replace('%', '.*')
        if re.search(regex_pattern, text):
            return True
    
    return False

def normalize_address(address):
    """Normalize address for faster initial comparison"""
    return ''.join(c.lower() for c in str(address) if c.isalnum())

def lev_win_distance(data, window_days, rep_threshold, distance_threshold):
    """
    Uses Levenshtein distance to find similar addresses.
    Counts duplications within the specified window_days period.
    Only blocks if similar addresses appear within the window period,
    even for previously blocked addresses.
    """
    from Levenshtein import distance as lev_distance
    
    # Sort the data by HASH and DATE
    data_sorted = data.sort_values(['HASH', 'SURVEY_DATE'])
    
    # Ensure SURVEY_DATE is datetime
    if not pd.api.types.is_datetime64_any_dtype(data_sorted['SURVEY_DATE']):
        data_sorted['SURVEY_DATE'] = pd.to_datetime(data_sorted['SURVEY_DATE'])
    
    fraud_records = []
    
    # Track blocked addresses with their dates
    blocked_addresses = {}  # {lowercase_address: (original_address, block_date)}
    
    # Group by HASH to process each individual's records
    for hash_val, group in data_sorted.groupby('HASH'):
        # Track address histories for this HASH
        address_histories = defaultdict(list)  # {lowercase_address: [(date, original_address), ...]}
        
        for current_row in group.itertuples():
            original_address = str(current_row.ADDRESS)
            current_address = original_address.lower()  # Convert to lowercase for comparison
            current_date = pd.to_datetime(current_row.SURVEY_DATE)
            
            # Skip if address is whitelisted
            if check_whitelist(original_address):
                continue
            
            # First check if similar to any recently blocked address
            was_blocked = False
            similar_blocked_addr = None
            block_date = None
            current_distance = 0
            
            # Check blocked addresses
            for blocked_addr_lower, (blocked_addr_original, blocked_date) in list(blocked_addresses.items()):
                # Skip if blocked address is too old
                if (current_date - blocked_date).days > window_days:
                    continue
                    
                # Check similarity using lowercase addresses
                distance = lev_distance(current_address, blocked_addr_lower)
                if distance <= distance_threshold:
                    was_blocked = True
                    similar_blocked_addr = blocked_addr_original
                    block_date = blocked_date
                    current_distance = distance
                    break
            
            # Find similar addresses in history
            similar_addresses = []
            window_start = current_date - pd.Timedelta(days=window_days)
            
            for tracked_addr_lower, history in list(address_histories.items()):
                # Remove entries outside the window
                history = [(d, a) for d, a in history if d >= window_start and d < current_date]
                if not history:
                    continue
                    
                # Check similarity using lowercase addresses
                distance = lev_distance(current_address, tracked_addr_lower)
                if distance <= distance_threshold:
                    similar_addresses.append((history[-1][1], history, distance))  # Use original address in results
            
            should_block = False
            block_reason = None
            similar_prior = None
            similar_count = 1
            days_since_last = 0
            
            # Check if should be blocked due to similarity with recently blocked address
            if was_blocked:
                should_block = True
                block_reason = 'SIMILAR_TO_BLOCKED'
                similar_prior = similar_blocked_addr
                days_since_last = (current_date - block_date).days
            
            # If not already blocked, check window violations
            if not should_block:
                for tracked_addr, history, distance in similar_addresses:
                    # Count similar addresses in the window
                    similar_count = len(history) + 1  # +1 for current occurrence
                    
                    if similar_count > rep_threshold:
                        should_block = True
                        block_reason = 'MULTIPLE_ADDRESS_DUPLICATIONS'
                        similar_prior = history[-1][1]  # Most recent prior address
                        current_distance = distance
                        days_since_last = (current_date - pd.to_datetime(history[-1][0])).days
                        break
            
            # If should be blocked, create fraud record
            if should_block:
                # Add to blocked addresses set with current date
                blocked_addresses[current_address] = (original_address, current_date)
                
                # Create fraud record with all original columns
                fraud_record = {col: getattr(current_row, col) for col in data.columns}
                
                # Add fraud detection columns
                fraud_record.update({
                    'FRAUD_TYPE': block_reason,
                    'SIMILAR_PRIOR_ADDRESS': similar_prior,
                    'SIMILAR_ADDRESSES_COUNT': similar_count,
                    'ADDRESS_DISTANCE': current_distance,
                    'IS_WHITELISTED': False,
                    'DAYS_SINCE_LAST': days_since_last
                })
                
                # Ensure DISBURSED and DR1 are numeric
                fraud_record['DISBURSED'] = pd.to_numeric(fraud_record.get('DISBURSED', 0), errors='coerce') or 0
                fraud_record['DR1'] = pd.to_numeric(fraud_record.get('DR1', 0), errors='coerce') or 0
                fraud_record['DEFAULT_RATE'] = fraud_record['DR1'] / fraud_record['DISBURSED'] if fraud_record['DISBURSED'] > 0 else 0
                
                fraud_records.append(fraud_record)
            
            # Update history for this address
            if not was_blocked:
                address_histories[current_address].append((current_date, original_address))
    
    # Convert to DataFrame
    if fraud_records:
        fraud_df = pd.DataFrame(fraud_records)
    else:
        # Create empty DataFrame with all columns
        fraud_df = pd.DataFrame(columns=list(data.columns) + [
            'FRAUD_TYPE', 'SIMILAR_PRIOR_ADDRESS', 'SIMILAR_ADDRESSES_COUNT',
            'ADDRESS_DISTANCE', 'IS_WHITELISTED', 'DEFAULT_RATE', 'DAYS_SINCE_LAST'
        ])
    
    # Ensure numeric columns are properly typed
    numeric_cols = ['DISBURSED', 'DR1', 'DEFAULT_RATE', 'ADDRESS_DISTANCE', 'DAYS_SINCE_LAST']
    for col in numeric_cols:
        if col in fraud_df.columns:
            fraud_df[col] = pd.to_numeric(fraud_df[col], errors='coerce').fillna(0)
    
    return fraud_df

def jaro_win(data, window_days, rep_threshold, distance_threshold):
    """
    Uses Jaro-Winkler similarity to find similar addresses.
    Counts duplications within the specified window_days period.
    Only blocks if similar addresses appear within the window period,
    even for previously blocked addresses.
    
    Note: similarity_threshold should be between 0 and 1,
    where higher values mean more similar (e.g., 0.85 means 85% similar).
    Current address must have similarity >= threshold to be considered similar.
    """

    
   # Sort the data by HASH and DATE
    data_sorted = data.sort_values(['HASH', 'SURVEY_DATE'])
    
    # Ensure SURVEY_DATE is datetime
    if not pd.api.types.is_datetime64_any_dtype(data_sorted['SURVEY_DATE']):
        data_sorted['SURVEY_DATE'] = pd.to_datetime(data_sorted['SURVEY_DATE'])
    
    fraud_records = []
    
    # Track blocked addresses with their dates
    blocked_addresses = {}  # {address: block_date}
    
    # Group by HASH to process each individual's records
    for hash_val, group in data_sorted.groupby('HASH'):
        # Track address histories for this HASH
        address_histories = defaultdict(list)  # {address: [(date, address), ...]}
        
        for current_row in group.itertuples():
            current_address = str(current_row.ADDRESS)
            current_date = pd.to_datetime(current_row.SURVEY_DATE)
            
            # Skip if address is whitelisted
            if check_whitelist(current_address):
                continue
            
            # First check if similar to any recently blocked address
            was_blocked = False
            similar_blocked_addr = None
            block_date = None
            
            # Check blocked addresses
            for blocked_addr, blocked_date in list(blocked_addresses.items()):
                # Skip if blocked address is too old
                if (current_date - blocked_date).days > window_days:
                    continue
                    
                # Check similarity
                if jaro_winkler_similarity(current_address, blocked_addr) >= distance_threshold:
                    was_blocked = True
                    similar_blocked_addr = blocked_addr
                    block_date = blocked_date
                    break
            
            # Find similar addresses in history
            similar_addresses = []
            window_start = current_date - pd.Timedelta(days=window_days)
            
            for tracked_addr, history in list(address_histories.items()):
                # Remove entries outside the window
                history = [(d, a) for d, a in history if d >= window_start and d < current_date]
                if not history:
                    continue
                    
                # Check similarity
                distance = jaro_winkler_similarity(current_address, tracked_addr)
                if distance >=  distance_threshold:
                    similar_addresses.append((tracked_addr, history, distance))
            
            should_block = False
            block_reason = None
            similar_prior = None
            similar_count = 1
            current_distance = 0
            days_since_last = 0
            
            # Check if should be blocked due to similarity with recently blocked address
            if was_blocked:
                should_block = True
                block_reason = 'SIMILAR_TO_BLOCKED'
                similar_prior = similar_blocked_addr
                current_distance = jaro_winkler_similarity(current_address, similar_blocked_addr)
                days_since_last = (current_date - block_date).days
            
            # If not already blocked, check window violations
            if not should_block:
                for tracked_addr, history, distance in similar_addresses:
                    # Count similar addresses in the window
                    similar_count = len(history) + 1  # +1 for current occurrence
                    
                    if similar_count > rep_threshold:
                        should_block = True
                        block_reason = 'MULTIPLE_ADDRESS_DUPLICATIONS'
                        similar_prior = history[-1][1]  # Most recent prior address
                        current_distance = distance
                        days_since_last = (current_date - pd.to_datetime(history[-1][0])).days
                        break
            
            # If should be blocked, create fraud record
            if should_block:
                # Add to blocked addresses set with current date
                blocked_addresses[current_address] = current_date
                
                # Create fraud record with all original columns
                fraud_record = {col: getattr(current_row, col) for col in data.columns}
                
                # Add fraud detection columns
                fraud_record.update({
                    'FRAUD_TYPE': block_reason,
                    'SIMILAR_PRIOR_ADDRESS': similar_prior,
                    'SIMILAR_ADDRESSES_COUNT': similar_count,
                    'ADDRESS_DISTANCE': current_distance,
                    'IS_WHITELISTED': False,
                    'DAYS_SINCE_LAST': days_since_last
                })
                
                # Ensure DISBURSED and DR1 are numeric
                fraud_record['DISBURSED'] = pd.to_numeric(fraud_record.get('DISBURSED', 0), errors='coerce') or 0
                fraud_record['DR1'] = pd.to_numeric(fraud_record.get('DR1', 0), errors='coerce') or 0
                fraud_record['DEFAULT_RATE'] = fraud_record['DR1'] / fraud_record['DISBURSED'] if fraud_record['DISBURSED'] > 0 else 0
                
                fraud_records.append(fraud_record)
            
            # Update history for this address
            if not was_blocked:
                address_histories[current_address].append((current_date, current_address))
    
    # Convert to DataFrame
    if fraud_records:
        fraud_df = pd.DataFrame(fraud_records)
    else:
        # Create empty DataFrame with all columns
        fraud_df = pd.DataFrame(columns=list(data.columns) + [
            'FRAUD_TYPE', 'SIMILAR_PRIOR_ADDRESS', 'SIMILAR_ADDRESSES_COUNT',
            'ADDRESS_DISTANCE', 'IS_WHITELISTED', 'DEFAULT_RATE', 'DAYS_SINCE_LAST'
        ])
    
    # Ensure numeric columns are properly typed
    numeric_cols = ['DISBURSED', 'DR1', 'DEFAULT_RATE', 'ADDRESS_DISTANCE', 'DAYS_SINCE_LAST']
    for col in numeric_cols:
        if col in fraud_df.columns:
            fraud_df[col] = pd.to_numeric(fraud_df[col], errors='coerce').fillna(0)
    
    return fraud_df


def lev_win_multi(data, window_days, addsim_threshold, possim_threshold, rep_threshold):
    """
    Uses Levenshtein distance to detect duplications based on:
    Similar addresses AND positions
    
    Parameters:
    -----------
    data : pandas DataFrame
        Input data containing HASH, SURVEY_DATE, ADDRESS, POSITION columns
    window_days : int
        Number of days to look back for duplicates
    addsim_threshold : float
        Threshold for address similarity (0-1, higher means more similar)
    possim_threshold : float
        Threshold for position similarity (0-1, higher means more similar)
    rep_threshold : int
        Minimum number of similar records required to flag as fraud
    """
    from jellyfish import jaro_winkler_similarity
    
    # Sort the data by HASH and DATE
    data_sorted = data.sort_values(['HASH', 'SURVEY_DATE'])
    
    # Ensure SURVEY_DATE is datetime
    if not pd.api.types.is_datetime64_any_dtype(data_sorted['SURVEY_DATE']):
        data_sorted['SURVEY_DATE'] = pd.to_datetime(data_sorted['SURVEY_DATE'])
    
    fraud_records = []
    
    # Group by HASH to process each individual's records
    for hash_val, group in data_sorted.groupby('HASH'):
        addresses = group['ADDRESS'].astype(str).values
        positions = group['POSITION'].fillna('').astype(str).values
        dates = pd.to_datetime(group['SURVEY_DATE']).values
        
        # Pre-calculate window indices
        window_indices = []
        for i, current_date in enumerate(dates):
            window_start = pd.Timestamp(current_date) - pd.Timedelta(days=window_days)
            window_mask = (dates >= window_start) & (dates < current_date)
            window_indices.append(np.where(window_mask)[0])
        
        # Process each record
        for i, (current_address, current_pos, current_row) in enumerate(zip(addresses, positions, group.itertuples())):
            # Skip if address is whitelisted
            if check_whitelist(current_address):
                continue
                
            # Skip if position is empty
            if not current_pos.strip():
                continue
                
            prior_indices = window_indices[i]
            if len(prior_indices) == 0:
                continue
            
            # Calculate similarities for non-whitelisted prior addresses
            valid_matches = []
            valid_prior_indices = []
            
            for idx in prior_indices:
                prior_address = addresses[idx]
                prior_pos = positions[idx]
                
                # Skip whitelisted or empty prior records
                if check_whitelist(prior_address) or not prior_pos.strip():
                    continue
                
                # Calculate both similarities
                add_similarity = jaro_winkler_similarity(current_address, prior_address)
                pos_similarity = jaro_winkler_similarity(current_pos, prior_pos)
                
                # Only include if both address and position are similar enough
                if add_similarity >= addsim_threshold and pos_similarity >= possim_threshold:
                    valid_matches.append((add_similarity, pos_similarity))
                    valid_prior_indices.append(idx)
            
            # Check if we have enough similar records
            if len(valid_matches) > rep_threshold:
                # Get the most similar record (maximum combined similarity)
                combined_similarities = [add_sim + pos_sim for add_sim, pos_sim in valid_matches]
                most_similar_idx = valid_prior_indices[np.argmax(combined_similarities)]
                
                # Create fraud record with all original columns
                fraud_record = {col: getattr(current_row, col) for col in data.columns}
                
                # Add fraud detection columns
                fraud_record.update({
                    'FRAUD_TYPE': 'MULTIPLE_ADDRESS_POSITION_DUPLICATIONS',
                    'SIMILAR_PRIOR_ADDRESS': addresses[most_similar_idx],
                    'SIMILAR_PRIOR_POSITION': positions[most_similar_idx],
                    'SIMILAR_RECORDS_COUNT': len(valid_matches),
                    'ADDRESS_SIMILARITY': valid_matches[np.argmax(combined_similarities)][0],
                    'POSITION_SIMILARITY': valid_matches[np.argmax(combined_similarities)][1],
                    'COMBINED_SIMILARITY': max(combined_similarities) / 2,  # Normalize to 0-1 range
                    'IS_WHITELISTED': False
                })
                
                # Ensure DISBURSED and DR1 are numeric
                fraud_record['DISBURSED'] = pd.to_numeric(fraud_record.get('DISBURSED', 0), errors='coerce') or 0
                fraud_record['DR1'] = pd.to_numeric(fraud_record.get('DR1', 0), errors='coerce') or 0
                fraud_record['DEFAULT_RATE'] = fraud_record['DR1'] / fraud_record['DISBURSED'] if fraud_record['DISBURSED'] > 0 else 0
                
                fraud_records.append(fraud_record)
    
    # Convert to DataFrame
    if fraud_records:
        fraud_df = pd.DataFrame(fraud_records)
    else:
        # Create empty DataFrame with all columns
        fraud_df = pd.DataFrame(columns=list(data.columns) + [
            'FRAUD_TYPE', 'SIMILAR_PRIOR_ADDRESS', 'SIMILAR_PRIOR_POSITION',
            'SIMILAR_RECORDS_COUNT', 'ADDRESS_SIMILARITY', 'POSITION_SIMILARITY',
            'COMBINED_SIMILARITY', 'IS_WHITELISTED', 'DEFAULT_RATE'
        ])
    
    # Ensure numeric columns are properly typed
    numeric_cols = ['DISBURSED', 'DR1', 'DEFAULT_RATE', 'ADDRESS_SIMILARITY', 
                   'POSITION_SIMILARITY', 'COMBINED_SIMILARITY']
    for col in numeric_cols:
        if col in fraud_df.columns:
            fraud_df[col] = pd.to_numeric(fraud_df[col], errors='coerce').fillna(0)
    
    return fraud_df



def calculate_monthly_blocks(fraud_df):
    """
    Calculate the number of blocked applications and total disbursed amount per month from fraud detection results.
    
    Parameters:
    -----------
    fraud_df : pandas DataFrame
        The fraud detection results DataFrame containing blocked applications
        
    Returns:
    --------
    monthly_blocks : pandas DataFrame
        DataFrame with columns:
        - YEAR_MONTH: The year and month
        - BLOCKED_COUNT: Number of applications blocked
        - DISBURSED_AMOUNT: Total amount that would have been disbursed for blocked applications
        - FRAUD_TYPE: Type of fraud that caused the block
    """
    # Ensure SURVEY_DATE is datetime
    if not pd.api.types.is_datetime64_any_dtype(fraud_df['SURVEY_DATE']):
        fraud_df['SURVEY_DATE'] = pd.to_datetime(fraud_df['SURVEY_DATE'])
    
    # Create YEAR_MONTH column
    fraud_df['YEAR_MONTH'] = fraud_df['SURVEY_DATE'].dt.to_period('M')
    
    # Group by YEAR_MONTH and FRAUD_TYPE and calculate metrics
    monthly_blocks = fraud_df.groupby(['YEAR_MONTH', 'FRAUD_TYPE']).agg({
        'FRAUD_TYPE': 'size',  # Count of blocks
        'DISBURSED': 'sum'     # Sum of disbursed amounts
    }).rename(columns={'FRAUD_TYPE': 'BLOCKED_COUNT', 'DISBURSED': 'DISBURSED_AMOUNT'}).reset_index()
    
    # Add total blocks and disbursed per month
    total_blocks = fraud_df.groupby('YEAR_MONTH').agg({
        'FRAUD_TYPE': 'size',  # Count of blocks
        'DISBURSED': 'sum'     # Sum of disbursed amounts
    }).rename(columns={'FRAUD_TYPE': 'BLOCKED_COUNT', 'DISBURSED': 'DISBURSED_AMOUNT'}).reset_index()
    total_blocks['FRAUD_TYPE'] = 'TOTAL'
    
    # Combine specific fraud types with totals
    monthly_blocks = pd.concat([monthly_blocks, total_blocks])
    
    # Sort by date and fraud type
    monthly_blocks = monthly_blocks.sort_values(['YEAR_MONTH', 'FRAUD_TYPE'])
    
    # Ensure disbursed amount is numeric and handle any NaN values
    monthly_blocks['DISBURSED_AMOUNT'] = pd.to_numeric(monthly_blocks['DISBURSED_AMOUNT'], errors='coerce').fillna(0)
    
    return monthly_blocks



