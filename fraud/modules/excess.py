def lev_win_multi_weighted(data, window_days, addsim_threshold, possim_threshold, rep_threshold, combined_distance_threshold, add_weight=1.0, pos_weight=1.0):
    """
    A weighted version that uses Jaro-Winkler similarity instead of Levenshtein distance.
    Note: thresholds should now be between 0 and 1, where higher values mean more similar.
    
    Parameters:
    -----------
    data : pandas DataFrame
        Input data containing HASH, SURVEY_DATE, ADDRESS, POSITION, INDUSTRY columns
    window_days : int
        Number of days to look back for duplicates
    addsim_threshold : float
        Threshold for address similarity (Jaro-Winkler similarity, 0-1)
    possim_threshold : float
        Threshold for position similarity (Jaro-Winkler similarity, 0-1)
    rep_threshold : int
        Minimum number of similar records required to flag as fraud
    combined_distance_threshold : float
        Maximum allowed weighted combined similarity (higher means more similar)
    add_weight : float, optional (default=1.0)
        Weight for address similarity in combined similarity calculation
    pos_weight : float, optional (default=1.0)
        Weight for position similarity in combined similarity calculation
    """
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
        addresses = group['ADDRESS'].fillna('').astype(str).values
        positions = group['POSITION'].fillna('').astype(str).values
        industries = group['INDUSTRY'].fillna('').astype(str).values
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
            # Skip if both address and position are empty
            if not current_address and not current_pos:
                continue
                
            # Get indices of records in the window
            prior_indices = window_indices[i]
            
            if len(prior_indices) == 0:
                continue
                
            # Get prior records' data
            prior_addresses = addresses[prior_indices]
            prior_positions = positions[prior_indices]
            prior_industries = industries[prior_indices]
            
            # Calculate similarities using Jaro-Winkler
            add_similarities = np.array([
                jaro_winkler_similarity(current_address, addr) if current_address and addr else 0
                for addr in prior_addresses
            ])
            
            pos_similarities = np.array([
                jaro_winkler_similarity(current_pos, pos) if current_pos and pos else 0
                for pos in prior_positions
            ])
            
            # Calculate weighted combined similarity
            combined_similarities = (
                add_weight * add_similarities +
                pos_weight * pos_similarities
            ) / (add_weight + pos_weight)  # Normalize by total weight
            
            # Find similar records based on combined similarity and individual thresholds
            similar_mask = (
                (combined_similarities >= combined_distance_threshold) &
                ((add_similarities >= addsim_threshold) | (pos_similarities >= possim_threshold))
            )
            similar_count = np.sum(similar_mask)
            
            if similar_count > rep_threshold:
                # Get the most similar prior record
                most_similar_idx = prior_indices[np.argmax(combined_similarities)]
                most_similar_addr = addresses[most_similar_idx] if addresses[most_similar_idx] else None
                most_similar_pos = positions[most_similar_idx] if positions[most_similar_idx] else None
                
                # Determine match type
                is_address_match = add_similarities[most_similar_idx - prior_indices[0]] >= addsim_threshold
                is_position_match = pos_similarities[most_similar_idx - prior_indices[0]] >= possim_threshold
                
                if is_address_match and is_position_match:
                    fraud_type = 'MULTIPLE_ADD_POS_WEIGHTED_DUPS'
                elif is_address_match:
                    fraud_type = 'MULTIPLE_ADDRESS_WEIGHTED_DUPS'
                else:
                    fraud_type = 'MULTIPLE_POSITION_WEIGHTED_DUPS'
                
                fraud_record = {
                    'HASH': current_row.HASH,
                    'SURVEY_DATE': current_row.SURVEY_DATE,
                    'ADDRESS': current_row.ADDRESS,
                    'POSITION': current_row.POSITION,
                    'INDUSTRY': current_row.INDUSTRY,
                    'FRAUD_TYPE': fraud_type,
                    'SIMILAR_PRIOR_ADDRESS': most_similar_addr,
                    'SIMILAR_PRIOR_POSITION': most_similar_pos,
                    'SIMILAR_SURVEY_COUNT': similar_count + 1,
                    'COMBINED_SIMILARITY': combined_similarities[np.argmax(combined_similarities)],
                    'ADDRESS_SIMILARITY': add_similarities[np.argmax(combined_similarities)],
                    'POSITION_SIMILARITY': pos_similarities[np.argmax(combined_similarities)],
                    'IS_ADDRESS_MATCH': is_address_match,
                    'IS_POSITION_MATCH': is_position_match
                }
                
                # Add all original columns from the current row
                for col in group.columns:
                    if col not in fraud_record:
                        fraud_record[col] = getattr(current_row, col)
                
                fraud_records.append(fraud_record)
    
    # Convert fraud records to DataFrame
    if fraud_records:
        fraud_df = pd.DataFrame(fraud_records)
        # Ensure all original columns are present
        for col in data.columns:
            if col not in fraud_df.columns:
                fraud_df[col] = None
    else:
        fraud_df = pd.DataFrame(columns=data.columns)
        fraud_df['FRAUD_TYPE'] = None
        fraud_df['SIMILAR_PRIOR_ADDRESS'] = None
        fraud_df['SIMILAR_PRIOR_POSITION'] = None
        fraud_df['SIMILAR_SURVEY_COUNT'] = None
        fraud_df['COMBINED_SIMILARITY'] = None
        fraud_df['ADDRESS_SIMILARITY'] = None
        fraud_df['POSITION_SIMILARITY'] = None
        fraud_df['IS_ADDRESS_MATCH'] = None
        fraud_df['IS_POSITION_MATCH'] = None
    
    # Ensure DISBURSED and DR1 columns exist and are numeric
    if 'DISBURSED' not in fraud_df.columns:
        fraud_df['DISBURSED'] = 0
    if 'DR1' not in fraud_df.columns:
        fraud_df['DR1'] = 0
    
    fraud_df['DISBURSED'] = pd.to_numeric(fraud_df['DISBURSED'], errors='coerce').fillna(0)
    fraud_df['DR1'] = pd.to_numeric(fraud_df['DR1'], errors='coerce').fillna(0)
    
    return fraud_df

def cosine_jaro_win(data, window_days, similarity_threshold, rep_threshold, weights=None):
    """
    Uses a combination of Jaro-Winkler and cosine similarity to compare records.
    First calculates Jaro-Winkler similarities for each field (address, position, industry),
    then uses cosine similarity to compare the resulting similarity vectors.
    
    Parameters:
    -----------
    data : pandas DataFrame
        Input data containing HASH, SURVEY_DATE, ADDRESS, POSITION, INDUSTRY columns
    window_days : int
        Number of days to look back for duplicates
    similarity_threshold : float
        Threshold for combined cosine similarity (0-1)
    rep_threshold : int
        Minimum number of similar records required to flag as fraud
    weights : dict, optional
        Weights for each field in the similarity vector. Default is equal weights.
        Example: {'address': 1.0, 'position': 1.0, 'industry': 0.5}
    """
    
    # Set default weights if none provided
    if weights is None:
        weights = {
            'address': 1.0,
            'position': 1.0,
            'industry': 1.0
        }
    
    def get_similarity_vector(text1, text2, field_type):
        """Calculate Jaro-Winkler similarity and apply weight"""
        if not text1 or not text2:
            return 0.0
        try:
            sim = jaro_winkler_similarity(str(text1), str(text2))
            return sim * weights[field_type]
        except:
            return 0.0
    
    # Sort the data by HASH and DATE
    data_sorted = data.sort_values(['HASH', 'SURVEY_DATE'])
    
    # Ensure SURVEY_DATE is datetime
    if not pd.api.types.is_datetime64_any_dtype(data_sorted['SURVEY_DATE']):
        data_sorted['SURVEY_DATE'] = pd.to_datetime(data_sorted['SURVEY_DATE'])
    
    # Create a fraud DataFrame to store duplications
    fraud_records = []
    
    # Group by HASH to process each individual's records
    for hash_val, group in data_sorted.groupby('HASH'):
        # Convert all relevant fields to strings
        addresses = group['ADDRESS'].fillna('').astype(str).values
        positions = group['POSITION'].fillna('').astype(str).values
        industries = group['INDUSTRY'].fillna('').astype(str).values
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
            # Skip if all fields are empty
            if not (current_address or current_pos or current_ind):
                continue
                
            # Get indices of records in the window
            prior_indices = window_indices[i]
            
            if len(prior_indices) == 0:
                continue
            
            # Create similarity vectors for current record and all prior records
            current_vector = np.array([[1.0, 1.0, 1.0]])  # Self-similarity vector
            
            prior_vectors = []
            field_similarities = {
                'address': [],
                'position': [],
                'industry': []
            }
            
            # Calculate similarity vectors for all prior records
            for j in prior_indices:
                add_sim = get_similarity_vector(current_address, addresses[j], 'address')
                pos_sim = get_similarity_vector(current_pos, positions[j], 'position')
                ind_sim = get_similarity_vector(current_ind, industries[j], 'industry')
                
                prior_vectors.append([add_sim, pos_sim, ind_sim])
                field_similarities['address'].append(add_sim)
                field_similarities['position'].append(pos_sim)
                field_similarities['industry'].append(ind_sim)
            
            prior_vectors = np.array(prior_vectors)
            
            # Calculate cosine similarities between current and prior vectors
            similarities = cosine_similarity(current_vector, prior_vectors).flatten()
            
            # Find similar records
            similar_mask = similarities >= similarity_threshold
            similar_count = np.sum(similar_mask)
            
            if similar_count > rep_threshold:
                # Get the most similar prior record
                most_similar_idx = prior_indices[np.argmax(similarities)]
                
                # Calculate contribution of each field to the similarity
                best_match_idx = np.argmax(similarities)
                field_contributions = {
                    'address': field_similarities['address'][best_match_idx] / weights['address'] if weights['address'] > 0 else 0,
                    'position': field_similarities['position'][best_match_idx] / weights['position'] if weights['position'] > 0 else 0,
                    'industry': field_similarities['industry'][best_match_idx] / weights['industry'] if weights['industry'] > 0 else 0
                }
                
                # Determine which fields contributed significantly to the match
                match_types = []
                if field_contributions['address'] >= 0.8:  # 80% similarity threshold for field contribution
                    match_types.append('ADDRESS')
                if field_contributions['position'] >= 0.8:
                    match_types.append('POSITION')
                if field_contributions['industry'] >= 0.8:
                    match_types.append('INDUSTRY')
                
                fraud_type = f"MULTIPLE_{'_'.join(match_types)}_COMBINED_DUPS"
                
                fraud_record = {
                    'HASH': current_row.HASH,
                    'SURVEY_DATE': current_row.SURVEY_DATE,
                    'ADDRESS': current_row.ADDRESS,
                    'POSITION': current_row.POSITION,
                    'INDUSTRY': current_row.INDUSTRY,
                    'FRAUD_TYPE': fraud_type,
                    'SIMILAR_PRIOR_ADDRESS': addresses[most_similar_idx] if addresses[most_similar_idx] else None,
                    'SIMILAR_PRIOR_POSITION': positions[most_similar_idx] if positions[most_similar_idx] else None,
                    'SIMILAR_PRIOR_INDUSTRY': industries[most_similar_idx] if industries[most_similar_idx] else None,
                    'SIMILAR_SURVEY_COUNT': similar_count + 1,
                    'COMBINED_SIMILARITY': similarities[np.argmax(similarities)],
                    'ADDRESS_CONTRIBUTION': field_contributions['address'],
                    'POSITION_CONTRIBUTION': field_contributions['position'],
                    'INDUSTRY_CONTRIBUTION': field_contributions['industry'],
                    'MATCH_FIELDS': '_'.join(match_types)
                }
                
                # Add all original columns from the current row
                for col in group.columns:
                    if col not in fraud_record:
                        fraud_record[col] = getattr(current_row, col)
                
                fraud_records.append(fraud_record)
    
    # Convert fraud records to DataFrame
    if fraud_records:
        fraud_df = pd.DataFrame(fraud_records)
        # Ensure all original columns are present
        for col in data.columns:
            if col not in fraud_df.columns:
                fraud_df[col] = None
    else:
        fraud_df = pd.DataFrame(columns=data.columns)
        fraud_df['FRAUD_TYPE'] = None
        fraud_df['SIMILAR_PRIOR_ADDRESS'] = None
        fraud_df['SIMILAR_PRIOR_POSITION'] = None
        fraud_df['SIMILAR_PRIOR_INDUSTRY'] = None
        fraud_df['SIMILAR_SURVEY_COUNT'] = None
        fraud_df['COMBINED_SIMILARITY'] = None
        fraud_df['ADDRESS_CONTRIBUTION'] = None
        fraud_df['POSITION_CONTRIBUTION'] = None
        fraud_df['INDUSTRY_CONTRIBUTION'] = None
        fraud_df['MATCH_FIELDS'] = None
    
    # Ensure DISBURSED and DR1 columns exist and are numeric
    if 'DISBURSED' not in fraud_df.columns:
        fraud_df['DISBURSED'] = 0
    if 'DR1' not in fraud_df.columns:
        fraud_df['DR1'] = 0
    
    fraud_df['DISBURSED'] = pd.to_numeric(fraud_df['DISBURSED'], errors='coerce').fillna(0)
    fraud_df['DR1'] = pd.to_numeric(fraud_df['DR1'], errors='coerce').fillna(0)
    
    return fraud_df

def jaro_win_multi(data, window_days, addsim_threshold, possim_threshold, rep_threshold):
    """
    Uses Jaro-Winkler similarity to detect duplications based on:
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

def jaro_win_flexible(data, window_days, addsim_threshold, possim_threshold, rep_threshold, pos_only_threshold=4):