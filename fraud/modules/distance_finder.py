import pandas as pd
import numpy as np
from Levenshtein import distance as lev_distance
from collections import defaultdict



def lev_dist2(data, hash, week, add, col2, col3):

    # List to store final DataFrame rows
    rows = []

    # Step 1: Group data by HASH and SURVEY_WEEK, collecting relevant fields in a list
    grouped = data.groupby([hash, week])[[add, col2, col3]].apply(lambda x: x.values.tolist()).reset_index()

    # Step 2: Process each group
    for _, row in grouped.iterrows():
        hash_val = row[hash]
        week = row[week]
        records = [tuple(map(str, rec)) for rec in row[0]]  # Convert each tuple (ADDRESS, POSITION, INDUSTRY) to string

        counted_records = {}  # Dictionary to store representative records and their counts
        
        for record in records:
            found_similar = None  # Track closest match
            
            # Check against already stored records
            for existing_record in counted_records:
                try:
                    # Compute Levenshtein distance across concatenated fields
                    dist = lev_distance(" | ".join(record), " | ".join(existing_record))
                    if dist <= 3:
                        found_similar = existing_record
                        break  # Stop checking once a close match is found
                except Exception as e:
                    print(f"Error calculating distance between '{record}' and '{existing_record}': {e}")
            
            if found_similar:
                counted_records[found_similar] += 1  # Increment count for existing match
            else:
                counted_records[record] = 1  # Create new key
        
        # Append results to list
        for rec, count in counted_records.items():
            rows.append([hash_val, week, rec[0], rec[1], rec[2], count])  # Unpack tuple

    return rows

    # # Convert to DataFrame
    # df_result = pd.DataFrame(rows, columns=[hash, 'SURVEY_WEEK', 'ADDRESS', 'POSITION', 'INDUSTRY', 'COUNT'])

    # # Display the resulting DataFrame
    # print(df_result)


def lev_dist4(data, lev_distance, period, bool_columns=None):
    if bool_columns is None:
        bool_columns = ['DISBURSED', 'DR1']
    
    result_rows = []
    
    # Ensure boolean columns exist and are properly formatted
    data_copy = data.copy()
    for col in bool_columns:
        if col in data_copy.columns:
            data_copy[col] = pd.to_numeric(data_copy[col], errors='coerce').fillna(0).astype(int)
        else:
            print(f"Warning: Column '{col}' not found in the data")
    
    # Group by HASH and period
    grouped = data_copy.groupby(['HASH', period])
    
    for (hash_val, week), group_df in grouped:
        unique_addresses = defaultdict(lambda: {'COUNT': 0, **{col: 0 for col in bool_columns}})
        
        for _, row in group_df.iterrows():
            address = str(row['ADDRESS'])
            rep_address = None
            
            # Find closest existing address
            for existing_addr in unique_addresses:
                try:
                    if lev_distance(address, existing_addr) <= 3:
                        rep_address = existing_addr
                        break
                except Exception as e:
                    print(f"Error in lev_distance between '{address}' and '{existing_addr}': {e}")
            
            # Use existing address or register a new one
            if rep_address is None:
                rep_address = address
            
            unique_addresses[rep_address]['COUNT'] += 1
            
            # Aggregate boolean columns
            for col in bool_columns:
                unique_addresses[rep_address][col] += int(row[col]) if pd.notna(row[col]) else 0
        
        # Convert results to rows
        for address, values in unique_addresses.items():
            row = {
                'HASH': hash_val,
                period: week,
                'ADDRESS': address,
                'COUNT': values['COUNT'],
                **{col: values[col] for col in bool_columns}
            }
            result_rows.append(row)
    
    return pd.DataFrame(result_rows)


def lev_dist_with_fields(data, lev_distance, period, bool_columns=None, text_columns=None):
    if bool_columns is None:
        bool_columns = ['DISBURSED', 'DR1']
    if text_columns is None:
        text_columns = ['INDUSTRY', 'POSITION']

    result_rows = []

    # Ensure boolean columns exist and are properly formatted
    data_copy = data.copy()
    for col in bool_columns:
        if col in data_copy.columns:
            data_copy[col] = pd.to_numeric(data_copy[col], errors='coerce').fillna(0).astype(int)
        else:
            print(f"Warning: Column '{col}' not found in the data")

    # Group by HASH and period
    grouped = data_copy.groupby(['HASH', period])

    for (hash_val, week), group_df in grouped:
        unique_combinations = defaultdict(lambda: {
            'COUNT': 0,
            **{col: 0 for col in bool_columns}
        })

        for _, row in group_df.iterrows():
            address = str(row['ADDRESS'])
            industry = str(row['INDUSTRY']) if 'INDUSTRY' in row and pd.notna(row['INDUSTRY']) else "UNKNOWN"
            position = str(row['POSITION']) if 'POSITION' in row and pd.notna(row['POSITION']) else "UNKNOWN"

            rep_address = None
            rep_position = None

            # Find closest existing address + industry
            for (existing_addr, existing_industry, existing_pos) in unique_combinations:
                try:
                    if lev_distance(address, existing_addr) <= 3 and industry == existing_industry:
                        rep_address = (existing_addr, existing_industry)
                        break
                except Exception as e:
                    print(f"Error in lev_distance between '{address}' and '{existing_addr}': {e}")

            if rep_address is None:
                rep_address = (address, industry)

            # Find closest position in the matched address group
            for (addr, ind, pos) in unique_combinations:
                if addr == rep_address[0] and ind == rep_address[1]:
                    try:
                        if lev_distance(position, pos) <= 5:
                            rep_position = pos
                            break
                    except Exception as e:
                        print(f"Error in lev_distance between '{position}' and '{pos}': {e}")

            if rep_position is None:
                rep_position = position

            # Use (Address, Industry, Position) as key
            rep_key = (rep_address[0], rep_address[1], rep_position)

            unique_combinations[rep_key]['COUNT'] += 1

            # Aggregate boolean columns
            for col in bool_columns:
                unique_combinations[rep_key][col] += int(row[col]) if pd.notna(row[col]) else 0

        # Convert results to rows
        for (address, industry, position), values in unique_combinations.items():
            row = {
                'HASH': hash_val,
                period: week,
                'ADDRESS': address,
                'INDUSTRY': industry,
                'POSITION': position,
                'COUNT': values['COUNT'],
                **{col: values[col] for col in bool_columns}
            }
            result_rows.append(row)

    return pd.DataFrame(result_rows)

