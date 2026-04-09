# formatter.py

import pandas as pd

def add_condition_headers(df):
    """
    Adds header rows (###condition) before each group of condition results.
    Also sorts each condition group by 'Symbol' before adding.
    """
    conditions = df['condition'].unique()
    df_with_headers = pd.DataFrame()

    for condition in conditions:
        condition_df = df[df['condition'] == condition].copy()

        # Sort each group by Symbol alphabetically
        if 'Symbol' in condition_df.columns:
            condition_df = condition_df.sort_values(by='Symbol', ascending=True)
        elif 'symbol' in condition_df.columns:
            condition_df = condition_df.sort_values(by='symbol', ascending=True)

        # Create a header row for this condition
        header_row = {col: f"###{condition}" for col in condition_df.columns}
        header_df = pd.DataFrame([header_row])

        # Append header + sorted data
        df_with_headers = pd.concat([df_with_headers, header_df, condition_df], ignore_index=True)
    
    return df_with_headers
