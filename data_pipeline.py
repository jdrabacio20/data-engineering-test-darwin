import pandas as pd
import sqlite3
from datetime import datetime

def load_data(file_path):
    """Loads data from a CSV file and returns a pandas DataFrame."""
    try:
        df = pd.read_csv(file_path)
        print("Data loaded successfully.")
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def valiDate_data(df):
    """ValiDates data by checking for missing values in critical columns."""
    if df is None:
        raise ValueError("Data not loaded. Please check the data source.")
    
    missing_values_count = df.isnull().sum().sum()
    if missing_values_count > 0:
        print(f"Found {missing_values_count} missing values. Cleaning data.")
        df.dropna(inplace=True)
    else:
        print("No missing values found.")
    return df

def transform_data(df):
    """Transforms data by standardising Date format and aggregating Sales by Category."""
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
        print("Dates standardized.")
    
    if 'Category' in df.columns and 'Sales' in df.columns:
        transformed_df = df.groupby('Category', as_index=False)['Sales'].sum()
        print("Data aggregated by Category.")
    else:
        print("No 'Category' or 'Sales' columns found for aggregation.")
        transformed_df = df
    return transformed_df

def load_to_sqlite(df, db_name='data_warehouse.db', table_name='Sales_summary'):
    """Loads the transformed data into a SQLite database."""
    try:
        conn = sqlite3.connect(db_name)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        print(f"Data loaded successfully into {table_name} table in {db_name}.")
    except Exception as e:
        print(f"Error loading data to database: {e}")

def main():
    file_path = 'data/sample_data.csv'  # Replace with actual file path or URL
    
    df = load_data(file_path)
    df = valiDate_data(df)
    df = transform_data(df)
    load_to_sqlite(df)

if __name__ == "__main__":
    main()