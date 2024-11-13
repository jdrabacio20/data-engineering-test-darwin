import pandas as pd
import sqlite3
from datetime import datetime

def load_data(file_path):
    """Loads data from a CSV file and returns a pandas DataFrame."""
    try:
        # Load data from the specified CSV file into a DataFrame
        df = pd.read_csv(file_path)
        print("Data loaded successfully.")
        return df
    except Exception as e:
        # Print an error message if loading fails and return None
        print(f"Error loading data: {e}")
        return None

def valiDate_data(df):
    """Validates data by checking for missing values in critical columns.
    
    Parameters:
    - df: The DataFrame containing the loaded data.
    
    Returns:
    - DataFrame with missing values removed if any were found.
    """
    if df is None:
        raise ValueError("Data not loaded. Please check the data source.")
    
    # Count total missing values in the DataFrame
    missing_values_count = df.isnull().sum().sum()
    if missing_values_count > 0:
        print(f"Found {missing_values_count} missing values. Cleaning data.")
        # Drop rows with missing values
        df.dropna(inplace=True)
    else:
        print("No missing values found.")
    return df

def transform_data(df):
    """Transforms data by standardising date format and aggregating sales by category.
    
    Parameters:
    - df: The DataFrame containing validated data.
    
    Returns:
    - Transformed DataFrame with standardized dates and aggregated sales by category.
    """
    # Standardize date format if 'Date' column exists
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
        print("Dates standardized.")
    
    # Aggregate sales by category if 'Category' and 'Sales' columns are present
    if 'Category' in df.columns and 'Sales' in df.columns:
        transformed_df = df.groupby('Category', as_index=False)['Sales'].sum()
        print("Data aggregated by Category.")
    else:
        # If necessary columns are missing, return the original DataFrame
        print("No 'Category' or 'Sales' columns found for aggregation.")
        transformed_df = df
    return transformed_df

def load_to_sqlite(df, db_name='data_warehouse.db', table_name='Sales_summary'):
    """Loads the transformed data into a SQLite database.
    
    Parameters:
    - df: The DataFrame containing the transformed data.
    - db_name: Name of the SQLite database file to store the data.
    - table_name: Name of the table in the database where data will be loaded.
    """
    try:
        # Establish a connection to the SQLite database
        conn = sqlite3.connect(db_name)
        # Write DataFrame to the specified SQLite table
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        print(f"Data loaded successfully into {table_name} table in {db_name}.")
    except Exception as e:
        # Print an error message if loading to the database fails
        print(f"Error loading data to database: {e}")

def main():
    """Main function to run the data pipeline."""
    # Define the path to the CSV file
    file_path = 'data/sample_data.csv'  # Replace with actual file path or URL
    
    # Run each step of the data pipeline
    df = load_data(file_path)
    df = valiDate_data(df)
    df = transform_data(df)
    load_to_sqlite(df)

if __name__ == "__main__":
    main()