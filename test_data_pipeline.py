import unittest
import pandas as pd
import sqlite3
from data_pipeline import load_data, valiDate_data, transform_data, load_to_sqlite

class TestDataPipeline(unittest.TestCase):
    
    def setUp(self):
        """Set up sample data for testing."""
        self.sample_data = pd.DataFrame({
            "Date": ["2023-01-01", "2023-01-02", None, "2023-01-04"],
            "Category": ["Electronics", "Clothing", "Electronics", "Toys"],
            "Sales": [100.0, 200.0, 150.0, None]
        })

    def test_validate_data(self):
        """Test the data validation function."""
        validated_data = valiDate_data(self.sample_data)
        self.assertFalse(validated_data.isnull().values.any(), "Data validation failed, missing values still present")

    def test_transform_data(self):
        """Test the data transformation function."""
        
        # Test date standardization
        df_standardized = self.sample_data.copy()
        if 'Date' in df_standardized.columns:
            df_standardized['Date'] = pd.to_datetime(df_standardized['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
        
            # Check that the date format is standardized
            for date in df_standardized['Date'].dropna():  # Avoid checking NaT (Not a Time) values
                self.assertRegex(date, r'\d{4}-\d{2}-\d{2}', "Date format is not standardized")
        
        # Test aggregation by category
        transformed_data = transform_data(self.sample_data)
        
        # Check if the data is aggregated by Category
        expected_categories = self.sample_data['Category'].nunique()
        self.assertEqual(len(transformed_data['Category'].unique()), expected_categories, "Aggregation by Category failed")

    def test_load_to_sqlite(self):
        """Test loading data to SQLite."""
        db_name = "test_data_warehouse.db"
        table_name = "Sales_summary"
        
        transformed_data = transform_data(self.sample_data)
        load_to_sqlite(transformed_data, db_name=db_name, table_name=table_name)
        
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        table_exists = cursor.fetchone() is not None
        conn.close()
        
        self.assertTrue(table_exists, "Table was not created in SQLite database")

if __name__ == "__main__":
    unittest.main()
