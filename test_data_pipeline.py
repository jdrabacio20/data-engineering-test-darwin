import unittest
import pandas as pd
import sqlite3
from data_pipeline import load_data, validate_data, transform_data, load_to_sqlite

class TestDataPipeline(unittest.TestCase):
    
    def setUp(self):
        """Set up sample data for testing."""
        self.sample_data = pd.DataFrame({
            "date": ["2023-01-01", "2023-01-02", None, "2023-01-04"],
            "category": ["Electronics", "Clothing", "Electronics", "Toys"],
            "sales": [100.0, 200.0, 150.0, None]
        })

    def test_validate_data(self):
        """Test the data validation function."""
        validated_data = validate_data(self.sample_data)
        self.assertFalse(validated_data.isnull().values.any(), "Data validation failed, missing values still present")

        def test_transform_data(self):
        """Test the data transformation function."""
        
        # Test date standardization
        df_standardized = self.sample_data.copy()
        if 'date' in df_standardized.columns:
            df_standardized['date'] = pd.to_datetime(df_standardized['date'], errors='coerce').dt.strftime('%Y-%m-%d')
        
            # Check that the date format is standardized
            for date in df_standardized['date'].dropna():  # Avoid checking NaT (Not a Time) values
                self.assertRegex(date, r'\d{4}-\d{2}-\d{2}', "Date format is not standardized")
        
        # Test aggregation by category
        transformed_data = transform_data(self.sample_data)
        
        # Check if the data is aggregated by category
        expected_categories = self.sample_data['category'].nunique()
        self.assertEqual(len(transformed_data['category'].unique()), expected_categories, "Aggregation by category failed")

    def test_load_to_sqlite(self):
        """Test loading data to SQLite."""
        db_name = "test_data_warehouse.db"
        table_name = "sales_summary"
        
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
