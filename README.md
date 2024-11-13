# Data Engineering Assessment

## Introduction

This project is a data engineering assessment aimed at demonstrating proficiency in data processing and ETL (Extract, Transform, Load) using Python.

## Objective

The purpose of this task is to construct a data pipeline that:
1. Ingests data from a publicly available dataset.
2. Cleanses and validates the data.
3. Applies transformations, including standardizing dates and performing aggregations.
4. Loads the transformed data into a mock data warehouse (CSV file or SQLite database).

## Dataset

The pipeline will utilize a sample dataset generated in Python, covering data from the previous year up to the current date. The dataset includes fields such as `[Date, Sales, Category, etc.]`.

## Requirements

- Python-based ETL pipeline.
- Data validation mechanisms.
- Data transformations (e.g., date formatting, data aggregation).
- Loading of the transformed data to a structured format (CSV or SQLite).
- Unit tests to ensure functionality.
- Comprehensive documentation and code comments.

## Pipeline Functions

- **load_data**: Loads data from a CSV file into a pandas DataFrame.
- **valiDate_data**: Checks for and removes any rows with missing values to ensure data integrity.
- **transform_data**: Standardises date formats and aggregates sales by category.
- **load_to_sqlite**: Loads the cleaned and transformed data into an SQLite database as a structured table.

## Setup and Installation

1. **Install Python**: Ensure Python 3.x is installed. You can download it from [python.org](https://www.python.org/downloads/).
2. **Install Dependencies**: Install the required libraries using `pip`:
   ```bash
   pip install pandas
   ```

## Running the Pipeline

To run the data pipeline, execute the following command:
```bash
python data_pipeline.py
```

Ensure that `sample_data.csv` (or your data file) is located in the specified path in `data_pipeline.py`. You may need to update the `file_path` variable in the script to match your data file location.

## Running the Tests

To run the tests, use the following command:
```bash
python -m unittest test_data_pipeline.py
```

This will execute the test suite to verify the functionality of each function in the data pipeline.

## File Structure

```
├── data_pipeline.py          # Main data pipeline script
├── test_data_pipeline.py     # Unit tests for the pipeline functions
├── sample_data.csv           # Sample data file (if applicable)
├── data_warehouse.db         # Output SQLite database (generated)
├── README.md                 # Project documentation
└── .gitignore                # Git ignore file
```

## Example Output

### Pipeline Output

After running `data_pipeline.py`, you should see output messages indicating each step's completion, such as:

```
Data loaded successfully.
No missing values found.
Dates standardized.
Data aggregated by Category.
Data loaded successfully into Sales_summary table in data_warehouse.db.
```

The transformed data will be stored in `data_warehouse.db` in a table named `Sales_summary`.

### Test Output

When running `test_data_pipeline.py`, you should see output similar to the following, indicating that all tests have passed successfully:

```
Dates standardized.
Data aggregated by Category.
Data loaded successfully into Sales_summary table in test_data_warehouse.db.
.Dates standardized.
Data aggregated by Category.
.Found 2 missing values. Cleaning data.
.
----------------------------------------------------------------------
Ran 3 tests in 0.016s

OK
```

This output shows:
- Each function’s output as it runs.
- A summary indicating that all tests passed (`OK`) and that the tests ran without any issues.

---