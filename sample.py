import pandas as pd
import random
from datetime import datetime, timedelta

# Define the number of records
num_records = 10000

# Generate sample data
def generate_sample_data(num_records):
    # Define categories and regions
    categories = ["Electronics", "Clothing", "Furniture", "Toys", "Books", "Groceries"]
    regions = ["North", "South", "East", "West", "Central"]

    # Start date for generating random dates
    start_date = datetime(2023, 11, 1)

    data = {
        "date": [],
        "category": [],
        "sales": [],
        "product_id": [],
        "region": []
    }

    for _ in range(num_records):
        # Random date within a year from start_date
        random_date = start_date + timedelta(days=random.randint(0, 365))
        data["date"].append(random_date.strftime("%Y-%m-%d"))
        
        # Random category
        data["category"].append(random.choice(categories))
        
        # Random sales amount between 10 and 1000
        data["sales"].append(round(random.uniform(10, 1000), 2))
        
        # Random product ID between 1000 and 2000
        data["product_id"].append(random.randint(1000, 2000))
        
        # Random region
        data["region"].append(random.choice(regions))

    # Create a DataFrame
    df = pd.DataFrame(data)
    return df

# Generate the data
df = generate_sample_data(num_records)

# Save to a CSV file
df.to_csv("sample_sales_data.csv", index=False)

print("Sample data generated and saved to sample_sales_data.csv")
