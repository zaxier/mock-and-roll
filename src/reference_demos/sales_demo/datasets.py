"""Sales Demo Dataset Generation

Generates synthetic sales and customer data for Databricks demonstrations.
Creates realistic datasets with proper foreign key relationships using mimesis.

Example Implementation:
- This module demonstrates how to generate related datasets using a star schema pattern
- Shows how to create dimension tables with primary keys for reference data
- Illustrates fact tables with foreign keys linking to dimension tables
- Demonstrates realistic data patterns and relationships between tables
- Provides example of configurable record counts for different scenarios
"""

from typing import List
import random
import pandas as pd
from mimesis import Person, Finance, Datetime, Numeric, Address, Choice
from mimesis.locales import Locale

from core.data import DataModel, Dataset

def generate_user_profiles(num_records=100) -> pd.DataFrame:
    """Generate user profile dataset"""
    person = Person(Locale.EN)
    dt = Datetime()
    address = Address()
    choice = Choice()

    return pd.DataFrame({
        'user_id': [person.identifier() for _ in range(num_records)],
        'full_name': [person.full_name() for _ in range(num_records)],
        'first_name': [person.first_name() for _ in range(num_records)],
        'last_name': [person.last_name() for _ in range(num_records)],
        'phone': [person.phone_number() for _ in range(num_records)],
        'address': [address.address() for _ in range(num_records)],
        'city': [address.city() for _ in range(num_records)],
        'state': [address.state() for _ in range(num_records)],
        'postal_code': [address.postal_code() for _ in range(num_records)],
        'email': [person.email() for _ in range(num_records)],
        'signup_date': [dt.date(start=2020, end=2025) for _ in range(num_records)],
        'customer_type': [choice.choice(['individual', 'business']) for _ in range(num_records)]
    })

def generate_sales_data(user_ids: List[str], num_records=50) -> pd.DataFrame:
    """Generate product sales dataset with user_id as foreign key"""
    finance = Finance()
    dt = Datetime()
    person = Person()
    numeric = Numeric()
    choice = Choice()
    
    return pd.DataFrame({
        'transaction_id': [person.identifier(mask='TXN-########') for _ in range(num_records)],
        'user_id': [random.choice(user_ids) for _ in range(num_records)],  # Common join key
        'product': [finance.company() for _ in range(num_records)],
        'amount': [finance.price(minimum=10, maximum=1000) for _ in range(num_records)],
        'sale_date': [dt.date(start=2024, end=2025) for _ in range(num_records)],
        'quantity': [numeric.integer_number(start=1, end=10) for _ in range(num_records)],
        'unit_price': [numeric.float_number(start=10, end=1000) for _ in range(num_records)],
        'payment_method': [choice.choice(['Credit Card', 'Debit Card', 'PayPal', 'Cash']) for _ in range(num_records)]
    })

# Create Dataset instances with proper relationships ------------------
# Generate users first to get their IDs
users_data = generate_user_profiles()
users_dataset = Dataset(
    name="user_profiles",
    data=users_data
)

# Generate sales data using actual user IDs from the users dataset
sales_data = generate_sales_data(user_ids=users_data['user_id'].tolist())
sales_dataset = Dataset(
    name="product_sales",
    data=sales_data
)

# Organize into DataModel ---------------------------------------------
# AI Agent Instructions: This DataModel provides a complete star schema
# with proper foreign key relationships for realistic retail analytics
data_model = DataModel(
    datasets=[users_dataset, sales_dataset]
)

# # Example usage -------------------------------------------------------
# print(f"DataModel contains {len(data_model.datasets)} datasets")
# print("Dataset names:", [ds.name for ds in data_model.datasets])

# # Access first dataset's DataFrame
# user_df = data_model.datasets[0].data
# print("\nUser profile sample:")
# print(user_df.head(3))

# # Access sales data with user_id join key
# sales_df = data_model.datasets[1].data
# print("\nSales data sample:")
# print(sales_df.head(3))

# # Calculate total sales
# total_sales = sales_df['amount'].sum()
# print(f"\nTotal sales: ${total_sales:,.2f}")

# # Example join operation
# joined_data = user_df.merge(sales_df, on='user_id', how='inner')
# print(f"\nJoined data sample (showing users with sales):")
# print(joined_data[['full_name', 'email', 'product', 'amount', 'sale_date']].head(3))
