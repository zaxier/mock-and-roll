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
from config import Config


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


def generate_datasets(config: Config, num_records: int = None) -> DataModel:
    """
    Generate all datasets for the sales demo.
    
    This creates a star schema with user profiles as the dimension table
    and sales transactions as the fact table.
    """
    # Use configured number of records or override
    base_records = num_records or config.data_generation.default_records
    
    # Scale datasets appropriately
    num_users = base_records
    num_sales = base_records * 5  # Average 5 transactions per user
    
    # Generate users first to get their IDs
    users_data = generate_user_profiles(num_users)
    users_dataset = Dataset(
        name="user_profiles",
        data=users_data,
        file_format="parquet"
    )
    
    # Generate sales data using actual user IDs from the users dataset
    sales_data = generate_sales_data(
        user_ids=users_data['user_id'].tolist(),
        num_records=num_sales
    )
    sales_dataset = Dataset(
        name="product_sales",
        data=sales_data,
        file_format="parquet"
    )
    
    # Organize into DataModel
    # AI Agent Instructions: This DataModel provides a complete star schema
    # with proper foreign key relationships for realistic retail analytics
    return DataModel(
        datasets=[users_dataset, sales_dataset]
    )


