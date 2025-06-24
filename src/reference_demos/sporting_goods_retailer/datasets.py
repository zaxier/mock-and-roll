"""
Sporting Goods Retailer Dataset Generator

This module generates synthetic data for a sporting goods retailer with:
- Physical stores and online channels (omnichannel)
- Product catalog with sporting goods categories
- Customer profiles and purchase history
- Inventory tracking across warehouses and stores
- Supply chain data for optimization analysis
"""

import random
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from mimesis import Person, Finance, Datetime, Numeric, Address, Choice, Code, Text
from mimesis.locales import Locale

from core.data import DataModel, Dataset
from config import Config


def generate_stores(num_stores: int = 25) -> pd.DataFrame:
    """Generate physical store locations with details."""
    address = Address(Locale.EN)
    choice = Choice()
    numeric = Numeric()
    
    stores = []
    store_types = ['Flagship', 'Standard', 'Outlet', 'Express']
    regions = ['Northeast', 'Southeast', 'Midwest', 'Southwest', 'West']
    
    for i in range(num_stores):
        store_id = f"STR{i+1:04d}"
        stores.append({
            'store_id': store_id,
            'store_name': f"{address.city()} Sports Center",
            'store_type': choice.choice(store_types),
            'address': address.address(),
            'city': address.city(),
            'state': address.state(abbr=True),
            'postal_code': address.postal_code(),
            'region': choice.choice(regions),
            'latitude': float(address.latitude()),
            'longitude': float(address.longitude()),
            'square_footage': numeric.integer_number(start=5000, end=50000),
            'employees': numeric.integer_number(start=10, end=150),
            'opening_date': datetime.now() - timedelta(days=random.randint(365, 3650)),
            'is_active': True
        })
    
    return pd.DataFrame(stores)


def generate_product_catalog(num_products: int = 500) -> pd.DataFrame:
    """Generate sporting goods product catalog."""
    finance = Finance()
    choice = Choice()
    code = Code()
    text = Text()
    numeric = Numeric()
    
    products = []
    
    # Define sporting goods categories and subcategories
    categories = {
        'Team Sports': ['Basketball', 'Football', 'Soccer', 'Baseball', 'Hockey', 'Volleyball'],
        'Outdoor': ['Camping', 'Hiking', 'Climbing', 'Fishing', 'Hunting'],
        'Fitness': ['Cardio Equipment', 'Strength Training', 'Yoga', 'Home Gym', 'Recovery'],
        'Water Sports': ['Swimming', 'Surfing', 'Kayaking', 'Paddleboarding', 'Diving'],
        'Winter Sports': ['Skiing', 'Snowboarding', 'Ice Skating', 'Snowshoeing'],
        'Cycling': ['Road Bikes', 'Mountain Bikes', 'Electric Bikes', 'Accessories'],
        'Running': ['Running Shoes', 'Apparel', 'Accessories', 'Nutrition'],
        'Sports Apparel': ['Men\'s Wear', 'Women\'s Wear', 'Kids\' Wear', 'Footwear']
    }
    
    brands = ['Nike', 'Adidas', 'Under Armour', 'Puma', 'New Balance', 'Reebok', 
              'Columbia', 'North Face', 'Patagonia', 'Wilson', 'Spalding', 'Rawlings',
              'Coleman', 'Yeti', 'Garmin', 'Fitbit', 'Speedo', 'TYR', 'Burton', 'Salomon']
    
    for i in range(num_products):
        category = choice.choice(list(categories.keys()))
        subcategory = choice.choice(categories[category])
        brand = choice.choice(brands)
        
        # Generate price based on category
        if category in ['Outdoor', 'Cycling', 'Winter Sports']:
            base_price = numeric.float_number(start=50, end=2000)
        elif category == 'Fitness':
            base_price = numeric.float_number(start=20, end=3000)
        else:
            base_price = numeric.float_number(start=15, end=500)
        
        products.append({
            'product_id': f"PRD{i+1:06d}",
            'sku': code.isbn(),
            'product_name': f"{brand} {subcategory} {text.word().title()}",
            'brand': brand,
            'category': category,
            'subcategory': subcategory,
            'description': text.sentence(),
            'unit_cost': round(base_price * 0.6, 2),
            'retail_price': round(base_price, 2),
            'weight_kg': round(numeric.float_number(start=0.1, end=50), 2),
            'dimensions_cm': f"{numeric.integer_number(start=10, end=200)}x{numeric.integer_number(start=10, end=100)}x{numeric.integer_number(start=5, end=50)}",
            'is_active': choice.choice([True, True, True, False]),  # 75% active
            'launch_date': datetime.now() - timedelta(days=random.randint(30, 1095))
        })
    
    return pd.DataFrame(products)


def generate_customers(num_customers: int = 10000) -> pd.DataFrame:
    """Generate customer profiles for omnichannel retail."""
    person = Person(Locale.EN)
    address = Address(Locale.EN)
    dt = Datetime()
    choice = Choice()
    numeric = Numeric()
    
    customers = []
    customer_types = ['Regular', 'Premium', 'VIP', 'New']
    preferred_channels = ['Online', 'In-Store', 'Omnichannel']
    
    for i in range(num_customers):
        signup_date = datetime.now() - timedelta(days=random.randint(1, 1825))
        
        customers.append({
            'customer_id': f"CUST{i+1:08d}",
            'email': person.email(),
            'first_name': person.first_name(),
            'last_name': person.last_name(),
            'phone': person.phone_number(),
            'address': address.address(),
            'city': address.city(),
            'state': address.state(abbr=True),
            'postal_code': address.postal_code(),
            'date_of_birth': dt.date(start=1950, end=2005),
            'gender': choice.choice(['M', 'F', 'Other', 'Prefer not to say']),
            'customer_type': choice.choice(customer_types),
            'preferred_channel': choice.choice(preferred_channels),
            'loyalty_points': numeric.integer_number(start=0, end=50000),
            'lifetime_value': round(numeric.float_number(start=100, end=25000), 2),
            'signup_date': signup_date,
            'last_purchase_date': signup_date + timedelta(days=random.randint(0, (datetime.now() - signup_date).days)),
            'email_opt_in': choice.choice([True, True, False]),  # 67% opt-in
            'sms_opt_in': choice.choice([True, False, False])   # 33% opt-in
        })
    
    return pd.DataFrame(customers)


def generate_inventory(stores_df: pd.DataFrame, products_df: pd.DataFrame, num_warehouses: int = 5) -> pd.DataFrame:
    """Generate inventory levels across stores and warehouses."""
    numeric = Numeric()
    choice = Choice()
    
    inventory = []
    
    # Generate warehouse IDs
    warehouse_ids = [f"WH{i+1:03d}" for i in range(num_warehouses)]
    
    # For each location (stores + warehouses)
    all_locations = list(stores_df['store_id']) + warehouse_ids
    
    # Sample products for each location (not all products in all locations)
    for location_id in all_locations:
        is_warehouse = location_id.startswith('WH')
        
        # Warehouses carry more products than stores
        num_products_in_location = int(len(products_df) * (0.8 if is_warehouse else 0.4))
        products_sample = products_df.sample(n=num_products_in_location)
        
        for _, product in products_sample.iterrows():
            # Warehouses have higher stock levels
            if is_warehouse:
                stock_quantity = numeric.integer_number(start=50, end=5000)
                min_stock = numeric.integer_number(start=100, end=500)
                max_stock = numeric.integer_number(start=1000, end=10000)
            else:
                stock_quantity = numeric.integer_number(start=0, end=200)
                min_stock = numeric.integer_number(start=5, end=20)
                max_stock = numeric.integer_number(start=50, end=300)
            
            inventory.append({
                'location_id': location_id,
                'location_type': 'Warehouse' if is_warehouse else 'Store',
                'product_id': product['product_id'],
                'sku': product['sku'],
                'quantity_on_hand': stock_quantity,
                'quantity_reserved': numeric.integer_number(start=0, end=min(20, stock_quantity)),
                'min_stock_level': min_stock,
                'max_stock_level': max_stock,
                'reorder_point': min_stock + numeric.integer_number(start=10, end=50),
                'last_restock_date': datetime.now() - timedelta(days=random.randint(1, 30)),
                'last_count_date': datetime.now() - timedelta(days=random.randint(1, 7))
            })
    
    return pd.DataFrame(inventory)


def generate_transactions(customers_df: pd.DataFrame, products_df: pd.DataFrame, 
                         stores_df: pd.DataFrame, num_transactions: int = 50000) -> pd.DataFrame:
    """Generate sales transactions for both online and in-store purchases."""
    dt = Datetime()
    numeric = Numeric()
    choice = Choice()
    person = Person()
    
    transactions = []
    channels = ['Online', 'In-Store', 'Mobile App', 'Phone']
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Apple Pay', 'Google Pay', 'Cash', 'Gift Card']
    
    # Generate transactions over the past year
    for i in range(num_transactions):
        customer = customers_df.sample(n=1).iloc[0]
        num_items = random.randint(1, 8)
        
        # Determine channel based on customer preference
        if customer['preferred_channel'] == 'Omnichannel':
            channel = choice.choice(channels)
        elif customer['preferred_channel'] == 'Online':
            channel = choice.choice(['Online', 'Mobile App'])
        else:
            channel = 'In-Store'
        
        # Select store for in-store purchases
        store_id = stores_df.sample(n=1).iloc[0]['store_id'] if channel == 'In-Store' else None
        
        transaction_date = datetime.now() - timedelta(days=random.randint(1, 365))
        
        # Generate line items
        products_sample = products_df.sample(n=num_items)
        subtotal = 0
        
        for _, product in products_sample.iterrows():
            quantity = random.randint(1, 3)
            unit_price = product['retail_price']
            discount_pct = choice.choice([0, 0, 0, 0.1, 0.15, 0.2, 0.25])  # Most items no discount
            line_total = quantity * unit_price * (1 - discount_pct)
            subtotal += line_total
            
            transactions.append({
                'transaction_id': f"TXN{i+1:010d}",
                'line_item_id': f"TXN{i+1:010d}-{products_sample.index.get_loc(product.name)+1}",
                'customer_id': customer['customer_id'],
                'product_id': product['product_id'],
                'sku': product['sku'],
                'store_id': store_id,
                'channel': channel,
                'transaction_date': transaction_date,
                'quantity': quantity,
                'unit_price': unit_price,
                'discount_percent': discount_pct,
                'line_total': round(line_total, 2),
                'payment_method': choice.choice(payment_methods),
                'loyalty_points_earned': int(line_total),
                'is_return': False
            })
        
        # Add some returns (5% of transactions)
        if random.random() < 0.05:
            return_date = transaction_date + timedelta(days=random.randint(1, 30))
            if return_date <= datetime.now():
                return_items = products_sample.sample(n=random.randint(1, min(3, len(products_sample))))
                for _, product in return_items.iterrows():
                    transactions.append({
                        'transaction_id': f"RTN{i+1:010d}",
                        'line_item_id': f"RTN{i+1:010d}-{return_items.index.get_loc(product.name)+1}",
                        'customer_id': customer['customer_id'],
                        'product_id': product['product_id'],
                        'sku': product['sku'],
                        'store_id': store_id,
                        'channel': channel,
                        'transaction_date': return_date,
                        'quantity': -1,
                        'unit_price': product['retail_price'],
                        'discount_percent': 0,
                        'line_total': -product['retail_price'],
                        'payment_method': 'Refund',
                        'loyalty_points_earned': -int(product['retail_price']),
                        'is_return': True
                    })
    
    return pd.DataFrame(transactions)


def generate_suppliers(num_suppliers: int = 50) -> pd.DataFrame:
    """Generate supplier information for supply chain management."""
    finance = Finance()
    address = Address()
    person = Person()
    choice = Choice()
    numeric = Numeric()
    
    suppliers = []
    supplier_types = ['Manufacturer', 'Distributor', 'Wholesaler', 'Direct Brand']
    reliability_ratings = ['Excellent', 'Good', 'Fair', 'Poor']
    
    for i in range(num_suppliers):
        suppliers.append({
            'supplier_id': f"SUP{i+1:05d}",
            'supplier_name': finance.company(),
            'contact_name': person.full_name(),
            'contact_email': person.email(),
            'contact_phone': person.phone_number(),
            'address': address.address(),
            'city': address.city(),
            'state': address.state(abbr=True),
            'country': address.country(),
            'supplier_type': choice.choice(supplier_types),
            'payment_terms': choice.choice(['Net 30', 'Net 45', 'Net 60', '2/10 Net 30']),
            'minimum_order_value': numeric.integer_number(start=500, end=10000),
            'lead_time_days': numeric.integer_number(start=3, end=45),
            'reliability_rating': choice.choice(reliability_ratings),
            'contract_start_date': datetime.now() - timedelta(days=random.randint(180, 1095)),
            'is_active': True
        })
    
    return pd.DataFrame(suppliers)


def generate_supply_chain_data(products_df: pd.DataFrame, suppliers_df: pd.DataFrame, 
                              inventory_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Generate purchase orders and shipments for supply chain optimization."""
    dt = Datetime()
    numeric = Numeric()
    choice = Choice()
    code = Code()
    
    # Product-Supplier mapping
    product_suppliers = []
    for _, product in products_df.iterrows():
        # Each product has 1-3 suppliers
        num_suppliers = random.randint(1, 3)
        suppliers_sample = suppliers_df.sample(n=num_suppliers)
        
        for _, supplier in suppliers_sample.iterrows():
            product_suppliers.append({
                'product_id': product['product_id'],
                'supplier_id': supplier['supplier_id'],
                'is_primary': True if num_suppliers == 1 else random.choice([True, False]),
                'unit_cost': product['unit_cost'] * random.uniform(0.9, 1.1),
                'minimum_order_quantity': numeric.integer_number(start=10, end=500)
            })
    
    product_suppliers_df = pd.DataFrame(product_suppliers)
    
    # Purchase Orders
    purchase_orders = []
    warehouse_inventory = inventory_df[inventory_df['location_type'] == 'Warehouse']
    
    # Generate POs for low stock items
    for _, item in warehouse_inventory.iterrows():
        if item['quantity_on_hand'] < item['reorder_point']:
            # Find supplier for this product
            product_supplier = product_suppliers_df[
                product_suppliers_df['product_id'] == item['product_id']
            ].sample(n=1).iloc[0]
            
            order_quantity = item['max_stock_level'] - item['quantity_on_hand']
            order_date = datetime.now() - timedelta(days=random.randint(1, 30))
            
            purchase_orders.append({
                'po_number': f"PO{len(purchase_orders)+1:08d}",
                'supplier_id': product_supplier['supplier_id'],
                'warehouse_id': item['location_id'],
                'order_date': order_date,
                'expected_delivery_date': order_date + timedelta(days=int(suppliers_df[
                    suppliers_df['supplier_id'] == product_supplier['supplier_id']
                ].iloc[0]['lead_time_days'])),
                'product_id': item['product_id'],
                'quantity_ordered': order_quantity,
                'unit_cost': product_supplier['unit_cost'],
                'total_cost': order_quantity * product_supplier['unit_cost'],
                'status': choice.choice(['Pending', 'Shipped', 'Delivered', 'Partial']),
                'tracking_number': code.isbn() if random.random() > 0.3 else None
            })
    
    return product_suppliers_df, pd.DataFrame(purchase_orders)


def generate_omnichannel_events(customers_df: pd.DataFrame, products_df: pd.DataFrame,
                               num_events: int = 100000) -> pd.DataFrame:
    """Generate customer interaction events across all channels."""
    dt = Datetime()
    choice = Choice()
    person = Person()
    
    events = []
    event_types = ['Product View', 'Add to Cart', 'Remove from Cart', 'Wishlist Add', 
                   'Search', 'Category Browse', 'Store Visit', 'Email Open', 'Email Click',
                   'App Open', 'Push Notification', 'Review Submitted', 'Customer Service Contact']
    
    channels = ['Website', 'Mobile App', 'In-Store', 'Email', 'Social Media', 'Customer Service']
    
    for i in range(num_events):
        customer = customers_df.sample(n=1).iloc[0]
        event_type = choice.choice(event_types)
        
        # Determine channel based on event type
        if event_type in ['Product View', 'Add to Cart', 'Remove from Cart', 'Search', 'Category Browse']:
            channel = choice.choice(['Website', 'Mobile App'])
        elif event_type == 'Store Visit':
            channel = 'In-Store'
        elif event_type in ['Email Open', 'Email Click']:
            channel = 'Email'
        else:
            channel = choice.choice(channels)
        
        event_timestamp = datetime.now() - timedelta(days=random.randint(1, 90))
        
        event_data = {
            'event_id': f"EVT{i+1:012d}",
            'customer_id': customer['customer_id'],
            'event_type': event_type,
            'channel': channel,
            'timestamp': event_timestamp,
            'session_id': person.identifier(mask='####-####-####'),
            'ip_address': f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
            'user_agent': choice.choice(['Chrome/Windows', 'Safari/Mac', 'Mobile/iOS', 'Mobile/Android'])
        }
        
        # Add product-specific data for relevant events
        if event_type in ['Product View', 'Add to Cart', 'Remove from Cart', 'Wishlist Add', 'Review Submitted']:
            product = products_df.sample(n=1).iloc[0]
            event_data['product_id'] = product['product_id']
            event_data['product_category'] = product['category']
        
        # Add search query for search events
        if event_type == 'Search':
            event_data['search_query'] = choice.choice(['running shoes', 'basketball', 'yoga mat', 
                                                       'camping tent', 'bike helmet', 'swimming goggles'])
        
        events.append(event_data)
    
    return pd.DataFrame(events)


def generate_datasets(config: Config, num_records: int = None) -> DataModel:
    """
    Generate all datasets for the sporting goods retailer demo.
    
    This creates a comprehensive omnichannel retail dataset including:
    - Store locations
    - Product catalog
    - Customer profiles
    - Inventory levels
    - Sales transactions
    - Supplier information
    - Supply chain data (POs and product-supplier mappings)
    - Customer interaction events
    """
    # Use configured number of records or override
    base_records = num_records or config.data_generation.default_records
    
    # Scale different datasets appropriately
    num_stores = max(10, base_records // 2000)
    num_products = max(100, base_records // 100)
    num_customers = base_records
    num_transactions = base_records * 5  # Multiple transactions per customer
    num_suppliers = max(20, base_records // 500)
    num_events = base_records * 10  # Many events per customer
    
    # Generate base datasets
    stores_df = generate_stores(num_stores)
    products_df = generate_product_catalog(num_products)
    customers_df = generate_customers(num_customers)
    
    # Generate dependent datasets
    inventory_df = generate_inventory(stores_df, products_df)
    transactions_df = generate_transactions(customers_df, products_df, stores_df, num_transactions)
    suppliers_df = generate_suppliers(num_suppliers)
    product_suppliers_df, purchase_orders_df = generate_supply_chain_data(products_df, suppliers_df, inventory_df)
    events_df = generate_omnichannel_events(customers_df, products_df, num_events)
    
    # Create Dataset instances
    datasets = [
        Dataset(name="stores", data=stores_df, file_format="parquet"),
        Dataset(name="products", data=products_df, file_format="parquet"),
        Dataset(name="customers", data=customers_df, file_format="parquet"),
        Dataset(name="inventory", data=inventory_df, file_format="parquet"),
        Dataset(name="transactions", data=transactions_df, file_format="parquet"),
        Dataset(name="suppliers", data=suppliers_df, file_format="parquet"),
        Dataset(name="product_suppliers", data=product_suppliers_df, file_format="parquet"),
        Dataset(name="purchase_orders", data=purchase_orders_df, file_format="parquet"),
        Dataset(name="customer_events", data=events_df, file_format="parquet")
    ]
    
    return DataModel(datasets=datasets)