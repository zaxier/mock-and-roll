# Mimesis Fake Data Generator - Usage Guide

## Overview
Mimesis is a high-performance Python library for generating realistic fake data. It supports 46 locales, is fully typed, and offers extensive customization options.

## Installation
```bash
pip install mimesis
# For Python 3.8-3.9, use: pip install mimesis==11.1.0
```

## Basic Usage

### Simple Data Generation
```python
from mimesis import Person, Address, Finance
from mimesis.locales import Locale

# Create providers
person = Person(Locale.EN)
address = Address(Locale.EN)
finance = Finance(Locale.EN)

# Generate data
print(person.full_name())           # 'John Smith'
print(person.email())               # 'john.smith@example.com'
print(address.city())               # 'New York'
print(finance.credit_card_number()) # '4532-1234-5678-9012'
```

### Locale Support
```python
from mimesis import Person
from mimesis.locales import Locale

# Different locales
person_en = Person(Locale.EN)      # English
person_de = Person(Locale.DE)      # German
person_fr = Person(Locale.FR)      # French
person_ja = Person(Locale.JA)      # Japanese

print(person_en.full_name())       # 'Alice Johnson'
print(person_de.full_name())       # 'Hans Müller'
```

## Available Providers

### Person Provider
```python
from mimesis import Person

person = Person()

# Names
person.first_name()                # 'Alice'
person.last_name()                 # 'Johnson'
person.full_name()                 # 'Alice Johnson'
person.username()                  # 'alice_johnson92'

# Contact
person.email()                     # 'alice@example.com'
person.email(domains=['myapp.com']) # 'alice@myapp.com'
person.phone_number()              # '+1-555-123-4567'

# Personal details
person.age()                       # 25
person.gender()                    # 'Female'
person.nationality()               # 'American'
person.occupation()                # 'Software Engineer'
```

### Address Provider
```python
from mimesis import Address

address = Address()

# Geographic
address.country()                  # 'United States'
address.state()                    # 'California'
address.city()                     # 'Los Angeles'
address.street_name()              # 'Main Street'
address.street_number()            # '123'
address.address()                  # '123 Main Street'
address.postal_code()              # '90210'

# Coordinates
address.latitude()                 # 34.0522
address.longitude()                # -118.2437
address.coordinates()              # (34.0522, -118.2437)
```

### Finance Provider
```python
from mimesis import Finance

finance = Finance()

# Banking
finance.bank()                     # 'Chase Bank'
finance.currency_code()            # 'USD'
finance.currency_symbol()          # '$'

# Cards
finance.credit_card_number()       # '4532-1234-5678-9012'
finance.credit_card_network()      # 'Visa'
finance.cvv()                      # '123'
finance.credit_card_expiration_date() # '12/25'

# Money
finance.price(minimum=10, maximum=1000) # 234.56
```

### Internet Provider
```python
from mimesis import Internet

internet = Internet()

# Web
internet.url()                     # 'https://example.com'
internet.domain_name()             # 'example.com'
internet.email_domain()            # 'gmail.com'
internet.hashtag()                 # '#technology'
internet.ip_v4()                   # '192.168.1.1'
internet.mac_address()             # '00:16:3e:12:34:56'

# Social
internet.emoji()                   # '😀'
internet.hashtag()                 # '#datascience'
```

### Datetime Provider
```python
from mimesis import Datetime
from datetime import datetime

dt = Datetime()

# Dates
dt.date()                          # datetime.date(2023, 5, 15)
dt.datetime()                      # datetime.datetime(2023, 5, 15, 14, 30)
dt.timestamp()                     # 1684157400

# Formatted dates
dt.formatted_date('%Y-%m-%d')      # '2023-05-15'
dt.formatted_time('%H:%M:%S')      # '14:30:25'

# Relative dates
dt.date(start=2020, end=2025)      # Date between 2020-2025
```

### Text Provider
```python
from mimesis import Text

text = Text()

# Words and sentences
text.word()                        # 'technology'
text.words(5)                      # ['data', 'science', 'machine', 'learning', 'AI']
text.sentence()                    # 'The quick brown fox jumps over the lazy dog.'
text.sentences(3)                  # List of 3 sentences

# Paragraphs
text.paragraph()                   # Full paragraph
text.title()                       # 'Data Science in Modern Business'
```

### Code Provider
```python
from mimesis import Code

code = Code()

# Identifiers
code.uuid()                        # 'f47ac10b-58cc-4372-a567-0e02b2c3d479'
code.password()                    # 'Kx9$mN2pQ7'
code.password(length=12, hashed=True) # Hashed password

# Codes
code.isbn()                        # '978-3-16-148410-0'
code.imei()                        # '012345678901234'
```

## Advanced Usage

### Schema-Based Generation
```python
from mimesis import Field, Locale
from mimesis.schema import Schema

# Define schema
def schema():
    return {
        'id': Field('uuid'),
        'name': Field('person.full_name'),
        'email': Field('person.email'),
        'age': Field('person.age', minimum=18, maximum=65),
        'salary': Field('finance.price', minimum=30000, maximum=120000),
        'address': {
            'street': Field('address.address'),
            'city': Field('address.city'),
            'country': Field('address.country')
        }
    }

# Generate data
schema_obj = Schema(schema=schema, locale=Locale.EN)
fake_user = schema_obj.create()

print(fake_user)
# Output: Complex nested dictionary with all fields populated
```

### Bulk Data Generation
```python
# Generate multiple records
users = [schema_obj.create() for _ in range(100)]

# Or use iterator for memory efficiency
def generate_users(count):
    for _ in range(count):
        yield schema_obj.create()

# Generate 1000 users efficiently
for user in generate_users(1000):
    # Process user data
    pass
```

### Seeding for Reproducible Data
```python
from mimesis import Person
from mimesis.random import get_random

# Set seed for reproducible results
random = get_random()
random.seed(12345)

person = Person(random=random)
print(person.full_name())  # Will always generate same name

# Or use global seed
import mimesis.random as mr
mr.reseed(12345)
```

### Custom Providers
```python
from mimesis.providers.base import BaseProvider

class CompanyProvider(BaseProvider):
    """Custom provider for company data."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.company_types = ['Corp', 'LLC', 'Inc', 'Ltd']
        self.departments = ['Engineering', 'Sales', 'Marketing', 'HR']
    
    def company_name(self):
        """Generate a company name."""
        name = self.random.choice(['Tech', 'Data', 'Digital', 'Smart'])
        suffix = self.random.choice(['Solutions', 'Systems', 'Labs', 'Works'])
        company_type = self.random.choice(self.company_types)
        return f"{name} {suffix} {company_type}"
    
    def department(self):
        """Generate a department name."""
        return self.random.choice(self.departments)

# Usage
company = CompanyProvider()
print(company.company_name())  # 'Tech Solutions Corp'
print(company.department())    # 'Engineering'
```

### Integration with Pandas
```python
import pandas as pd
from mimesis import Person, Address, Finance

def generate_employee_data(count=1000):
    person = Person()
    address = Address()
    finance = Finance()
    
    data = []
    for _ in range(count):
        employee = {
            'employee_id': person.identifier(mask='####-####'),
            'first_name': person.first_name(),
            'last_name': person.last_name(),
            'email': person.email(),
            'phone': person.phone_number(),
            'department': person.occupation(),
            'salary': finance.price(minimum=40000, maximum=150000),
            'hire_date': person.birthdate(minimum_age=22, maximum_age=60),
            'city': address.city(),
            'state': address.state(),
            'country': address.country()
        }
        data.append(employee)
    
    return pd.DataFrame(data)

# Generate DataFrame
df = generate_employee_data(1000)
print(df.head())
```

## Financial Data Specific Examples

### Banking Data
```python
from mimesis import Finance, Person

finance = Finance()
person = Person()

# Account information
account_data = {
    'account_number': finance.account_number(),
    'routing_number': finance.routing_number(),
    'account_type': finance.account_type(),
    'balance': finance.price(minimum=100, maximum=50000),
    'currency': finance.currency_code(),
    'bank_name': finance.bank(),
    'account_holder': person.full_name()
}
```

### Transaction Data
```python
from mimesis import Finance, Datetime
from datetime import datetime, timedelta

def generate_transactions(count=1000):
    finance = Finance()
    dt = Datetime()
    
    transactions = []
    for _ in range(count):
        transaction = {
            'transaction_id': finance.transaction_id(),
            'amount': finance.price(minimum=1, maximum=5000),
            'currency': finance.currency_code(),
            'transaction_type': finance.transaction_type(),
            'date': dt.datetime(start=datetime.now() - timedelta(days=365)),
            'merchant': finance.company(),
            'category': finance.business_category()
        }
        transactions.append(transaction)
    
    return transactions
```

## Best Practices

1. **Use appropriate locales** for your target audience
2. **Seed your random generators** for reproducible test data
3. **Use schema-based generation** for complex data structures
4. **Create custom providers** for domain-specific data
5. **Generate data in batches** for better performance with large datasets
6. **Validate generated data** to ensure it meets your requirements

## Common Use Cases

- **Database seeding** for development/testing
- **API mock data** generation
- **Data pipeline testing** with realistic datasets
- **Demo data** for presentations and training
- **Load testing** with varied, realistic data
- **Data anonymization** for production data masking