# Providers API Reference

This document provides a comprehensive reference of all available provider classes and their methods for synthetic data generation.

## Core Usage Pattern
```python
from providers.generic import Generic
generic = Generic()

# Access any provider via generic object
address_data = generic.address.address()
person_name = generic.person.full_name()
random_price = generic.finance.price(minimum=10, maximum=1000)
```

## Provider Classes

### Address Provider (`generic.address`)
Generates geographic and location data.

**Key Methods:**
- `address()` - Full street address
- `city()` - City name
- `state(abbr=False)` - State/province
- `postal_code()` - ZIP/postal code
- `country()` - Country name
- `coordinates(dms=False)` - Latitude/longitude
- `street_number(maximum=1400)` - Street number
- `calling_code()` - Phone country code

### Person Provider (`generic.person`)
Generates personal information and demographics.

**Key Methods:**
- `full_name(gender=None)` - Complete name
- `first_name(gender=None)` - Given name
- `last_name(gender=None)` - Family name
- `email(domains=None, unique=False)` - Email address
- `phone_number(mask="")` - Phone number
- `birthdate(min_year=1980, max_year=2023)` - Date of birth
- `occupation()` - Job/profession
- `username()` - Username
- `password(length=8, hashed=False)` - Password
- `identifier(mask="##-##/##")` - Custom ID pattern

### Finance Provider (`generic.finance`)
Generates financial and business data.

**Key Methods:**
- `company()` - Company name
- `price(minimum=500, maximum=1500)` - Monetary amount
- `currency_iso_code()` - Currency code (USD, EUR, etc.)
- `bank()` - Bank name
- `stock_ticker()` - Stock symbol
- `stock_name()` - Company stock name
- `cryptocurrency_symbol()` - Crypto symbol

### Datetime Provider (`generic.datetime`)
Generates dates, times, and temporal data.

**Key Methods:**
- `date(start=2000, end=current_year)` - Date object
- `datetime(start=current_year, end=current_year)` - Datetime object
- `time()` - Time object
- `timestamp(fmt=TimestampFormat.POSIX)` - Unix timestamp
- `formatted_date(fmt="")` - Custom date format
- `day_of_week(abbr=False)` - Weekday name
- `month(abbr=False)` - Month name
- `year(minimum=1990, maximum=current_year)` - Year

### Numeric Provider (`generic.numeric`)
Generates numerical data and sequences.

**Key Methods:**
- `integer_number(start=-1000, end=1000)` - Random integer
- `float_number(start=-1000.0, end=1000.0)` - Random float
- `integers(start=0, end=10, n=10)` - List of integers
- `floats(start=0, end=1, n=10)` - List of floats
- `decimal_number(start=-1000.0, end=1000.0)` - Decimal number
- `matrix(m=10, n=10, num_type=NumType.FLOAT)` - Numerical matrix

### Choice Provider (`generic.choice`)
Selects random items from sequences.

**Key Methods:**
- `choice(*args)` - Single random choice
- `__call__(items, length=0, unique=False)` - Multiple random choices

### Internet Provider (`generic.internet`)
Generates web and network-related data.

**Key Methods:**
- `url(scheme=URLScheme.HTTPS)` - Web URL
- `email()` - Email address
- `ip_v4()` - IPv4 address
- `ip_v6()` - IPv6 address
- `mac_address()` - MAC address
- `hostname()` - Domain hostname
- `user_agent()` - Browser user agent
- `port()` - Network port number

### Text Provider (`generic.text`)
Generates textual content and strings.

**Key Methods:**
- `text(quantity=5)` - Multiple sentences
- `sentence()` - Single sentence
- `words(quantity=5)` - List of words
- `word()` - Single word
- `title()` - Title text
- `quote()` - Famous quote
- `color()` - Color name
- `hex_color(safe=False)` - Hex color code

### Hardware Provider (`generic.hardware`)
Generates computer and device specifications.

**Key Methods:**
- `cpu()` - Processor name
- `ram_size()` - Memory size
- `graphics()` - Graphics card
- `manufacturer()` - Hardware maker
- `phone_model()` - Mobile device model
- `resolution()` - Screen resolution

### Transport Provider (`generic.transport`)
Generates vehicle and transportation data.

**Key Methods:**
- `manufacturer()` - Vehicle manufacturer
- `car()` - Car model
- `airplane()` - Aircraft model
- `vehicle_registration_code()` - License plate format

### Payment Provider (`generic.payment`)
Generates payment and financial account data.

**Key Methods:**
- `credit_card_number(card_type=None)` - Credit card number
- `credit_card_expiration_date()` - Expiry date
- `cvv()` - Security code
- `paypal()` - PayPal account
- `bitcoin_address()` - Crypto wallet address

### Food Provider (`generic.food`)
Generates food and beverage data.

**Key Methods:**
- `dish()` - Food dish name
- `fruit()` - Fruit/berry name
- `vegetable()` - Vegetable name
- `drink()` - Beverage name
- `spices()` - Spice/herb name

### Development Provider (`generic.development`)
Generates software development data.

**Key Methods:**
- `programming_language()` - Language name
- `version()` - Semantic version
- `boolean()` - True/False
- `os()` - Operating system
- `software_license()` - License type

### File Provider (`generic.file`)
Generates file and filesystem data.

**Key Methods:**
- `file_name(file_type=None)` - Filename with extension
- `extension(file_type=None)` - File extension
- `mime_type(type_=None)` - MIME type
- `size(minimum=1, maximum=100)` - File size string

### Cryptographic Provider (`generic.cryptographic`)
Generates cryptographic and security data.

**Key Methods:**
- `uuid()` - UUID string
- `hash(algorithm=None)` - Hash value
- `token_hex(entropy=32)` - Hex token
- `token_urlsafe(entropy=32)` - URL-safe token

### Code Provider (`generic.code`)
Generates codes and identifiers.

**Key Methods:**
- `isbn()` - ISBN number
- `issn(mask="####-####")` - ISSN number
- `pin(mask="####")` - PIN code
- `ean()` - EAN barcode

### Science Provider (`generic.science`)
Generates scientific data.

**Key Methods:**
- `dna_sequence(length=10)` - DNA sequence
- `rna_sequence(length=10)` - RNA sequence
- `measure_unit(name=None)` - SI unit

### Path Provider (`generic.path`)
Generates filesystem paths.

**Key Methods:**
- `root()` - Root directory
- `home()` - Home directory
- `user()` - User path
- `project_dir()` - Project directory

### BinaryFile Provider (`generic.binaryfile`)
Generates binary file content.

**Key Methods:**
- `image(file_type=ImageFile.PNG)` - Image bytes
- `video(file_type=VideoFile.MP4)` - Video bytes
- `audio(file_type=AudioFile.MP3)` - Audio bytes
- `document(file_type=DocumentFile.PDF)` - Document bytes

## Common Patterns

### Date Generation
```python
# Use integer years, not datetime objects
transaction_date = generic.datetime.date(start=2020, end=2025)
recent_datetime = generic.datetime.datetime(start=2024, end=2025)
```

### Numeric Ranges
```python
# Use start/end parameters, not min/max
price = generic.numeric.float_number(start=10.0, end=1000.0)
quantity = generic.numeric.integer_number(start=1, end=100)
```

### Related Data Generation
```python
# Generate related datasets with foreign keys
users = []
for i in range(100):
    user_id = generic.person.identifier()
    users.append({
        'user_id': user_id,
        'name': generic.person.full_name(),
        'email': generic.person.email()
    })

# Use user_ids for related transactions
transactions = []
for i in range(500):
    transactions.append({
        'transaction_id': generic.person.identifier(),
        'user_id': random.choice([u['user_id'] for u in users]),
        'amount': generic.finance.price(minimum=10, maximum=1000)
    })
```