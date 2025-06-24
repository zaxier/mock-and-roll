# Sporting Goods Retailer Demo

## Overview
This demo generates a comprehensive synthetic dataset for a sporting goods retailer with omnichannel retail operations (physical stores + online) and supply chain optimization capabilities.

## Features

### Datasets Generated
1. **stores** - Physical store locations with details (flagship, standard, outlet, express)
2. **products** - Sporting goods product catalog across 8 major categories
3. **customers** - Customer profiles with channel preferences and loyalty data
4. **inventory** - Real-time inventory levels across stores and warehouses
5. **transactions** - Sales transactions across all channels (in-store, online, mobile app)
6. **suppliers** - Supplier information for supply chain management
7. **product_suppliers** - Product-to-supplier mappings with costs
8. **purchase_orders** - Purchase orders for inventory replenishment
9. **customer_events** - Omnichannel customer interaction events

### Business Use Cases
- **Omnichannel Analytics**: Analyze customer behavior across online and physical channels
- **Inventory Optimization**: Identify reorder requirements and overstock situations
- **Supply Chain Management**: Track supplier performance and optimize purchasing
- **Customer Segmentation**: Segment customers by value, channel preference, and behavior
- **Product Performance**: Analyze sales performance by category, brand, and location

## Running the Demo

```bash
# Standard execution
python -m customer_demos.sporting_goods_retailer

# With custom parameters
python -m customer_demos.sporting_goods_retailer --records 5000 --schema my_schema

# Available CLI options
--records    Number of customer records to generate (default: 1000)
--schema     Target Databricks schema name
--catalog    Target Databricks catalog name
--volume     Target Databricks volume name
--format     File format for volume storage (default: parquet)
--log-level  Logging level (DEBUG, INFO, WARNING, ERROR)
```

## Data Model

### Product Categories
- Team Sports (Basketball, Football, Soccer, Baseball, Hockey, Volleyball)
- Outdoor (Camping, Hiking, Climbing, Fishing, Hunting)
- Fitness (Cardio Equipment, Strength Training, Yoga, Home Gym, Recovery)
- Water Sports (Swimming, Surfing, Kayaking, Paddleboarding, Diving)
- Winter Sports (Skiing, Snowboarding, Ice Skating, Snowshoeing)
- Cycling (Road Bikes, Mountain Bikes, Electric Bikes, Accessories)
- Running (Running Shoes, Apparel, Accessories, Nutrition)
- Sports Apparel (Men's Wear, Women's Wear, Kids' Wear, Footwear)

### Key Relationships
- Customers → Transactions (via customer_id)
- Products → Transactions (via product_id)
- Products → Inventory (via product_id)
- Products → Suppliers (via product_suppliers mapping)
- Stores → Transactions (via store_id for in-store purchases)
- Customers → Events (via customer_id)

## Silver Layer Tables
The pipeline automatically creates business-ready aggregations:

1. **daily_sales_silver** - Daily sales summaries by channel
2. **product_performance_silver** - Product sales analytics
3. **inventory_optimization_silver** - Stock status and reorder recommendations
4. **customer_segments_silver** - Customer value segmentation

## Example Queries

```sql
-- Top selling products by category
SELECT category, product_name, revenue
FROM product_performance_silver
ORDER BY revenue DESC
LIMIT 20;

-- Inventory items requiring reorder
SELECT location_id, product_name, quantity_on_hand, suggested_order_quantity
FROM inventory_optimization_silver
WHERE stock_status = 'Reorder Required';

-- Channel performance comparison
SELECT channel, SUM(net_sales) as total_sales, COUNT(DISTINCT unique_customers) as customers
FROM daily_sales_silver
GROUP BY channel
ORDER BY total_sales DESC;

-- High-value customer analysis
SELECT value_segment, COUNT(*) as customer_count, AVG(lifetime_value) as avg_ltv
FROM customer_segments_silver
GROUP BY value_segment
ORDER BY avg_ltv DESC;
```