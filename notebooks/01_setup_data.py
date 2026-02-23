# Fabric Notebook: Setup Sales Analytics Environment
# ==================================================
# 
# HOW TO USE:
# 1. Create a new Notebook in your Fabric workspace
# 2. Copy this entire file into the notebook
# 3. Attach to a Lakehouse (create one first if needed)
# 4. Run all cells
#
# This will create all tables and the semantic model.

# %% [markdown]
# # 🚀 Sales Analytics Setup
# This notebook sets up the complete data environment for the Fabric Refactor project.

# %% Cell 1: Configuration
LAKEHOUSE_NAME = "sales_lakehouse"  # Will use attached lakehouse

# %% Cell 2: Create Dimension Tables

# DIM_PRODUCTS
from pyspark.sql.types import *

products_data = [
    ("PROD-A001", "Pro Wireless Mouse", "SKU-MOU-001", "CAT-01", "Electronics", "Computer Peripherals", "TechBrand", 45.00, 149.99),
    ("PROD-A002", "Ergonomic Keyboard", "SKU-KEY-001", "CAT-01", "Electronics", "Computer Peripherals", "TechBrand", 62.00, 199.99),
    ("PROD-A003", "USB-C Hub 7-Port", "SKU-HUB-001", "CAT-01", "Electronics", "Computer Peripherals", "TechBrand", 38.00, 129.99),
    ("PROD-B001", "Noise Canceling Headphones", "SKU-AUD-001", "CAT-01", "Electronics", "Audio Equipment", "SoundMax", 55.00, 179.99),
    ("PROD-B002", "Bluetooth Speaker Pro", "SKU-AUD-002", "CAT-01", "Electronics", "Audio Equipment", "SoundMax", 68.00, 219.99),
    ("PROD-B003", "Studio Monitor Pair", "SKU-AUD-003", "CAT-01", "Electronics", "Audio Equipment", "SoundMax", 95.00, 299.99),
    ("PROD-C001", "Smart LED Desk Lamp", "SKU-LGT-001", "CAT-02", "Home Office", "Lighting", "LumiTech", 28.00, 89.99),
    ("PROD-C002", "Standing Desk Electric", "SKU-FRN-001", "CAT-02", "Home Office", "Furniture", "ErgoDesk", 185.00, 599.99),
    ("PROD-D001", "4K Webcam Pro", "SKU-CAM-001", "CAT-01", "Electronics", "Video Equipment", "VisionPro", 240.00, 799.99),
    ("PROD-D002", "Ring Light Kit", "SKU-LGT-002", "CAT-01", "Electronics", "Video Equipment", "VisionPro", 135.00, 449.99),
]

products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("product_sku", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("category_name", StringType(), True),
    StructField("subcategory_name", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("unit_cost", DoubleType(), True),
    StructField("list_price", DoubleType(), True),
])

df_products = spark.createDataFrame(products_data, products_schema)
df_products.write.mode("overwrite").format("delta").saveAsTable("dim_products")
print(f"✅ Created dim_products: {df_products.count()} rows")

# %% Cell 3: Create Customers Dimension

customers_data = [
    ("CUST-10042", "Acme Corporation", "B2B", "Enterprise", "Platinum", "NA-EAST", "US", 125000.00),
    ("CUST-10055", "Beta Industries", "B2B", "Mid-Market", "Gold", "NA-CENTRAL", "US", 45000.00),
    ("CUST-10067", "Gamma Solutions", "B2B", "Mid-Market", "Gold", "NA-CENTRAL", "US", 52000.00),
    ("CUST-10089", "Delta Tech", "B2B", "Enterprise", "Platinum", "NA-WEST", "US", 280000.00),
    ("CUST-10156", "Epsilon Corp", "B2B", "Enterprise", "Gold", "NA-EAST", "US", 95000.00),
    ("CUST-10178", "Zeta Holdings", "B2B", "Mid-Market", "Silver", "NA-WEST", "US", 38000.00),
    ("CUST-10201", "Eta Services", "B2B", "SMB", "Bronze", "NA-EAST", "US", 22000.00),
    ("CUST-10255", "Theta Group", "B2B", "SMB", "Bronze", "NA-EAST", "US", 12000.00),
    ("CUST-10302", "Iota Manufacturing", "B2B", "Mid-Market", "Silver", "NA-CENTRAL", "US", 48000.00),
    ("CUST-10333", "Kappa Retail", "B2B", "Enterprise", "Gold", "NA-EAST", "US", 110000.00),
    ("CUST-20001", "Novus GmbH", "B2B", "Enterprise", "Platinum", "EU-WEST", "DE", 185000.00),
    ("CUST-20034", "Omega BV", "B2B", "Mid-Market", "Gold", "EU-WEST", "NL", 58000.00),
    ("CUST-20045", "Pi Enterprises", "B2B", "Mid-Market", "Silver", "EU-CENTRAL", "FR", 42000.00),
    ("CUST-20089", "Tau Ltd", "B2B", "Enterprise", "Platinum", "EU-WEST", "GB", 220000.00),
    ("CUST-30008", "Pacific Trading Co", "B2B", "Mid-Market", "Silver", "APAC", "AU", 40000.00),
    ("CUST-30022", "Golden Dragon HK", "B2B", "Mid-Market", "Gold", "APAC", "HK", 72000.00),
    ("CUST-30045", "Cherry Blossom Inc", "B2B", "Enterprise", "Gold", "APAC", "JP", 160000.00),
]

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("customer_type", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("tier", StringType(), True),
    StructField("region_code", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("lifetime_value", DoubleType(), True),
])

df_customers = spark.createDataFrame(customers_data, customers_schema)
df_customers.write.mode("overwrite").format("delta").saveAsTable("dim_customers")
print(f"✅ Created dim_customers: {df_customers.count()} rows")

# %% Cell 4: Create Stores Dimension

stores_data = [
    ("STR-101", "NYC Flagship", "Flagship", "NA-EAST", "North America East", "US", "New York", True),
    ("STR-102", "Boston Downtown", "Standard", "NA-EAST", "North America East", "US", "Boston", False),
    ("STR-103", "Chicago Loop", "Standard", "NA-CENTRAL", "North America Central", "US", "Chicago", False),
    ("STR-104", "Columbus Center", "Express", "NA-CENTRAL", "North America Central", "US", "Columbus", False),
    ("STR-105", "Dallas Galleria", "Standard", "NA-CENTRAL", "North America Central", "US", "Dallas", False),
    ("STR-106", "Jersey City", "Express", "NA-EAST", "North America East", "US", "Jersey City", False),
    ("STR-107", "Miami Beach", "Standard", "NA-EAST", "North America East", "US", "Miami", False),
    ("STR-205", "SF Union Square", "Flagship", "NA-WEST", "North America West", "US", "San Francisco", True),
    ("STR-206", "Seattle Pike Place", "Standard", "NA-WEST", "North America West", "US", "Seattle", False),
    ("STR-207", "Portland Pearl", "Express", "NA-WEST", "North America West", "US", "Portland", False),
    ("STR-301", "Munich Marienplatz", "Flagship", "EU-CENTRAL", "Europe Central", "DE", "Munich", True),
    ("STR-302", "Paris Champs-Élysées", "Standard", "EU-CENTRAL", "Europe Central", "FR", "Paris", False),
    ("STR-401", "Tokyo Ginza", "Flagship", "APAC", "Asia Pacific", "JP", "Tokyo", True),
    ("STR-402", "Singapore Orchard", "Standard", "APAC", "Asia Pacific", "SG", "Singapore", False),
]

stores_schema = StructType([
    StructField("store_id", StringType(), False),
    StructField("store_name", StringType(), True),
    StructField("store_type", StringType(), True),
    StructField("region_code", StringType(), True),
    StructField("region_name", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("city", StringType(), True),
    StructField("is_flagship", BooleanType(), True),
])

df_stores = spark.createDataFrame(stores_data, stores_schema)
df_stores.write.mode("overwrite").format("delta").saveAsTable("dim_stores")
print(f"✅ Created dim_stores: {df_stores.count()} rows")

# %% Cell 5: Create Date Dimension

from datetime import datetime, timedelta

def generate_dates(start_date, end_date):
    dates = []
    current = start_date
    while current <= end_date:
        fiscal_year = f"FY{current.year + 1}" if current.month >= 7 else f"FY{current.year}"
        fiscal_quarter = ((current.month - 7) % 12) // 3 + 1
        
        dates.append((
            int(current.strftime("%Y%m%d")),  # date_key
            current.strftime("%Y-%m-%d"),      # full_date
            current.year,
            (current.month - 1) // 3 + 1,      # quarter
            f"Q{(current.month - 1) // 3 + 1} {current.year}",
            current.month,
            current.strftime("%B"),
            current.strftime("%b"),
            current.isocalendar()[1],          # week_of_year
            current.day,
            current.weekday() + 1,             # day_of_week (1=Mon)
            current.strftime("%A"),
            current.strftime("%a"),
            current.weekday() >= 5,            # is_weekend
            fiscal_year,
            f"FQ{fiscal_quarter}",
        ))
        current += timedelta(days=1)
    return dates

date_data = generate_dates(datetime(2024, 1, 1), datetime(2024, 12, 31))

date_schema = StructType([
    StructField("date_key", IntegerType(), False),
    StructField("full_date", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("quarter", IntegerType(), True),
    StructField("quarter_name", StringType(), True),
    StructField("month_num", IntegerType(), True),
    StructField("month_name", StringType(), True),
    StructField("month_short", StringType(), True),
    StructField("week_of_year", IntegerType(), True),
    StructField("day_of_month", IntegerType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("day_name", StringType(), True),
    StructField("day_short", StringType(), True),
    StructField("is_weekend", BooleanType(), True),
    StructField("fiscal_year", StringType(), True),
    StructField("fiscal_quarter", StringType(), True),
])

df_dates = spark.createDataFrame(date_data, date_schema)
df_dates.write.mode("overwrite").format("delta").saveAsTable("dim_date")
print(f"✅ Created dim_date: {df_dates.count()} rows")

# %% Cell 6: Create Fact Sales Table

import random

def generate_transactions(num_transactions=200):
    """Generate realistic sales transactions"""
    
    product_ids = ["PROD-A001", "PROD-A002", "PROD-A003", "PROD-B001", "PROD-B002", 
                   "PROD-B003", "PROD-C001", "PROD-C002", "PROD-D001", "PROD-D002"]
    
    customer_ids = ["CUST-10042", "CUST-10055", "CUST-10067", "CUST-10089", "CUST-10156",
                    "CUST-10178", "CUST-10201", "CUST-10255", "CUST-10302", "CUST-10333",
                    "CUST-20001", "CUST-20034", "CUST-20045", "CUST-20089", 
                    "CUST-30008", "CUST-30022", "CUST-30045"]
    
    store_ids = ["STR-101", "STR-102", "STR-103", "STR-104", "STR-105", "STR-106", 
                 "STR-107", "STR-205", "STR-206", "STR-207", "STR-301", "STR-302",
                 "STR-401", "STR-402", "ONLINE"]
    
    # Product prices and costs
    products = {
        "PROD-A001": (149.99, 45.00),
        "PROD-A002": (199.99, 62.00),
        "PROD-A003": (129.99, 38.00),
        "PROD-B001": (179.99, 55.00),
        "PROD-B002": (219.99, 68.00),
        "PROD-B003": (299.99, 95.00),
        "PROD-C001": (89.99, 28.00),
        "PROD-C002": (599.99, 185.00),
        "PROD-D001": (799.99, 240.00),
        "PROD-D002": (449.99, 135.00),
    }
    
    transactions = []
    
    for i in range(num_transactions):
        txn_id = f"TXN-2024-{str(i+1).zfill(6)}"
        
        # Random date in Jan-Mar 2024
        day = random.randint(1, 90)
        date_key = 20240101 + day - 1
        if date_key > 20240131:
            date_key = 20240200 + (date_key - 20240131)
        if date_key > 20240229:
            date_key = 20240300 + (date_key - 20240229)
        
        product_id = random.choice(product_ids)
        customer_id = random.choice(customer_ids)
        store_id = random.choice(store_ids)
        
        # Determine region from store
        if store_id.startswith("STR-1"):
            region = "NA-EAST" if store_id in ["STR-101", "STR-102", "STR-106", "STR-107"] else "NA-CENTRAL"
        elif store_id.startswith("STR-2"):
            region = "NA-WEST"
        elif store_id.startswith("STR-3"):
            region = "EU-CENTRAL"
        elif store_id.startswith("STR-4"):
            region = "APAC"
        else:
            region = random.choice(["NA-EAST", "NA-WEST", "EU-CENTRAL", "APAC"])
        
        channel = "Online" if store_id == "ONLINE" else "Retail"
        
        quantity = random.randint(1, 5)
        unit_price, unit_cost = products[product_id]
        
        discount_pct = random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20])
        discount_amt = round(unit_price * quantity * discount_pct, 2)
        
        gross_sales = round(unit_price * quantity, 2)
        net_sales = round(gross_sales - discount_amt, 2)
        
        tax_rate = random.choice([0.06, 0.0725, 0.08, 0.10, 0.19, 0.20])
        tax_amt = round(net_sales * tax_rate, 2)
        
        total = round(net_sales + tax_amt, 2)
        cogs = round(unit_cost * quantity, 2)
        gross_profit = round(net_sales - cogs, 2)
        margin_pct = round(gross_profit / net_sales * 100, 2) if net_sales > 0 else 0
        
        transactions.append((
            txn_id, date_key, customer_id, product_id, store_id, region, channel,
            quantity, unit_price, discount_amt, gross_sales, net_sales,
            tax_amt, total, cogs, gross_profit, margin_pct, "USD", net_sales
        ))
    
    return transactions

txn_data = generate_transactions(200)

txn_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("date_key", IntegerType(), True),
    StructField("customer_key", StringType(), True),
    StructField("product_key", StringType(), True),
    StructField("store_key", StringType(), True),
    StructField("region_key", StringType(), True),
    StructField("channel_key", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_amount", DoubleType(), True),
    StructField("gross_sales", DoubleType(), True),
    StructField("net_sales", DoubleType(), True),
    StructField("tax_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("cost_of_goods", DoubleType(), True),
    StructField("gross_profit", DoubleType(), True),
    StructField("margin_percent", DoubleType(), True),
    StructField("currency_code", StringType(), True),
    StructField("sales_amount_usd", DoubleType(), True),
])

df_sales = spark.createDataFrame(txn_data, txn_schema)
df_sales.write.mode("overwrite").format("delta").saveAsTable("fact_sales")
print(f"✅ Created fact_sales: {df_sales.count()} rows")

# %% Cell 7: Verify Tables

print("\n" + "=" * 60)
print("📊 TABLE SUMMARY")
print("=" * 60)

tables = ["dim_products", "dim_customers", "dim_stores", "dim_date", "fact_sales"]

for table in tables:
    df = spark.table(table)
    print(f"\n{table}:")
    print(f"  Rows: {df.count()}")
    print(f"  Columns: {len(df.columns)}")

# %% Cell 8: Sample Queries

print("\n" + "=" * 60)
print("📈 SAMPLE ANALYTICS")
print("=" * 60)

# Total Sales
total_sales = spark.sql("""
    SELECT 
        SUM(net_sales) as total_sales,
        SUM(gross_profit) as total_profit,
        COUNT(DISTINCT transaction_id) as transactions,
        COUNT(DISTINCT customer_key) as customers
    FROM fact_sales
""")
print("\nOverall Metrics:")
total_sales.show()

# Sales by Region
print("\nSales by Region:")
spark.sql("""
    SELECT 
        region_key,
        SUM(net_sales) as sales,
        SUM(gross_profit) as profit,
        ROUND(SUM(gross_profit) / SUM(net_sales) * 100, 1) as margin_pct
    FROM fact_sales
    GROUP BY region_key
    ORDER BY sales DESC
""").show()

# Top Products
print("\nTop 5 Products:")
spark.sql("""
    SELECT 
        p.product_name,
        p.brand,
        SUM(f.net_sales) as sales,
        SUM(f.quantity) as units
    FROM fact_sales f
    JOIN dim_products p ON f.product_key = p.product_id
    GROUP BY p.product_name, p.brand
    ORDER BY sales DESC
    LIMIT 5
""").show()

# %% Cell 9: Next Steps

print("""
✅ DATA SETUP COMPLETE!

Next Steps:
-----------
1. Create Semantic Model:
   • Go to Lakehouse → "New semantic model"
   • Select all tables (fact_sales, dim_*)
   • Name it: sales_analytics

2. Create Relationships:
   • fact_sales[date_key] → dim_date[date_key]
   • fact_sales[customer_key] → dim_customers[customer_id]  
   • fact_sales[product_key] → dim_products[product_id]
   • fact_sales[store_key] → dim_stores[store_id]

3. Add Key Measures (in Model view):

   Sales = SUM(fact_sales[net_sales])
   
   Gross Profit = SUM(fact_sales[gross_profit])
   
   Gross Margin % = DIVIDE([Gross Profit], [Sales], 0)
   
   Quantity Sold = SUM(fact_sales[quantity])
   
   Customer Count = DISTINCTCOUNT(fact_sales[customer_key])
   
   Avg Order Value = DIVIDE([Sales], DISTINCTCOUNT(fact_sales[transaction_id]), 0)

4. Create Report:
   • From semantic model → "Create report"
   • Add visuals using your measures
   • Save as "regional_sales_dashboard"
""")
