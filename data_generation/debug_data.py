import pandas as pd

# Read the CSV files
customers = pd.read_csv("./data/customers.csv")
products = pd.read_csv("./data/products.csv")
orders = pd.read_csv("./data/orders.csv")

print("=== CHECKING FOR STRING 'None' VALUES ===")
print("\nCustomers with 'None' strings:")
for col in customers.columns:
    none_count = (customers[col].astype(str) == "None").sum()
    if none_count > 0:
        print(f"  {col}: {none_count} 'None' values")

print("\nProducts with 'None' strings:")
for col in products.columns:
    none_count = (products[col].astype(str) == "None").sum()
    if none_count > 0:
        print(f"  {col}: {none_count} 'None' values")

print("\nOrders with 'None' strings:")
for col in orders.columns:
    none_count = (orders[col].astype(str) == "None").sum()
    if none_count > 0:
        print(f"  {col}: {none_count} 'None' values")

print("\n=== SAMPLE VALUES FROM PROBLEMATIC COLUMNS ===")
print("Customer IDs sample:", customers["customer_id"].head(10).tolist())
print("Product IDs sample:", products["product_id"].head(10).tolist())
print("Order customer_ids sample:", orders["customer_id"].head(10).tolist())
print("Order product_ids sample:", orders["product_id"].head(10).tolist())
