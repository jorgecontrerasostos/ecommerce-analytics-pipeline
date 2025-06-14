import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()


def format_phone_number(phone_number: str) -> str:
    digits = "".join(filter(str.isdigit, phone_number))
    if len(digits) != 10:
        raise ValueError("Phone number must contain exactly 10 digits")
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"


def generate_email(first_name: str, last_name: str) -> str:
    return f"{first_name.lower()}_{last_name.lower()}_{random.randint(1, 10000)}@{fake.free_email_domain()}"


def generate_customers(num_customers: int = 1000) -> pd.DataFrame:
    customers = []
    for i in range(num_customers):
        # Generate exactly 10 digits using Faker's numerify
        phone_digits = fake.numerify("##########")
        first_name = fake.first_name()
        last_name = fake.last_name()
        customer = {
            "customer_id": i + 1,
            "first_name": first_name,
            "last_name": last_name,
            "email": generate_email(first_name, last_name),
            "phone": format_phone_number(phone_digits),
            "created_at": fake.date_time_between(start_date="-2y", end_date="now"),
        }
        customers.append(customer)
    return pd.DataFrame(customers)


def generate_products(num_products: int = 500) -> pd.DataFrame:
    categories = [
        "Electronics",
        "Clothing",
        "Home & Garden",
        "Sports",
        "Books",
        "Beauty",
        "Toys",
        "Music",
        "Movies",
        "Games",
        "Tools",
        "Automotive",
        "Health & Wellness",
        "Pet Supplies",
        "Office Supplies",
        "Art & Crafts",
        "Garden & Outdoor",
        "Baby & Toddler",
        "Shoes",
        "Handbags & Accessories",
        "Watches",
        "Sunglasses",
        "Beauty & Personal Care",
        "Jewelry",
    ]
    products = []
    for i in range(num_products):
        product = {
            "product_id": i + 1,
            "product_name": fake.catch_phrase(),
            "category": random.choice(categories),
            "price": round(random.uniform(10.00, 500.00), 2),
            "created_at": fake.date_time_between(start_date="-2y", end_date="now"),
        }
        products.append(product)
    return pd.DataFrame(products)


def generate_orders(
    num_orders: int = 5000, num_customers: int = 1000, num_products: int = 500
) -> pd.DataFrame:
    orders = []

    for i in range(num_orders):
        order = {
            "order_id": i + 1,
            "customer_id": random.randint(1, num_customers),
            "product_id": random.randint(1, num_products),
            "quantity": random.randint(1, 5),
            "order_date": fake.date_between(start_date="-1y", end_date="today"),
            "status": random.choice(["completed", "shipped", "pending", "cancelled"]),
        }
        orders.append(order)

    return pd.DataFrame(orders)


print("Generating customers...")
customers_df = generate_customers(1000)
customers_df.to_csv("customers.csv", index=False)

print("Generating products...")
products_df = generate_products(500)
products_df.to_csv("products.csv", index=False)

print("Generating orders...")
orders_df = generate_orders(5000, 1000, 500)
orders_df.to_csv("orders.csv", index=False)

print(f"Generated:")
print(f"- {len(customers_df)} customers")
print(f"- {len(products_df)} products")
print(f"- {len(orders_df)} orders")
